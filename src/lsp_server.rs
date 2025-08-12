use crate::{
    logger::{self, Logger},
    msg::{Message, Notification, Request, RequestId, Response},
    name_id,
    stdio::{IoThreads, ThreadResult},
    utils::{kill_child_and_wait_with_timeout, wait_child_with_timeout},
    GRACEFUL_SHUTDOWN_TIMEOUT, KILL_WAIT_TIMEOUT, MAX_DIAGNOSTICS_COUNT, MAX_NOTIFICATIONS, POLL_INTERVAL,
};

use anyhow::{anyhow, Context};
use atomic_enum::atomic_enum;
use crossbeam_channel::{Receiver, Sender};
use emacs::Result as EmacsResult;
use lsp_types::{
    Diagnostic, DidChangeTextDocumentParams, InitializeResult, InitializedParams, PublishDiagnosticsParams,
    VersionedTextDocumentIdentifier,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    collections::{HashMap, VecDeque},
    io,
    process::{Child, Command, ExitStatus, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicI32, Ordering},
        Arc, Mutex,
    },
    thread,
    time::{Duration, Instant},
};

#[derive(Debug)]
pub(crate) struct FileInfo {
    pub uri: String, // file name
    pub diagnostics: Vec<Diagnostic>,
}

impl FileInfo {
    pub fn new(uri: impl AsRef<str>) -> FileInfo {
        FileInfo { uri: uri.as_ref().into(), diagnostics: Vec::new() }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct LspServerInfo {
    pub name: String,
    pub version: String,
    pub id: String, // server_id at the moment
    pub capabilities: String,
}

impl LspServerInfo {
    pub fn new(id: u32) -> LspServerInfo {
        LspServerInfo { name: String::new(), version: String::new(), id: id.to_string(), capabilities: String::new() }
    }

    pub fn to_json_string(&self) -> EmacsResult<String> {
        serde_json::to_string(self).context("Failed to serialize server info")
    }
}

#[atomic_enum]
#[derive(PartialEq, Eq)]
pub enum ServerStatus {
    Starting = 0,
    Running = 1,
    ShuttingDown = 2,
    Exiting = 3,
    TearingDown = 4,
}

pub(crate) struct LspServerData {
    latest_request_id: RequestId,
    latest_request_tick: String,
    latest_response_id: RequestId,

    latest_response_tick: String,
    request_ticks: HashMap<RequestId, String>,

    pub(crate) file_infos: HashMap<String, FileInfo>,

    requests: VecDeque<Request>,
    pub(crate) responses: VecDeque<Response>,
    notifications: VecDeque<Notification>,
}

impl LspServerData {
    pub fn new() -> LspServerData {
        LspServerData {
            latest_request_id: RequestId::from(-1),
            latest_request_tick: String::new(),
            latest_response_id: RequestId::from(-1),
            latest_response_tick: String::new(),
            request_ticks: HashMap::new(),
            file_infos: HashMap::new(),
            requests: VecDeque::new(),
            responses: VecDeque::new(),
            notifications: VecDeque::new(),
        }
    }
}

/// Represents the resources associated with LSP server instance
/// - The child process handle
/// - Transport threads (reader, writer, stderr (optional))
/// - Dispatcher thread
/// - State of resources (join status of threads, exit status)
pub struct Resources {
    pub(crate) child: Option<Child>,
    pub(crate) transport: Option<IoThreads>,
    pub(crate) dispatcher: Option<thread::JoinHandle<()>>,
    pub state: ResourceState,
}
pub struct ResourceState {
    pub transport: [ThreadResult; 3],
    pub exit: Option<ExitStatus>,
}

/// Represents a Language Server Protocol (LSP) server instance.
///
/// This struct manages the lifecycle of an LSP server process, including:
/// - Resources (LSP child process and threads)
/// - Server information (name, version, capabilities)
/// - Connection state and transport threads
/// - Server data and exit flag
pub struct LspServer {
    pub resources: Resources,
    pub server_info: LspServerInfo,
    pub(crate) status: AtomicServerStatus,
    pub(crate) sender: Option<Sender<Message>>,
    pub(crate) server_data: Arc<Mutex<LspServerData>>,
    pub(crate) exit: Arc<AtomicBool>,
    pub(crate) name_id: String,
}

impl LspServer {
    pub fn set_status(&self, status: ServerStatus) {
        self.status.store(status, Ordering::SeqCst);
    }

    pub fn status(&self) -> ServerStatus {
        self.status.load(Ordering::SeqCst)
    }

    pub fn new(cmd: &str, cmd_args: &str, emacs_envs: &str) -> EmacsResult<LspServer> {
        let args = cmd_args.split_ascii_whitespace().collect::<Vec<&str>>();

        let mut command = Command::new(cmd);
        command.args(args);
        Logger::info(format!("Creating new LSP server: <{} {}>", cmd, cmd_args));

        if !emacs_envs.is_empty() {
            Logger::info(format!("emacs_envs: {}", emacs_envs));

            let envs: HashMap<String, String> =
                serde_json::from_str(emacs_envs).context("Failed to parse emacs_envs JSON")?;
            command.envs(envs);
        }
        command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped());

        let mut child = command.spawn().context("Failed to spawn LSP server process")?;

        let stdin = child.stdin.take().context("Failed to obtain LSP server's stdin")?;
        let stdout = child.stdout.take().context("Failed to obtain LSP server's stdout")?;
        let stderr = child.stderr.take().context("Failed to obtain LSP server's stderr")?;

        let exit = Arc::new(AtomicBool::new(false));
        let (sender, receiver, mut transport) = crate::connection::stdio(stdin, stdout, stderr, Arc::clone(&exit));

        let mut server_info = LspServerInfo::new(child.id());
        if let Some(name) = std::path::Path::new(cmd).file_name().and_then(|n| n.to_str()) {
            server_info.name = name.to_string();
        }

        use ThreadResult::NotJoined;
        let mut resources = Resources {
            child: Some(child),
            transport: Some(transport),
            dispatcher: None,
            state: ResourceState { transport: [NotJoined, NotJoined, NotJoined], exit: None },
        };

        let mut server = LspServer {
            resources,
            name_id: name_id(&server_info.name, &server_info.id),
            server_info: server_info,
            status: AtomicServerStatus::new(ServerStatus::Starting),
            sender: Some(sender),
            server_data: Arc::new(Mutex::new(LspServerData::new())),
            exit: exit,
        };
        server.resources.dispatcher =
            Some(LspServer::start_dispatcher(receiver, Arc::clone(&server.exit), Arc::clone(&server.server_data)));

        Ok(server)
    }

    /// Tears down and cleanup LSP child process and associated threads
    ///
    /// 1. Set exit and dtop sender to signal the dispatcher and transport threads to stop
    /// 2. Gracefully wait for the child LSP process to exit, if provided graceful timeout is non-zero
    /// 4. If the child process does not exit within the provided timeout, or error occured, or
    ///    no graceful timeout provided kill the child the process
    /// 5. Wait (with a timeout) for child process status to avoid leaving a zombie process.
    /// 6. Join dispatcher and trainsport threads.
    ///
    /// # Arguments
    /// * `graceful_timeout` - The maximum duration to wait for child to exit before sending kill signal.
    ///   Use `Duration::ZERO` to skip graceful wait and immediately kill.
    ///
    /// # Returns
    /// * `Ok(Some(status))` if the process exited (gracefully or forcibly) and an exit status is available.
    /// * `Ok(None)` if the process did not exit within the allowed time.
    /// * `Err(e)` if an error occurred during shutdown.
    pub fn teardown(&mut self, graceful_timeout: Duration) -> EmacsResult<Option<ExitStatus>> {
        self.exit.store(true, Ordering::SeqCst);

        // Just to be on the safe-side. Projects map is enclosed in mutex anyway,
        // so teardown shouldn't be called from multiple threads simultaneosly
        if self.status.swap(ServerStatus::TearingDown, Ordering::SeqCst) == ServerStatus::TearingDown {
            Logger::debug(format!("teardown already in progress for {}", self.name_id));
            return Ok(None);
        }
        self.set_status(ServerStatus::TearingDown);
        Logger::debug(format!("teardown {}", self.name_id));

        // Drop the sender to close the channel and signal transport threads to exit
        self.sender.take(); // drop channel to cause writer thread to exit

        let mut status = Ok(None);
        if let Some(mut child) = self.resources.child.take() {
            // wait for the child process to exit
            if graceful_timeout > Duration::ZERO {
                Logger::debug(format!("waiting for exit of {}", self.name_id));
                status = wait_child_with_timeout(&mut child, graceful_timeout, &self.name_id);
            }

            // Proceed to force exit if needed
            if !matches!(status, Ok(Some(_))) {
                self.resources.child = Some(child); // set back the child so kill_child can be used
                status = self.kill_child();
            }
        }

        Logger::debug(format!("Joining threads for {}", self.name_id));
        if let Some(mut threads) = self.resources.transport.take() {
            if let Err(e) = threads.join() {
                Logger::error(format!("Error joining transport for {}: {}", self.name_id, e));
            }
            Logger::debug(format!("joined transport: {:?}", threads.results));
            self.resources.state.transport = threads.results;
        }

        if let Some(handle) = self.resources.dispatcher.take() {
            if let Err(e) = handle.join() {
                Logger::error(format!("Error joining dispatcher for {}: {:?}", self.name_id, e));
            }
        }

        Logger::debug(format!("finished teardown for {}", self.name_id));
        status
    }

    fn kill_child(&mut self) -> EmacsResult<Option<ExitStatus>> {
        if let Some(mut child) = self.resources.child.take() {
            return kill_child_and_wait_with_timeout(&mut child, KILL_WAIT_TIMEOUT, &self.name_id);
        }
        Ok(None)
    }

    /// Attempts a graceful shutdown using the LSP protocol.
    ///
    /// This function sends the `shutdown` request and waits for a corresponding response within
    /// the specified timeout. If successful, it sends the `exit` notification.
    ///
    /// It does NOT tear down resources; it only handles the protocol handshake.
    ///
    /// # Returns
    /// * Ok if the protocol handshake completed successfully within the timeout.
    /// * Err if the handshake failed or timed out.
    pub fn shutdown(&mut self, req: Request, timeout: Duration) -> EmacsResult<()> {
        self.set_status(ServerStatus::ShuttingDown);
        Logger::info(format!("Starting shutdown protocol for {}", self.name_id));
        let req_id = req.id.clone();

        self.request_async(req)?;

        let start_time = Instant::now();
        while start_time.elapsed() <= timeout {
            if matches!(self.read_response(), Some(ref resp) if resp.id == req_id) {
                self.write(Notification::new_exit());
                self.set_status(ServerStatus::Exiting);
                Logger::info(format!("Shutdown protocol finished for {}", self.name_id));
                return Ok(()); // graceful shutdown prococol completed sucessfully
            }
            thread::sleep(POLL_INTERVAL);
        }

        let msg = format!("Shutdown protocol timed out for {}", self.name_id);
        Logger::info(&msg);
        return Err(io::Error::new(io::ErrorKind::TimedOut, msg).into());
    }

    fn start_dispatcher(
        receiver: Receiver<Message>, exit: Arc<AtomicBool>, server_data: Arc<Mutex<LspServerData>>,
    ) -> thread::JoinHandle<()> {
        let handle = thread::spawn(move || {
            logger::set_log_prefix("[DISP] - ");
            for msg in receiver {
                if exit.load(Ordering::Relaxed) {
                    Logger::info(&format!("requested to exit"));
                    break;
                }
                match msg {
                    Message::Request(r) => {
                        if r.method == "workspace/configuration" {
                            let mut server_data = server_data.lock().unwrap();
                            server_data.requests.push_back(r);
                        }
                    }
                    Message::Response(mut r) => {
                        let id = r.id.clone();

                        let mut server_data = server_data.lock().unwrap();

                        if let Some(request_tick) = server_data.request_ticks.remove(&id) {
                            Logger::debug(format!("Request tick for id {} is {}", id, request_tick));
                            if request_tick == server_data.latest_request_tick {
                                r.request_tick = request_tick.clone();
                                server_data.responses.push_back(r);
                            }
                            Logger::debug(format!(
                                "Latest response id is {}, current response id {}",
                                server_data.latest_response_id, &id
                            ));
                            // FIXME: what about String ids? how we define order?
                            if server_data.latest_response_id < id {
                                server_data.latest_response_id = id;
                                server_data.latest_response_tick = request_tick;
                                Logger::debug(format!(
                                    "Set latest response tick for id {} to {}",
                                    server_data.latest_response_id, &server_data.latest_response_tick
                                ));
                            }
                        } else {
                            Logger::trace(format!("No request tick for id {}", id));
                            // if server_data.latest_response_id.lt(&id) {
                            //     server_data.latest_response_id = id.clone();
                            // }
                        }
                    }
                    Message::Notification(r) => {
                        if r.method == "exit" {
                            // Self exit notification from IO writer.
                            // Not really needed since we'll get an error once writer drop it's channel end
                            Logger::info(format!("exit notification"));
                            break;
                        } else if r.method == "textDocument/publishDiagnostics" {
                            // cache diagnostics so they won't pour into Emacs
                            match serde_json::from_value::<PublishDiagnosticsParams>(r.params) {
                                Ok(mut params) => {
                                    let uri_string: String = params.uri.to_string();
                                    let mut file_info = FileInfo::new(&uri_string);
                                    // cache no more than MAX_DIAGNOSTICS_COUNT diagnostics
                                    let max_diagnostic_count = MAX_DIAGNOSTICS_COUNT.load(Ordering::Relaxed);
                                    if max_diagnostic_count >= 0
                                        && params.diagnostics.len() > max_diagnostic_count as usize
                                    {
                                        params.diagnostics.truncate(max_diagnostic_count as usize);
                                    }
                                    file_info.diagnostics = params.diagnostics;
                                    let mut server_data = server_data.lock().unwrap();
                                    server_data.file_infos.insert(uri_string, file_info);
                                }
                                Err(e) => {
                                    // parsing failed. Skip this notification
                                    Logger::error(format!("Failed to parse PublishDiagnosticsParams: {}", e));
                                }
                            }
                        } else {
                            // other notifications
                            let mut server_data = server_data.lock().unwrap();
                            if server_data.notifications.len() > MAX_NOTIFICATIONS {
                                server_data.notifications.pop_front();
                            }
                            server_data.notifications.push_back(r);
                        }
                    }
                }
            }
            Logger::info("finished");
        });
        handle
    }

    fn update_request_info(&self, id: RequestId, tick: String) {
        let mut server_data = self.server_data.lock().unwrap();
        if server_data.latest_request_id < id {
            server_data.latest_request_id = id.clone();
        }
        server_data.latest_request_tick = tick.clone();
        server_data.request_ticks.insert(id, tick);
    }

    pub fn get_latest_response_id(&self) -> RequestId {
        let server_data = self.server_data.lock().unwrap();
        server_data.latest_response_id.clone()
    }

    pub fn get_latest_response_tick(&self) -> String {
        let server_data = self.server_data.lock().unwrap();
        server_data.latest_response_tick.clone()
    }

    pub fn write<M: Into<Message>>(&self, msg: M) -> EmacsResult<()> {
        if let Some(sender) = &self.sender {
            sender.send(msg.into()).context("Failed to send to LSP")
        } else {
            Err(anyhow!("no LSP sender channel"))
        }
    }

    pub fn request_async(&self, req: Request) -> EmacsResult<Option<bool>> {
        let request_tick = req.request_tick.as_ref().map(|s| s.clone()).unwrap_or_else(|| {
            use std::time::{SystemTime, UNIX_EPOCH};
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            format!("{}.{}", now.as_secs(), now.subsec_micros())
        });
        self.update_request_info(req.id.clone(), request_tick);

        // TODO: should we let LSP server to manage and clear diagnostics and remove this entirely?
        if req.method == "textDocument/didChange" || req.method == "textDocument/didClose" {
            // extract uri, wihotut parsing the whole request
            if let Some(uri) = req.params.get("textDocument").and_then(|td| td.get("uri")).and_then(|uri| uri.as_str())
            {
                self.clear_diagnostics(uri);
            }
        }
        self.write(req)?;
        Ok(Some(true))
    }

    pub fn read_response(&self) -> Option<Response> {
        let mut server_data = self.server_data.lock().unwrap();
        server_data.responses.pop_front()
    }

    //
    pub fn read_response_exact(&self, id: RequestId, method: impl AsRef<str>) -> Option<Response> {
        let mut result: Option<Response> = None;
        let mut server_data = self.server_data.lock().unwrap();
        let latest_request_tick = server_data.latest_request_tick.clone();

        let mut reserved: VecDeque<Response> = VecDeque::new();
        for response in server_data.responses.drain(..) {
            Logger::debug(format!("read_response_exact response {:#?}", response));
            if response.id == id {
                result = Some(response);
            } else if response.request_tick == latest_request_tick {
                reserved.push_back(response);
            }
            // responses that don't match either condition are dropped
        }

        server_data.responses.append(&mut reserved);

        if result.is_none() {
            Logger::trace(format!("read_response_exact get null for request_id {}, method {}", id, method.as_ref()));
        }
        result
    }

    pub fn read_notification(&self) -> Option<Notification> {
        let mut server_data = self.server_data.lock().unwrap();
        server_data.notifications.pop_front()
    }

    pub fn read_request(&self) -> Option<Request> {
        let mut server_data = self.server_data.lock().unwrap();
        server_data.requests.pop_front()
    }

    pub fn clear_diagnostics(&self, uri: impl AsRef<str>) {
        let mut server_data = self.server_data.lock().unwrap();

        if let Some(mut file_info) = server_data.file_infos.get_mut(uri.as_ref()) {
            file_info.diagnostics.clear();
        }
    }
}

impl Drop for LspServer {
    fn drop(&mut self) {
        // We could check either handle or status to ensure that teardown wasn't started
        if self.resources.child.is_some() {
            Logger::trace(format!("drop {}", self.name_id));
            let _ = self.teardown(match self.status() {
                ServerStatus::Exiting => GRACEFUL_SHUTDOWN_TIMEOUT,
                _ => Duration::ZERO,
            });
        }
    }
}
