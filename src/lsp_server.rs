use crate::{
    bounded_queue::VecDequeExt,
    break_if_should_exit,
    logger::{self, Logger},
    msg::{Message, Notification, Request, RequestId, Response},
    stdio::{IoThreads, ThreadResult},
    utils::{kill_child_and_wait_with_timeout, wait_child_with_timeout},
    GRACEFUL_SHUTDOWN_TIMEOUT, KILL_WAIT_TIMEOUT, MAX_DIAGNOSTICS, MAX_NONTICKED_RESPONSES, MAX_NOTIFICATIONS,
    MAX_TICKED_RESPONSES, POLL_INTERVAL,
};

use anyhow::{anyhow, bail, Context};
use atomic_enum::atomic_enum;
use crossbeam_channel::{Receiver, Sender};
use emacs::Result as EmacsResult;
use lsp_types::{
    Diagnostic, DidChangeTextDocumentParams, InitializeResult, InitializedParams, PublishDiagnosticsParams,
    ServerCapabilities, ServerInfo, VersionedTextDocumentIdentifier,
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

#[derive(Debug, Clone)]
pub(crate) struct FileInfo {
    pub uri: String, // file name
    pub diagnostics: Vec<Diagnostic>,
}

impl FileInfo {
    pub fn new(uri: impl AsRef<str>) -> FileInfo {
        FileInfo { uri: uri.as_ref().into(), diagnostics: Vec::new() }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct LspServerInfo {
    pub id: String, // server_id at the moment
    #[serde(flatten)]
    pub info: ServerInfo, // name + optional version
    pub capabilities: ServerCapabilities,
}

impl LspServerInfo {
    pub fn new(id: u32) -> LspServerInfo {
        LspServerInfo { id: id.to_string(), info: ServerInfo::default(), capabilities: ServerCapabilities::default() }
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
    pub(crate) latest_request_tick: String,
    latest_response_id: RequestId,
    latest_response_tick: String,
    pub(crate) request_ticks: HashMap<RequestId, String>,

    pub(crate) file_infos: HashMap<String, FileInfo>,

    // requests: VecDeque<Request>, // REVIEW: unused?
    responses: VecDeque<Response>,          // ticked
    responses_unticked: VecDeque<Response>, // non-ticked
    notifications: VecDeque<Notification>,
}

impl LspServerData {
    pub fn new() -> LspServerData {
        LspServerData {
            latest_request_tick: String::new(),
            latest_response_id: RequestId::from(-1),
            latest_response_tick: String::new(),
            request_ticks: HashMap::new(),
            file_infos: HashMap::new(),

            // requests: VecDeque::new(), // REVIEW: unused?
            responses: VecDeque::with_capacity(MAX_TICKED_RESPONSES),
            responses_unticked: VecDeque::with_capacity(MAX_NONTICKED_RESPONSES),
            notifications: VecDeque::with_capacity(MAX_NOTIFICATIONS),
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

fn name_id(name: &str, id: &str) -> String {
    format!("<{}:[{}]>", name, id)
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
            server_info.info.name = name.to_string();
        }

        let server_data = Arc::new(Mutex::new(LspServerData::new()));
        let dispatcher = Self::start_dispatcher(receiver, &exit, &server_data);

        use ThreadResult::NotJoined;
        let mut resources = Resources {
            child: Some(child),
            transport: Some(transport),
            dispatcher: Some(dispatcher),
            state: ResourceState { transport: [NotJoined, NotJoined, NotJoined], exit: None },
        };

        let mut server = LspServer {
            resources,
            name_id: name_id(&server_info.info.name, &server_info.id),
            server_info: server_info,
            status: AtomicServerStatus::new(ServerStatus::Starting),
            sender: Some(sender),
            server_data: server_data,
            exit: exit,
        };

        Ok(server)
    }

    pub fn initialize(&mut self, mut req: Request, timeout: Duration) -> EmacsResult<()> {
        req.request_tick = None; // just ensure that we are not sending tick for initialize request from esisp
        let req_id = req.id.clone();
        self.send_message(req)?;

        let start_time = Instant::now();
        loop {
            if let Some(response) = self.find_response_unticked(&req_id) {
                if let Some(error) = &response.error {
                    bail!("Failed to initialize - LSP server error {:?}", error);
                }

                let init_resp: InitializeResult =
                    serde_json::from_value(response.result.context("Failed to initialize - bad initialize response")?)?;
                self.server_info.capabilities = init_resp.capabilities;
                if let Some(server_info) = init_resp.server_info {
                    self.server_info.info = server_info;
                    self.name_id = name_id(&self.server_info.info.name, &self.server_info.id);
                }

                self.send_message(Notification::new_params("initialized", InitializedParams {})?)?;
                self.set_status(ServerStatus::Running);
                return Ok(());
            }

            if !timeout.is_zero() && start_time.elapsed() > timeout {
                bail!("Failed to initializing - timeout");
            }

            thread::sleep(POLL_INTERVAL);
        }
    }

    /// Tears down and cleanup LSP child process and associated threads
    ///
    /// 1. Set exit flag and drop sender to signal the dispatcher and transport threads to stop
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
    pub fn shutdown(&mut self, mut req: Request, timeout: Duration) -> EmacsResult<()> {
        self.set_status(ServerStatus::ShuttingDown);
        Logger::info(format!("Starting shutdown protocol for {}", self.name_id));

        req.request_tick = None; // ensure there is no tick for shutdown request
        let req_id = req.id.clone();
        self.send_message(req)?;

        let start_time = Instant::now();
        while start_time.elapsed() <= timeout {
            if self.find_response_unticked(&req_id).is_some() {
                self.send_message(Notification::new("exit"))?;
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

    fn dispatcher_loop(receiver: Receiver<Message>, exit: Arc<AtomicBool>, server_data: Arc<Mutex<LspServerData>>) {
        for msg in receiver {
            break_if_should_exit!(&exit);

            match msg {
                Message::Request(r) => {
                    // REVIEW: unused? read_requests() is reading, but there is no API to call read_requests

                    // if r.method == "workspace/configuration" {
                    //     let mut server_data = server_data.lock().unwrap();
                    //     server_data.requests.push_back(r);
                    // }
                }
                Message::Response(mut r) => {
                    let id = r.id.clone();

                    let mut server_data = server_data.lock().unwrap();

                    if let Some(request_tick) = server_data.request_ticks.remove(&id) {
                        Logger::debug(format!("Request tick for id {} is {}", id, request_tick));
                        if request_tick == server_data.latest_request_tick {
                            r.request_tick = request_tick.clone();
                            server_data.responses.bounded_push_back(r);
                        }
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
                        // Unticked responses (for requests sent internally by us, lspce-initiated)
                        // TODO: should we limit this only to `shutdown` and `initialize`?
                        Logger::trace(format!("No request tick for id {}", id));
                        server_data.responses_unticked.bounded_push_back(r);
                    }
                }
                Message::Notification(notif) => {
                    if notif.method == "exit" {
                        // Self-exit notification from IO writer to stop dispatcher loop and exit thread.
                        // Not really needed since we'll get an error once writer drop it's channel end
                        Logger::info(format!("exit notification"));
                        break;
                    } else if notif.method == "textDocument/publishDiagnostics" {
                        // cache diagnostics so they won't pour into Emacs
                        match serde_json::from_value::<PublishDiagnosticsParams>(notif.params) {
                            Ok(mut params) => {
                                // cache no more than MAX_DIAGNOSTICS_COUNT diagnostics per file
                                let max_diagnostic_count = MAX_DIAGNOSTICS.load(Ordering::Relaxed);
                                if max_diagnostic_count >= 0 && params.diagnostics.len() > max_diagnostic_count as usize
                                {
                                    params.diagnostics.truncate(max_diagnostic_count as usize);
                                }
                                let uri_str: String = params.uri.to_string();
                                let mut file_info = FileInfo::new(&uri_str);
                                file_info.diagnostics = params.diagnostics;

                                let mut server_data = server_data.lock().unwrap();
                                if file_info.diagnostics.is_empty() {
                                    server_data.file_infos.remove(&uri_str); // clear
                                } else {
                                    server_data.file_infos.insert(uri_str, file_info);
                                }
                            }
                            Err(e) => {
                                Logger::error(format!("Failed to parse PublishDiagnosticsParams: {}", e));
                            }
                        }
                    } else {
                        // other notifications
                        let mut server_data = server_data.lock().unwrap();
                        server_data.notifications.bounded_push_back(notif);
                    }
                }
            }
        }
        Logger::info("finished");
    }

    pub(crate) fn start_dispatcher(
        receiver: Receiver<Message>, exit: &Arc<AtomicBool>, server_data: &Arc<Mutex<LspServerData>>,
    ) -> thread::JoinHandle<()> {
        let exit_clone = Arc::clone(exit);
        let server_data_clone = Arc::clone(server_data);
        let handle = thread::spawn(move || {
            logger::set_log_prefix("[DISP] - ");
            Self::dispatcher_loop(receiver, exit_clone, server_data_clone);
        });
        handle
    }

    fn update_request_info(&self, id: RequestId, tick: String) {
        let mut server_data = self.server_data.lock().unwrap();
        server_data.latest_request_tick = tick.clone();
        server_data.request_ticks.insert(id, tick);
    }

    fn send_message_raw(&self, msg: Message) -> EmacsResult<()> {
        if let Some(sender) = &self.sender {
            Logger::trace(format!("[-->] {}", msg));
            sender.send(msg).context("Failed to send to LSP")
        } else {
            Err(anyhow!("No LSP sender channel"))
        }
    }

    pub fn send_message<M: Into<Message>>(&self, msg: M) -> EmacsResult<()> {
        let msg = msg.into();
        match &msg {
            Message::Request(req) => {
                if req.method == "textDocument/didChange" || req.method == "textDocument/didClose" {
                    if let Some(uri) =
                        req.params.get("textDocument").and_then(|td| td.get("uri")).and_then(|uri| uri.as_str())
                    {
                        self.clear_diagnostics(uri); // clean diagnostics on change/close
                    }
                }

                // always update, even if consequent send will fail. Prevent race condition if we send then update.
                if let Some(tick) = &req.request_tick {
                    self.update_request_info(req.id.clone(), tick.clone());
                }
            }
            _ => {} // do nothing for Notification, Response and Request w/o tick
        }
        self.send_message_raw(msg)?;
        Ok(())
    }

    pub fn read_response(&self) -> Option<Response> {
        let mut server_data = self.server_data.lock().unwrap();
        server_data.responses.pop_front()
    }

    // find matching response by id, discard/remove non-matching ones
    pub fn find_response_unticked(&self, id: &RequestId) -> Option<Response> {
        let mut server_data = self.server_data.lock().unwrap();
        while let Some(response) = server_data.responses_unticked.pop_front() {
            if &response.id == id {
                return Some(response);
            }
            // Discard non-matching responses
        }
        None
    }

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

    pub fn read_last_notification(&self) -> Option<Notification> {
        let mut server_data = self.server_data.lock().unwrap();
        server_data.notifications.pop_front()
    }

    // REVIEW: unused?
    // pub fn read_request(&self) -> Option<Request> {
    //     let mut server_data = self.server_data.lock().unwrap();
    //     server_data.requests.pop_front()
    // }

    pub fn clear_diagnostics(&self, uri: impl AsRef<str>) {
        let mut server_data = self.server_data.lock().unwrap();

        if let Some(mut file_info) = server_data.file_infos.get_mut(uri.as_ref()) {
            file_info.diagnostics.clear();
        }
    }

    pub fn get_latest_response_id(&self) -> RequestId {
        let server_data = self.server_data.lock().unwrap();
        server_data.latest_response_id.clone()
    }

    pub fn get_latest_response_tick(&self) -> String {
        let server_data = self.server_data.lock().unwrap();
        server_data.latest_response_tick.clone()
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

#[cfg(test)]
mod test_lspserver_new {
    use super::LspServer;
    use std::io;

    #[cfg(unix)] // using echo/true
    #[test]
    fn test_lsp_server_new_valid_schenarios() {
        let test_cases = vec![
            ("true", "", "", "no args and empty envs"),
            ("echo", "something", "", "cmd with args"),
            ("echo", "", r#"{"PATH": "/usr/bin", "HOME": "/home/test"}"#, "valid envs"),
        ];

        for (cmd, args, envs, description) in test_cases {
            let res = LspServer::new(cmd, args, envs);

            // May return spawn error on non-linux systems (due to presence of echo/true),
            // but should not return JSON parsing error.
            if let Err(err) = &res {
                if !matches!(err.downcast_ref::<io::Error>(), Some(e) if e.kind() == io::ErrorKind::NotFound) {
                    panic!("Test '{description}' failed. Unexpected error: {err}");
                }
            }
        }
    }

    #[test]
    fn test_lsp_server_new_invalid_scenarios() {
        let test_cases = vec![
            ("unexisting_binary", "", "Failed to spawn LSP server process", "unexisting binary"),
            ("echo", "not JSON", "Failed to parse emacs_envs JSON", "bad JSON"),
        ];

        for (cmd, envs, error_str, description) in test_cases {
            let res = LspServer::new(cmd, "", envs);
            assert!(res.is_err(), "Test '{description}' should return an error");
            assert!(matches!(res, Err(ref e) if e.to_string().contains(error_str)));
        }
    }
}

#[cfg(test)]
mod test_lspserver_initialize {
    use super::*;
    use crate::test_utils::{mock_server, ONE_SEC, TWO_SECS};
    use lsp_types::{InitializeResult, InitializedParams, ServerCapabilities, ServerInfo};

    #[test]
    fn test_initialize_success() {
        let (mut server, r_lsp, s_lsp) = mock_server();
        let server_data = Arc::clone(&server.server_data);

        let init_params = InitializedParams {};
        let init_req = Request::new("initialize", "initialize", Some(init_params)).expect("Bad init request");
        let req_id = init_req.id.clone();

        let mut server_info = LspServerInfo::new(123); // the same as mock id
        server_info.info.name = "fake".to_string();
        let si = server_info.info.clone(); // clone lsp_types::ServerInfo to move to thread

        // Simulate the LSP server's response in a separate thread
        let lsp_thread = std::thread::spawn(move || {
            // assert fake LSP got INITIALIZE request
            let init_msg = r_lsp.recv_timeout(ONE_SEC).expect("Did not get INITIALIZE message");
            assert!(matches!(init_msg, Message::Request(req) if req.id == req_id && req.method == "initialize"));

            // send response with init results back (i.e. push directly to server_data)
            let init_result = InitializeResult { capabilities: ServerCapabilities::default(), server_info: Some(si) };
            let init_resp = Response::new_ok(req_id, Some(init_result)).expect("Bad init response");
            server_data.lock().unwrap().responses_unticked.push_back(init_resp);

            // assert fake LSP gets the INITIALIZED notification
            let init_notif = r_lsp.recv_timeout(ONE_SEC).expect("Did not get INITIALIZED notification");
            assert!(matches!(init_notif, Message::Notification(notif) if notif.method == "initialized"));
        });

        // test initialize() and compare server_info
        let res = server.initialize(init_req, TWO_SECS).expect("Initialize should succeed");
        assert_eq!(server.status(), ServerStatus::Running, "Server status should be RUNNING");
        assert_eq!(server.server_info, server_info, "Server info should be updated");

        lsp_thread.join().expect("LSP thread panicked");
    }
}

/// Tests spawining real CMDs on linux and their exit statuses and signals. Mocking child is overkill
#[cfg(unix)]
#[cfg(test)]
mod test_shutdown_with_real_cmd_as_fake_lspserver {
    use super::*;
    use crate::test_utils::*;
    use std::{process, thread, time::Duration};

    fn fake_server(cmd: &str, args: &str) -> LspServer {
        //setup_test_logger(); // enable to see logs
        let mut server = LspServer::new(cmd, args, "{}").expect("Should create test server");
        server
    }

    fn assert_resources(server: &LspServer) {
        assert!(server.resources.dispatcher.is_none(), "Dispatcher should be None after shutdown");
        assert!(server.resources.transport.is_none(), "Transport should be None after shutdown");
        assert!(server.resources.child.is_none(), "Child should be None after shutdown");
        assert!(server.sender.is_none(), "Sender should be None after shutdown");
    }

    fn assert_teardown_idempotent(mut server: LspServer, timeout: Duration, desc: &str, expected_exit: ExitType) {
        // First shutdown
        let exit_status = server.teardown(timeout);
        assert!(exit_status.is_ok(), "{desc}: first shutdown should succeed");

        assert_resources(&server);
        assert_exit_status(exit_status.unwrap(), expected_exit);

        // Second shutdown (idempotency) ----v
        let level = logger::get_log_level();
        logger::disable_logging(); // disable logging to avoid duplicated messages in log

        let result2 = server.teardown(timeout);
        assert!(result2.is_ok(), "{desc}: second shutdown should not panic");
        assert!(result2.unwrap().is_none(), "{desc}: second shutdown should return None");
        assert_resources(&server);

        logger::set_log_level(level); // reenable log
    }

    #[test]
    fn test_graceful_shutdown_lsp_exit() {
        let server = fake_server("true", ""); // true returns immediately, .e.g. simulates LSP voluntary exit
        assert_teardown_idempotent(server, Duration::from_secs(1), "graceful + LSP exit", ExitType::Code(0));
    }

    #[test]
    fn test_graceful_shutdown_lsp_killed() {
        let mut server = fake_server("sleep", "5"); // simulate external kill for LSP
        server.resources.child.as_mut().unwrap().kill().expect("Failed to kill child LSP process");
        assert_teardown_idempotent(server, Duration::from_secs(1), "graceful + LSP killed", ExitType::Signal((9)));
    }

    #[test]
    fn test_forced_shutdown() {
        let server = fake_server("sleep", "5");
        assert_teardown_idempotent(server, Duration::ZERO, "forced", ExitType::Signal(9));
    }

    #[test]
    fn test_graceful_escalates_to_forced_shutdown() {
        let server = fake_server("sleep", "5");
        assert_teardown_idempotent(server, Duration::from_millis(100), "graceful -> forced", ExitType::Signal(9));
    }
}

#[cfg(test)]
mod test_lspserver_shutdown_with_mock {
    use super::*;
    use crate::test_utils::*;

    #[test]
    fn test_shutdown_protocol_success() {
        let (mut server, r_lsp, s_lsp) = mock_server();
        let server_data = Arc::clone(&server.server_data);
        let shutdown_req = Request::new_shutdown();
        let req_id = shutdown_req.id.clone();

        // Simulate the LSP server's in a separate thread
        let lsp_thread = std::thread::spawn(move || {
            // assert fake LSP got the SHUTDOWN request
            let shutdown_msg = r_lsp.recv_timeout(ONE_SEC).expect("Did not get SHUTDOWN message");
            assert!(matches!(shutdown_msg, Message::Request(req) if req.id == req_id && req.method == "shutdown"));

            // send response back (e.g. push directly to server_data)
            let shutdown_resp = Response::new_ok(req_id, None::<bool>).unwrap();
            server_data.lock().unwrap().responses_unticked.push_back(shutdown_resp);

            // assert fake LSP get the final EXIT notification.
            let exit_msg = r_lsp.recv_timeout(ONE_SEC).expect("Did not get EXIT message");
            assert!(matches!(exit_msg, Message::Notification(notif) if notif.method == "exit"));
        });

        // test shutdown() logic and assert the status
        let res = server.shutdown(shutdown_req, TWO_SECS).expect("Shutdown should succeed");
        assert_eq!(server.status(), ServerStatus::Exiting, "Server status should be EXITING");

        lsp_thread.join().expect("LSP thread panicked");
    }

    #[test]
    fn test_shutdown_protocol_timeout() {
        let (mut server, r_lsp, s_lsp) = mock_server();
        let shutdown_req = Request::new_shutdown();

        // Run the shutdown() logic, but provide no response. Use short timeout as well.
        let result = server.shutdown(shutdown_req, Duration::from_millis(50));
        assert!(
            matches!(result, Err(ref e) if e.downcast_ref::<io::Error>().unwrap().kind() == io::ErrorKind::TimedOut),
            "Shutdown should return Timeout error"
        );
        assert_eq!(server.status(), ServerStatus::ShuttingDown, "Server status should remain SHUTTING_DOWN");
    }

    #[test]
    fn test_shutdown_protocol_err() {
        let (mut server, _, _) = mock_server(); // note that channels are dropped
        let shutdown_req = Request::new_shutdown();

        // Run the shutdown logic, but channel is already dropped. So we'll get an error on send
        let result = server.shutdown(shutdown_req, Duration::from_millis(50));
        assert!(
            matches!(result, Err(ref e) if e.downcast_ref::<crossbeam_channel::SendError<Message>>().is_some()),
            "Shutdown should return channel error"
        );
        assert_eq!(server.status(), ServerStatus::ShuttingDown, "Server status should remain SHUTTING_DOWN");
    }
}

#[cfg(test)]
mod test_lspserver_teardown {
    use super::LspServer;
    use std::{sync::Arc, thread, time::Duration};

    #[test]
    fn test_teardown_single_entry() {
        let mut server = LspServer::new("true", "", "{}").unwrap();
        let server = Arc::new(std::sync::Mutex::new(server));

        let mut handles = vec![];
        for _ in 0..5 {
            let server = Arc::clone(&server);
            handles.push(thread::spawn(move || server.lock().unwrap().teardown(Duration::ZERO)));
        }

        // gather all threads results
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        let enter_count = results.iter().filter(|r| matches!(r, Ok(Some(_)))).count();
        let skip_count = results.iter().filter(|r| matches!(r, Ok(None))).count();

        assert_eq!(enter_count, 1, "Only one thread should perform teardown");
        assert_eq!(skip_count, results.len() - 1, "Other threads should see teardown already in progress");
    }
}

#[cfg(test)]
mod test_send_message {
    use super::*;
    use crate::{lsp_server::FileInfo, test_utils::mock_server, test_utils::TENTH_OF_SEC};

    #[test]
    fn test_send_message_errors() {
        let test_cases = [("drop", "Failed to send to LSP"), ("take", "No LSP sender channel")];

        for (case, expected) in test_cases {
            let (mut server, r_lsp, s_lsp) = mock_server();
            match case {
                "drop" => drop(r_lsp),
                "take" => server.sender = None, // same as take for our case
                _ => unreachable!(),
            }
            let err = server.send_message(Request::new_shutdown()).expect_err("send_message should fail");
            assert!(err.to_string().contains(expected), "unexpected error: {err} != {expected}");
        }
    }

    #[test]
    fn test_send_message_ok() {
        fn test_send<M: Into<Message> + Clone>(msg: M) {
            let (mut server, r_lsp, s_lsp) = mock_server();
            let sent_msg = msg.clone();
            let res = server.send_message(msg).expect("send_message shuld succeed");

            let lsp_msg = r_lsp.recv_timeout(TENTH_OF_SEC).expect("Should receive message");
            assert_eq!(lsp_msg, sent_msg.into(), "Messages should match");
        }

        // test sending of all message types and conversions via into()
        test_send(Request::new_shutdown());
        test_send(Message::Request(Request::new_shutdown()));

        test_send(Notification::new("exit"));
        test_send(Message::Notification(Notification::new("exit")));

        test_send(Response::new_ok(3, "success").unwrap());
        test_send(Message::Response(Response::new_ok(3, "success").unwrap()));
        test_send(Response::new_err("", -1, "fail"));
        test_send(Message::Response(Response::new_err("", -1, "fail")));
    }

    #[test]
    fn test_send_message_update_server_data() {
        fn test_send<M: Into<Message>>(msg: M, drop_sender: bool, tick: &str) {
            let (mut server, r_lsp, s_lsp) = mock_server();
            {
                // check initial state
                let mut server_data = server.server_data.lock().unwrap();
                assert_eq!(server_data.request_ticks.len(), 0, "Should be empty");
                assert_eq!(server_data.request_ticks.len(), 0, "Should be empty");
                assert_eq!(server_data.latest_request_tick, String::new(), "Latest request tick on init");
            }

            let msg = msg.into();
            let msg_orig = msg.clone();
            let msg: Message = if drop_sender {
                drop(s_lsp); // simulate send error
                let res = server.send_message(msg); //.expect_err("send_message should fail");
                println!("{:?}", res);
                msg_orig
            } else {
                server.send_message(msg).expect("send_message should succeed");
                let msg = r_lsp.recv_timeout(TENTH_OF_SEC).expect("Should receive message");
                assert_eq!(msg_orig, msg, "messages should match");
                msg_orig
            };

            // assert server_data (updated only on Request with tick)
            let server_data = server.server_data.lock().unwrap();
            match &msg {
                Message::Request(req) if req.request_tick.is_some() => {
                    //println!("Yes tick: {:?}", req);
                    if let Some(req_tick) = &req.request_tick {
                        assert_eq!(req_tick, tick, "Ticks should be equal: {req_tick} vs {tick}");
                        assert_eq!(server_data.latest_request_tick, *tick, "Latest request tick");
                        assert_eq!(server_data.request_ticks.len(), 1, "One tick recorded");
                        assert_eq!(server_data.request_ticks.get(&req.id).unwrap(), tick, "Request ticks map");
                    }
                }
                other => {
                    // println!("No tick: {:?}", other);
                    // Response, Notification and Request without tick should not update server_data
                    // Response with tick
                    assert_eq!(server_data.request_ticks.len(), 0, "Should be no ticks recorded");
                    assert_eq!(server_data.latest_request_tick, String::new(), "Latest tick should be unset");
                }
            }
        }

        // passthrough for Response, Notification, Request without tick
        test_send(Request::new_shutdown(), false, "");
        test_send(Notification::new("exit"), false, "");
        test_send(Response::new_ok(3, "success").unwrap(), false, "");
        test_send(Response::new_err("err", -1, "fail"), false, "");

        // data update: Request with tick
        let mut req = Request::new("id", "method", Some(serde_json::Value::Null)).unwrap();
        let mut tick = "request_tick1";
        req.request_tick = Some(tick.to_string());
        test_send(req, false, tick);

        // passthrough: Response with tick (should not happen in practice, since those responses are not sent to LSP and not passing via send_msg())
        let mut resp = Response::new_err("id", -1, "fail");
        tick = "response_tick";
        resp.request_tick = tick.to_string();
        test_send(resp, false, tick);

        // data should be updated even if senfing Request with tick fails
        let mut req = Request::new("id", "method", Some(serde_json::Value::Null)).unwrap();
        tick = "request_tick2";
        req.request_tick = Some(tick.to_string());
        test_send(req, true, tick);
    }

    #[test]
    fn test_send_message_clear_diagnostic() {
        let (mut server, r_lsp, s_lsp) = mock_server();

        {
            // Manually add diagnostics for a URIs
            let mut server_data = server.server_data.lock().unwrap();

            for uri in ["a", "b"] {
                let mut file_info = FileInfo::new(uri);
                file_info.diagnostics.push(Diagnostic::default());
                server_data.file_infos.insert(uri.to_string(), file_info);
            }
        }

        // create and send a didChange request for one of the files
        let req =
            Request::new("id", "textDocument/didChange", serde_json::json!({"textDocument": {"uri": "a"}})).unwrap();

        // Send the message
        server.send_message(req).expect("send_message should succeed");

        // verify diagnosticcs cleared for one file and kept for another
        let server_data = server.server_data.lock().unwrap();
        for (uri, cleared) in [("a", true), ("b", false)] {
            assert_eq!(
                server_data.file_infos.get(uri).unwrap().diagnostics.is_empty(),
                cleared,
                "Diagnostics for file {uri} should be cleared={cleared}"
            );
        }
    }
}

#[cfg(test)]
mod test_dispatcher {
    use super::*;
    use crate::test_utils::{mock_server, setup_test_logger, ONE_SEC};
    use crossbeam_channel::{bounded, unbounded};
    use lsp_types::{Diagnostic, Position, PublishDiagnosticsParams, Range};

    fn start_dispatcher_thread(server: &LspServer) -> (thread::JoinHandle<()>, Sender<Message>, Receiver<()>) {
        let (dispatcher_tx, dispatcher_rx) = unbounded();
        let (done_tx, done_rx) = bounded::<()>(0);

        let exit = Arc::clone(&server.exit);
        let server_data = Arc::clone(&server.server_data);
        let dispatcher = std::thread::spawn(move || {
            LspServer::dispatcher_loop(dispatcher_rx, exit, server_data);
            done_tx.send(()).ok();
        });
        return (dispatcher, dispatcher_tx, done_rx);
    }

    fn run_dispatcher_test<F>(test_fn: F) -> HashMap<String, FileInfo>
    where
        F: FnOnce(&LspServer, &Sender<Message>),
    {
        let (server, r_lsp, s_lsp) = mock_server();
        let (dispatcher, dispatcher_tx, done_rx) = start_dispatcher_thread(&server);
        {
            let server_data = server.server_data.lock().unwrap();
            assert!(server_data.file_infos.is_empty(), "Should store no diagnostics");
        }

        test_fn(&server, &dispatcher_tx);

        assert!(done_rx.recv_timeout(ONE_SEC).is_ok(), "dispatcher should exit");
        dispatcher.join().ok();

        let server_data = server.server_data.lock().unwrap();
        return server_data.file_infos.clone();
    }

    fn create_publish_diagnostics_notification(uri: &str, range: std::ops::Range<u32>) -> Notification {
        let diagnostics = range
            .map(|i| {
                Diagnostic::new(
                    Range::new(Position::new(i, 0), Position::new(i, 1)),
                    None,
                    None,
                    None,
                    format!("diagnostic {}", i),
                    None,
                    None,
                )
            })
            .collect();

        let params = PublishDiagnosticsParams::new(uri.parse().unwrap(), diagnostics, None);
        Notification::new_params("textDocument/publishDiagnostics", serde_json::to_value(params).unwrap()).unwrap()
    }

    mod exit {
        use super::*;

        #[test]
        fn test_dispatcher_exit_on_notification() {
            run_dispatcher_test(|_server, dispatcher_tx| {
                let notif = Notification::new("exit"); // self-exit notification
                dispatcher_tx.send(Message::Notification(notif)).unwrap();
            });
        }

        #[test]
        fn test_dispatcher_exit_on_exit_flag() {
            run_dispatcher_test(|server, dispatcher_tx| {
                server.exit.store(true, Ordering::Relaxed); // set exit flag
                dispatcher_tx.send(Notification::new("dummy").into()).unwrap();
            });
        }

        #[test]
        fn test_dispatcher_exit_on_close_channel() {
            let (server, r_lsp, s_lsp) = mock_server();
            let (dispatcher, dispatcher_tx, done_rx) = start_dispatcher_thread(&server);
            drop(dispatcher_tx); // drop/close channel
            assert!(done_rx.recv_timeout(ONE_SEC).is_ok(), "dispatcher should exit");
            dispatcher.join().ok();
        }
    }

    mod notification {
        use std::i16::MAX;

        use super::*;
        use serde_json::json;

        #[test]
        fn test_dispatcher_notification_skip() {
            setup_test_logger();

            let test_cases = vec![
                Notification::new("testDcocument/didOpen"), // not a publishDiagnostics notification
                Notification::new_params("textDocument/publishDiagnostics", json!({"uri":"dummy", "diagnostics":[]}))
                    .expect("Notification with empty diagnostics"), // empty diagnostic vector
                Notification::new("textDocument/publishDiagnostics"), // no PublishDiagnosticsParams, will fail to parse
                Notification::new_params("textDocument/publishDiagnostics", r#"{"file":"dummy"}"#)
                    .expect("Notification with valid JSON but bad DiagnosticParam"), //bad param json, will fail to parse
            ];

            for notif in test_cases {
                let infos = run_dispatcher_test(|server, dispatcher_tx| {
                    dispatcher_tx.send(notif.into()).unwrap();
                    dispatcher_tx.send(Notification::new("exit").into()).unwrap();
                });
                assert!(infos.is_empty(), "Should store no diagnostics");
            }
        }

        #[test]
        fn test_dispatcher_notification_diag_truncate_max() {
            let uri = "file://a";

            let max = MAX_DIAGNOSTICS.load(Ordering::Relaxed) as u32;
            let infos = run_dispatcher_test(|server, dispatcher_tx| {
                let many_diag = create_publish_diagnostics_notification(uri, 0..(max + 10));
                dispatcher_tx.send(many_diag.into()).unwrap();
                dispatcher_tx.send(Notification::new("exit").into()).unwrap();
            });
            assert_eq!(infos.len(), 1, "Should be single entry");
            assert_eq!(infos.get(uri).unwrap().diagnostics.len(), max as usize, "Should have {max} diagnostics");
        }

        #[test]
        fn test_dispatcher_notification_diag_update() {
            let uri = "file://b";

            let infos = run_dispatcher_test(|server, dispatcher_tx| {
                let diag_1_2 = create_publish_diagnostics_notification(uri, 1..2);
                dispatcher_tx.send(diag_1_2.into()).unwrap();
                let diag_5_9 = create_publish_diagnostics_notification(uri, 5..9);
                dispatcher_tx.send(diag_5_9.into()).unwrap();
                dispatcher_tx.send(Notification::new("exit").into()).unwrap();
            });
            assert_eq!(infos.len(), 1, "Should be single entry");
            let diags = &infos.get(uri).unwrap().diagnostics;
            assert_eq!(diags.len(), 4, "Should have 4 diagnostics");
            assert_eq!(diags[0].message, "diagnostic 5", "First diag should be 5");
            assert_eq!(diags[3].message, "diagnostic 8", "Last diag should be 8");
        }

        #[test]
        fn test_dispatcher_notification_diag_clean() {
            let uri = "file://c";

            let infos = run_dispatcher_test(|server, dispatcher_tx| {
                let diag_1_2 = create_publish_diagnostics_notification(uri, 1..2);
                dispatcher_tx.send(diag_1_2.into()).unwrap();
                let diag_5_9 = create_publish_diagnostics_notification(uri, 0..0);
                dispatcher_tx.send(diag_5_9.into()).unwrap();
                dispatcher_tx.send(Notification::new("exit").into()).unwrap();
            });
            assert_eq!(infos.len(), 0, "Should be empty");
        }

        #[test]
        fn test_dispatcher_notification_limit() {
            let (server, r_lsp, s_lsp) = mock_server();
            let (dispatcher, dispatcher_tx, done_rx) = start_dispatcher_thread(&server);

            for i in 0..(MAX_NOTIFICATIONS * 2) {
                let notif = Notification::new(format!("{i}")); // dummy notification with num
                dispatcher_tx.send(notif.into()).unwrap(); // should use bounded_push_back
            }
            drop(dispatcher_tx); // close channel to exit dispatcher
            assert!(done_rx.recv_timeout(ONE_SEC).is_ok(), "dispatcher should exit");
            dispatcher.join().ok();

            let max = MAX_NOTIFICATIONS;
            let server_data = server.server_data.lock().unwrap();
            assert_eq!(server_data.notifications.len(), max, "Should store max notifications");
            assert_eq!(server_data.notifications[0].method, format!("{max}"), "Oldest notification");
            assert_eq!(server_data.notifications[max - 1].method, format!("{}", max * 2 - 1), "Newest notification");
        }
    }
}
