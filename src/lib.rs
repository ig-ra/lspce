#![allow(unused)]

mod bufext;
mod connection;
mod env;
mod errors;
pub mod logger;
mod msg;
mod safe_call;
mod socket;
mod stdio;
mod utils;

// for both lib and integrations tests
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(test)]
mod tests;

use anyhow::{anyhow, bail, Context};
use atomic_enum::atomic_enum;
use crossbeam_channel::{Receiver, Sender};
use emacs::{defun, Env, IntoLisp, Result as EmacsResult, Value};

use lsp_types::{
    Diagnostic, DidChangeTextDocumentParams, InitializeResult, InitializedParams, PublishDiagnosticsParams,
    VersionedTextDocumentIdentifier,
};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;

use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    io::{self, Read, Write},
    ops::{Deref, DerefMut},
    panic::Location,
    process::{Child, Command, ExitStatus, Stdio},
    sync::atomic::{AtomicBool, AtomicI32, Ordering},
    sync::{Arc, LazyLock, Mutex},
    thread::{self, JoinHandle, Thread},
    time::{Duration, Instant},
};

use env::{EnvExt, UserMsgEnv};
use errors::UserFacing;
use logger::Logger;
use lspce_macros::defun_safe;
pub use msg::{Message, Notification, Request, RequestId, Response};
use safe_call::safe_call;
use stdio::IoThreads;
pub use stdio::ThreadResult;
use utils::*;

pub static MAX_DIAGNOSTICS_COUNT: AtomicI32 = AtomicI32::new(30);

#[derive(Debug)]
struct FileInfo {
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

const POLL_INTERVAL: Duration = Duration::from_millis(10);
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const KILL_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const REAPER_INTERVAL: Duration = Duration::from_secs(5);

const MAX_NOTIFICATIONS: usize = 10;

struct LspServerData {
    latest_request_id: RequestId,
    latest_request_tick: String,
    latest_response_id: RequestId,
    latest_response_tick: String,
    request_ticks: HashMap<RequestId, String>,
    file_infos: HashMap<String, FileInfo>,
    requests: VecDeque<Request>,
    responses: VecDeque<Response>,
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
    child: Option<Child>,
    transport: Option<IoThreads>,
    dispatcher: Option<thread::JoinHandle<()>>,
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
    status: AtomicServerStatus,
    sender: Option<Sender<Message>>,
    server_data: Arc<Mutex<LspServerData>>,
    exit: Arc<AtomicBool>,
    name_id: String,
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

        _request_async(self, req)?;

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

    pub fn update_request_info(&self, id: RequestId, tick: String) {
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

    pub fn kill_child(&mut self) -> EmacsResult<Option<ExitStatus>> {
        if let Some(mut child) = self.resources.child.take() {
            return kill_child_and_wait_with_timeout(&mut child, KILL_WAIT_TIMEOUT, &self.name_id);
        }
        Ok(None)
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
    fn teardown(&mut self, graceful_timeout: Duration) -> EmacsResult<Option<ExitStatus>> {
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

pub struct Project {
    pub root_uri: String, // root URI of the project, e.g. "file:///home/user/project"
    pub servers: HashMap<String, Option<LspServer>>, // map each language_id to a lsp server
}

impl Project {
    pub fn new(root_uri: String) -> Project {
        Project { root_uri, servers: HashMap::new() }
    }
}

pub struct Projects(pub HashMap<String, Project>);

impl Projects {
    pub fn new() -> Self {
        Projects(HashMap::new())
    }

    pub fn add_server(&mut self, root_uri: String, lsp_type: String, server: LspServer) {
        let prj = self.0.entry(root_uri.clone()).or_insert_with(|| Project::new(root_uri));
        prj.servers.insert(lsp_type, Some(server));
    }
}

impl Deref for Projects {
    type Target = HashMap<String, Project>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Projects {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

static PROJECTS: LazyLock<Arc<Mutex<Projects>>> = LazyLock::new(|| Arc::new(Mutex::new(Projects::new())));

fn projects() -> &'static Arc<Mutex<Projects>> {
    &PROJECTS
}

fn with_projects_mut<F, R>(f: F) -> R
where
    F: FnOnce(&mut Projects) -> R,
{
    f(&mut projects().lock().unwrap())
}

/// Acqures a lock on projects, finds the project by `root_uri`, and applies the function `f` to it.
/// Returns `None` if the project is not found or Option<T>, if found
fn with_project_mut<F, T>(root_uri: &str, f: F) -> Option<T>
where
    F: FnOnce(&mut Project) -> T,
{
    with_projects_mut(|projects| projects.get_mut(root_uri).map(f))
}

///
/// This function iterates through all servers and, upon finding a dead one, takes
/// ownership of it. This leaves `None` in its place in the map and immediately
/// triggers the `LspServer::drop` implementation, which ensures resource teardown.
///
/// A server is considered dead if its dispatcher thread has terminated or its exit
/// flag has been set due to an I/O error.
pub fn reap_dead_servers_once() {
    with_projects_mut(|projects| {
        for project in projects.values_mut() {
            for server_option in project.servers.values_mut() {
                if let Some(server) = server_option {
                    let exit_requested = server.exit.load(Ordering::Relaxed);
                    let dispatcher_finished = server.resources.dispatcher.as_ref().map_or(true, |h| h.is_finished());

                    if exit_requested || dispatcher_finished {
                        Logger::info(format!("Reaper detected dead server: {}. Dropping", server.name_id));
                        // Take ownership, causing server to be dropped, which will trigger teardown logic
                        let _ = server_option.take();
                    }
                }
            }
        }
    })
}

/// A background thread that periodically checks for and cleans up dead LSP servers.
fn reap_dead_servers() {
    loop {
        thread::sleep(REAPER_INTERVAL);
        reap_dead_servers_once();
    }
}

// Emacs won't load the module without this.
#[cfg(not(test))]
emacs::plugin_is_GPL_compatible!();

use std::sync::Once;
static INIT: Once = Once::new();

// need to be called both in profuction as part of initialization hook and in lib + integration tests
pub fn lspce_init() {
    INIT.call_once(|| {
        logger::set_log_prefix("[MAIN] - ");
        thread::spawn(reap_dead_servers);
    });
}

// Register the initialization hook that Emacs will call when it loads the module.
#[cfg(not(test))]
#[emacs::module(name("lspce-module"))]
fn init(env: &Env) -> EmacsResult<Value<'_>> {
    lspce_init();
    env.lspce_message("Done loading")
}

#[defun]
fn change_max_diagnostics_count(env: &Env, count: i32) -> EmacsResult<Value<'_>> {
    MAX_DIAGNOSTICS_COUNT.store(count, Ordering::Relaxed);
    env.lspce_message(format!("Set max diagnostics count to {}", count))
}

#[defun]
fn read_max_diagnostics_count(env: &Env) -> EmacsResult<i32> {
    let count = MAX_DIAGNOSTICS_COUNT.load(Ordering::Relaxed);
    Ok(count)
}

/// disable logging to /tmp/lspce.log
#[defun]
fn disable_logging(env: &Env) -> EmacsResult<Value<'_>> {
    logger::disable_logging();
    env.lspce_message("Logging is disabled")
}

/// enable logging to /tmp/lspce.log
#[defun]
fn enable_logging(env: &Env) -> EmacsResult<Value<'_>> {
    logger::enable_logging();
    env.lspce_message("Logging is enabled")
}

#[defun]
fn set_log_level(env: &Env, level: u8) -> EmacsResult<Value<'_>> {
    logger::set_log_level(level);
    env.lspce_message(format!("Set log level to {}", level))
}

#[defun]
fn get_log_level(env: &Env) -> EmacsResult<u8> {
    Ok(logger::get_log_level())
}

/// set logging file name
#[defun]
fn set_log_file(env: &Env, file: String) -> EmacsResult<Value<'_>> {
    let message = format!("Set logging file to {}", file);
    logger::set_log_file_name(file);
    env.lspce_message(message)
}

fn with_project<F, T>(root_uri: &str, f: F) -> EmacsResult<Option<T>>
where
    F: FnOnce(&mut Project) -> EmacsResult<Option<T>>,
{
    match with_project_mut(root_uri, f) {
        Some(result) => result,
        None => Err(anyhow::anyhow!("No project found for '{}'", root_uri).context(UserFacing)),
    }
}

fn with_server<F, T>(root_uri: &str, file_type: &str, require_running: bool, f: F) -> EmacsResult<Option<T>>
where
    F: FnOnce(&mut LspServer) -> EmacsResult<Option<T>>,
{
    with_project(root_uri, |project| match project.servers.get_mut(file_type) {
        Some(Some(server)) => {
            if require_running && server.status() != ServerStatus::Running {
                Err(anyhow::anyhow!("LSP server for {}({}) is not ready", root_uri, file_type).context(UserFacing))
            } else {
                f(server)
            }
        }
        _ => Err(anyhow::anyhow!("No LSP server for {}({})", root_uri, file_type).context(UserFacing)),
    })
}

/// Connect to an existing server or create a server subprocess and then connect to it.
#[defun_safe]
#[defun]
fn connect(
    env: &Env, root_uri: String, lsp_type: String, cmd: String, cmd_args: String, initialize_req_str: String,
    timeout: i32, emacs_envs: String,
) -> EmacsResult<Option<String>> {
    let prj_name_type = format!("{}({})", root_uri, lsp_type);
    Logger::info(format!("Creating and initializing LSP server for {}", prj_name_type));

    if let Ok(Some(server_info_json)) =
        with_server(&root_uri, &lsp_type, false, |server| Ok(Some(server.server_info.to_json_string()?)))
    {
        Logger::info(format!("Using existing LSP server {}", prj_name_type));
        return Ok(Some(server_info_json));
    }

    let mut server = LspServer::new(&cmd, &cmd_args, &emacs_envs)
        .with_context(|| format!("Failed to create LSP server for {}, <{} {}>", prj_name_type, cmd, cmd_args))?;

    Logger::debug(format!("raw initialize request {:#?}", initialize_req_str));
    let req = Message::from_str_typed::<Request>(&initialize_req_str).context("initialize")?;
    initialize(env, &mut server, req, Duration::from_secs(timeout.max(0) as u64))
        .inspect_err(|_| {
            let _ = server.teardown(Duration::ZERO); // forcibly kill server and join threads
        })
        .with_context(|| format!("Failed to initialize LSP server for {}", prj_name_type))?;

    let server_info = server.server_info.clone();

    with_projects_mut(|projects| {
        projects.add_server(root_uri, lsp_type, server);
    });

    Logger::info(format!("Connected to server successfully. server capabilities {}", &server_info.capabilities));
    Ok(Some(server_info.to_json_string()?))
}

pub fn initialize(env: &Env, server: &mut LspServer, req: Request, timeout: Duration) -> EmacsResult<()> {
    Logger::info(format!("initialize request {}", &req));
    _request_async(server, req)?;

    let start_time = Instant::now();
    loop {
        if let Some(response) = server.read_response() {
            if let Some(error) = &response.error {
                bail!("LSP error: {:?}", error);
            }

            // FIXME: why do we need pretty? what do we do after?
            Logger::info(format!("initialize response {}", &response));

            let ir: InitializeResult = serde_json::from_value(response.result.context("Empty initialize response")?)?;

            let initialized = Notification::new("initialized", InitializedParams {})?;
            server.write(initialized)?;

            server.server_info.capabilities = serde_json::to_string(&ir.capabilities)?;
            if let Some(si) = ir.server_info {
                server.server_info.name = si.name;
                server.name_id = name_id(&server.server_info.name, &server.server_info.id);
                server.server_info.version = si.version.unwrap_or_default();
            }
            server.set_status(ServerStatus::Running);

            return Ok(());
        }

        if !timeout.is_zero() && start_time.elapsed() > timeout {
            bail!("Timeout while initializing LSP server");
        }

        thread::sleep(POLL_INTERVAL);
    }
}

/// Orchestrates the server shutdown sequence. Intended to be run in a background thread.
///
/// This function first attempts a polite, protocol-level `shutdown`.
/// Then it proceeds to final cleanup and resource `teardown`.
///
/// # Arguments
/// * `server` - The `LspServer` instance to shut down.
/// * `req` - The `shutdown` request to be sent to the server.
/// * `timeout` - Timeout to complete gracefull shutdown process
///
pub fn shutdown_server(
    server: &mut LspServer, req: Request, timeout: Option<Duration>,
) -> EmacsResult<Option<ExitStatus>> {
    let mut timeout = timeout.unwrap_or(GRACEFUL_SHUTDOWN_TIMEOUT);

    let start_time = Instant::now();
    let shutdown_proto_res = server.shutdown(req, timeout);
    let elapsed = start_time.elapsed();
    timeout = timeout.checked_sub(elapsed).unwrap_or(Duration::ZERO);

    if let Err(err) = shutdown_proto_res {
        Logger::error(format!("Failed shutdown protocol for {}: {}", server.name_id, err));
        timeout = Duration::ZERO; // don't wait exit of LSP, escalate to kill.
    }

    server.teardown(timeout)
}

// Spans a thread to shut down the server to avoid blocking emacs while performing LSP shutdown sequence.
#[defun_safe]
#[defun]
fn shutdown(env: &Env, root_uri: String, file_type: String, request: String) -> EmacsResult<Option<bool>> {
    with_project(&root_uri, |project| {
        if let Some(Some(mut server)) = project.servers.remove(&file_type) {
            let shutdown_req = Message::from_str_typed::<Request>(&request).unwrap_or_else(|e| {
                Logger::info(format!("Failed to parse shutdown request: {}. Using default", e));
                Request::new_shutdown()
            });
            std::thread::spawn(move || shutdown_server(&mut server, shutdown_req, None));
            return Ok(Some(true));
        }
        Ok(Some(false))
    })
}

#[defun_safe]
#[defun]
fn server(env: &Env, root_uri: String, file_type: String) -> EmacsResult<Option<String>> {
    with_server(&root_uri, &file_type, false, |server| Ok(Some(server.server_info.to_json_string()?)))
}

fn _request_async(server: &mut LspServer, req: Request) -> EmacsResult<Option<bool>> {
    let request_tick = req.request_tick.as_ref().map(|s| s.clone()).unwrap_or_else(|| {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        format!("{}.{}", now.as_secs(), now.subsec_micros())
    });
    server.update_request_info(req.id.clone(), request_tick);

    // TODO: should we let LSP server to manage and clear diagnostics and remove this entirely?

    if req.method == "textDocument/didChange" || req.method == "textDocument/didClose" {
        // extract uri, wihotut parsing the whole request
        if let Some(uri) = req.params.get("textDocument").and_then(|td| td.get("uri")).and_then(|uri| uri.as_str()) {
            server.clear_diagnostics(uri);
        }
    }
    server.write(req)?;
    Ok(Some(true))
}

#[defun_safe]
#[defun]
fn request_async(env: &Env, root_uri: String, file_type: String, json: String) -> EmacsResult<Option<bool>> {
    with_server(&root_uri, &file_type, true, |server| {
        Logger::trace(format!("request {}", &json));
        let msg = Message::from_str_typed::<Request>(&json).context("request_async")?;
        _request_async(server, msg)
    })
}

#[defun_safe]
#[defun]
fn notify(env: &Env, root_uri: String, file_type: String, json: String) -> EmacsResult<Option<bool>> {
    with_server(&root_uri, &file_type, true, |server| {
        Logger::trace(format!("notify {}", &json));
        let msg = Message::from_str_typed::<Notification>(&json).context("notify")?;
        server.write(msg)?;
        Ok(Some(true))
    })
}

/// precondition: have called read_latest_response_id and gotten the id.
#[defun_safe]
#[defun]
fn read_response_exact(
    env: &Env, root_uri: String, file_type: String, id: String, method: String,
) -> EmacsResult<Option<String>> {
    with_server(&root_uri, &file_type, true, |server| {
        Ok(server.read_response_exact(RequestId::from(id), method).map(|r| r.into_string()))
    })
}

#[defun_safe]
#[defun]
fn read_notification(env: &Env, root_uri: String, file_type: String) -> EmacsResult<Option<String>> {
    with_server(&root_uri, &file_type, true, |server| Ok(server.read_notification().map(|r| r.into_string())))
}

#[defun_safe]
#[defun]
fn read_file_diagnostics(env: &Env, root_uri: String, file_type: String, uri: String) -> EmacsResult<Option<String>> {
    with_server(&root_uri, &file_type, true, |server| {
        let mut server_data = server.server_data.lock().unwrap();
        Ok(server_data
            .file_infos
            .get(&uri)
            .map(|file_info| serde_json::to_string(&file_info.diagnostics).context("Failed to serialize diagnostics"))
            .transpose()?)
    })
}

#[defun_safe]
#[defun]
fn read_latest_response_id(env: &Env, root_uri: String, file_type: String) -> EmacsResult<Option<String>> {
    with_server(&root_uri, &file_type, true, |server| Ok(Some(server.get_latest_response_id().to_string())))
}

#[defun_safe]
#[defun]
fn read_latest_response_tick(env: &Env, root_uri: String, file_type: String) -> EmacsResult<Option<String>> {
    with_server(&root_uri, &file_type, true, |server| Ok(Some(server.get_latest_response_tick())))
}
