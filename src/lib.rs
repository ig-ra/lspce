#![allow(unused)]

mod bufext;
mod connection;
pub mod logger;
mod msg;
mod socket;
mod stdio;

#[cfg(test)]
mod tests;

use anyhow::{anyhow, bail, Context};
use connection::Connection;
use emacs::{defun, Env, IntoLisp, Result, Value};
use logger::Logger;
use lspce_macros::defun_safe;

use lsp_types::{
    Diagnostic, DidChangeTextDocumentParams, InitializeResult, InitializedParams, PublishDiagnosticsParams,
    VersionedTextDocumentIdentifier,
};
pub use msg::{Message, Notification, Request, RequestId, Response};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;

use std::panic::Location;
use std::result::Result as RustResult;

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU8, Ordering};
use std::time::{Duration, Instant};
use stdio::IoThreads;

use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    io::{Read, Write},
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, LazyLock, Mutex},
    thread::{self, JoinHandle, Thread},
};

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
}

const SERVER_STATUS_NEW: u8 = 0;
const SERVER_STATUS_STARTING: u8 = 1;
const SERVER_STATUS_RUNNING: u8 = 2;
const SERVER_STATUS_EXITING: u8 = 3;

const POLL_INTERVAL: Duration = Duration::from_millis(10);
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const KILL_WAIT_TIMEOUT: Duration = GRACEFUL_SHUTDOWN_TIMEOUT;
const MAX_NOTIFICATIONS: usize = 10;
const DISPATCHER_SLEEP: Duration = Duration::from_millis(1);

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

/// Represents a Language Server Protocol (LSP) server instance.
///
/// This struct manages the lifecycle of an LSP server process, including:
/// - The child process handle
/// - Server information (name, version, capabilities)
/// - Connection state and transport threads
/// - Server data and exit flag
pub struct LspServer {
    pub child: Option<Child>,
    pub server_info: LspServerInfo,
    pub status: u8,
    transport: Arc<Mutex<Option<Connection>>>,
    transport_threads: Option<IoThreads>,
    dispatcher: Option<thread::JoinHandle<()>>,
    server_data: Arc<Mutex<LspServerData>>,
    exit: Arc<AtomicBool>,
}

impl LspServer {
    pub fn new(cmd: &str, cmd_args: &str, emacs_envs: &str) -> Result<LspServer> {
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

        let (mut transport, mut transport_threads) = Connection::stdio(stdin, stdout, stderr);

        let mut server_info = LspServerInfo::new(child.id());
        if let Some(name) = std::path::Path::new(cmd).file_name().and_then(|n| n.to_str()) {
            server_info.name = name.to_string();
        }

        let mut server = LspServer {
            child: Some(child),
            server_info: server_info,
            status: SERVER_STATUS_STARTING,
            transport: Arc::new(Mutex::new(Some(transport))),
            transport_threads: Some(transport_threads),
            dispatcher: None,
            server_data: Arc::new(Mutex::new(LspServerData::new())),
            exit: Arc::new(AtomicBool::new(false)),
        };

        server.dispatcher = Some(LspServer::start_dispatcher(
            Arc::clone(&server.transport),
            Arc::clone(&server.exit),
            Arc::clone(&server.server_data),
        ));
        Ok(server)
    }

    fn start_dispatcher(
        transport: Arc<Mutex<Option<Connection>>>, exit: Arc<AtomicBool>, server_data: Arc<Mutex<LspServerData>>,
    ) -> thread::JoinHandle<()> {
        let handle = thread::spawn(move || loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }

            let mut message: Option<Message> = None;
            {
                let transport = transport.lock().unwrap();
                message = transport.as_ref().unwrap().read();
            }

            if let Some(m) = message {
                match m {
                    Message::Request(r) => {
                        if r.method == "workspace/configuration" {
                            let mut server_data = server_data.lock().unwrap();
                            server_data.requests.push_back(r);
                        }
                        // save request into the queue FIXME
                        // {
                        //     let mut requests = requests2.lock().unwrap();
                        //     requests.push_back(r);
                        // }
                    }
                    Message::Response(mut r) => {
                        Logger::trace(format!("Response {}", r));
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
                        // cache diagnostics so they won't pour into Emacs
                        if r.method == "textDocument/publishDiagnostics" {
                                Ok(params) => {
                            let uri_string: String = params.uri.into();
                            let mut file_info = FileInfo::new(&uri_string);
                            // cache no more than MAX_DIAGNOSTICS_COUNT diagnostics
                            let max_diagnostic_count = MAX_DIAGNOSTICS_COUNT.load(Ordering::Relaxed);
                            match serde_json::from_value::<PublishDiagnosticsParams>(r.params) {
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
            } else {
                thread::sleep(DISPATCHER_SLEEP);
            }
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

    pub fn write<M: Into<Message>>(&self, msg: M) -> Result<()> {
        self.transport
            .lock()
            .map_err(|_| anyhow!("transport mutex poisoned"))?
            .as_ref()
            .context("transport not established")?
            .write(msg.into())
            .context("failed to write to transport")
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

    pub fn name_id(&self) -> String {
        format!("<{}:[{}]>", self.server_info.name, self.server_info.id)
    }

    pub fn stop_dispatcher(&mut self) {
        self.exit.store(true, Ordering::Relaxed);
    }

    pub fn exit_transport(&self) {
        let transport = self.transport.lock().unwrap();
        transport.to_exit();
    }

    /// Kill the child process (if any) and wait for it to exit, avoiding zombies.
    /// Returns Ok(Some(status)) if the process exited, Ok(None) if it did not exit in time, or Err(e) on error.
    pub fn kill_child(&mut self) -> Result<Option<ExitStatus>> {
        if let Some(mut child) = self.child.take() {
            Logger::info(format!("forcefully terminating {}", self.name_id()));
            if let Err(e) = child.kill() {
                Logger::error(format!("failed to kill child process for {}: {}", self.name_id(), e));
                return Err(e.into());
            }
            return Self::wait_child_with_timeout(&mut child, KILL_WAIT_TIMEOUT, &self.name_id(), "forced kill_child");
        }
        Ok(None)
    }

    /// joins dispatcher and transport threads.
    pub fn join_threads(&mut self) {
        if let Some(threads) = self.transport_threads.take() {
            if let Err(e) = threads.join() {
                Logger::error(format!("error joining transport threads for {}: {}", self.name_id(), e));
            }
        }

        if let Some(handle) = self.dispatcher.take() {
            if let Err(e) = handle.join() {
                Logger::error(format!("error joining dispatcher thread for {}: {:?}", self.name_id(), e));
            }
        }
    }

    // Helper for waiting on a child with timeout
    fn wait_child_with_timeout(
        child: &mut std::process::Child, timeout: Duration, name_id: &str, exit_type: &str,
    ) -> Result<Option<ExitStatus>> {
        use wait_timeout::ChildExt;
        let msg = format!("{}: child process for {}", exit_type, name_id);
        match child.wait_timeout(timeout) {
            Ok(Some(status)) => {
                Logger::info(format!("{} exited with status {}", msg, status));
                Ok(Some(status))
            }
            Ok(None) => {
                Logger::info(format!("{} did not exit after timeout {:?}", msg, timeout));
                Ok(None)
            }
            Err(e) => {
                Logger::error(format!("{} wait timeout error: {}", msg, e));
                Err(e.into())
            }
        }
    }

    /// Shutdown LSP child process and associated threads
    ///
    /// 1. Signal the dispatcher thread and transport threads to stop
    /// 2. Join both dispatcher and transport threads
    /// 3. Attempt a graceful shutdown of the child LSP process, if provided graceful timeout is non-zero
    /// 4. If the process does not exit within the provided timeout, or error occured, or no graceful timeout provided
    ///    proceed to the forced shutdown, e.g. kill the process and try to wait for its status for KILL_WAIT_TIMEOUT
    ///    to avoid leaving a zombie process.
    ///
    /// # Arguments
    /// * `graceful_timeout` - The maximum duration to wait for graceful shutdown before escalating to forced termination.
    ///   Use `Duration::ZERO` to skip graceful shutdown and immediately force kill.
    ///
    /// # Returns
    /// * `Ok(Some(status))` if the process exited (gracefully or forcibly) and an exit status is available.
    /// * `Ok(None)` if the process did not exit within the allowed time.
    /// * `Err(e)` if an error occurred during shutdown.
    pub fn shutdown(&mut self, graceful_timeout: Duration) -> Result<Option<ExitStatus>> {
        let name_id = self.name_id();
        Logger::debug(format!("begin shutdown sequence for {}", name_id));

        self.stop_dispatcher();
        self.exit_transport();
        Logger::debug(format!("after stopping transport and dispatcher for {}", name_id));

        let mut status = Ok(None);
        if let Some(mut child) = self.child.take() {
            // Try graceful shutdown first
            if graceful_timeout > Duration::ZERO {
                Logger::debug(format!("attempting graceful shutdown for {}", name_id));
                status = Self::wait_child_with_timeout(&mut child, graceful_timeout, &name_id, "graceful shutdown");
            }

            // Either graceful shutdown did not succeed or no graceful timeout provided
            if !matches!(status, Ok(Some(_))) {
                Logger::debug("attempting forced shutdown");
                self.child = Some(child); // set back the child so kill_child can be used
                status = self.kill_child();
            }
        }

        Logger::debug(format!("joining transport and dispatcher threads for {}", name_id));
        self.join_threads();
        Logger::debug(format!("end shutdown sequence for {}", name_id));

        status
    }
}

struct Project {
    pub root_uri: String,                    //
    pub servers: HashMap<String, LspServer>, // map each language_id to a lsp server
}

impl Project {
    pub fn new(root_uri: String) -> Project {
        Project { root_uri, servers: HashMap::new() }
    }
}
static PROJECTS: LazyLock<Arc<Mutex<HashMap<String, Project>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));

fn projects() -> &'static Arc<Mutex<HashMap<String, Project>>> {
    &PROJECTS
}

// Emacs won't load the module without this.
emacs::plugin_is_GPL_compatible!();

trait EnvExt {
    fn lspce_message(&self, text: impl AsRef<str>) -> Result<Value<'_>>;
}

impl EnvExt for Env {
    fn lspce_message(&self, text: impl AsRef<str>) -> Result<Value<'_>> {
        self.message(format!("[lspce-module] {}", text.as_ref()))
    }
}

// Register the initialization hook that Emacs will call when it loads the module.
#[emacs::module(name("lspce-module"))]
fn init(env: &Env) -> Result<Value<'_>> {
    env.lspce_message("Done loading")
}

#[defun]
fn change_max_diagnostics_count(env: &Env, count: i32) -> Result<Value<'_>> {
    MAX_DIAGNOSTICS_COUNT.store(count, Ordering::Relaxed);
    env.lspce_message(format!("Set max diagnostics count to {}", count))
}

#[defun]
fn read_max_diagnostics_count(env: &Env) -> Result<i32> {
    let count = MAX_DIAGNOSTICS_COUNT.load(Ordering::Relaxed);
    Ok(count)
}

/// disable logging to /tmp/lspce.log
#[defun]
fn disable_logging(env: &Env) -> Result<Value<'_>> {
    logger::disable_logging();
    env.lspce_message("Logging is disabled")
}

/// enable logging to /tmp/lspce.log
#[defun]
fn enable_logging(env: &Env) -> Result<Value<'_>> {
    logger::enable_logging();
    env.lspce_message("Logging is enabled")
}

#[defun]
fn set_log_level(env: &Env, level: u8) -> Result<Value<'_>> {
    logger::set_log_level(level);
    env.lspce_message(format!("Set log level to {}", level))
}

#[defun]
fn get_log_level(env: &Env) -> Result<u8> {
    Ok(logger::get_log_level())
}

/// set logging file name
#[defun]
fn set_log_file(env: &Env, file: String) -> Result<Value<'_>> {
    let message = format!("Set logging file to {}", file);
    logger::set_log_file_name(file);
    env.lspce_message(message)
}

macro_rules! env_message_and_bail {
    ($env:expr, @ $location:expr, $($arg:tt)*) => {
        env_message_and_bail!($env, loc: $location, $($arg)*)
    };

    ($env:expr, loc: $location:expr, $($arg:tt)*) => {{
        let msg = format!($($arg)*);
        let _ = $env.lspce_message(&msg);
        let bail_msg = match $location {
            None => msg,
            Some(loc) => format!("{}. @{}", msg, loc),
        };
        anyhow::bail!(bail_msg)
    }};

    ($env:expr, $($arg:tt)*) => {
        env_message_and_bail!($env, loc: None::<&std::panic::Location>, $($arg)*)
    };
}

#[track_caller]
fn with_project<F, T>(env: &Env, root_uri: &str, caller_loc: Option<&Location<'static>>, f: F) -> Result<Option<T>>
where
    F: FnOnce(&mut Project) -> Result<Option<T>>,
{
    let mut projects_guard = projects().lock().unwrap();
    let caller = Some(caller_loc.unwrap_or_else(|| Location::caller()));

    match projects_guard.get_mut(root_uri) {
        Some(project) => f(project),
        None => {
            env_message_and_bail!(env, @ caller, "no project found for '{}'", root_uri)
        }
    }
}

#[track_caller]
fn with_server<F, T>(env: &Env, root_uri: &str, file_type: &str, f: F) -> Result<Option<T>>
where
    F: FnOnce(&mut LspServer) -> Result<Option<T>>,
{
    let caller = Some(Location::caller());
    with_project(env, root_uri, caller, |project| match project.servers.get_mut(file_type) {
        Some(server) => {
            if server.status != SERVER_STATUS_RUNNING {
                env_message_and_bail!(env, @ caller, "LSP server for {}({}) is not ready", root_uri, file_type)
            }
            f(server)
        }
        None => {
            env_message_and_bail!(env, @ caller, "No LSP server for {}({})", root_uri, file_type)
        }
    })
}

/// Executes a closure `f` and on error, log and return `Ok(None)`.
#[track_caller]
fn safe_call<T, F>(f: F) -> Result<Option<T>>
where
    F: FnOnce() -> Result<Option<T>>, // Result<T> is result::Result<T, anyhow::Error>
{
    match f() {
        ok @ Ok(_) => ok,
        Err(e) => {
            Logger::error(format!("Error: @{}: {}", Location::caller(), e));
            Ok(None)
        }
    }
}

/// Connect to an existing server or create a server subprocess and then connect to it.
#[defun_safe]
#[defun]
fn connect(
    env: &Env, root_uri: String, lsp_type: String, cmd: String, cmd_args: String, initialize_req_str: String,
    timeout: i32, emacs_envs: String,
) -> Result<Option<String>> {
    let prj_name_type = format!("{}({})", root_uri, lsp_type);
    Logger::info(format!("Creating and initializing LSP server for {}", prj_name_type));

    let mut projects = projects().lock().unwrap();

    if let Some(p) = projects.get(&root_uri) {
        if let Some(s) = p.servers.get(&lsp_type) {
            Logger::info(format!("Using existing LSP server {}", prj_name_type));
            return Ok(Some(serde_json::to_string(&s.server_info).context("failed to serialize server info")?));
        }
    }

    let mut server = LspServer::new(&cmd, &cmd_args, &emacs_envs)
        .with_context(|| format!("Failed to create LSP server for {}, <{} {}>", prj_name_type, cmd, cmd_args))?;

    Logger::debug(format!("raw initialize request {:#?}", initialize_req_str));
    let req = Message::from_str_typed::<Request>(&initialize_req_str).context("initialize")?;
    initialize(env, &mut server, req, Duration::from_secs(timeout.max(0) as u64))
        .inspect_err(|_| {
            let _ = server.shutdown(Duration::ZERO); // forcibly kill server and join threads
        })
        .with_context(|| format!("Failed to initialize LSP server for {}", prj_name_type))?;

    let server_info = server.server_info.clone();

    let project = projects.entry(root_uri.clone()).or_insert_with(|| Project::new(root_uri));
    project.servers.insert(lsp_type, server);

    Logger::info(format!("Connected to server successfully. server capabilities {}", &server_info.capabilities));
    Ok(Some(serde_json::to_string(&server_info)?))
}

pub fn initialize(env: &Env, server: &mut LspServer, req: Request, timeout: Duration) -> Result<()> {
    Logger::info(format!("initialize request {}", &req));
    _request_async(server, req)?;

    let start_time = Instant::now();
    loop {
        if let Some(response) = server.read_response() {
            if let Some(error) = response.error {
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
                server.server_info.version = si.version.unwrap_or_default();
            }
            server.status = SERVER_STATUS_RUNNING;

            return Ok(());
        }

        if !timeout.is_zero() && start_time.elapsed() > timeout {
            bail!("Timeout while initializing LSP server");
        }

        thread::sleep(POLL_INTERVAL);
    }
}

/// Shuts down LSP server:
/// 1. Sends the shutdown request to the server and waits for a matching response.
/// 2. Upon receiving the response, sends an 'exit' notification and tries to gracefully shut down the server.
///    If server fails to gracefully shut itself down within the specified timeout it would be forcibly closed.
/// 3. If the server does not respond within the specified timeout, escalates to a forced shutdown.
///
/// Note that this function will run in a background thread
///
/// # Arguments
/// * `server` - The LspServer instance to shut down.
/// * `req` - The shutdown request to send to the server.
pub fn shutdown_server(mut server: LspServer, req: Request) -> Result<Option<ExitStatus>> {
    let name_id = server.name_id();
    Logger::info(format!("request to shutdown {}", name_id));

    let req_id = req.id.clone();
    Logger::debug(format!("sent shutdown request for {}: {:?}", name_id, req));
    let _ = _request_async(&mut server, req);

    let start_time = Instant::now();
    let shutdown_timeout = GRACEFUL_SHUTDOWN_TIMEOUT;

    loop {
        match server.read_response() {
            Some(resp) if resp.id == req_id => {
                let exit = Notification::new("exit", json!({}))?;
                let _ = server.write(exit);
                return server.shutdown(shutdown_timeout); // Graceful + forced, if needed
            }
            Some(_) => continue, // Ignore responses with non-matched id
            None => {
                thread::sleep(POLL_INTERVAL);
            }
        }

        if start_time.elapsed() > shutdown_timeout {
            Logger::info(format!("Graceful termination for {} timed out. Forcing shutdown", name_id));
            return server.shutdown(Duration::ZERO); // Forced
        }
    }
}

// Spans a thread to shut down the server to avoid blocking emacs while performing LSP shutdown sequence.
#[defun_safe]
#[defun]
fn shutdown(env: &Env, root_uri: String, file_type: String, request: String) -> Result<Option<bool>> {
    with_project(env, &root_uri, None, |project| match project.servers.remove(&file_type) {
        Some(server) => {
            let msg = Message::from_str_typed::<Request>(&request).context("shutdown")?;
            std::thread::spawn(move || shutdown_server(server, msg));
            Ok(Some(true))
        }
        None => {
            env_message_and_bail!(env, "No {} server found in project '{}'", file_type, root_uri);
        }
    })
}

#[defun_safe]
#[defun]
fn server(env: &Env, root_uri: String, file_type: String) -> Result<Option<String>> {
    with_project(env, &root_uri, None, |project| match project.servers.get(&file_type) {
        Some(server) => {
            Ok(Some(serde_json::to_string(&server.server_info).context("failed to serialize server info")?))
        }
        None => {
            env_message_and_bail!(env, "No {} server found in project '{}'", file_type, root_uri)
        }
    })
}

fn _request_async(server: &mut LspServer, req: Request) -> Result<Option<bool>> {
    let request_tick = req.request_tick.as_ref().context("no request_tick in request")?;
    server.update_request_info(req.id.clone(), request_tick.clone());

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
fn request_async(env: &Env, root_uri: String, file_type: String, json: String) -> Result<Option<bool>> {
    with_server(env, &root_uri, &file_type, |server| {
        Logger::trace(format!("request {}", &json));
        let msg = Message::from_str_typed::<Request>(&json).context("request_async")?;
        _request_async(server, msg)
    })
}

#[defun_safe]
#[defun]
fn notify(env: &Env, root_uri: String, file_type: String, json: String) -> Result<Option<bool>> {
    with_server(env, &root_uri, &file_type, |server| {
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
) -> Result<Option<String>> {
    with_server(env, &root_uri, &file_type, |server| {
        Ok(server.read_response_exact(RequestId::from(id), method).map(|r| r.into_string()))
    })
}

#[defun_safe]
#[defun]
fn read_notification(env: &Env, root_uri: String, file_type: String) -> Result<Option<String>> {
    with_server(env, &root_uri, &file_type, |server| Ok(server.read_notification().map(|r| r.into_string())))
}

#[defun_safe]
#[defun]
fn read_file_diagnostics(env: &Env, root_uri: String, file_type: String, uri: String) -> Result<Option<String>> {
    with_server(env, &root_uri, &file_type, |server| {
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
fn read_latest_response_id(env: &Env, root_uri: String, file_type: String) -> Result<Option<String>> {
    with_server(env, &root_uri, &file_type, |server| Ok(Some(server.get_latest_response_id().to_string())))
}

#[defun_safe]
#[defun]
fn read_latest_response_tick(env: &Env, root_uri: String, file_type: String) -> Result<Option<String>> {
    with_server(env, &root_uri, &file_type, |server| Ok(Some(server.get_latest_response_tick())))
}
