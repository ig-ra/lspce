#![allow(unused)]

mod bufext;
mod connection;
pub mod logger;
mod msg;
mod socket;
mod stdio;
mod utils;

#[cfg(test)]
mod tests;

use anyhow::{anyhow, bail, Context};
use crossbeam_channel::{Receiver, Sender};
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

use crate::utils::*;
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
const SERVER_STATUS_SHUTTTING_DOWN: u8 = 3;
const SERVER_STATUS_EXITING: u8 = 4;

const POLL_INTERVAL: Duration = Duration::from_millis(10);
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const KILL_WAIT_TIMEOUT: Duration = GRACEFUL_SHUTDOWN_TIMEOUT;
const MAX_NOTIFICATIONS: usize = 10;
const DISPATCHER_SLEEP: Duration = Duration::from_millis(1);
const REAPER_INTERVAL: Duration = Duration::from_secs(5);

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
    sender: Sender<Message>,
    transport_threads: Option<IoThreads>,
    dispatcher: Option<thread::JoinHandle<()>>,
    server_data: Arc<Mutex<LspServerData>>,
    exit: Arc<AtomicBool>,
    name_id: String,
}

fn name_id(name: &str, id: &str) -> String {
    format!("<{}:[{}]>", name, id)
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

        let exit = Arc::new(AtomicBool::new(false));
        let (sender, receiver, mut transport_threads) =
            crate::connection::stdio(stdin, stdout, stderr, Arc::clone(&exit));

        let mut server_info = LspServerInfo::new(child.id());
        if let Some(name) = std::path::Path::new(cmd).file_name().and_then(|n| n.to_str()) {
            server_info.name = name.to_string();
        }

        let mut server = LspServer {
            child: Some(child),
            name_id: name_id(&server_info.name, &server_info.id),
            server_info: server_info,
            status: SERVER_STATUS_STARTING,
            sender,
            transport_threads: Some(transport_threads),
            dispatcher: None,
            server_data: Arc::new(Mutex::new(LspServerData::new())),
            exit: exit,
        };

        server.dispatcher =
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
    /// * `true` if the protocol handshake completed successfully within the timeout.
    /// * `false` if the handshake failed or timed out.
    pub fn shutdown(&mut self, req: Request, timeout: Duration) -> bool {
        self.status = SERVER_STATUS_SHUTTTING_DOWN;
        Logger::info(format!("Starting shutdown protocol for {}.", self.name_id));
        let req_id = req.id.clone();

        if self.write(req).is_ok() {
            let start_time = Instant::now();
            while start_time.elapsed() <= timeout {
                if matches!(self.read_response(), Some(ref resp) if resp.id == req_id) {
                    let _ = self.write(Notification::new_exit());
                    return true; // graceful shutdown prococol completed sucessfully
                }
                thread::sleep(POLL_INTERVAL);
            }
        }
        Logger::info(format!("Shutdown protocol failed for <{}>", self.name_id));
        false // Timed out waiting for response
    }

    fn start_dispatcher(
        receiver: Receiver<Message>, exit: Arc<AtomicBool>, server_data: Arc<Mutex<LspServerData>>,
    ) -> thread::JoinHandle<()> {
        let handle = thread::spawn(move || loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }

            let mut message: Option<Message> = None;
            match receiver.recv_timeout(Duration::from_millis(1)) {
                Ok(msg) => message = Some(msg),
                Err(_) => {}
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
                        // cache diagnostics so they won't pour into Emacs
                        if r.method == "textDocument/publishDiagnostics" {
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
        self.sender.send(msg.into()).context("failed to write to transport")
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

    pub fn stop_dispatcher(&mut self) {
        self.exit.store(true, Ordering::Relaxed);
    }

    pub fn exit_transport(&self) {
        // FIXME: self.sender.close(); // signal by closing channel?
        self.exit.store(true, Ordering::Relaxed);
    }

    pub fn kill_child(&mut self) -> Result<Option<ExitStatus>> {
        if let Some(mut child) = self.child.take() {
            return kill_child_and_wait_with_timeout(&mut child, KILL_WAIT_TIMEOUT, &self.name_id);
        }
        Ok(None)
    }

    /// joins dispatcher and transport threads.
    pub fn join_threads(&mut self) {
        if let Some(threads) = self.transport_threads.take() {
            if let Err(e) = threads.join() {
                Logger::error(format!("error joining transport threads for {}: {}", self.name_id, e));
            }
        }

        if let Some(handle) = self.dispatcher.take() {
            if let Err(e) = handle.join() {
                Logger::error(format!("error joining dispatcher thread for {}: {:?}", self.name_id, e));
            }
        }
    }

    /// Tears down and cleanup LSP child process and associated threads
    ///
    /// 1. Signal the dispatcher thread and transport threads to stop
    /// 2. Join both dispatcher and transport threads
    /// 3. Attempt to wait for the child LSP process, if provided graceful timeout is non-zero
    /// 4. If the child process does not exit within the provided timeout, or error occured, or
    ///    no graceful timeout provided kill the child the process
    /// 5. Wait (wth a timeout) for child process status to avoid leaving a zombie process.
    ///
    /// # Arguments
    /// * `graceful_timeout` - The maximum duration to wait for child to exit before sending kill signal.
    ///   Use `Duration::ZERO` to skip graceful wait and immediately kill.
    ///
    /// # Returns
    /// * `Ok(Some(status))` if the process exited (gracefully or forcibly) and an exit status is available.
    /// * `Ok(None)` if the process did not exit within the allowed time.
    /// * `Err(e)` if an error occurred during shutdown.
    pub fn teardown(&mut self, graceful_timeout: Duration) -> Result<Option<ExitStatus>> {
        self.status = SERVER_STATUS_EXITING;
        Logger::debug(format!("begin teardown for {}", self.name_id));

        self.stop_dispatcher();
        self.exit_transport();

        let mut status = Ok(None);
        if let Some(mut child) = self.child.take() {
            // Try graceful shutdown first
            if graceful_timeout > Duration::ZERO {
                Logger::debug(format!("gracefully waiting for {}", self.name_id));
                status = wait_child_with_timeout(&mut child, graceful_timeout, &self.name_id);
            }

            // Either graceful shutdown did not succeed or no graceful timeout provided
            if !matches!(status, Ok(Some(_))) {
                self.child = Some(child); // set back the child so kill_child can be used
                status = self.kill_child();
            }
        }

        Logger::debug(format!("joining threads for {}", self.name_id));
        self.join_threads();
        Logger::debug(format!("finished teardown for {}", self.name_id));

        status
    }
}

impl Drop for LspServer {
    fn drop(&mut self) {
        // We could check either handle or status to ensure that teardown wasn't started
        if self.child.is_some() {
            Logger::info(format!("Dropping <{}> and forcing teardown.", self.name_id));
            let _ = self.teardown(Duration::ZERO);
        }
    }
}

struct Project {
    pub root_uri: String, // root URI of the project, e.g. "file:///home/user/project"
    pub servers: HashMap<String, Option<LspServer>>, // map each language_id to a lsp server
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

/// A background thread that periodically checks for and cleans up dead LSP servers.
///
/// A server is considered dead if its dispatcher thread has terminated or its exit
/// flag has been set due to an I/O error.
///
/// This function iterates through all servers and, upon finding a dead one, takes
/// ownership of it. This leaves `None` in its place in the map and immediately
/// triggers the `LspServer::drop` implementation, which ensures resource teardown.
fn reap_dead_servers() {
    loop {
        thread::sleep(REAPER_INTERVAL);
        let mut projects = projects().lock().unwrap();

        for project in projects.values_mut() {
            for server_option in project.servers.values_mut() {
                if let Some(server) = server_option {
                    let exit_requested = server.exit.load(Ordering::Relaxed);
                    let dispatcher_finished = server.dispatcher.as_ref().map_or(true, |h| h.is_finished());

                    if exit_requested || dispatcher_finished {
                        Logger::info(format!("Reaper detected dead server: <{}>. Dropping", server.name_id));
                        // Take ownership, causing server to be dropped, which will trigger teardown logic
                        let _ = server_option.take();
                    }
                }
            }
        }
    }
}

// Emacs won't load the module without this.
#[cfg(not(test))]
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
#[cfg(not(test))]
#[emacs::module(name("lspce-module"))]
fn init(env: &Env) -> Result<Value<'_>> {
    thread::spawn(reap_dead_servers);
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
        Some(Some(server)) => {
            if server.status != SERVER_STATUS_RUNNING {
                env_message_and_bail!(env, @ caller, "LSP server for {}({}) is not ready", root_uri, file_type)
            }
            f(server)
        }
        _ => {
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
        if let Some(Some(s)) = p.servers.get(&lsp_type) {
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
            let _ = server.teardown(Duration::ZERO); // forcibly kill server and join threads
        })
        .with_context(|| format!("Failed to initialize LSP server for {}", prj_name_type))?;

    let server_info = server.server_info.clone();

    let project = projects.entry(root_uri.clone()).or_insert_with(|| Project::new(root_uri));
    project.servers.insert(lsp_type, Some(server));

    Logger::info(format!("Connected to server successfully. server capabilities {}", &server_info.capabilities));
    Ok(Some(serde_json::to_string(&server_info)?))
}

pub fn initialize(env: &Env, server: &mut LspServer, req: Request, timeout: Duration) -> Result<()> {
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

            let ir: InitializeResult =
                serde_json::from_value(response.result.context("Empty initialize response")?.clone())?;

            let initialized = Notification::new("initialized", InitializedParams {})?;
            server.write(initialized)?;

            server.server_info.capabilities = serde_json::to_string(&ir.capabilities)?;
            if let Some(si) = ir.server_info {
                server.server_info.name = si.name;
                server.name_id = name_id(&server.server_info.name, &server.server_info.id);
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

/// Orchestrates the server shutdown sequence. Intended to be run in a background thread.
///
/// This function first attempts a polite, protocol-level shutdown by calling
/// `LspServer::shutdown`. Regardless of the outcome, it then relies on the
/// `LspServer`'s `Drop` implementation to automatically trigger the final
/// resource teardown, ensuring cleanup always occurs.
///
/// # Arguments
/// * `server` - The `LspServer` instance to shut down.
/// * `req` - The `shutdown` request to send to the server.
///
fn shutdown_orchestrator(mut server: LspServer, req: Request) {
    // --- Phase 1: Polite Shutdown via LSP Protocol ---
    let _ = server.shutdown(req, GRACEFUL_SHUTDOWN_TIMEOUT);

    // --- Phase 2: Automatic Teardown via Drop  ---
    // Assumed that this is the last copy of server variable.
    // When this function returns, the `server` variable goes out of scope,
    // `LspServer::drop` will be automatically called, which in turn calls `teardown`.
}

// Spans a thread to shut down the server to avoid blocking emacs while performing LSP shutdown sequence.
#[defun_safe]
#[defun]
fn shutdown(env: &Env, root_uri: String, file_type: String, request: String) -> Result<Option<bool>> {
    with_project(env, &root_uri, None, |project| {
        if let Some(Some(server)) = project.servers.remove(&file_type) {
            let shutdown_req = Message::from_str_typed::<Request>(&request).unwrap_or_else(|e| {
                Logger::info(format!("Failed to parse shutdown request: {}. Using default", e));
                Request::new_shutdown()
            });
            std::thread::spawn(move || shutdown_orchestrator(server, shutdown_req));
            Ok(Some(true))
        } else {
            env_message_and_bail!(env, "No {} server found in project '{}'", file_type, root_uri)
        }
    })
}

#[defun_safe]
#[defun]
fn server(env: &Env, root_uri: String, file_type: String) -> Result<Option<String>> {
    with_project(env, &root_uri, None, |project| match project.servers.get(&file_type) {
        Some(Some(server)) => {
            Ok(Some(serde_json::to_string(&server.server_info).context("failed to serialize server info")?))
        }
        _ => {
            env_message_and_bail!(env, "No {} server found in project '{}'", file_type, root_uri)
        }
    })
}

fn _request_async(server: &mut LspServer, req: Request) -> Result<Option<bool>> {
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
