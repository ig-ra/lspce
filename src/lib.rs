#![allow(unused)]

mod connection;
mod error;
mod logger;
mod msg;
mod socket;
mod stdio;

use connection::Connection;
use emacs::{defun, Env, IntoLisp, Result, Value};
use error::LspceError;
use logger::{Logger, LOG_DEBUG, LOG_DISABLED, LOG_FILE_NAME, LOG_LEVEL};

use lsp_types::{
    Diagnostic, DidChangeTextDocumentParams, InitializeResult, InitializedParams, PublishDiagnosticsParams,
};
use msg::{Message, Notification, Request, RequestId, Response};
use once_cell::sync::Lazy;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;

use std::panic::Location;
use std::result::Result as RustResult;

use std::sync::atomic::AtomicI32;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering;
use std::time::Instant;
use stdio::IoThreads;

use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    io::{Read, Write},
    process::{Child, Command, Stdio},
    sync::{Arc, Mutex},
    thread::{self, JoinHandle, Thread},
};

pub static MAX_DIAGNOSTICS_COUNT: AtomicI32 = AtomicI32::new(30);

#[derive(Debug)]
struct FileInfo {
    pub uri: String, // file name
    pub diagnostics: Vec<Diagnostic>,
}

impl FileInfo {
    pub fn new(uri: String) -> FileInfo {
        FileInfo { uri, diagnostics: Vec::new() }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct LspServerInfo {
    pub name: String,
    pub version: String,
    pub id: String, // server_id at the moment
    pub capabilities: String,
}

impl LspServerInfo {
    pub fn new() -> LspServerInfo {
        LspServerInfo {
            name: "".to_string(),
            version: "".to_string(),
            id: "".to_string(),
            capabilities: "".to_string(),
        }
    }
}

const SERVER_STATUS_NEW: u8 = 0;
const SERVER_STATUS_STARTING: u8 = 1;
const SERVER_STATUS_RUNNING: u8 = 2;
const SERVER_STATUS_EXITING: u8 = 3;

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
struct LspServer {
    pub child: Option<Child>,
    pub server_info: LspServerInfo,
    pub status: u8,
    transport: Arc<Mutex<Option<Connection>>>,
    transport_threads: Option<IoThreads>,
    dispatcher: Option<thread::JoinHandle<()>>,
    server_data: Arc<Mutex<LspServerData>>,
    exit: Arc<Mutex<bool>>,
}

impl LspServer {
    pub fn new(
        root_uri: String, cmd: String, cmd_args: String, initialize_req: String, emacs_envs: String,
    ) -> Option<LspServer> {
        let args = cmd_args.split_ascii_whitespace().collect::<Vec<&str>>();

        Logger::info(&format!("emacs_envs: {}", &emacs_envs));

        let mut child;
        if !emacs_envs.is_empty() {
            let envs = parse_json::<HashMap<String, String>>(&emacs_envs);
            match envs {
                Some(envs) => {
                    child = Command::new(cmd)
                        .args(args)
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped())
                        .envs(envs)
                        .spawn();
                }
                None => {
                    return None;
                }
            }
        } else {
            child = Command::new(cmd)
                .args(args)
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn();
        }

        if let Ok(mut c) = child {
            let mut stdin = c.stdin.take().unwrap();
            let mut stdout = c.stdout.take().unwrap();
            let mut stderr = c.stderr.take().unwrap();

            let (mut transport, mut transport_threads) = Connection::stdio(stdin, stdout, stderr);

            let mut server_info = LspServerInfo::new();
            server_info.id = c.id().to_string();
            let mut server = LspServer {
                child: Some(c),
                server_info,
                status: SERVER_STATUS_STARTING,
                transport: Arc::new(Mutex::new(Some(transport))),
                transport_threads: Some(transport_threads),
                dispatcher: None,
                server_data: Arc::new(Mutex::new(LspServerData::new())),
                exit: Arc::new(Mutex::new(false)),
            };
            server.dispatcher = Some(LspServer::start_dispatcher(
                Arc::clone(&server.transport),
                Arc::clone(&server.exit),
                Arc::clone(&server.server_data),
            ));

            Some(server)
        } else {
            Logger::error(&format!("create child process failed with error {:?}", child.err().unwrap(),));
            None
        }
    }

    fn start_dispatcher(
        transport: Arc<Mutex<Option<Connection>>>, exit: Arc<Mutex<bool>>, server_data: Arc<Mutex<LspServerData>>,
    ) -> thread::JoinHandle<()> {
        let handle = thread::spawn(move || loop {
            {
                let exit = exit.lock().unwrap();
                if *exit {
                    break;
                }
            }

            let mut message: Option<Message> = None;
            {
                let transport = transport.lock().unwrap();
                message = transport.as_ref().unwrap().read();
            }

            if let Some(m) = message {
                match m {
                    Message::Request(r) => {
                        if r.method.eq("workspace/configuration") {
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
                        Logger::trace(&format!("Response {}", &r.content));
                        let id = r.id.clone();

                        let mut server_data = server_data.lock().unwrap();
                        let request_tick = server_data.request_ticks.get(&id);
                        if (request_tick.is_some()) {
                            let request_tick = request_tick.unwrap().clone();
                            Logger::debug(&format!("Request tick for id {} is {}", &id, &request_tick));
                            if (request_tick.eq(&server_data.latest_request_tick)) {
                                r.request_tick = request_tick.clone();
                                server_data.responses.push_back(r);
                            }
                            Logger::debug(&format!(
                                "Latest response id is {}, current response id {}",
                                &server_data.latest_response_id, &id
                            ));
                            if server_data.latest_response_id.lt(&id) {
                                server_data.latest_response_id = id.clone();
                                server_data.latest_response_tick = request_tick.clone();
                                Logger::debug(&format!(
                                    "Change Latest response tick for id {} to {}",
                                    &server_data.latest_response_id, &request_tick
                                ));
                            }

                            server_data.request_ticks.remove(&id);
                        } else {
                            Logger::trace(&format!("No request tick for id {}", id));
                            // if server_data.latest_response_id.lt(&id) {
                            //     server_data.latest_response_id = id.clone();
                            // }
                        }
                    }
                    Message::Notification(r) => {
                        // cacha diagnostics so they won't pour into Emacs
                        if r.method.eq("textDocument/publishDiagnostics") {
                            let mut params = serde_json::from_value::<PublishDiagnosticsParams>(r.params).unwrap();

                            let uri = params.uri.as_str().to_string();
                            let mut file_info = FileInfo::new(uri.clone());
                            // cache no more than MAX_DIAGNOSTICS_COUNT diagnostics
                            let max_diagnostic_count = MAX_DIAGNOSTICS_COUNT.load(Ordering::Relaxed);
                            if max_diagnostic_count < 0 {
                                file_info.diagnostics = params.diagnostics;
                            } else if params.diagnostics.len() > max_diagnostic_count as usize {
                                params.diagnostics.truncate(max_diagnostic_count as usize);
                                file_info.diagnostics = params.diagnostics;
                            } else {
                                file_info.diagnostics = params.diagnostics;
                            }

                            let mut server_data = server_data.lock().unwrap();
                            server_data.file_infos.insert(uri.clone(), file_info);
                        } else {
                            // other notifications
                            let mut server_data = server_data.lock().unwrap();
                            if server_data.notifications.len() > 10 {
                                server_data.notifications.pop_front();
                            }
                            server_data.notifications.push_back(r);
                        }
                    }
                }
            } else {
                thread::sleep(std::time::Duration::from_millis(1));
            }
        });

        handle
    }

    pub fn stop_dispatcher(&mut self) {
        let mut exit = self.exit.lock().unwrap();
        *exit = true;
    }

    pub fn kill_child(&mut self) {
        if self.child.is_some() {
            self.child.take().unwrap().kill();
            self.child = None;
        }
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

    pub fn exit_transport(&self) {
        let transport = self.transport.lock().unwrap();
        if transport.is_some() {
            transport.as_ref().unwrap().to_exit();
        }
    }

    pub fn write(&self, request: Message) -> RustResult<(), LspceError> {
        let transport = self.transport.lock().unwrap();
        if transport.is_some() {
            let result = transport.as_ref().unwrap().write(request);
            match result {
                Ok(_) => Ok(()),
                Err(e) => Err(LspceError(e.to_string())),
            }
        } else {
            Err(LspceError("transport is not established.".to_string()))
        }
    }

    pub fn read_response(&self) -> Option<Response> {
        let mut server_data = self.server_data.lock().unwrap();
        server_data.responses.pop_front()
    }

    //
    pub fn read_response_exact(&self, id: RequestId, method: String) -> Option<Response> {
        let mut result: Option<Response> = None;
        let mut server_data = self.server_data.lock().unwrap();

        let latest_request_tick = server_data.latest_request_tick.clone();
        let mut reserved: VecDeque<Response> = VecDeque::new();
        for iter in server_data.responses.iter() {
            Logger::debug(&format!("read_response_exact response {:#?}", &iter));
            if iter.id.eq(&id) {
                result = Some(iter.clone());
            }

            let request_tick = iter.request_tick.clone();
            if request_tick.eq(&latest_request_tick) && iter.id.ne(&id) {
                reserved.push_back(iter.clone());
            }
        }

        server_data.responses.clear();
        server_data.responses.append(&mut reserved);

        if result.is_none() {
            Logger::trace(&format!("read_response_exact get null for request_id {}, method {}", id, method));
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

    pub fn clear_diagnostics(&self, uri: &str) {
        let mut server_data = self.server_data.lock().unwrap();

        if let Some(mut file_info) = server_data.file_infos.get_mut(uri) {
            let result = serde_json::to_string(&file_info.diagnostics);
            file_info.diagnostics = Vec::new();
        }
    }

    fn shutdown(&mut self, force: bool) {
        let server_name = self.server_info.name.clone();
        let server_id = self.server_info.id.clone();

        self.stop_dispatcher();
        self.exit_transport();
        Logger::debug(&format!("after exit transport for server {}, server_id {}.", &server_name, &server_id));

        if force {
            // Forced cleanup - kill the child process
            self.kill_child();
            Logger::info(&format!("forcefully terminated server {}, server_id {}", &server_name, &server_id));
        } else {
            // Graceful cleanup
            if let Some(threads) = self.transport_threads.take() {
                threads.join();
                Logger::info(&format!("after thread join for server {}, server_id {}.", &server_name, &server_id));
            }

            if let Some(mut child) = self.child.take() {
                let _ = child.wait();
                Logger::info(&format!("after child wait for server {}, server_id {}.", &server_name, &server_id));
            }
        }
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
static PROJECTS: Lazy<Arc<Mutex<HashMap<String, Project>>>> = Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

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
    LOG_LEVEL.store(LOG_DISABLED, Ordering::Relaxed);
    env.lspce_message("Logging is disabled")
}

/// enable logging to /tmp/lspce.log
#[defun]
fn enable_logging(env: &Env) -> Result<Value<'_>> {
    LOG_LEVEL.store(LOG_DEBUG, Ordering::Relaxed);
    env.lspce_message("Logging is enabled")
}

#[defun]
fn set_log_level(env: &Env, level: u8) -> Result<Value<'_>> {
    LOG_LEVEL.store(level, Ordering::Relaxed);
    env.lspce_message(format!("Set log level to {}", level))
}

#[defun]
fn get_log_level(env: &Env) -> Result<u8> {
    let log_level = LOG_LEVEL.load(Ordering::Relaxed);
    return Ok(log_level);
}

/// set logging file name
#[defun]
fn set_log_file(env: &Env, file: String) -> Result<Value<'_>> {
    *LOG_FILE_NAME.lock().unwrap() = file.clone();
    env.lspce_message(format!("Set logging file to {}", file))
}

#[track_caller]
fn with_project<F, T>(env: &Env, root_uri: &str, caller_loc: Option<&Location<'static>>, f: F) -> Result<Option<T>>
where
    F: FnOnce(&mut Project) -> Result<Option<T>>,
{
    let mut projects_guard = projects().lock().unwrap();
    let caller = caller_loc.unwrap_or_else(|| Location::caller());

    match projects_guard.get_mut(root_uri) {
        Some(project) => f(project),
        None => {
            env.lspce_message(&format!("No project found for '{}'. @{}", root_uri, caller));
            Ok(None)
        }
    }
}

#[track_caller]
fn with_server<F, T>(env: &Env, root_uri: &str, file_type: &str, f: F) -> Result<Option<T>>
where
    F: FnOnce(&mut LspServer) -> Result<Option<T>>,
{
    let caller = Location::caller();
    with_project(env, root_uri, Some(&caller), |project| match project.servers.get_mut(file_type) {
        Some(server) => {
            if server.status != SERVER_STATUS_RUNNING {
                env.lspce_message("Server is not ready");
                return Ok(None);
            }
            f(server)
        }
        None => {
            env.lspce_message(&format!("No server for {}. @{}", file_type, Location::caller()));
            Ok(None)
        }
    })
}

fn find_and_remove_server(env: &Env, root_uri: &str, file_type: &str) -> Result<Option<LspServer>> {
    with_project(env, root_uri, None, |project| {
        let server = project.servers.remove(file_type);
        if server.is_none() {
            env.lspce_message(&format!("No {} server found in project '{}'", file_type, root_uri));
        }
        Ok(server)
    })
}

/// Connect to an existing server or create a server subprocess and then connect to it.
#[defun]
fn connect(
    env: &Env, root_uri: String, lsp_type: String, cmd: String, cmd_args: String, initialize_req: String, timeout: i32,
    emacs_envs: String,
) -> Result<Option<String>> {
    Logger::info(&format!("start initializing server for lsp_type {} in project {}", lsp_type, root_uri));

    let mut projects = projects().lock().unwrap();

    if let Some(p) = projects.get(&root_uri) {
        if let Some(s) = p.servers.get(&lsp_type) {
            Logger::info(&format!("server created already for lsp_type {} in project {}", lsp_type, root_uri));

            return Ok(Some(serde_json::to_string(&s.server_info).unwrap()));
        }
    }

    let mut server =
        LspServer::new(root_uri.clone(), cmd.clone(), cmd_args.clone(), initialize_req.clone(), emacs_envs.clone());
    if let Some(mut s) = server {
        let server_info: LspServerInfo;

        let mut project = projects.get_mut(&root_uri);
        if let Some(p) = project.as_mut() {
            if initialize(env, root_uri.clone(), &mut s, initialize_req, timeout) {
                server_info = s.server_info.clone();
                p.servers.insert(lsp_type, s);
            } else {
                s.kill_child();
                return Ok(None);
            }
        } else {
            let mut proj = Project::new(root_uri.clone());
            if initialize(env, root_uri.clone(), &mut s, initialize_req, timeout) {
                server_info = s.server_info.clone();
                proj.servers.insert(lsp_type, s);

                projects.insert(root_uri.clone(), proj);
            } else {
                s.kill_child();
                return Ok(None);
            }
        }

        Logger::info(&format!("Connected to server successfully. server capabilities {}", &server_info.capabilities));
        Ok(Some(serde_json::to_string(&server_info).unwrap()))
    } else {
        Logger::error(&format!(
            "Failed to connect to server {}, {} for project {} {}.",
            cmd.clone(),
            cmd_args.clone(),
            root_uri.clone(),
            lsp_type.clone()
        ));
        Ok(None)
    }
}

macro_rules! unwrap_or_return {
    ($expr:expr, $ret:expr) => {
        match $expr {
            Some(val) => val,
            None => return $ret,
        }
    };
}

fn parse_json<T>(json_str: &str) -> Option<T>
where
    T: DeserializeOwned,
{
    match serde_json::from_str::<T>(json_str) {
        Ok(value) => Some(value),
        Err(e) => {
            let type_name = std::any::type_name::<T>();
            Logger::error(&format!("Failed to parse JSON as {}: {}", type_name, e));
            None
        }
    }
}

fn initialize(env: &Env, root_uri: String, server: &mut LspServer, req_str: String, timeout: i32) -> bool {
    Logger::debug(&format!("raw initialize request {:#?}", req_str));

    let msg = unwrap_or_return!(parse_json::<Request>(&req_str), false);
    let id = msg.id.clone();

    Logger::info(&format!("initialize request {}", serde_json::to_string_pretty(&msg).unwrap()));

    if !_request_async(server, msg) {
        return false;
    }

    let start_time = Instant::now();
    loop {
        let response = server.read_response();
        match response {
            Some(m) => {
                if m.error.is_some() {
                    Logger::error(&format!("Lsp error {:?}", m.error));
                    return false;
                }
                Logger::info(&format!("initialize response {}", serde_json::to_string_pretty(&m).unwrap()));

                if let Ok(ir) = serde_json::from_value::<InitializeResult>(m.result.unwrap()) {
                    let initialized = Notification::new(
                        "initialized".to_string(),
                        serde_json::to_value(InitializedParams {}).unwrap(),
                    );

                    _notify(server, initialized);

                    server.status = SERVER_STATUS_RUNNING;

                    if let Some(si) = ir.server_info {
                        server.server_info.name = si.name.clone();
                        server.server_info.version = si.version.expect("");
                    }
                    server.server_info.capabilities = serde_json::to_string(&ir.capabilities).unwrap();

                    return true;
                } else {
                    return false;
                }
            }
            None => {
                thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        if timeout > 0 && Instant::now().duration_since(start_time).as_millis() > timeout as u128 * 1000 {
            Logger::error("timeout when initializing server.");
            return false;
        }
    }
}

fn shutdown_server(mut server: LspServer, req: Request) {
    Logger::info(&format!(
        "start to shut down server {}, server_id {}",
        &server.server_info.name, &server.server_info.id
    ));

    let req_id = req.id.clone();

    if !_request_async(&mut server, req) {
        return;
    }

    let start_time = Instant::now();
    loop {
        match server.read_response() {
            Some(resp) if resp.id.eq(&req_id) => {
                let exit = Notification::new("exit".to_string(), json!({}));
                _notify(&mut server, exit);
                server.shutdown(false); // Graceful
                return;
            }
            Some(_) => continue, // Ignore responses with non-matched id
            None => {
                thread::sleep(std::time::Duration::from_millis(10));
            }
        }

        if Instant::now().duration_since(start_time).as_millis() > 3 * 1000 {
            server.shutdown(true); // Forced
            return;
        }
    }
}

#[defun]
fn shutdown(env: &Env, root_uri: String, file_type: String, request: String) -> Result<Option<bool>> {
    let server = match find_and_remove_server(env, &root_uri, &file_type)? {
        Some(server) => server,
        None => return Ok(None),
    };

    let req = unwrap_or_return!(parse_json::<Request>(&request), Ok(Some(false)));
    thread::spawn(move || shutdown_server(server, req));
    Ok(Some(true))
}

#[defun]
fn server(env: &Env, root_uri: String, file_type: String) -> Result<Option<String>> {
    with_project(env, &root_uri, None, |project| {
        Ok(project.servers.get(&file_type).map(|s| serde_json::to_string(&s.server_info).unwrap()))
    })
}

fn _request_async(server: &mut LspServer, req: Request) -> bool {
    let request_tick = match &req.request_tick {
        Some(tick) => tick.clone(),
        None => {
            Logger::error(&format!("no request_tick in req {:?}", &req));
            return false;
        }
    };

    server.update_request_info(req.id.clone(), request_tick);

    if req.method == "textDocument/didChange" || req.method == "textDocument/didClose" {
        let param = serde_json::from_value::<DidChangeTextDocumentParams>(req.params.clone());
        if let Ok(param) = param {
            server.clear_diagnostics(param.text_document.uri.as_ref());
        }
    }

    match server.write(Message::Request(req)) {
        Ok(_) => true,
        Err(e) => {
            Logger::error(&format!("request error {:#?}", e));
            false
        }
    }
}

#[defun]
fn request_async(env: &Env, root_uri: String, file_type: String, req: String) -> Result<Option<bool>> {
    with_server(env, &root_uri, &file_type, |server| {
        Logger::trace(&format!("request {}", &req));
        let msg = unwrap_or_return!(parse_json::<Request>(&req), Ok(None));
        Ok(_request_async(server, msg).then_some(true))
    })
}

fn _notify(server: &mut LspServer, req: Notification) -> Result<Option<bool>> {
    match server.write(Message::Notification(req)) {
        Ok(_) => Ok(Some(true)),
        Err(e) => {
            Logger::error(&format!("notify error {:#?}", e));
            Ok(None)
        }
    }
}

#[defun]
fn notify(env: &Env, root_uri: String, file_type: String, req: String) -> Result<Option<bool>> {
    with_server(env, &root_uri, &file_type, |server| {
        Logger::trace(&format!("notify {}", &req));
        let n = unwrap_or_return!(parse_json::<Notification>(&req), Ok(None));
        return _notify(server, n);
    })
}

/// precondition: have called read_latest_response_id and gotten the id.
#[defun]
fn read_response_exact(
    env: &Env, root_uri: String, file_type: String, id: String, method: String,
) -> Result<Option<String>> {
    with_server(env, &root_uri, &file_type, |server| {
        Ok(server.read_response_exact(RequestId::from(id), method).map(|r| r.content))
    })
}

#[defun]
fn read_notification(env: &Env, root_uri: String, file_type: String) -> Result<Option<String>> {
    with_server(env, &root_uri, &file_type, |server| Ok(server.read_notification().map(|r| r.content)))
}

#[defun]
fn read_file_diagnostics(env: &Env, root_uri: String, file_type: String, uri: String) -> Result<Option<String>> {
    with_server(env, &root_uri, &file_type, |server| {
        let mut server_data = server.server_data.lock().unwrap();
        match server_data.file_infos.get(&uri) {
            Some(file_info) => match serde_json::to_string(&file_info.diagnostics) {
                Ok(result) => Ok(Some(result)),
                Err(e) => {
                    Logger::error(&format!("Failed to serialize diagnostics for {}: {}", &uri, e));
                    Ok(None)
                }
            },
            None => Ok(None),
        }
    })
}

#[defun]
fn read_latest_response_id(env: &Env, root_uri: String, file_type: String) -> Result<Option<String>> {
    with_server(env, &root_uri, &file_type, |server| Ok(Some(server.get_latest_response_id().to_string())))
}

#[defun]
fn read_latest_response_tick(env: &Env, root_uri: String, file_type: String) -> Result<Option<String>> {
    with_server(env, &root_uri, &file_type, |server| Ok(Some(server.get_latest_response_tick())))
}
