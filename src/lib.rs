#![allow(unused)]

mod bounded_queue;
mod bufext;
mod connection;
mod env;
mod errors;
pub mod logger;
mod lsp_server;
mod msg;
mod safe_call;
mod socket;
mod stdio;
mod utils;

// for both lib and integrations tests
#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

use anyhow::{anyhow, bail, Context};

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
use lsp_server::{AtomicServerStatus, LspServerData, LspServerInfo, Resources, ServerStatus};
pub use lsp_server::{LspServer, ResourceState};
use lspce_macros::defun_safe;
pub use msg::{Message, Notification, Request, RequestId, Response};
use safe_call::safe_call;
use stdio::IoThreads;
pub use stdio::ThreadResult;
use utils::*;

const MAX_NOTIFICATIONS: usize = 5;
const MAX_NONTICKED_RESPONSES: usize = 5; // responses for which ticks are not expected, e.g. not via lspce API
const MAX_TICKED_RESPONSES: usize = 50; // responses for requests sent via lspce API
const KILL_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const GRACEFUL_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(3);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

static MAX_DIAGNOSTICS: AtomicI32 = AtomicI32::new(30);

const REAPER_INTERVAL: Duration = Duration::from_secs(5);

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
    MAX_DIAGNOSTICS.store(count, Ordering::Relaxed);
    env.lspce_message(format!("Set max diagnostics count to {}", count))
}

#[defun]
fn read_max_diagnostics_count(env: &Env) -> EmacsResult<i32> {
    let count = MAX_DIAGNOSTICS.load(Ordering::Relaxed);
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
        None => Err(anyhow!("No project found for '{}'", root_uri).context(UserFacing)),
    }
}

fn with_server<F, T>(root_uri: &str, file_type: &str, require_running: bool, f: F) -> EmacsResult<Option<T>>
where
    F: FnOnce(&mut LspServer) -> EmacsResult<Option<T>>,
{
    with_project(root_uri, |project| match project.servers.get_mut(file_type) {
        Some(Some(server)) => {
            if require_running && server.status() != ServerStatus::Running {
                Err(anyhow!("LSP server for {}({}) is not ready", root_uri, file_type).context(UserFacing))
            } else {
                f(server)
            }
        }
        _ => Err(anyhow!("No LSP server for {}({})", root_uri, file_type).context(UserFacing)),
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

    Logger::debug(format!("API: Initialize Request {:#?}", initialize_req_str));
    let req = Message::from_str_typed::<Request>(&initialize_req_str)?;

    let res = server.initialize(req, Duration::from_secs(timeout.max(0) as u64));
    if let Err(ref e) = res {
        let _ = server.teardown(Duration::ZERO); // forcibly kill server and join threads
        res.with_context(|| format!("Failed to initialize LSP server for {}", prj_name_type))?;
    }

    let server_info = server.server_info.clone();

    with_projects_mut(|projects| {
        projects.add_server(root_uri, lsp_type, server);
    });

    Logger::info(format!("Connected to server successfully. server capabilities {}", &server_info.to_json_string()?));
    Ok(Some(server_info.to_json_string()?))
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

#[defun_safe]
#[defun]
fn request_async(env: &Env, root_uri: String, file_type: String, json: String) -> EmacsResult<Option<bool>> {
    with_server(&root_uri, &file_type, true, |server| {
        Logger::trace(format!("API: Request {}", &json));
        let msg = Message::from_str_typed::<Request>(&json)?;
        let _ = server.send_message(msg)?;
        Ok(Some(true))
    })
}

#[defun_safe]
#[defun]
fn notify(env: &Env, root_uri: String, file_type: String, json: String) -> EmacsResult<Option<bool>> {
    with_server(&root_uri, &file_type, true, |server| {
        Logger::trace(format!("API: Notify {}", &json));
        let msg = Message::from_str_typed::<Notification>(&json)?;
        let _ = server.send_message(msg)?;
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
    with_server(&root_uri, &file_type, true, |server| Ok(server.read_last_notification().map(|n| n.into_string())))
}

#[defun_safe]
#[defun]
fn read_file_diagnostics(env: &Env, root_uri: String, file_type: String, uri: String) -> EmacsResult<Option<String>> {
    with_server(&root_uri, &file_type, true, |server| {
        return Ok(match server.server_data.lock().unwrap().file_infos.get(&uri) {
            Some(file_info) => Some(serde_json::to_string(&file_info.diagnostics).context("Serialize diagnostics")?),
            None => None,
        });
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
