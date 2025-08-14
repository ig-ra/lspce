#![allow(unused)]

use lspce_module::test_utils::*;
use lspce_module::{logger, shutdown_server, LspServer, Notification, Request, ResourceState, ThreadResult};
use std::{io, time::Duration};

const DUMMY_LSP_CMD: &str = "target/debug/dummy_lsp";
const ONE_SEC: Duration = Duration::from_secs(1);

// handy for manual testing and to see log messages
pub fn setup_test_logger() {
    #[cfg(unix)]
    logger::set_log_file_name("/dev/stderr".to_string());

    logger::enable_logging();
}

fn make_request(id: &mut i32, method: &str, params: serde_json::Value) -> Request {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let tick = format!("{}.{}", now.as_secs(), now.subsec_micros());

    let json = serde_json::json!({
        "jsonrpc": "2.0",
        "id": *id,
        "method": method,
        "params": params,
        "request_tick": tick
    });

    *id += 1; // autoincrement

    serde_json::from_value(json).expect(&format!("Failed to parse {} request JSON", method))
}

fn make_notification(id: &mut i32, method: &str) -> Request {
    let json = serde_json::json!({
        "jsonrpc": "2.0",
        "method": method,
        "params": serde_json::Value::Null,
    });

    serde_json::from_value(json).expect(&format!("Failed to parse {} notification JSON", method))
}

fn start_and_initialize_server(_id: &mut i32, args: &str) -> LspServer {
    //setup_test_logger();
    lspce_module::lspce_init();

    // create new dummy_lsp server with provided args
    let server = LspServer::new(DUMMY_LSP_CMD, args, "{}").expect("should create test server");

    // // Create initialize request
    // let initialize_req = make_request(
    //     id,
    //     "initialize",
    //     serde_json::json!({
    //         "processId": serde_json::Value::Null, // FIXME: should we pass real one?
    //         "rootUri": serde_json::Value::Null,
    //         "capabilities": {}
    //     }),
    // );

    // with_mock_env(|env| {
    //     lspce_module::initialize(env, &mut server, initialize_req, Duration::from_secs(1))
    //         .expect("Failed to initialize LSP server")
    // });

    server
}

fn assert_thread_states(state: &ResourceState, expected: [Vec<ThreadResult>; 3]) {
    // - LSP<(STDIN) / LSPCE WRITER should exit on
    //   - normal exit with either flag or due to channel close, depends on thread timing.
    //   - abnormal exit due to channel close.
    // - LSP>(STDOUT) / LSPCE READER shoudl exit on EOF, since it's blocked on reading from LSP.
    // - LSP!/STDERR should either exit with OK (exit flag), or EOF (blocking read), depends on timing

    const THREAD_NAMES: [&str; 3] = ["LSP< Writer", "LSP> Reader", "LSP! Stderr"];
    for (i, (actual, allowed)) in state.transport.iter().zip(expected.iter()).enumerate() {
        let mut matched = false;
        for exp in allowed {
            match (actual, exp) {
                (ThreadResult::NotJoined, ThreadResult::NotJoined) => matched = true,
                (ThreadResult::Ok, ThreadResult::Ok) => matched = true,
                (ThreadResult::Panic(_), ThreadResult::Panic(_)) => matched = true,
                (ThreadResult::IoError(e), ThreadResult::IoError(exp)) if e.kind() == exp.kind() => matched = true,
                _ => {}
            }
            if matched {
                break;
            }
        }
        assert!(
            matched,
            "Thread #{}({}): state <{:?}> did not match any of <{:?}>",
            i, THREAD_NAMES[i], actual, allowed
        );
    }
}

fn thread_res_io_err(kind: io::ErrorKind) -> ThreadResult {
    ThreadResult::IoError(io::Error::from(kind))
}

fn thread_vec_reader_and_stderr() -> (Vec<ThreadResult>, Vec<ThreadResult>) {
    (
        vec![thread_res_io_err(io::ErrorKind::UnexpectedEof)],
        vec![ThreadResult::Ok, thread_res_io_err(io::ErrorKind::UnexpectedEof)],
    )
}

fn normal_exit_thread_states() -> [Vec<ThreadResult>; 3] {
    let (reader, stderr) = thread_vec_reader_and_stderr();
    [
        vec![ThreadResult::Ok, thread_res_io_err(io::ErrorKind::NotConnected)],
        reader,
        stderr,
    ]
}

fn abnormal_exit_thread_states() -> [Vec<ThreadResult>; 3] {
    let (reader, stderr) = thread_vec_reader_and_stderr();
    [vec![thread_res_io_err(io::ErrorKind::NotConnected)], reader, stderr]
}

/// Tests normal graceful shutdown of LSP server, e.g. shutdown request, followed by exit notification and actual exit.
#[test]
fn test_graceful_shutdown() {
    let mut id = 5;
    let exit_val = id;

    let mut server = start_and_initialize_server(&mut id, &format!("--exit-value {}", exit_val));
    let shutdown_req = make_request(&mut id, "shutdown", serde_json::Value::Null);
    let exit_status = shutdown_server(&mut server, shutdown_req, Some(ONE_SEC));

    assert_thread_states(&server.resources.state, normal_exit_thread_states());
    assert_exit_status(exit_status.unwrap(), ExitType::Code(exit_val));
}

/// Tests both failure in shutdown protocol and stalled LSP server cases, which should lead to forced shutdown.
#[test]
fn test_graceful_shutdown_escalated_to_forced() {
    let mut id = 10;

    let mut server = start_and_initialize_server(&mut id, "--stall-on-shutdown");
    let shutdown_req = make_request(&mut id, "shutdown", serde_json::Value::Null);
    let exit_status = shutdown_server(&mut server, shutdown_req, Some(ONE_SEC));

    assert_thread_states(&server.resources.state, abnormal_exit_thread_states());
    assert_exit_status(exit_status.unwrap(), ExitType::Signal(SIGKILL));
}

/// Tests abnormal voluntary exit of LSP
#[test]
fn test_volunary_exited_lsp() {
    let mut id = 15;
    let exit_val = id;

    let mut server = start_and_initialize_server(&mut id, &format!("--exit-on-shutdown --exit-value {}", exit_val));
    let shutdown_req = make_request(&mut id, "shutdown", serde_json::Value::Null);
    let exit_status = shutdown_server(&mut server, shutdown_req, Some(ONE_SEC));

    assert_thread_states(&server.resources.state, abnormal_exit_thread_states());
    assert_exit_status(exit_status.unwrap(), ExitType::Code(exit_val));
}

// Test ABORTed LSP.
// Note that as skip testing killed LSP. Testing it here adds no additional value vs. the simple test done in `tests.rs`
// and requires to expose child process in Resources as well.
#[test]
fn test_aborted_lsp() {
    let mut id = 20;

    let mut server = start_and_initialize_server(&mut id, &format!("--abort-on-shutdown"));
    let shutdown_req = make_request(&mut id, "shutdown", serde_json::Value::Null);
    let exit_status = shutdown_server(&mut server, shutdown_req, Some(ONE_SEC));

    assert_thread_states(&server.resources.state, abnormal_exit_thread_states());
    assert_exit_status(exit_status.unwrap(), ExitType::Signal(SIGABRT));
}

/// Ask DUMMY_LSP to close its STDIN without exit.
/// This will cause DUMMY_LSP to fail in reading messages (ClosedPipe, UnexpectedEOF) and it will be stalled (by design).
/// Then LSP Writer should fail (since LSP< is closed) with BrokenPipe on subsequent write.
#[test]
fn test_lsp_closes_stdin() {
    let mut id = 25;

    let mut server = start_and_initialize_server(&mut id, "--allow-close-fd");
    let msg = server.write(Notification::new("test_close_stdin")).unwrap();
    std::thread::sleep(Duration::from_millis(100));

    // subsequent message will cause lspce to discover the error
    server.write(Notification::new("dummy")).unwrap();

    // server will be dropped anyway whem test will be finished. But let's do it explicitly
    let exit_status = server.teardown(Duration::ZERO);
    let (reader, stderr) = thread_vec_reader_and_stderr();
    let expected = [vec![thread_res_io_err(io::ErrorKind::BrokenPipe)], reader, stderr];
    assert_thread_states(&server.resources.state, expected);
    assert_exit_status(exit_status.unwrap(), ExitType::Signal(SIGKILL));
}

/// Ask DUMMY_LSP to close its STDOUT without exit.
/// This should unblock LSPCE reader thread, raise exit flag and lead to exit of all threads consequently.
#[test]
fn test_lsp_closes_stdout() {
    let mut id = 25;
    let mut server = start_and_initialize_server(&mut id, "--allow-close-fd");
    let msg = server.write(Notification::new("test_close_stdout")).unwrap();
    std::thread::sleep(Duration::from_millis(100));

    // server will be dropped anyway whem test will be finished. But let's do it explicitly
    let exit_status = server.teardown(Duration::ZERO);
    assert_thread_states(&server.resources.state, abnormal_exit_thread_states());
    assert_exit_status(exit_status.unwrap(), ExitType::Signal(SIGKILL));
}

// FIXME: project and reaper tests?
// Test stalled or stdin is enough? maybe test stalled messages that we are not getting answers? need to recheck server logic.
// Should we rely on reaper check in exit, unexpected kill, and close stdin/stdout
