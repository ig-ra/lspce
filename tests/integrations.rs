use lspce_module::{shutdown_server, LspServer, Request};
use std::time::Duration;

const DUMMY_LSP_CMD: &str = "target/debug/dummy_lsp";

/// Creates a mock Env, uses it for the provided function, and then
/// safely disposes of it without running its destructor
fn with_mock_env<F, R>(f: F) -> R
where
    F: FnOnce(&emacs::Env) -> R,
{
    use std::ffi::c_void;

    // Create a dummy raw pointer that won't be dereferenced
    let dummy_ptr = Box::into_raw(Box::new(42u8)) as *mut c_void;

    // Create a mock Env with our dummy pointer
    let env = unsafe { emacs::Env::new(dummy_ptr as *mut _) };

    // Call the function with a reference to our env
    let result = f(&env);

    // Prevent the destructor from running (avoids null pointer dereference)
    std::mem::forget(env);

    // Return the result from the function
    result
}

fn setup_logger() {
    lspce_module::set_log_file("/dev/stdout").unwrap();
    // Enable debug logging to see message contents
    lspce_module::LOG_LEVEL.store(lspce_module::LOG_DEBUG, std::sync::atomic::Ordering::Relaxed);
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

fn start_end_initialize_server(id: &mut i32, args: &str) -> LspServer {
    setup_logger();

    // create new dummy_lsp server with provided args
    let mut server = LspServer::new(DUMMY_LSP_CMD, args, "{}").expect("should create test server");

    // Create initialize request
    let initialize_req = make_request(
        id,
        "initialize",
        serde_json::json!({
            "processId": serde_json::Value::Null, // FIXME: should we pass real one?
            "rootUri": serde_json::Value::Null,
            "capabilities": {}
        }),
    );

    with_mock_env(|env| {
        lspce_module::initialize(env, &mut server, initialize_req, Duration::from_secs(1))
            .expect("Failed to initialize LSP server")
    });

    server
}

fn assert_exit_status(exit_status: Option<std::process::ExitStatus>, expected_code: Option<i32>) {
    assert!(exit_status.is_some(), "Should return Some(exit_status)");
    let exit_status = exit_status.unwrap();

    match expected_code {
        Some(code) => {
            assert_eq!(exit_status.code().unwrap(), code, "Should exit with expected code");
        }
        None => {
            assert!(exit_status.code().is_none(), "Expected exit code to be None");
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                const SIGKILL: i32 = 9;
                assert_eq!(exit_status.signal().unwrap(), SIGKILL, "Should be killed by expected signal");
            }
        }
    }
}

#[test]
fn test_graceful_shutdown() {
    let mut id = 5;
    let exit_val = 5;

    let server = start_end_initialize_server(&mut id, &format!("--exit-value {}", exit_val));
    let shutdown_req = make_request(&mut id, "shutdown", serde_json::Value::Null);
    let result = shutdown_server(server, shutdown_req);

    assert!(result.is_ok(), "shutdown_server should succeed");
    assert_exit_status(result.unwrap(), Some(exit_val));
}

#[test]
fn test_graceful_shutdown_escalated_no_shutdown() {
    let mut id = 10;

    let server = start_end_initialize_server(&mut id, "--ignore-shutdown");
    let shutdown_req = make_request(&mut id, "shutdown", serde_json::Value::Null);
    let result = shutdown_server(server, shutdown_req);

    assert!(result.is_ok(), "shutdown_server should succeed");
    assert_exit_status(result.unwrap(), None);
}

#[test]
fn test_graceful_shutdown_escalated_stuck_exit() {
    let mut id = 10;

    let server = start_end_initialize_server(&mut id, "--ignore-exit");
    let shutdown_req = make_request(&mut id, "shutdown", serde_json::Value::Null);
    let result = shutdown_server(server, shutdown_req);

    assert!(result.is_ok(), "shutdown_server should succeed");
    assert_exit_status(result.unwrap(), None);
}
