//! Dummy LSP process for for LSPCE testing

use argh::FromArgs;
use lspce_module::{Message, RequestId};
use serde_json::json;
use std::io::{BufReader, Write};
use std::process;

const SLEEP_TIME: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(FromArgs, Debug)]
/// Dummy LSP process for integration testing
struct Args {
    /// ignore shutdown requests
    #[argh(switch)]
    ignore_shutdown: bool,

    /// ignore exit requests
    #[argh(switch)]
    ignore_exit: bool,

    /// specific exit value for process
    #[argh(option)]
    exit_value: Option<i32>,
}

fn make_response(id: &RequestId, result: serde_json::Value) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result
    })
    .to_string()
}

fn send_response(stdout: &mut std::io::Stdout, response: &str) {
    write!(stdout, "Content-Length: {}\r\n\r\n{}", response.len(), response).unwrap();
    stdout.flush().unwrap();
}

macro_rules! log_stderr {
    ($($arg:tt)*) => {
        eprintln!($($arg)*);
        std::io::stderr().flush().unwrap();
    };
}

fn main() {
    let args: Args = argh::from_env();

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut reader = BufReader::new(stdin.lock());

    log_stderr!("dummy-lsp: starting");
    loop {
        match Message::read(&mut reader) {
            Ok(Some(Message::Request(req))) => {
                log_stderr!("dummy-lsp: got request <{}>", req.method);
                match req.method.as_str() {
                    "initialize" => {
                        let response = make_response(&req.id, json!({"capabilities": {}}));
                        send_response(&mut stdout, &response);
                    }
                    "shutdown" => {
                        if !args.ignore_shutdown {
                            let response = make_response(&req.id, serde_json::Value::Null);
                            send_response(&mut stdout, &response);
                        } else {
                            log_stderr!("dummy lsp: ignoring shutdown");
                            std::thread::sleep(SLEEP_TIME);
                        }
                    }
                    _ => {}
                }
            }
            Ok(Some(Message::Notification(notification))) => {
                log_stderr!("dummy-lsp: got notification <{}>", notification.method);
                if notification.method == "exit" {
                    if !args.ignore_exit {
                        break;
                    } else {
                        log_stderr!("dummy lsp: ignoring exit and entering infinite sleep");
                        loop {
                            std::thread::sleep(SLEEP_TIME);
                        }
                    }
                }
            }
            Ok(Some(Message::Response(_))) => {
                log_stderr!("dummy-lsp: got unexpected response");
            }
            Ok(None) => {
                //
                log_stderr!("dummy-lsp: got EOF. ignore closed stdin");
                if args.ignore_exit {
                    log_stderr!("dummy lsp: ignoring EOF and entering infinite sleep");
                    loop {
                        std::thread::sleep(SLEEP_TIME);
                    }
                } else {
                    break;
                }
            } // EOF
            Err(e) => {
                log_stderr!("dummy-lsp: failed to parse message: {}", e);
            }
        }
    }
    let exit_value = args.exit_value.unwrap_or(0);
    log_stderr!("dummy-lsp: exiting with {}", exit_value);
    process::exit(exit_value);
}
