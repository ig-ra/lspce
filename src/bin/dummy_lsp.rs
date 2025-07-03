//! Dummy LSP process for for LSPCE testing

use argh::FromArgs;
use serde_json::json;
use std::io::{BufReader, Write};
use std::process;
use lspce_module::{Message, RequestId};

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

fn main() {
    let args: Args = argh::from_env();

    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut reader = BufReader::new(stdin.lock());

    loop {
        match Message::read(&mut reader) {
            Ok(Some(Message::Request(req))) => {
                eprintln!("dummy-lsp: got request '{}'", req.method);
                match req.method.as_str() {
                    "initialize" => {
                        let response = make_response(&req.id, json!({"capabilities": {}}));
                        send_response(&mut stdout, &response);
                    }
                    "shutdown" => {
                        if !args.ignore_shutdown {
                            let response = make_response(&req.id, serde_json::Value::Null);
                            send_response(&mut stdout, &response);
                        }
                    }
                    _ => {}
                }
            }
            Ok(Some(Message::Notification(notif))) => {
                eprintln!("dummy-lsp: got notification '{}'", notif.method);
                if notif.method == "exit" {
                    if !args.ignore_exit {
                        break;
                    } else {
                        eprintln!("dummy lsp: ignoring exit");
                    }
                }
            }
            Ok(Some(Message::Response(_))) => {
                eprintln!("dummy-lsp: got unexpected response");
            }
            Ok(None) => break, // EOF
            Err(e) => {
                eprintln!("dummy-lsp: failed to parse message: {}", e);
                break;
            }
        }
    }
    process::exit(args.exit_value.unwrap_or(0));
}
