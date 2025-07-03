use std::io::{BufRead, Read};
use std::time::Instant;
use std::{
    collections::VecDeque,
    io::{self, stdin, stdout},
    ops::ControlFlow,
    process::{ChildStderr, ChildStdin, ChildStdout},
    sync::{Arc, Mutex},
    thread,
};

use bytes::BytesMut;
use crossbeam_channel::{bounded, Receiver, Sender};

use crate::logger::{log_enabled, LOG_DEBUG};
use crate::msg::{ErrorCode, Message, RequestId, Response};
use crate::{
    connection::{NOTIFICATION_MAX, REQUEST_MAX},
    logger::Logger,
};

/// Creates an LSP connection via stdio.
pub(crate) fn stdio_transport(
    mut child_stdin: ChildStdin, mut child_stdout: ChildStdout, mut child_stderr: ChildStderr, exit: Arc<Mutex<bool>>,
) -> (Sender<Message>, Receiver<Message>, IoThreads) {
    let exit_writer = Arc::clone(&exit);
    let (sender_for_client, receiver_from_client) = bounded::<Message>(10);
    let writer_thread = thread::spawn(move || {
        let mut stdin = child_stdin;
        loop {
            {
                match exit_writer.lock() {
                    Ok(exit) => {
                        if *exit {
                            Logger::info("stdio writer_thread exited normally.");
                            break;
                        }
                    }
                    Err(e) => {
                        Logger::error(&format!("stdio writer_thread exit error {}", e));
                    }
                }
            }
            let recv_value = receiver_from_client.recv_timeout(std::time::Duration::from_millis(1));
            match recv_value {
                Ok(r) => {
                    Logger::debug(&format!(
                        "stdio write {}",
                        serde_json::to_string_pretty(&r).unwrap_or("invalid json".to_string())
                    ));

                    r.write(&mut stdin)
                }
                Err(t) => Ok(()),
            };
        }
        Ok(())
    });

    let exit_reader = Arc::clone(&exit);
    let (sender_to_client, receiver_for_client) = bounded::<Message>(10);
    let reader_thread = thread::spawn(move || {
        let mut stdout = child_stdout;
        let mut reader = std::io::BufReader::new(stdout);

        loop {
            {
                match exit_reader.lock() {
                    Ok(exit) => {
                        if *exit {
                            Logger::info("stdio reader_thread exited normally.");
                            break;
                        }
                    }
                    Err(e) => {
                        Logger::error(&format!("stdio reader_thread exit error {}", e));
                    }
                }
            }
            match Message::read(&mut reader) {
                Ok(m) => {
                    if let Some(msg) = m {
                        if log_enabled(LOG_DEBUG) {
                            let msg_log = msg.clone();
                            match msg_log {
                                Message::Request(r) => Logger::debug(&format!(
                                    "stdio read request {}",
                                    serde_json::to_string_pretty(&r).unwrap_or(r.content)
                                )),
                                Message::Response(r) => Logger::debug(&format!(
                                    "stdio read response {}",
                                    serde_json::to_string_pretty(&r).unwrap_or(r.content)
                                )),
                                Message::Notification(r) => Logger::debug(&format!(
                                    "stdio read notification {}",
                                    serde_json::to_string_pretty(&r).unwrap_or(r.content)
                                )),
                            }
                        }
                        let r = sender_to_client.send(msg);
                        if r.is_err() {
                            Logger::error(&format!("stdio read error {}", r.err().unwrap()));
                        }
                    } else {
                        thread::sleep(std::time::Duration::from_millis(1));
                    }
                }
                Err(e) => {
                    Logger::error(&format!("stdio read error {}", e));

                    let msg = Response::new_err(RequestId::from(1), -32603, format!("{}", e));
                    sender_to_client.send(Message::Response(msg));
                }
            }
        }

        Ok(())
    });

    let exit_stderr = Arc::clone(&exit);
    let stderr_thread = thread::spawn(move || {
        let mut stderr = child_stderr;
        let mut reader = std::io::BufReader::new(stderr);
        let mut buffer = String::new();
        loop {
            {
                match exit_stderr.lock() {
                    Ok(exit) => {
                        if *exit {
                            Logger::info(&format!("stdio stderr_thread exited normally."));
                            break;
                        }
                    }
                    Err(e) => {
                        Logger::error(&format!("stdio stderr_thread error {}", e));
                    }                    
                }
            }

            buffer.clear();
            match reader.read_line(&mut buffer) {
                Ok(0) => {
                    // Logger::error(&format!("stderr reach EOF"));
                },
                Ok(n) => {
                    Logger::error(&format!("[stderr] {}", &buffer));
                },
                Err(e) => {
                    Logger::error(&format!("stderr read error {}", e));
                },
            }
        }
    });

    let threads = IoThreads { reader: reader_thread, writer: writer_thread };
    (sender_for_client, receiver_for_client, threads)
}

// Creates an IoThreads
pub(crate) fn make_io_threads(
    reader: thread::JoinHandle<io::Result<()>>, writer: thread::JoinHandle<io::Result<()>>,
) -> IoThreads {
    IoThreads { reader, writer }
}

pub struct IoThreads {
    reader: thread::JoinHandle<io::Result<()>>,
    writer: thread::JoinHandle<io::Result<()>>,
}

fn join_thread(handle: thread::JoinHandle<io::Result<()>>, name: &str) -> Option<String> {
    match handle.join() {
        Ok(Ok(())) => None,
        Ok(Err(e)) => {
            let msg = format!("{name} thread error: {e}");
            Logger::error(&msg);
            Some(msg)
        }
        Err(e) => {
            let msg = format!("{name} thread panicked: {:?}", e);
            Logger::error(&msg);
            Some(msg)
        }
    }
}

impl IoThreads {
    pub fn join(self) -> io::Result<()> {
        Logger::info("IoThreads join");

        let mut error_msg = String::new();

        if let Some(e) = join_thread(self.reader, "reader") {
            error_msg.push_str(&e);
        }
        if let Some(e) = join_thread(self.writer, "writer") {
            if !error_msg.is_empty() {
                error_msg.push_str("; ");
            }
            error_msg.push_str(&e);
        }

        Logger::info("IoThreads join finished.");

        if !error_msg.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, error_msg));
        }
        Ok(())
    }
}
