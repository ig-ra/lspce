use std::io::{BufRead, Read};
use std::time::Instant;
use std::{
    collections::VecDeque,
    io::{self, stdin, stdout},
    ops::ControlFlow,
    process::{ChildStderr, ChildStdin, ChildStdout},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
};

use bytes::BytesMut;
use crossbeam_channel::{bounded, Receiver, Sender};

use crate::bufext::BufReadEofExt;
use crate::msg::{ErrorCode, Message, RequestId, Response};
use crate::{
    connection::{NOTIFICATION_MAX, REQUEST_MAX},
    logger::Logger,
};

/// Checks if thread should exit based on exit flag
fn should_exit(exit_flag: &Arc<AtomicBool>, thread_name: &str) -> bool {
    let exit = exit_flag.load(Ordering::Relaxed);
    if exit {
        Logger::info(&format!("[LSP {}] thread requested to exit", thread_name));
    }
    exit
}

macro_rules! break_if_should_exit {
    ($exit_flag:expr, $thread_name:expr) => {{
        if should_exit($exit_flag, $thread_name) {
            break;
        }
    }};
}

/// Creates an LSP connection via stdio.
pub(crate) fn stdio_transport(
    mut child_stdin: ChildStdin, mut child_stdout: ChildStdout, mut child_stderr: ChildStderr, exit: Arc<AtomicBool>,
) -> (Sender<Message>, Receiver<Message>, IoThreads) {
    let exit_writer = Arc::clone(&exit);
    let (sender_for_client, receiver_from_client) = bounded::<Message>(10);
    let writer_thread = thread::spawn(move || {
        let mut stdin = child_stdin;
        loop {
            break_if_should_exit!(&exit_writer, "stdout");

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
            break_if_should_exit!(&exit_reader, "stdin");

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
    let stderr_thread = thread::spawn(move || -> io::Result<()> {
        let mut stderr = child_stderr;
        let mut reader = std::io::BufReader::new(stderr);
        let mut buffer = String::new();
        loop {
            break_if_should_exit!(&exit_stderr, "stderr");

            buffer.clear();
            match reader.read_line_or_eof(&mut buffer) {
                Ok(_) => {
                    Logger::error(&format!("[LSP stderr] {}", &buffer.trim_end()));
                }
                Err(e) => {
                    // we may signal coordinated shutdown on error here as well
                    // but let's leave the decision to stdin/stdout threads
                    Logger::error(&format!("[LSP stderr] - error: {}", e));
                    return Err(e); // Exit on unrecoverable errors including pipe close/EOF
                }
            }
        }
        Ok(())
    });

    let threads = make_io_threads(reader_thread, writer_thread, Some(stderr_thread));
    (sender_for_client, receiver_for_client, threads)
}

// Creates an IoThreads
pub(crate) fn make_io_threads(
    reader: thread::JoinHandle<io::Result<()>>, writer: thread::JoinHandle<io::Result<()>>,
    stderr: Option<thread::JoinHandle<io::Result<()>>>,
) -> IoThreads {
    IoThreads { reader, writer, stderr }
}

pub struct IoThreads {
    reader: thread::JoinHandle<io::Result<()>>,
    writer: thread::JoinHandle<io::Result<()>>,
    stderr: Option<thread::JoinHandle<io::Result<()>>>, // only when stdio
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

        let errors: Vec<String> = [
            join_thread(self.reader, "reader"),
            join_thread(self.writer, "writer"),
            self.stderr.and_then(|h| join_thread(h, "stderr")),
        ]
        .into_iter()
        .flatten()
        .collect();

        Logger::info("IoThreads join finished.");

        if !errors.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, errors.join("; ")));
        }
        Ok(())
    }
}
