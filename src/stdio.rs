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
use crate::msg::{Message, RequestId, Response};
use crate::{
    connection::{NOTIFICATION_MAX, REQUEST_MAX},
    logger::Logger,
};

const LSPCE_ID: &str = "LSPCE";
const LSPCE_ERR: i32 = -375; // -sum([ord(c) for c in list("LSPCE")])

/// Checks if thread should exit based on exit flag
fn should_exit(exit_flag: &Arc<AtomicBool>, thread_name: &str) -> bool {
    let exit = exit_flag.load(Ordering::Relaxed);
    if exit {
        Logger::info(&format!("[LSP {}] thread requested to exit", thread_name));
    }
    exit
}

macro_rules! bail_if_should_exit {
    ($exit_flag:expr, $thread_name:expr) => {{
        if should_exit($exit_flag, $thread_name) {
            return Ok(());
        }
    }};
}

/// Creates an LSP connection via stdio.
pub(crate) fn stdio_transport(
    mut child_stdin: ChildStdin, mut child_stdout: ChildStdout, mut child_stderr: ChildStderr, exit: Arc<AtomicBool>,
) -> (Sender<Message>, Receiver<Message>, IoThreads) {
    let exit_writer = Arc::clone(&exit);
    let (s_to_lsp, r_to_lsp) = bounded::<Message>(10);
    let writer_thread = thread::spawn(move || {
        let mut stdin = child_stdin;
        loop {
            bail_if_should_exit!(&exit_writer, "<");

            let recv_value = r_to_lsp.recv_timeout(std::time::Duration::from_millis(1));
            match recv_value {
                Ok(msg) => msg.write(&mut stdin),
                Err(t) => Ok(()),
            };
        }
    });

    // Reader is a blocking I/O thread that reads from LSP stdio pipe.
    //
    // It reads Message and sends to the dispatcher via channel.
    // On error try to notify dispatcher both via message, via exit flag, and by dropping channel.
    // Blocking read will be released on I/O error (pipe closed) due to LSP crash or shutdown.
    let exit_reader = Arc::clone(&exit);
    let (s_from_lsp, r_from_lsp) = bounded::<Message>(10);
    let reader_thread = thread::spawn(move || {
        let mut reader = std::io::BufReader::new(child_stdout);
        let res = Ok(());

        loop {
            if exit_reader.load(Ordering::Relaxed) {
                Logger::info(&format!("[LSP>] - requested to exit"));
                break;
            }

            match Message::read(&mut reader) {
                Ok(m) => {
                    if let Some(msg) = m {
                        if let Err(e) = s_from_lsp.send(msg) {
                            Logger::error(&format!("[LSP>] - channel closed {}", e));
                            break;
                        }
                    }
                }
                //
                // do nothing on OK(None) - no messsage to handle (malformed). Just continue
                //
                // err is unrecoverable I/O error (pipe closed)
                Err(e) => {
                    Logger::error(&format!("[LSP>] - error {}", e));
                    res = Err(e);
                    break;
                }
            }
        }
        // notify dispatcher in all ways - exit flag, message and dropping channel
        exit_reader.store(true, Ordering::Relaxed);
        let _ = s_from_lsp.send(Response::new_err(LSPCE_ID, LSPCE_ERR, "").into());
        res;
    });

    const MAX_STDERR_LINE_LEN: usize = 4096;
    let exit_stderr = Arc::clone(&exit);
    let stderr_thread = thread::spawn(move || -> io::Result<()> {
        let mut reader = std::io::BufReader::new(child_stderr);
        let mut buffer = String::new();
        loop {
            bail_if_should_exit!(&exit_stderr, "!");

            buffer.clear();
            match reader.read_line_limited_or_eof(&mut buffer, MAX_STDERR_LINE_LEN) {
                Ok(_) => {
                    Logger::error(&format!("[LSP!] {}", &buffer.trim_end()));
                }
                Err(e) if e.kind() == io::ErrorKind::QuotaExceeded => {
                    Logger::error(&format!("[LSP!] - ...Truncated.. {}", &buffer.trim_end()));
                }
                Err(e) => {
                    // we may signal coordinated shutdown on error here as well
                    // but let's leave the decision to stdin/stdout threads
                    Logger::error(&format!("[LSP!] - error: {}", e));
                    return Err(e); // Exit on unrecoverable errors including pipe close/EOF
                }
            }
        }
    });

    let threads = make_io_threads(reader_thread, writer_thread, Some(stderr_thread));
    (s_to_lsp, r_from_lsp, threads)
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
