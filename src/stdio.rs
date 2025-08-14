use std::io::{BufRead, Read};
use std::time::Instant;
use std::{
    collections::VecDeque,
    fmt,
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
use crate::logger;
use crate::msg::{Message, Notification, Response};
use crate::{
    connection::{NOTIFICATION_MAX, REQUEST_MAX},
    logger::Logger,
};

macro_rules! break_if_should_exit {
    ($exit_flag:expr) => {{
        if $exit_flag.load(Ordering::Relaxed) {
            Logger::info(&format!("requested to exit"));
            break;
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
        let mut res = Ok(());
        logger::set_log_prefix("[LSP<] - ");

        loop {
            break_if_should_exit!(&exit_writer);

            match r_to_lsp.recv() {
                Ok(msg) => {
                    if let Err(e) = msg.write(&mut stdin) {
                        Logger::error(&format!("I/O error: {}", e));
                        res = Err(e);
                        break;
                    }
                }
                Err(e) => {
                    Logger::error(&format!("channel error: {}", e));
                    res = Err(io::Error::new(io::ErrorKind::NotConnected, e));
                    break;
                }
            }
        }
        exit_writer.store(true, Ordering::Relaxed);
        Logger::info("finished");
        res
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
        let mut res = Ok(());

        logger::set_log_prefix("[LSP>] - ");

        loop {
            break_if_should_exit!(&exit_reader);

            match Message::read(&mut reader) {
                Ok(m) => {
                    if let Some(msg) = m {
                        if let Err(e) = s_from_lsp.send(msg) {
                            Logger::error(&format!("channel error: {}", e));
                            res = Err(io::Error::new(io::ErrorKind::NotConnected, e));
                            break;
                        }
                    }
                }
                //
                // do nothing on OK(None) - no messsage to handle (malformed). Just continue
                //
                // err is unrecoverable I/O error (pipe closed)
                Err(e) => {
                    Logger::error(&format!("I/O error: {}", e));
                    res = Err(e);
                    break;
                }
            }
        }
        // notify dispatcher in all ways - exit flag, message and dropping channel
        exit_reader.store(true, Ordering::Relaxed);
        let _ = s_from_lsp.send(Notification::new("exit").into()); // just to unblock channel
        Logger::info("finished");
        res
    });

    // Stderr is a blocking I/O thread that reads from LSP stdio pipe.
    // It reads lines and logs them as errors.
    // Lines are limited to MAX_STDERR_LINE_LEN (in read, to avoid DoS).
    // Blocking read will be released on I/O error (pipe closed) due to LSP crash or shutdown.
    const MAX_STDERR_LINE_LEN: usize = 4096;
    let exit_stderr = Arc::clone(&exit);
    let stderr_thread = thread::spawn(move || -> io::Result<()> {
        let mut reader = std::io::BufReader::new(child_stderr);
        let mut buffer = String::new();
        logger::set_log_prefix("[LSP!] - ");

        loop {
            break_if_should_exit!(&exit_stderr);

            buffer.clear();
            match reader.read_line_limited_or_eof(&mut buffer, MAX_STDERR_LINE_LEN) {
                Ok(_) => {
                    Logger::error(buffer.trim_end());
                }
                Err(e) if e.kind() == io::ErrorKind::QuotaExceeded => {
                    Logger::error(&format!("...Truncated.. {}", buffer.trim_end()));
                }
                Err(e) => {
                    // don't signal coordinated shutdown on error here. Leave the decision to stdin/stdout threads
                    Logger::error(&format!("I/O error: {}", e));
                    return Err(e); // Exit on unrecoverable errors including pipe close/EOF
                }
            }
        }
        Logger::info("finished");
        Ok(())
    });

    let threads = IoThreads::new(reader_thread, writer_thread, Some(stderr_thread));
    (s_to_lsp, r_from_lsp, threads)
}

#[repr(u8)]
pub enum ThreadTypes {
    Writer = 0, // 0 / STDIN. LSPCE -> LSP
    Reader = 1, // 1 / STDOUT. LSP -> LSPCE
    Stderr = 2, // 2 / STDERR . LSP -> stderr
}
pub const MAX_THREADS: usize = 3;

pub struct IoThreads {
    pub threads: [Option<thread::JoinHandle<io::Result<()>>>; MAX_THREADS],
    pub results: [ThreadResult; MAX_THREADS],
}

pub enum ThreadResult {
    NotJoined,
    Ok,
    IoError(std::io::Error),
    Panic(Box<dyn std::any::Any + Send + 'static>),
}

impl fmt::Debug for ThreadResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotJoined => write!(f, "NotJoined"),
            Self::Ok => write!(f, "Ok"),
            Self::IoError(e) => write!(f, "IoError({:?})", e),
            Self::Panic(_) => write!(f, "Panic(...)"),
        }
    }
}

impl IoThreads {
    pub fn new(
        reader: thread::JoinHandle<io::Result<()>>, writer: thread::JoinHandle<io::Result<()>>,
        stderr: Option<thread::JoinHandle<io::Result<()>>>,
    ) -> Self {
        use ThreadResult::NotJoined;
        IoThreads { threads: [Some(writer), Some(reader), stderr], results: [NotJoined, NotJoined, NotJoined] }
    }

    pub fn join(&mut self) -> io::Result<()> {
        let join_result = |handle: thread::JoinHandle<io::Result<()>>| -> ThreadResult {
            return match handle.join() {
                Ok(Ok(())) => ThreadResult::Ok,
                Ok(Err(e)) => ThreadResult::IoError(e),
                Err(e) => ThreadResult::Panic(e),
            };
        };

        for i in 0..self.threads.len() {
            if let Some(handle) = self.threads[i].take() {
                self.results[i] = join_result(handle);
            }
        }

        if self.results.iter().any(|r| matches!(r, ThreadResult::Panic(_))) {
            return Err(io::Error::new(io::ErrorKind::Other, "I/O thread panicked"));
        }

        Ok(())
    }
}
