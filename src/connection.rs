use crate::{msg::Message, socket, stdio, stdio::IoThreads};
use crossbeam_channel::{Receiver, Sender};
use std::{
    io,
    net::{TcpStream, ToSocketAddrs},
    process::{ChildStderr, ChildStdin, ChildStdout},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

pub const NOTIFICATION_MAX: usize = 10;
pub const REQUEST_MAX: usize = 10;

/// Create LSP stdio communicaiton channel
pub fn stdio(
    mut stdin: ChildStdin, mut stdout: ChildStdout, mut stderr: ChildStderr, exit: Arc<AtomicBool>,
) -> (Sender<Message>, Receiver<Message>, IoThreads) {
    stdio::stdio_transport(stdin, stdout, stderr, exit)
}

/// Create LSP socket communicaiton channel
pub fn connect<A: ToSocketAddrs>(
    addr: A, exit: Arc<AtomicBool>,
) -> io::Result<(Sender<Message>, Receiver<Message>, IoThreads)> {
    let stream = TcpStream::connect(addr)?;
    Ok(socket::socket_transport(stream, exit))
}
