use crate::logger::Logger;
use std::process::{Child, ExitStatus};
use std::time::Duration;
use wait_timeout::ChildExt;

/// Waits for a child process to exit with a timeout.
/// Returns Ok(Some(status)) if the process exited, Ok(None) if it did not exit in time, or Err(e).
pub fn wait_child_with_timeout(child: &mut Child, timeout: Duration, id: &str) -> anyhow::Result<Option<ExitStatus>> {
    match child.wait_timeout(timeout) {
        Ok(Some(status)) => {
            Logger::info(format!("wait_timeout for {}: {}", id, status));
            Ok(Some(status))
        }
        Ok(None) => {
            Logger::info(format!("wait_timeout for {}: timeout", id));
            Ok(None)
        }
        Err(e) => {
            Logger::error(format!("wait for {}: error: {}", id, e));
            Err(e.into())
        }
    }
}

/// Kills a child process and waits for it to exit with a timeout.
/// Returns Ok(Some(status)) if the process exited, Ok(None) if it did not exit in time, or Err(e).
pub fn kill_child_and_wait_with_timeout(
    child: &mut Child, timeout: Duration, id: &str,
) -> anyhow::Result<Option<ExitStatus>> {
    Logger::info(format!("forcefully terminating {}", id));
    if let Err(e) = child.kill() {
        Logger::error(format!("failed to kill {}: {}", id, e));
        return Err(e.into());
    }
    wait_child_with_timeout(child, timeout, id)
}

// break the loop if exit flag is set
#[macro_export]
macro_rules! break_if_should_exit {
    ($exit_flag:expr) => {{
        if $exit_flag.load(Ordering::Relaxed) {
            Logger::info(&format!("requested to exit"));
            break;
        }
    }};
}
