use std::process::{Child, ExitStatus};
use std::time::Duration;
use crate::logger::Logger;
use anyhow::Result;

/// Waits for a child process to exit with a timeout.
/// Returns Ok(Some(status)) if the process exited, Ok(None) if it did not exit in time, or Err(e) on error.
pub fn wait_child_with_timeout(
    child: &mut Child,
    timeout: Duration,
    name_id: &str,
    exit_type: &str,
) -> Result<Option<ExitStatus>> {
    use wait_timeout::ChildExt;
    let msg = format!("{}: child process for {}", exit_type, name_id);
    match child.wait_timeout(timeout) {
        Ok(Some(status)) => {
            Logger::info(format!("{} exited with status {}", msg, status));
            Ok(Some(status))
        }
        Ok(None) => {
            Logger::info(format!("{} did not exit after timeout {:?}", msg, timeout));
            Ok(None)
        }
        Err(e) => {
            Logger::error(format!("{} wait timeout error: {}", msg, e));
            Err(e.into())
        }
    }
}
