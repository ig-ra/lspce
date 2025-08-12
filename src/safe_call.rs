use crate::env::UserMsgEnv;
use crate::errors::UserFacing;
use crate::logger::Logger;

/// Executes a closure `f` and converts Err result.
/// On error, logs (optionally send a user message if Error is tagged as UserFacing) and return `Ok(None)`.
#[track_caller]
pub fn safe_call<T, F>(env: &dyn UserMsgEnv, f: F) -> emacs::Result<Option<T>>
where
    F: FnOnce() -> emacs::Result<Option<T>>,
{
    match f() {
        ok @ Ok(_) => ok,
        Err(e) => {
            // User-facing error. Extract the root cause message and send to Emacs
            if e.downcast_ref::<UserFacing>().is_some() {
                env.user_message(&e.root_cause().to_string());
            }
            Logger::error(format!("Error: @{}: {}", std::panic::Location::caller(), e));
            Ok(None)
        }
    }
}
