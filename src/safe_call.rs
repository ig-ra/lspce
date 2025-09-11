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


#[cfg(test)]
mod test_safe_call {
    use crate::errors::UserFacing;
    use crate::safe_call::safe_call;
    use crate::test_utils::MockEnv;
    use anyhow::{bail, Context};
    use emacs::Result as EmacsResult;

    #[test]
    fn test_safe_call() {
        // we will use unwrap, since safe_call should ALWAYS return OK(...). On Err it logs and return Ok(None)

        let mock_env = MockEnv::new();

        // Case 1: OK(Some(true)) -> transparently pass -> OK(Some(true))
        assert_eq!(safe_call(&mock_env, || Ok(Some(true))).unwrap(), Some(true), "case OK(Some(true))");

        // Case 2: Ok(None) -> transparently pass -> Ok(None)
        assert_eq!(safe_call(&mock_env, || { Ok(None) }).unwrap(), None::<bool>, "case OK(None)");

        // Case 3: Err() -> convert to Ok(None)
        assert_eq!(safe_call(&mock_env, || { bail!("fail") }).unwrap(), None::<bool>, "case Err()");

        // Case 4: UserFacing Err() -> convert to Ok(None) and call user_message
        mock_env.clear_messages();
        let err = anyhow::anyhow!("user facing error").context(UserFacing);
        assert_eq!(safe_call(&mock_env, || Err(err)).unwrap(), None::<bool>, "case UserFacing Err()");
        let messages = mock_env.get_messages();
        assert_eq!(messages.len(), 1, "Expected 1 user message");
        assert_eq!(messages[0], "user facing error", "Expected correct error message");
    }
}
