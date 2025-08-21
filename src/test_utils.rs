use std::process;
pub const TENTH_OF_SEC: Duration = Duration::from_millis(100);
pub const ONE_SEC: Duration = Duration::from_secs(1);
pub const TWO_SECS: Duration = Duration::from_secs(2);

pub enum ExitType {
    Code(i32),
    Signal(i32),
}

pub fn assert_exit_status(exit_status: Option<process::ExitStatus>, expected: ExitType) {
    assert!(exit_status.is_some(), "Should return Some(exit_status)");
    let exit_status = exit_status.unwrap();

    match expected {
        ExitType::Code(exp) => {
            let code = exit_status.code().unwrap();
            assert_eq!(code, exp, "Should exit with code: {} != {}", exp, code);
        }
        #[cfg(unix)]
        ExitType::Signal(exp) => {
            use std::os::unix::process::ExitStatusExt;
            let signal = exit_status.signal().unwrap();
            assert_eq!(signal, exp, "Should exit with signal: {} != {:?}", exp, signal);
        }
    }
}

/// Creates a mock Env, uses it for the provided function, and then
/// safely disposes of it without running its destructor
pub fn with_mock_env<F, R>(f: F) -> R
where
    F: FnOnce(&emacs::Env) -> R,
{
    use std::ffi::c_void;

    // Create a dummy raw pointer that won't be dereferenced
    let dummy_ptr = Box::into_raw(Box::new(42u8)) as *mut c_void;

    // Create a mock Env with our dummy pointer
    let env = unsafe { emacs::Env::new(dummy_ptr as *mut _) };

    // Call the function with a reference to our env
    let result = f(&env);

    // Prevent the destructor from running (avoids null pointer dereference)
    std::mem::forget(env);

    // Return the result from the function
    result
}

// Actual mock for testing (via safe_call) and observing messages
pub struct MockEnv {
    pub messages: Arc<Mutex<Vec<String>>>,
}

impl MockEnv {
    pub fn new() -> Self {
        Self { messages: Arc::new(Mutex::new(Vec::new())) }
    }

    pub fn get_messages(&self) -> Vec<String> {
        self.messages.lock().unwrap().clone()
    }

    pub fn clear_messages(&self) {
        self.messages.lock().unwrap().clear();
    }
}

impl crate::env::UserMsgEnv for MockEnv {
    fn user_message(&self, text: &str) {
        self.messages.lock().unwrap().push(text.to_string());
    }
}
