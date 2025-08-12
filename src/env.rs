/// Extension trait for Emacs environment to clearly mark our messages
pub trait EnvExt {
    fn lspce_message(&self, text: impl AsRef<str>) -> emacs::Result<emacs::Value<'_>>;
}

impl EnvExt for emacs::Env {
    fn lspce_message(&self, text: impl AsRef<str>) -> emacs::Result<emacs::Value<'_>> {
        self.message(format!("[lspce-module] {}", text.as_ref()))
    }
}

/// Trait for safe_call to use + mocking. Send message through Emacs::Env back to the user
pub trait UserMsgEnv {
    fn user_message(&self, text: &str);
}

impl UserMsgEnv for emacs::Env {
    fn user_message(&self, text: &str) {
        let _ = self.lspce_message(text);
    }
}
