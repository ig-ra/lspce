use std::process;
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
