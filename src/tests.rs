use super::*;

// handy for manual testing and to see log messages
pub fn setup_test_logger() {
    #[cfg(unix)]
    logger::set_log_file_name("/dev/stderr".to_string());

    logger::enable_logging();
}
