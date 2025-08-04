use super::*;

#[cfg(test)]
mod test_safe_call {
    use super::{safe_call, Result};
    use anyhow::bail;

    #[test]
    fn test_safe_call() {
        // we will use unwarp, since safe_call should ALWAYS return OK(...). On Err it logs and return Ok(None)

        // Case 1: OK(Some(true)) -> transarently pass -> OK(Some(true))
        assert_eq!(safe_call(|| { Ok(Some(true)) }).unwrap(), Some(true), "case OK(Some(true))");

        // Case 2: Ok(None) -> transparently pass -> Ok(None)
        assert_eq!(safe_call(|| { Ok(None) }).unwrap(), None::<bool>, "case OK(None)");

        // Case 3: Еrr() -> convert to Ok(None)
        assert_eq!(safe_call(|| -> Result<Option<bool>> { bail!("fail") }).unwrap(), None::<bool>, "Case: test_error");
    }
}

mod test_lspserver_new {
    use super::LspServer;
    use std::io;

    #[cfg(unix)]
    #[test]
    fn test_lsp_server_new_valid_scenarios() {
        let test_cases = vec![
            ("true", "", "", "no args and empty envs"),
            ("echo", "something", "", "cmd with args"),
            ("echo", "", r#"{"PATH": "/usr/bin", "HOME": "/home/test"}"#, "valid envs"),
        ];

        for (cmd, args, envs, description) in test_cases {
            let res = LspServer::new(cmd, args, envs);

            // May return spawn error on non-linux systems (due to presence of echo/true),
            // but should not return JSON parsing error.
            if let Err(e) = &res {
                if !matches!(e.downcast_ref::<io::Error>(), Some(err) if err.kind() == io::ErrorKind::NotFound) {
                    panic!("Test '{}' failed. Unexpected error: {:?}", description, e);
                }
            }
        }
    }

    #[test]
    fn test_lsp_server_new_invalid_scenarios() {
        let test_cases = vec![
            ("unexisting_binary", "", "Failed to spawn LSP server process", "unexisting binary"),
            ("echo", "not JSON", "Failed to parse emacs_envs JSON", "bad JSON"),
        ];

        for (cmd, envs, error_str, description) in test_cases {
            let res = LspServer::new(cmd, "", envs);
            assert!(res.is_err(), "Test '{}' should return an error", description);
            //let err = res.err().unwrap();
            assert!(matches!(res, Err(ref e) if e.to_string().contains(error_str)));
        }
    }
}

// handy for manual testing and to see log messages
pub fn setup_test_logger() {
    #[cfg(unix)]
    logger::set_log_file_name("/dev/stderr".to_string());

    logger::enable_logging();
}

#[cfg(test)]
mod shutdown_lspserver {
    use super::*;
    use std::thread;
    use std::time::Duration;

    fn make_test_server(cmd: &str, args: &str) -> LspServer {
        let mut server = LspServer::new(cmd, args, "{}").expect("should create test server");
        server.status = SERVER_STATUS_RUNNING;
        server
    }

    fn assert_shutdown_state(server: &LspServer) {
        assert!(server.dispatcher.is_none(), "Dispatcher should be None after shutdown");
        assert!(server.transport_threads.is_none(), "Transport threads should be None after shutdown");
        assert!(server.child.is_none(), "Child should be None after shutdown");
    }

    fn assert_shutdown_idempotent(mut server: LspServer, timeout: Duration, desc: &str, expect_some: bool) {
        // First shutdown
        let result = server.shutdown(timeout);
        assert!(result.is_ok(), "{desc}: first shutdown should succeed");
        if expect_some {
            assert!(matches!(result.unwrap(), Some(_)), "{desc}: first shutdown should return Some(exit_status)");
        }
        assert_shutdown_state(&server);

        // Second shutdown (idempotency)

        // disable logging to avoid duplicated messages and noise in idenpotent tests
        let level = logger::get_log_level();
        logger::disable_logging();

        let result2 = server.shutdown(timeout);
        assert!(result2.is_ok(), "{desc}: second shutdown should not panic");
        assert!(result2.unwrap().is_none(), "{desc}: second shutdown should return None");
        assert_shutdown_state(&server);

        logger::set_log_level(level); // reenable
    }

    #[test]
    fn test_graceful_shutdown() {
        //setup_test_logger();
        let server = make_test_server("true", "");
        assert_shutdown_idempotent(server, Duration::from_secs(2), "graceful", true);
    }

    #[test]
    fn test_forced_shutdown() {
        //setup_test_logger();
        let server = make_test_server("sleep", "5");
        assert_shutdown_idempotent(server, Duration::ZERO, "forced", false);
    }

    #[test]
    fn test_graceful_escalates_to_forced_shutdown() {
        //setup_test_logger();
        let server = make_test_server("sleep", "5");
        assert_shutdown_idempotent(server, Duration::from_millis(100), "graceful escalates to forced", false);
    }
}
