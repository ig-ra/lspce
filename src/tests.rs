use super::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_safe_call() {
        // Case 1: Closure returns Result<Option<bool>> -> safe_call will return Result<Option<bool>>
        assert_eq!(safe_call(|| { Ok(Some(true)) }).unwrap(), Some(true), "Case: test_ok_some");

        // Case 2: Closure returns Result<bool> -> safe_call will return Result<Option<bool>>
        assert_eq!(safe_call(|| { Ok(true) }).unwrap(), Some(true), "Case: test_ok");

        // Case 3: Closure returns Result<Option<bool>> with None -> safe_call will return Result<Option<bool>> with None
        assert_eq!(safe_call(|| { Ok(None) }).unwrap(), None::<bool>, "Case: test_ok_none");

        // Case 4: Closure returns Result<bool> with Err -> safe_call will return Result<bool> with None
        assert_eq!(safe_call(|| -> Result<bool> { bail!("fail") }).unwrap(), None::<bool>, "Case: test_");
    }

    #[test]
    fn test_lsp_server_new_invalid_command() {
        let server = LspServer::new("nonexistent_command_with_random_suffix", "", "");
        assert!(matches!(server, Err(ref error) if error.to_string().contains("Failed to spawn LSP server process")));
    }

    #[test]
    fn test_lsp_server_new_json_parsing_errors() {
        let malformed_json_cases = vec![
            ("{", "unclosed brace"),
            ("{invalid}", "invalid syntax"),
            ("{invalid:}", "invalid syntax 2"),
            ("not_json", "not json - string"),
            (r#"{"missing_quote: "value"}"#, "missing quote"),
            ("[1,2,3]", "not json - array"),
        ];

        for (invalid_json, description) in malformed_json_cases {
            let server = LspServer::new("echo", "", invalid_json);
            if let Err(e) = server {
                assert!(
                    e.to_string().contains("Failed to parse emacs_envs JSON"),
                    "Test '{}' failed. Expected JSON parsing error, but got: <{}>",
                    description,
                    e.to_string()
                );
            } else {
                assert!(
                    false,
                    "Test '{}' failed. Expected fail to parse JSON <{}>, but got success",
                    description, invalid_json
                );
            }
        }
    }

    #[test]
    fn test_lsp_server_new_valid_scenarios() {
        let test_cases = vec![
            ("echo", "", "", "empty envs"),
            ("echo", "something", "", "args"),
            ("echo", "", r#"{"PATH": "/usr/bin", "HOME": "/home/test"}"#, "valid envs"),
            ("true", "", "", "fast exit command"),
        ];

        for (cmd, args, envs, description) in test_cases {
            let server = LspServer::new(cmd, args, envs);

            // Platform-dependent. Could return spawn error on non-linux systems (echo/true), but should not return JSON parsing error.
            if let Err(e) = &server {
                assert!(
                    !e.to_string().contains("Failed to parse emacs_envs JSON"),
                    "Test '{}' failed. Unexpected JSON parsing error: <{}>",
                    description,
                    e.to_string()
                );
            }
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
