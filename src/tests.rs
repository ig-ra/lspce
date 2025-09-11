use super::*;

// handy for manual testing and to see log messages
pub fn setup_test_logger() {
    #[cfg(unix)]
    logger::set_log_file_name("/dev/stderr".to_string());

    logger::enable_logging();
}

fn mock_server() -> (LspServer, Receiver<Message>, Sender<Message>) {
    use ThreadResult::NotJoined;

    let (s_lsp, r_emacs) = crossbeam_channel::unbounded::<Message>();
    let (s_emacs, r_lsp) = crossbeam_channel::unbounded::<Message>();

    let server = LspServer {
        resources: Resources {
            child: None,
            transport: None,
            dispatcher: None,
            state: ResourceState { transport: [NotJoined, NotJoined, NotJoined], exit: None },
        },
        server_info: LspServerInfo::new(123),
        status: AtomicServerStatus::new(ServerStatus::Running),
        sender: Some(s_emacs),
        server_data: Arc::new(Mutex::new(LspServerData::new())),
        exit: Arc::new(AtomicBool::new(false)),
        name_id: "mock_server".to_string(),
    };
    (server, r_lsp, s_lsp)
}

mod test_lspserver_new {
    use super::LspServer;
    use std::io;

    #[cfg(unix)] // using echo/true
    #[test]
    fn test_lsp_server_new_valid_schenarios() {
        let test_cases = vec![
            ("true", "", "", "no args and empty envs"),
            ("echo", "something", "", "cmd with args"),
            ("echo", "", r#"{"PATH": "/usr/bin", "HOME": "/home/test"}"#, "valid envs"),
        ];

        for (cmd, args, envs, description) in test_cases {
            let res = LspServer::new(cmd, args, envs);

            // May return spawn error on non-linux systems (due to presence of echo/true),
            // but should not return JSON parsing error.
            if let Err(err) = &res {
                if !matches!(err.downcast_ref::<io::Error>(), Some(e) if e.kind() == io::ErrorKind::NotFound) {
                    panic!("Test '{description}' failed. Unexpected error: {err}");
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
            assert!(res.is_err(), "Test '{description}' should return an error");
            assert!(matches!(res, Err(ref e) if e.to_string().contains(error_str)));
        }
    }
}

#[cfg(test)]
mod test_lspserver_initialize {
    use super::*;
    use lsp_types::{InitializeResult, InitializedParams, ServerCapabilities, ServerInfo};
    use test_utils::*;

    #[test]
    fn test_initialize_success() {
        let (mut server, r_lsp, s_lsp) = mock_server();
        let server_data = Arc::clone(&server.server_data);

        let init_params = InitializedParams {};
        let init_req = Request::new("initialize", "initialize", Some(init_params)).expect("Bad init request");
        let req_id = init_req.id.clone();

        let mut server_info = LspServerInfo::new(123); // the same as mock id
        server_info.info.name = "fake".to_string();
        let si = server_info.info.clone(); // clone lsp_types::ServerInfo to move to thread

        // Simulate the LSP server's response in a separate thread
        let lsp_thread = std::thread::spawn(move || {
            // assert fake LSP got INITIALIZE request
            let init_msg = r_lsp.recv_timeout(ONE_SEC).expect("Did not get INITIALIZE message");
            assert!(matches!(init_msg, Message::Request(req) if req.id == req_id && req.method == "initialize"));

            // send response with init results back (i.e. push directly to server_data)
            let init_result = InitializeResult { capabilities: ServerCapabilities::default(), server_info: Some(si) };
            let init_resp = Response::new_ok(req_id, Some(init_result)).expect("Bad init response");
            server_data.lock().unwrap().responses_unticked.push_back(init_resp);

            // assert fake LSP gets the INITIALIZED notification
            let init_notif = r_lsp.recv_timeout(ONE_SEC).expect("Did not get INITIALIZED notification");
            assert!(matches!(init_notif, Message::Notification(notif) if notif.method == "initialized"));
        });

        // test initialize() and compare server_info
        let res = server.initialize(init_req, TWO_SECS).expect("Initialize should succeed");
        assert_eq!(server.status(), ServerStatus::Running, "Server status should be RUNNING");
        assert_eq!(server.server_info, server_info, "Server info should be updated");

        lsp_thread.join().expect("LSP thread panicked");
    }
}

/// Tests spawining real CMDs on linux and their exit statuses and signals. Mocking child is overkill
#[cfg(unix)]
#[cfg(test)]
mod test_shutdown_with_real_cmd_as_fake_lspserver {
    use super::test_utils::*;
    use super::*;
    use std::{process, thread, time::Duration};

    fn fake_server(cmd: &str, args: &str) -> LspServer {
        //setup_test_logger(); // enable to see logs
        let mut server = LspServer::new(cmd, args, "{}").expect("Should create test server");
        server
    }

    fn assert_resources(server: &LspServer) {
        assert!(server.resources.dispatcher.is_none(), "Dispatcher should be None after shutdown");
        assert!(server.resources.transport.is_none(), "Transport should be None after shutdown");
        assert!(server.resources.child.is_none(), "Child should be None after shutdown");
        assert!(server.sender.is_none(), "Sender should be None after shutdown");
    }

    fn assert_teardown_idempotent(mut server: LspServer, timeout: Duration, desc: &str, expected_exit: ExitType) {
        // First shutdown
        let exit_status = server.teardown(timeout);
        assert!(exit_status.is_ok(), "{desc}: first shutdown should succeed");

        assert_resources(&server);
        assert_exit_status(exit_status.unwrap(), expected_exit);

        // Second shutdown (idempotency) ----v
        let level = logger::get_log_level();
        logger::disable_logging(); // disable logging to avoid duplicated messages in log

        let result2 = server.teardown(timeout);
        assert!(result2.is_ok(), "{desc}: second shutdown should not panic");
        assert!(result2.unwrap().is_none(), "{desc}: second shutdown should return None");
        assert_resources(&server);

        logger::set_log_level(level); // reenable log
    }

    #[test]
    fn test_graceful_shutdown_lsp_exit() {
        let server = fake_server("true", ""); // true returns immediately, .e.g. simulates LSP voluntary exit
        assert_teardown_idempotent(server, Duration::from_secs(1), "graceful + LSP exit", ExitType::Code(0));
    }

    #[test]
    fn test_graceful_shutdown_lsp_killed() {
        let mut server = fake_server("sleep", "5"); // simulate external kill for LSP
        server.resources.child.as_mut().unwrap().kill().expect("Failed to kill child LSP process");
        assert_teardown_idempotent(server, Duration::from_secs(1), "graceful + LSP killed", ExitType::Signal((9)));
    }

    #[test]
    fn test_forced_shutdown() {
        let server = fake_server("sleep", "5");
        assert_teardown_idempotent(server, Duration::ZERO, "forced", ExitType::Signal(9));
    }

    #[test]
    fn test_graceful_escalates_to_forced_shutdown() {
        let server = fake_server("sleep", "5");
        assert_teardown_idempotent(server, Duration::from_millis(100), "graceful -> forced", ExitType::Signal(9));
    }
}

#[cfg(test)]
mod test_lspserver_shutdown_with_mock {
    use super::*;
    use test_utils::*;

    #[test]
    fn test_shutdown_protocol_success() {
        let (mut server, r_lsp, s_lsp) = mock_server();
        let server_data = Arc::clone(&server.server_data);
        let shutdown_req = Request::new_shutdown();
        let req_id = shutdown_req.id.clone();

        // Simulate the LSP server's in a separate thread
        let lsp_thread = std::thread::spawn(move || {
            // assert fake LSP got the SHUTDOWN request
            let shutdown_msg = r_lsp.recv_timeout(ONE_SEC).expect("Did not get SHUTDOWN message");
            assert!(matches!(shutdown_msg, Message::Request(req) if req.id == req_id && req.method == "shutdown"));

            // send response back (e.g. push directly to server_data)
            let shutdown_resp = Response::new_ok(req_id, None::<bool>).unwrap();
            server_data.lock().unwrap().responses_unticked.push_back(shutdown_resp);

            // assert fake LSP get the final EXIT notification.
            let exit_msg = r_lsp.recv_timeout(ONE_SEC).expect("Did not get EXIT message");
            assert!(matches!(exit_msg, Message::Notification(notif) if notif.method == "exit"));
        });

        // test shutdown() logic and assert the status
        let res = server.shutdown(shutdown_req, TWO_SECS).expect("Shutdown should succeed");
        assert_eq!(server.status(), ServerStatus::Exiting, "Server status should be EXITING");

        lsp_thread.join().expect("LSP thread panicked");
    }

    #[test]
    fn test_shutdown_protocol_timeout() {
        let (mut server, r_lsp, s_lsp) = mock_server();
        let shutdown_req = Request::new_shutdown();

        // Run the shutdown() logic, but provide no response. Use short timeout as well.
        let result = server.shutdown(shutdown_req, Duration::from_millis(50));
        assert!(
            matches!(result, Err(ref e) if e.downcast_ref::<io::Error>().unwrap().kind() == io::ErrorKind::TimedOut),
            "Shutdown should return Timeout error"
        );
        assert_eq!(server.status(), ServerStatus::ShuttingDown, "Server status should remain SHUTTING_DOWN");
    }

    #[test]
    fn test_shutdown_protocol_err() {
        let (mut server, _, _) = mock_server(); // note that channels are dropped
        let shutdown_req = Request::new_shutdown();

        // Run the shutdown logic, but channel is already dropped. So we'll get an error on send
        let result = server.shutdown(shutdown_req, Duration::from_millis(50));
        assert!(
            matches!(result, Err(ref e) if e.downcast_ref::<crossbeam_channel::SendError<Message>>().is_some()),
            "Shutdown should return channel error"
        );
        assert_eq!(server.status(), ServerStatus::ShuttingDown, "Server status should remain SHUTTING_DOWN");
    }
}

#[cfg(test)]
mod test_lspserver_teardown {
    use super::LspServer;
    use std::{sync::Arc, thread, time::Duration};

    #[test]
    fn test_teardown_single_entry() {
        let mut server = LspServer::new("true", "", "{}").unwrap();
        let server = Arc::new(std::sync::Mutex::new(server));

        let mut handles = vec![];
        for _ in 0..5 {
            let server = Arc::clone(&server);
            handles.push(thread::spawn(move || server.lock().unwrap().teardown(Duration::ZERO)));
        }

        // gather all threads results
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        let enter_count = results.iter().filter(|r| matches!(r, Ok(Some(_)))).count();
        let skip_count = results.iter().filter(|r| matches!(r, Ok(None))).count();

        assert_eq!(enter_count, 1, "Only one thread should perform teardown");
        assert_eq!(skip_count, results.len() - 1, "Other threads should see teardown already in progress");
    }
}

#[cfg(test)]
mod test_send_message {
    use super::{mock_server, LspServer, Message, Notification, Request, Response};
    use crate::test_utils::TENTH_OF_SEC;

    #[test]
    fn test_send_message_errors() {
        let test_cases = [("drop", "Failed to send to LSP"), ("take", "No LSP sender channel")];

        for (case, expected) in test_cases {
            let (mut server, r_lsp, s_lsp) = mock_server();
            match case {
                "drop" => drop(r_lsp),
                "take" => server.sender = None,
                _ => unreachable!(),
            }
            let res = server.send_message(Request::new_shutdown());
            assert!(res.is_err(), "send_message should fail");
            let actual = res.unwrap_err().to_string();
            assert!(actual.contains(expected), "unexpected error: {actual} != {expected}");

            let server_data = server.server_data.lock().unwrap();
            assert!(server_data.request_ticks.is_empty(), "No tick should be recorded");
        }
    }

    #[test]
    fn test_send_message_ok() {
        fn test_send<M: Into<Message>>(msg: M) {
            let (mut server, r_lsp, s_lsp) = mock_server();
            let res = server.send_message(msg);
            assert_eq!(res.unwrap(), Some(true), "send_message should succeed");

            let lsp_msg = r_lsp.recv_timeout(TENTH_OF_SEC).expect("Should receive message");
            let server_data = server.server_data.lock().unwrap();
            match lsp_msg {
                Message::Request(req) => {
                    // TODO: test clear diagnostic
                    // TODO: test tick creation?
                    assert!(server_data.request_ticks.contains_key(&req.id), "Expected to record request ID")
                }
                _ => assert!(server_data.request_ticks.is_empty(), "Expected to record request ID"),
            }
        }

        test_send(Request::new_shutdown());
        test_send(Message::Request(Request::new_shutdown()));

        test_send(Notification::new("exit"));
        test_send(Message::Notification(Notification::new("exit")));

        test_send(Response::new_ok(3, "success").unwrap());
        test_send(Message::Response(Response::new_ok(3, "success").unwrap()));
        test_send(Response::new_err("", -1, "fail"));
        test_send(Message::Response(Response::new_err("", -1, "fail")));
    }

    #[test]
    }
}
