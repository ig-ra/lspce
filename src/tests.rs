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
