#[cfg(test)]
use super::*;

#[test]
fn test_lsp_server_new_invalid_command() {
    let server = LspServer::new("nonexistent_command_with_random_suffix", "", "");
    assert!(matches!(server, Err(ref error) if error.0.contains("Failed to spawn LSP server process")));
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
                e.0.contains("Failed to parse emacs_envs JSON"),
                "Test '{}' failed. Expected JSON parsing error, but got: <{}>",
                description,
                e.0
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
                !e.0.contains("Failed to parse emacs_envs JSON"),
                "Test '{}' failed. Unexpected JSON parsing error: <{}>",
                description,
                e.0
            );
        }
    }
}
