use std::{
    fmt,
    io::{self, BufRead, Read, Write},
};

use crate::bufext::BufReadEofExt;

use serde::de::Error as SerdeError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// # Examples
/// Creating Requests, Notification, Responses (both types) and Messages
///
/// ```rust
/// use lspce_module::{Request, RequestId, Response, Notification};
/// let _ = Request::new(3, "shutdown", serde_json::Value::Null);
/// let _ = Notification::new("exit", serde_json::Value::Null);
/// let _ = Response::new_ok(1, "success");
/// let _ = Response::new_err("", -1, "fail");
///
/// let _ = Request::default();
/// let _ = Notification::default();
/// let _ = Response::default();
/// ```
///
/// This should fail, since the intended way to create Requests, Notifications and Responses are via ::new
/// ```compile_fail
/// use lspce_module::{Request, RequestId};
/// let _ =  Request{id: RequestId::from(3), method: "shutdown".to_string(), params: serde_json::Value::Null, content: String::new(), request_tick: None};
/// ```
/// ```compile_fail
/// use lspce_module::Notification;
/// let _ = Notification { method: "exit".to_string(), params: serde_json::Value::Null, content: String::new() };
/// ```
/// ```compile_fail
/// use lspce_module::{Response, RequestId};
/// let _ = Response { id: RequestId::from(1), result: Some(serde_json::Value::Null), error: None, content: String::new(), request_tick: String::new() };
/// ```
///
/// Generic and specific Message type creation
/// ```rust
/// use lspce_module::{Message, Request, Response};
/// let request_json = r#"{"id": 1, "method": "shutdown"}"#;
/// let m = Message::from_str(request_json).unwrap(); // Messsage::Request
/// let r = Message::from_str_typed::<Request>(request_json).unwrap(); // Request
/// match m {
///    Message::Request(req) => assert_eq!(req, r),
///    _ => panic!("Expected Message::Request variant"),
/// }
/// assert!(Message::from_str_typed::<Response>(request_json).is_err(), "Not a Response JSON");
/// ```

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum Message {
    Request(Request),
    Response(Response),
    Notification(Notification),
}

#[derive(Debug, Default, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct RequestId(IdRepr);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(untagged)]
enum IdRepr {
    Number(i32),
    String(String),
}

impl Default for IdRepr {
    fn default() -> Self {
        IdRepr::Number(0)
    }
}

impl From<i32> for RequestId {
    fn from(id: i32) -> RequestId {
        RequestId(IdRepr::Number(id))
    }
}

macro_rules! impl_from_string_for_idrepr {
    ($($t:ty),*) => {
        $(
            impl From<$t> for RequestId {
                fn from(id: $t) -> RequestId {
                    RequestId(IdRepr::String(id.into()))
                }
            }
        )*
    };
}
impl_from_string_for_idrepr!(String, &str, &String); // all String-like types

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            IdRepr::Number(n) => fmt::Display::fmt(n, f),
            // Use debug here, to make it clear that `92` and `"92"` are different,
            // and to reduce WTF factor if the sever uses `" "` as an ID.
            IdRepr::String(s) => fmt::Debug::fmt(s, f),
        }
    }
}

#[derive(Clone, Copy, Debug)]
#[allow(unused)]
pub enum ErrorCode {
    // Defined by JSON RPC:
    ParseError = -32700,
    InvalidRequest = -32600,
    MethodNotFound = -32601,
    InvalidParams = -32602,
    InternalError = -32603,
    ServerErrorStart = -32099,
    ServerErrorEnd = -32000,

    /// Error code indicating that a server received a notification or
    /// request before the server has received the `initialize` request.
    ServerNotInitialized = -32002,
    UnknownError = -32001,

    // Defined by the protocol:
    /// The client has canceled a request and a server has detected
    /// the cancel.
    RequestCanceled = -32800,

    /// The server detected that the content of a document got
    /// modified outside normal conditions. A server should
    /// NOT send this error code if it detects a content change
    /// in it unprocessed messages. The result even computed
    /// on an older state might still be useful for the client.
    ///
    /// If a client decides that a result is not of any use anymore
    /// the client should cancel the request.
    ContentModified = -32801,

    /// The server cancelled the request. This error code should
    /// only be used for requests that explicitly support being
    /// server cancellable.
    ///
    /// @since 3.17.0
    ServerCancelled = -32802,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct Request {
    pub id: RequestId,
    pub method: String,
    #[serde(default = "serde_json::Value::default")]
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub params: serde_json::Value,
    #[serde(skip)]
    content: String,
    #[serde(skip)]
    pub request_tick: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct Response {
    // JSON RPC allows this to be null if it was impossible
    // to decode the request's id. Ignore this special case
    // and just die horribly.
    pub id: RequestId,

    // serde will treat identically both missing field and explicit null
    // e.g. receiving no result and "result": null will result in None
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ResponseError>,
    #[serde(skip)]
    content: String,
    #[serde(skip)]
    pub request_tick: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ResponseError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq)]
pub struct Notification {
    pub method: String,
    #[serde(default = "serde_json::Value::default")]
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub params: serde_json::Value,
    #[serde(skip)]
    content: String,
}

// implement From, TryFrom and Display and into_string for each message type
macro_rules! impl_methods {
    ($($variant:ident),*) => {
        $(
            impl From<$variant> for Message {
                fn from(value: $variant) -> Self {
                    Message::$variant(value)
                }
            }

            impl TryFrom<Message> for $variant {
                type Error = anyhow::Error;

                fn try_from(message: Message) -> Result<Self, Self::Error> {
                    match message {
                        Message::$variant(value) => Ok(value),
                        _ => anyhow::bail!("Expected {} but got {}", stringify!($variant), message.msg_type()),
                    }
                }
            }

            impl $variant {
                pub fn into_string(self) -> String {
                    self.content
                }
            }

             impl fmt::Display for $variant {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    display_message(f, self, stringify!($variant), &self.content)
                }
            }
        )*
    };
}

impl_methods!(Request, Response, Notification);

fn display_message<T: Serialize>(f: &mut fmt::Formatter<'_>, value: &T, type_name: &str, content: &str) -> fmt::Result {
    match serde_json::to_string_pretty(value) {
        Ok(pretty) => write!(f, "{} {}", type_name, pretty),
        Err(e) => write!(f, "{} {} {}", type_name, e, content),
    }
}

impl Message {
    /// Serialize the message to a JSON string.
    pub fn to_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserialize a Message from a JSON string.
    pub fn from_str(json: &str) -> anyhow::Result<Message, serde_json::Error> {
        // Although we could just use serde_json::from_str(json), but then Request could be
        // deserialized as Response or Notification. So let's validate message type manually
        let value: serde_json::Value = serde_json::from_str(json)?;

        // Simple detection of message type based on `method' and `id' fields
        match (value.get("method").is_some(), value.get("id").is_some()) {
            (true, true) => Ok(Message::Request(serde_json::from_value::<Request>(value)?)),
            (true, false) => Ok(Message::Notification(serde_json::from_value::<Notification>(value)?)),
            (false, true) => {
                if value.get("result").is_some() == value.get("error").is_some() {
                    return Err(SerdeError::custom("Response must have either result XOR error"));
                }
                Ok(Message::Response(serde_json::from_value::<Response>(value)?))
            }
            (false, false) => Err(SerdeError::custom("Message must have either method or id")),
        }
    }

    /// Deserialize specific message type from a JSON string.
    pub fn from_str_typed<T>(json: &str) -> anyhow::Result<T>
    where
        T: TryFrom<Message, Error = anyhow::Error>,
    {
        Self::from_str(json)?.try_into()
    }

    fn content(&self) -> &str {
        match self {
            Message::Request(req) => &req.content,
            Message::Response(resp) => &resp.content,
            Message::Notification(notif) => &notif.content,
        }
    }

    fn set_content(&mut self, content: String) {
        match self {
            Message::Request(req) => req.content = content,
            Message::Response(resp) => resp.content = content,
            Message::Notification(notif) => notif.content = content,
        }
    }

    pub fn msg_type(&self) -> &'static str {
        match self {
            Message::Request(_) => "Request",
            Message::Response(_) => "Response",
            Message::Notification(_) => "Notification",
        }
    }

    pub fn read(r: &mut impl BufRead) -> io::Result<Option<Message>> {
        Message::_read(r)
    }
    fn _read(r: &mut dyn BufRead) -> io::Result<Option<Message>> {
        let text = match read_msg_text(r)? {
            None => return Ok(None),
            Some(text) => text,
        };
        let mut msg: Message = Message::from_str(&text)?;
        msg.set_content(text);
        Ok(Some(msg))
    }

    pub fn write(self, w: &mut impl Write) -> io::Result<()> {
        self._write(w)
    }
    fn _write(self, w: &mut dyn Write) -> io::Result<()> {
        #[derive(Serialize)]
        struct JsonRpc {
            jsonrpc: &'static str,
            #[serde(flatten)]
            msg: Message,
        }
        let text = serde_json::to_string(&JsonRpc { jsonrpc: "2.0", msg: self })?;
        write_msg_text(w, &text)
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Message::Request(request) => request.fmt(f),
            Message::Response(response) => response.fmt(f),
            Message::Notification(notification) => notification.fmt(f),
        }
    }
}

impl Response {
    pub fn new_err(id: impl Into<RequestId>, code: i32, message: impl Into<String>) -> Response {
        let error = ResponseError { code, message: message.into(), data: None };
        Response { id: id.into(), result: None, error: Some(error), ..Default::default() }
    }

    pub fn new_ok(id: impl Into<RequestId>, result: impl Serialize) -> Result<Response, serde_json::Error> {
        Ok(Response { id: id.into(), result: Some(serde_json::to_value(result)?), ..Default::default() })
    }
}

impl Request {
    pub fn new(
        id: impl Into<RequestId>, m: impl Into<String>, params: impl Serialize,
    ) -> Result<Request, serde_json::Error> {
        Ok(Request { id: id.into(), method: m.into(), params: serde_json::to_value(params)?, ..Default::default() })
    }
}

impl Notification {
    pub fn new(method: impl Into<String>, params: impl Serialize) -> Result<Notification, serde_json::Error> {
        Ok(Notification { method: method.into(), params: serde_json::to_value(params)?, ..Default::default() })
    }
}

fn read_msg_text(inp: &mut dyn BufRead) -> io::Result<String> {
    fn invalid_data(msg: &str, line: &str) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, format!("{}: {:?}", msg, line))
    }

    let mut content_length = None;
    let mut line = String::new();

    loop {
        line.clear();
        inp.read_line_or_eof(&mut line)?; // read_line_or_eof returns Err on EOF

        if !line.ends_with("\r\n") {
            return Err(invalid_data("Malformed header (no CRLF)", &line));
        }
        if line.len() == 2 {
            break; // empty line, just "\r\n". This is the end of headers
        }
        if let Some(rest) = &line[..line.len() - 2].strip_prefix("Content-Length: ") {
            content_length = Some(rest.parse().map_err(|_| invalid_data("Invalid Content-Length value", &line))?);
        }
    }

    let size = content_length.ok_or_else(|| invalid_data("Missing Content-Length header", &line))?;
    let mut buf = vec![0u8; size];
    inp.read_exact(&mut buf)?;
    let buf = String::from_utf8(buf).map_err(|_| invalid_data("Body isn't a valid UTF-8", &line))?;

    Ok(buf)
}

fn write_msg_text(out: &mut dyn Write, msg: &str) -> io::Result<()> {
    out.write_all(format!("Content-Length: {}\r\n\r\n{}", msg.len(), &msg).as_bytes())?;
    out.flush()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::read_msg_text;
    use super::{Message, Notification, Request, RequestId, Response, ResponseError};
    use std::io::{self, BufReader};

    #[test]
    fn test_msg_serialization() {
        let test_cases: [(Message, &str); 4] = [
            (Request::new(1, "shutdown", serde_json::Value::Null).unwrap().into(), r#"{"id":1,"method":"shutdown"}"#),
            (Notification::new("exit", serde_json::Value::Null).unwrap().into(), r#"{"method":"exit"}"#),
            (Response::new_ok(3, "success").unwrap().into(), r#"{"id":3,"result":"success"}"#),
            (Response::new_err("", -1, "fail").into(), r#"{"id":"","error":{"code":-1,"message":"fail"}}"#),
        ];

        for (msg, expected_json) in test_cases {
            let serialized = msg.to_string().unwrap();
            assert_eq!(serialized, expected_json);
        }
    }
    #[test]
    fn test_msg_deserialization() {
        let test_cases = [
            (
                "request",
                vec![
                    (r#""id": 1, "method": "shutdown", "params": null"#, "shutdown"), // null params
                    (r#""id": "req-abc", "method": "textDocument/hover""#, "textDocument/hover"), // string id, missing params
                    (r#""id": 42, "method": "workspace_symbol", "params": {}"#, "workspace_symbol"), // empty object params
                    (r#""id": 999, "method": "custom/method_123", "params": [1,2,3]"#, "custom/method_123"), // array params
                ],
            ),
            (
                "notification",
                vec![
                    (r#""method": "exit", "params": null"#, "exit"), // null params
                    (r#""method": "initialized""#, "initialized"),   // missing params
                    (r#""method": "textDocument/didOpen", "params": {"uri": "file://test"}"#, "textDocument/didOpen"), // object params, slash method
                ],
            ),
            (
                "response",
                vec![
                    (r#""id": 1, "result": "success""#, ""),   // success with string result
                    (r#""id": "resp-2", "result": null"#, ""), // success with explicit null result
                    // (r#""id": 3"#, ""),                             // implicit null for both result and error - should fail on proto verification
                    (r#""id": 4, "result": {}"#, ""), // success with empty result
                    (r#""id": 5, "result": {"status": "ok"}"#, ""), // success with data
                    (r#""id": 6, "error": null"#, ""), // explicit null error
                    (r#""id": 8, "error": {"code": -1, "message": "err1"}"#, ""), // error with no data
                    (r#""id": 9, "error": {"code": -2, "message": "err2", "data": {}}"#, ""), // error with empty data
                    (r#""id": 10, "error": {"code": -2, "message": "err2", "data": {"k":"v"}}"#, ""), // error with some data
                ],
            ),
        ];

        for (msg_type, cases) in test_cases {
            for (json_fields, expected) in cases {
                let full_json_str = format!(r#"{{"jsonrpc": "2.0", {}}}"#, json_fields);

                let lsp_message = format!("Content-Length: {}\r\n\r\n{}", full_json_str.len(), full_json_str);
                let mut cursor = std::io::Cursor::new(lsp_message.as_bytes());
                let msg = Message::read(&mut cursor).unwrap().unwrap();

                assert_eq!(msg.content(), &full_json_str, "Content should contain original JSON");

                match (msg_type, &msg) {
                    ("request", Message::Request(req)) => {
                        assert_eq!(req.method, expected, "Request method mismatch for: {}", json_fields);
                    }

                    ("notification", Message::Notification(notif)) => {
                        assert_eq!(notif.method, expected, "Notification method mismatch for: {}", json_fields);
                    }

                    ("response", Message::Response(resp)) => {
                        // convert input json to expected result and error
                        let json: serde_json::Value = serde_json::from_str(&full_json_str).unwrap();

                        let extract_expected = |key: &str| match json.get(key) {
                            None => None,
                            Some(v) if v.is_null() => None,
                            Some(v) => Some(v.clone()),
                        };
                        let expected_result = extract_expected("result");
                        let expected_error = extract_expected("error")
                            .and_then(|v| serde_json::from_value::<ResponseError>(v.clone()).ok());

                        fn compare_fields<T>(
                            actual: &Option<T>, expected: &Option<T>, field_name: &str, json_fields: &str,
                        ) where
                            T: PartialEq + std::fmt::Debug,
                        {
                            match (actual, expected) {
                                (None, None) => {} // ok. Both are None
                                (Some(actual), Some(expected)) => {
                                    assert_eq!(actual, expected, "{} mismatch for: {}", field_name, json_fields);
                                }
                                _ => panic!(
                                    "{} mismatch for: {} | actual={:?}, expected={:?}",
                                    field_name, json_fields, actual, expected
                                ),
                            }
                        }

                        compare_fields(&resp.result, &expected_result, "Result", json_fields);
                        compare_fields(&resp.error, &expected_error, "Error", json_fields);
                    }
                    _ => panic!("Type mismatch for {} | {} {}", json_fields, msg_type, &msg),
                }
            }
        }
    }

    #[test]
    fn test_from_str() {
        let test_cases = [
            (r#"{"id": 1, "method": "shutdown"}"#, "Request"),
            (r#"{"id": 1, "result": "success"}"#, "Response"),
            (r#"{"id": 3, "error": {"code": -1, "message": "test"}}"#, "Response"),
            (r#"{"method": "exit"}"#, "Notification"),
        ];

        for (json, expected_type) in test_cases {
            // always succeed to create a Message from a valid json. Ensure expected type
            let message =
                Message::from_str(json).expect(&format!("from_str should succeed for valid msg json: {}", json));
            assert_eq!(message.msg_type(), expected_type, "from_str should return {} for: {}", expected_type, json);

            // Try to parse to specific types. Succeed only for expected one
            for (type_name, result_ok) in [
                ("Request", Message::from_str_typed::<Request>(json).is_ok()),
                ("Response", Message::from_str_typed::<Response>(json).is_ok()),
                ("Notification", Message::from_str_typed::<Notification>(json).is_ok()),
            ] {
                let should_succeed = type_name == expected_type;
                assert_eq!(result_ok, should_succeed, "{} parse for: {}", type_name, json);
            }
        }
    }

    struct TestCase {
        name: &'static str,
        input: &'static [u8],
        expected: Result<Option<String>, io::ErrorKind>,
    }

    #[test]
    fn test_read_msg_text() {
        let test_cases = vec![
            // valid cases --------------------------------------------------v
            TestCase { name: "Valid", input: b"Content-Length: 2\r\n\r\n{}", expected: Ok(Some("{}".to_string())) },
            TestCase {
                name: "Valid with 2 headers",
                input: b"Content-Type: application/jsonrpc; charset=utf-8\r\nContent-Length: 2\r\n\r\n{}",
                expected: Ok(Some("{}".to_string())),
            },
            TestCase { name: "Empty", input: b"Content-Length: 0\r\n\r\n", expected: Ok(Some("".to_string())) },
            TestCase {
                name: "Valid With junk",
                input: b"Content-Length: 2\r\n\r\n{}junk",
                expected: Ok(Some("{}".to_string())),
            },
            // invalid cases ------------------------------------------------v
            TestCase {
                name: "Malformed header (header part doesn't end with CRLF)",
                input: b"Content-Length: 2\r\n{}",
                expected: Err(io::ErrorKind::InvalidData),
            },
            TestCase {
                name: "Malformed header (no colon)",
                input: b"Content-Length 2\r\n\r\n",
                expected: Err(io::ErrorKind::InvalidData),
            },
            TestCase {
                name: "Malformed header (no space after colon)",
                input: b"Content-Length:2\r\n\r\n{}",
                expected: Err(io::ErrorKind::InvalidData),
            },
            TestCase {
                name: "No mandatory Content-Length header",
                input: b"Header: value\r\n\r\n{}",
                expected: Err(io::ErrorKind::InvalidData),
            },
            TestCase {
                name: "Malformed header (content-Length isn't a number)",
                input: b"Content-Length: abc\r\n\r\n",
                expected: Err(io::ErrorKind::InvalidData),
            },
            TestCase {
                name: "Content shorter than Content-Length (EOF)",
                input: b"Content-Length: 20\r\n\r\nshort",
                expected: Err(io::ErrorKind::UnexpectedEof),
            },
            TestCase {
                name: "EOF right after headers",
                input: b"Content-Length: 10\r\n\r\n",
                expected: Err(io::ErrorKind::UnexpectedEof),
            },
            TestCase { name: "Empty input (EOF)", input: b"", expected: Err(io::ErrorKind::UnexpectedEof) },
            TestCase {
                name: "Invalid UTF-8 in content",
                input: b"Content-Length: 4\r\n\r\n\xff\xfe\xfd\xfc",
                expected: Err(io::ErrorKind::InvalidData),
            },
            // edge cases --------------------------------------------------v
            // will be handled as valid but wrong text (RCRLF instead of {}), since headers should END with \r\n\r\n sequence
            TestCase {
                name: "Additional CRLF",
                input: b"Content-Length: 2\r\n\r\n\r\n{}",
                expected: Ok(Some("\r\n".to_string())),
            },
        ];

        for case in test_cases {
            let mut reader = BufReader::new(case.input);
            let result = read_msg_text(&mut reader);

            match (result, case.expected) {
                (Ok(res), Ok(Some(exp))) => assert_eq!(res, exp, "Test case <{}> failed (text)", case.name),
                (Err(res_err), Err(exp_err_kind)) => {
                    assert_eq!(res_err.kind(), exp_err_kind, "Test case <{}> failed (err)", case.name)
                }
                (res, exp) => panic!("Test case '{}' failed: Expected {:?}, got {:?}", case.name, exp, res),
            }
        }
    }
}
