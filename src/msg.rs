use std::{
    fmt,
    io::{self, BufRead, Read, Write},
};

use serde::de::Error as SerdeError;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum Message {
    Request(Request),
    Response(Response),
    Notification(Notification),
}

macro_rules! impl_message_from {
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
        )*
    };
}

impl_message_from!(Request, Response, Notification);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(transparent)]
pub struct RequestId(IdRepr);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(untagged)]
enum IdRepr {
    Number(i32),
    String(String),
}

impl From<i32> for RequestId {
    fn from(id: i32) -> RequestId {
        RequestId(IdRepr::Number(id))
    }
}

impl From<String> for RequestId {
    fn from(id: String) -> RequestId {
        RequestId(IdRepr::String(id))
    }
}

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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Request {
    pub id: RequestId,
    pub method: String,
    #[serde(default = "serde_json::Value::default")]
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub params: serde_json::Value,
    #[serde(skip)]
    pub content: String,
    #[serde(skip)]
    pub request_tick: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
    pub content: String,
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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Notification {
    pub method: String,
    #[serde(default = "serde_json::Value::default")]
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub params: serde_json::Value,
    #[serde(skip)]
    pub content: String,
}

impl Message {
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

    pub fn from_str_typed<T>(json: &str) -> anyhow::Result<T>
    where
        T: TryFrom<Message, Error = anyhow::Error>,
    {
        Self::from_str(json)?.try_into()
    }

    pub fn content(&self) -> &str {
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

    pub fn msg_type(&self) -> &'static str {
        match self {
            Message::Request(_) => "Request",
            Message::Response(_) => "Response",
            Message::Notification(_) => "Notification",
        }
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match serde_json::to_string_pretty(self) {
            Ok(pretty) => write!(f, "{} {}", self.msg_type(), pretty),
            Err(e) => write!(f, "{} {} {}", self.msg_type(), e, self.content()),
        }
    }
}

impl Response {
    pub fn new_err(id: RequestId, code: i32, message: String) -> Response {
        let error = ResponseError { code, message, data: None };
        Response { id, result: None, error: Some(error), content: String::new(), request_tick: String::new() }
    }
}

impl Notification {
    pub fn new(method: impl Into<String>, params: impl Serialize) -> Result<Notification, serde_json::Error> {
        Ok(Notification { method: method.into(), params: serde_json::to_value(params)?, content: String::new() })
    }
}

fn read_msg_text(inp: &mut dyn BufRead) -> io::Result<Option<String>> {
    fn invalid_data(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, error)
    }
    macro_rules! invalid_data {
        ($($tt:tt)*) => (invalid_data(format!($($tt)*)))
    }

    let mut size = None;
    let mut buf = String::new();
    loop {
        buf.clear();
        if inp.read_line(&mut buf)? == 0 {
            return Ok(None);
        }
        if !buf.ends_with("\r\n") {
            return Err(invalid_data!("malformed header: {:?}", buf));
        }
        let buf = &buf[..buf.len() - 2];
        if buf.is_empty() {
            break;
        }
        let mut parts = buf.splitn(2, ": ");
        let header_name = parts.next().unwrap();
        let header_value = parts.next().ok_or_else(|| invalid_data!("malformed header: {:?}", buf))?;
        if header_name == "Content-Length" {
            size = Some(header_value.parse::<usize>().map_err(invalid_data)?);
        }
    }
    let size: usize = size.ok_or_else(|| invalid_data!("no Content-Length"))?;
    let mut buf = buf.into_bytes();
    buf.resize(size, 0);
    inp.read_exact(&mut buf)?;
    let buf = String::from_utf8(buf).map_err(invalid_data)?;

    Ok(Some(buf))
}

fn write_msg_text(out: &mut dyn Write, msg: &str) -> io::Result<()> {
    out.write_all(format!("Content-Length: {}\r\n\r\n{}", msg.len(), &msg).as_bytes())?;
    out.flush()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{Message, Notification, Request, RequestId, Response, ResponseError};

    #[test]
    fn serialize_request_with_null_params() {
        let msg = Message::Request(Request {
            id: RequestId::from(3),
            method: "shutdown".into(),
            params: serde_json::Value::Null,
            content: "".to_string(),
            request_tick: Some("".to_string()),
        });
        let serialized = serde_json::to_string(&msg).unwrap();

        assert_eq!("{\"id\":3,\"method\":\"shutdown\"}", serialized);
    }

    #[test]
    fn serialize_notification_with_null_params() {
        let msg = Message::Notification(Notification {
            method: "exit".into(),
            params: serde_json::Value::Null,
            content: "".to_string(),
        });
        let serialized = serde_json::to_string(&msg).unwrap();

        assert_eq!("{\"method\":\"exit\"}", serialized);
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
}
