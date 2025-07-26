use std::io::{self, BufRead};

pub trait BufReadEofExt {
    /// read_line that returns an error on EOF. Normal BufReader is blocked reader and will return OK(0) on EOF
    fn read_line_or_eof(&mut self, buf: &mut String) -> io::Result<usize>;
}

impl<T: BufRead + ?Sized> BufReadEofExt for T {
    fn read_line_or_eof(&mut self, buf: &mut String) -> io::Result<usize> {
        match self.read_line(buf) {
            Ok(0) => Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Stream closed (EOF)")),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::BufReadEofExt;
    use std::io::{BufRead, Cursor};

    /// compare read_line_or_eof with read_line on an empty buffer. This should simulate closed pipe/EOF,
    /// due to blocking reader behavior of BufReader.
    #[test]
    fn test_read_line_or_eof_vs_read_line_on_empty_buffer() {
        let mut reader = Cursor::new("".as_bytes());
        let mut buf = String::new();

        let result = reader.read_line_or_eof(&mut buf);
        assert!(result.is_err() && result.unwrap_err().kind() == std::io::ErrorKind::UnexpectedEof);

        let result = reader.read_line(&mut buf);
        assert!(result.is_ok() && result.unwrap() == 0);
    }
}
