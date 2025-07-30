use std::io::{self, BufRead, Read};

/// Extension trait for BufRead to limit line length and convert EOF (e.g. OK(0)) to an error.

pub trait BufReadEofExt {
    /// Reads a line, returning an error (io::ErrorKind::UnexpectedEof) if EOF is reached
    fn read_line_or_eof(&mut self, buf: &mut String) -> io::Result<usize>;

    /// Reads up to `max_len` chars from a line, returning either
    /// - io::ErrorKind::UnexpectedEof if EOF is reached, or
    /// - io::ErrorKind::QuotaExceeded if the line exceeds `max_len` chars.
    /// In he latter case the rest of the line is discarded.    
    fn read_line_limited_or_eof(&mut self, buf: &mut String, max_len: usize) -> io::Result<usize>;
}

fn check_eof(n: usize) -> io::Result<usize> {
    if n == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Stream closed (EOF)"));
    }
    Ok(n)
}

impl<T: BufRead + Read> BufReadEofExt for T {
    /// Pay attention that internally we use OK(0) result to check for EOF.
    /// We could possibly check that buffer ends with \n, detecting EOF earlier. Although Ok(0) approach requires one
    /// more read (e.g. one read to consume everything in internal buffer and consequitive read to read 0 bytes) but
    /// it's more aligned with the expected behaviour of BufReader.
    fn read_line_or_eof(&mut self, buf: &mut String) -> io::Result<usize> {
        check_eof(self.read_line(buf)?)
    }

    fn read_line_limited_or_eof(&mut self, buf: &mut String, max_len: usize) -> io::Result<usize> {
        let mut limited_reader = self.by_ref().take(max_len as u64);
        let n = check_eof(limited_reader.read_line(buf)?)?;

        if limited_reader.limit() == 0 && !buf.ends_with('\n') {
            let _ = self.skip_until(b'\n'); // discard remaining portion till the nearest what end of line
            buf.push('\n');
            return Err(io::Error::new(io::ErrorKind::QuotaExceeded, format!("Line exceeds {} limit", max_len)));
        }
        Ok(n)
    }
}

#[cfg(test)]
mod tests {
    use super::BufReadEofExt;
    use std::io::{BufRead, Cursor};

    #[test]
    fn test_read_line_or_eof_vs_read_line_on_empty_buffer() {
        let mut reader = Cursor::new("".as_bytes());
        let mut buf = String::new();

        let result = reader.read_line_or_eof(&mut buf);
        assert!(result.is_err() && result.unwrap_err().kind() == std::io::ErrorKind::UnexpectedEof);

        let result = reader.read_line(&mut buf);
        assert!(result.is_ok() && result.unwrap() == 0);
    }

    #[test]
    fn test_read_line_limited_discards_long_line() {
        use std::io::ErrorKind;
        let mut reader = Cursor::new(b"12345\n67890\r\nabcdefg JUNK DATA\r\nABCDEF\nEOF");
        let mut buf = String::new();
        const MAX_LEN: usize = 7;

        let steps = vec![
            ("12345\n", Ok(6)),                           // line shorter than MAX_LEN,  ends with \n
            ("67890\r\n", Ok(7)),                         // line exactly MAX_LEN, ends with \r\n
            ("abcdefg\n", Err(ErrorKind::QuotaExceeded)), // line longer than MAX_LEN. rest of line discarded
            ("ABCDEF\n", Ok(7)),                          // normal line, after he long one
            ("EOF", Ok(3)),                               // some data before EOF
            ("", Err(ErrorKind::UnexpectedEof)),          // attempt reading after EOF
        ];

        for (i, (expected_buf, expected_res)) in steps.into_iter().enumerate() {
            buf.clear();
            let result = reader.read_line_limited_or_eof(&mut buf, MAX_LEN);

            let err_msg = format!("Failed in step {}", i + 1);
            match (result, expected_res) {
                (Ok(len), Ok(exp)) => assert_eq!(len, exp, "{} (len) - {} vs. {}", err_msg, len, exp),
                (Err(e), Err(kind)) => assert_eq!(e.kind(), kind, "{} (err) - {:?} {:?}", err_msg, kind, e.kind()),
                (res, exp) => panic!("Expected {:?}, got {:?}", exp, res),
            }
            assert_eq!(buf, expected_buf, "{} (txt) - {} {}", err_msg, buf, expected_buf);
        }
    }
}
