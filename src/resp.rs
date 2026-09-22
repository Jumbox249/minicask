//! Just enough of RESP, the Redis wire protocol, for `redis-cli` and the
//! ordinary client libraries to talk to the server.
//!
//! RESP is a handful of one-character type prefixes, each followed by a
//! payload and `\r\n`:
//!
//! ```text
//! +OK\r\n                      simple string
//! -ERR unknown command\r\n     error
//! :42\r\n                      integer
//! $5\r\nhello\r\n              bulk string, length-prefixed and binary safe
//! $-1\r\n                      null
//! *2\r\n$3\r\nGET\r\n$1\r\nk\r\n   array, here a command with two arguments
//! ```
//!
//! Clients send commands as arrays of bulk strings. Servers reply with any
//! of the above. Interactive tools like `telnet` may send a bare line such as
//! `GET k`, which the protocol calls an inline command.

use std::io::{self, BufRead, Write};

/// Roughly Redis's `proto-max-bulk-len`. A bulk length beyond this is far
/// more likely to be garbage on the wire than a real value, and refusing it
/// keeps a bad header from turning into a 4GB allocation.
const MAX_BULK_LEN: usize = 512 * 1024 * 1024;
/// Likewise for the element count of a command array.
const MAX_ARRAY_LEN: usize = 1024 * 1024;

/// Anything the server can send back, and equally anything a client can
/// read. Command arguments are just the bulk strings inside an `Array`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reply {
    Simple(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    Array(Vec<Reply>),
}

impl Reply {
    /// The `+OK` that most write commands answer with.
    pub fn ok() -> Reply {
        Reply::Simple("OK".to_string())
    }

    /// A `-ERR` reply, prefixed the way Redis does so clients that parse the
    /// first word of an error see something familiar.
    pub fn err(message: impl Into<String>) -> Reply {
        Reply::Error(format!("ERR {}", message.into()))
    }

    pub fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        match self {
            Reply::Simple(s) => {
                w.write_all(b"+")?;
                w.write_all(s.as_bytes())?;
                w.write_all(b"\r\n")
            }
            Reply::Error(s) => {
                w.write_all(b"-")?;
                w.write_all(s.as_bytes())?;
                w.write_all(b"\r\n")
            }
            Reply::Integer(n) => write!(w, ":{n}\r\n"),
            Reply::Bulk(bytes) => {
                write!(w, "${}\r\n", bytes.len())?;
                w.write_all(bytes)?;
                w.write_all(b"\r\n")
            }
            Reply::Null => w.write_all(b"$-1\r\n"),
            Reply::Array(items) => {
                write!(w, "*{}\r\n", items.len())?;
                for item in items {
                    item.write_to(w)?;
                }
                Ok(())
            }
        }
    }
}

/// Read one command: the arguments of a RESP array, or the whitespace-split
/// words of an inline line. `Ok(None)` means the client hung up cleanly.
///
/// Anything malformed comes back as `io::ErrorKind::InvalidData`. The server
/// reports that to the client and closes the connection, since there is no
/// way to know where the next command starts once framing has been lost.
pub fn read_command<R: BufRead>(r: &mut R) -> io::Result<Option<Vec<Vec<u8>>>> {
    let Some(line) = read_line(r)? else {
        return Ok(None);
    };
    if line.is_empty() {
        // A stray blank line, which redis-cli sends when you hit enter on
        // nothing. Redis ignores it too.
        return Ok(Some(Vec::new()));
    }

    if line[0] != b'*' {
        let args = line
            .split(|b| b.is_ascii_whitespace())
            .filter(|word| !word.is_empty())
            .map(<[u8]>::to_vec)
            .collect();
        return Ok(Some(args));
    }

    let count = parse_len(&line[1..], MAX_ARRAY_LEN)?;
    let Some(count) = count else {
        // A null array. Nothing to run.
        return Ok(Some(Vec::new()));
    };

    let mut args = Vec::with_capacity(count.min(64));
    for _ in 0..count {
        let Some(header) = read_line(r)? else {
            return Err(invalid("connection closed inside a command"));
        };
        if header.first() != Some(&b'$') {
            return Err(invalid("expected a bulk string inside a command array"));
        }
        let Some(len) = parse_len(&header[1..], MAX_BULK_LEN)? else {
            return Err(invalid("null bulk string inside a command array"));
        };
        args.push(read_bulk(r, len)?);
    }
    Ok(Some(args))
}

/// Read one reply. This is what a client does with the bytes a server sends,
/// and what the test suite uses to talk to a running server.
pub fn read_reply<R: BufRead>(r: &mut R) -> io::Result<Reply> {
    let Some(line) = read_line(r)? else {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "connection closed while waiting for a reply",
        ));
    };
    let Some((&prefix, rest)) = line.split_first() else {
        return Err(invalid("empty line where a reply was expected"));
    };
    match prefix {
        b'+' => Ok(Reply::Simple(text(rest)?)),
        b'-' => Ok(Reply::Error(text(rest)?)),
        b':' => text(rest)?
            .parse()
            .map(Reply::Integer)
            .map_err(|_| invalid("integer reply is not a number")),
        b'$' => match parse_len(rest, MAX_BULK_LEN)? {
            Some(len) => Ok(Reply::Bulk(read_bulk(r, len)?)),
            None => Ok(Reply::Null),
        },
        b'*' => match parse_len(rest, MAX_ARRAY_LEN)? {
            Some(count) => {
                let mut items = Vec::with_capacity(count.min(64));
                for _ in 0..count {
                    items.push(read_reply(r)?);
                }
                Ok(Reply::Array(items))
            }
            None => Ok(Reply::Null),
        },
        other => Err(invalid_owned(format!(
            "unknown reply type {:?}",
            other as char
        ))),
    }
}

/// One line without its terminator. `None` at a clean end of stream.
fn read_line<R: BufRead>(r: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    let n = r.read_until(b'\n', &mut line)?;
    if n == 0 {
        return Ok(None);
    }
    if line.last() == Some(&b'\n') {
        line.pop();
    }
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    Ok(Some(line))
}

/// A length-prefixed payload plus its trailing `\r\n`.
fn read_bulk<R: BufRead>(r: &mut R, len: usize) -> io::Result<Vec<u8>> {
    let mut buf = vec![0u8; len + 2];
    r.read_exact(&mut buf)?;
    if &buf[len..] != b"\r\n" {
        return Err(invalid("bulk string is not terminated by CRLF"));
    }
    buf.truncate(len);
    Ok(buf)
}

/// The number after a `$` or `*`. `-1` means null; anything else negative,
/// non-numeric or absurdly large is a protocol error.
fn parse_len(digits: &[u8], max: usize) -> io::Result<Option<usize>> {
    let n: i64 = text(digits)?
        .parse()
        .map_err(|_| invalid("length is not a number"))?;
    if n == -1 {
        return Ok(None);
    }
    if n < 0 {
        return Err(invalid("negative length"));
    }
    let n = n as usize;
    if n > max {
        return Err(invalid("length exceeds the protocol limit"));
    }
    Ok(Some(n))
}

fn text(bytes: &[u8]) -> io::Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(|_| invalid("non-utf8 bytes in a protocol line"))
}

fn invalid(detail: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, detail)
}

fn invalid_owned(detail: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(bytes: &[u8]) -> Vec<Vec<u8>> {
        read_command(&mut &bytes[..]).unwrap().unwrap()
    }

    #[test]
    fn parses_an_array_command() {
        let args = cmd(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n");
        assert_eq!(
            args,
            vec![b"SET".to_vec(), b"k".to_vec(), b"hello".to_vec()]
        );
    }

    #[test]
    fn bulk_strings_are_binary_safe() {
        let args = cmd(b"*2\r\n$3\r\nGET\r\n$4\r\n\r\n\0\xff\r\n");
        assert_eq!(args[1], b"\r\n\0\xff");
    }

    #[test]
    fn parses_an_inline_command() {
        assert_eq!(cmd(b"GET  key\r\n"), vec![b"GET".to_vec(), b"key".to_vec()]);
        assert_eq!(cmd(b"PING\n"), vec![b"PING".to_vec()]);
    }

    #[test]
    fn end_of_stream_is_not_an_error() {
        assert_eq!(read_command(&mut &b""[..]).unwrap(), None);
    }

    #[test]
    fn framing_errors_are_invalid_data() {
        for bad in [
            &b"*2\r\n$3\r\nGET\r\n"[..],
            b"*1\r\n+GET\r\n",
            b"*1\r\n$3\r\nGETxx",
            b"*x\r\n",
            b"*1\r\n$-5\r\n",
        ] {
            let err = read_command(&mut &bad[..]).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData, "{bad:?}");
        }
    }

    #[test]
    fn replies_round_trip() {
        let replies = [
            Reply::ok(),
            Reply::err("boom"),
            Reply::Integer(-7),
            Reply::Bulk(b"\r\nbinary\0".to_vec()),
            Reply::Bulk(Vec::new()),
            Reply::Null,
            Reply::Array(vec![Reply::Integer(1), Reply::Null, Reply::Array(vec![])]),
        ];
        for reply in replies {
            let mut wire = Vec::new();
            reply.write_to(&mut wire).unwrap();
            assert_eq!(read_reply(&mut &wire[..]).unwrap(), reply, "{wire:?}");
        }
    }

    #[test]
    fn replies_match_the_redis_wire_format() {
        let mut wire = Vec::new();
        Reply::Array(vec![Reply::Bulk(b"a".to_vec()), Reply::Null])
            .write_to(&mut wire)
            .unwrap();
        assert_eq!(wire, b"*2\r\n$1\r\na\r\n$-1\r\n");
    }
}
