//! Raft messages on a socket.
//!
//! A fixed header, then the body, then a checksum over both. Nothing here
//! is clever: the peer link is a trusted, ordered byte stream, and the only
//! jobs are framing it and noticing when a message has been mangled.
//!
//! ```text
//!  0 ..  4   magic, so a mismatched protocol fails loudly at the first byte
//!  4 ..  8   length of everything that follows the length itself
//!  8 .. 12   crc32 of the body
//! 12 .. 20   the sending node's id
//! 20 ..      the message
//! ```

use super::log::{Command, Entry, NodeId};
use super::message::Message;
use crate::crc::crc32_parts;
use std::io::{self, Read, Write};

const MAGIC: u32 = 0x4d_43_52_46; // "MCRF"
const PREFIX_LEN: usize = 20;
/// A frame beyond this is garbage or a hostile peer, not a message.
pub const MAX_FRAME: u32 = 64 * 1024 * 1024;

/// Tags for the four message types.
const REQUEST_VOTE: u8 = 1;
const REQUEST_VOTE_REPLY: u8 = 2;
const APPEND_ENTRIES: u8 = 3;
const APPEND_ENTRIES_REPLY: u8 = 4;
const PRE_VOTE: u8 = 5;
const PRE_VOTE_REPLY: u8 = 6;
const INSTALL_SNAPSHOT: u8 = 7;
const INSTALL_SNAPSHOT_REPLY: u8 = 8;

/// Tags for a log entry's payload.
const ENTRY_NOOP: u8 = 0;
const ENTRY_DATA: u8 = 1;

pub fn write_message<W: Write>(w: &mut W, from: NodeId, message: &Message) -> io::Result<()> {
    let body = encode(message);
    let mut frame = Vec::with_capacity(PREFIX_LEN + body.len());
    frame.extend_from_slice(&MAGIC.to_le_bytes());
    frame.extend_from_slice(&((body.len() + 12) as u32).to_le_bytes());
    frame.extend_from_slice(&crc32_parts(&[&body]).to_le_bytes());
    frame.extend_from_slice(&from.to_le_bytes());
    frame.extend_from_slice(&body);
    w.write_all(&frame)
}

/// Read one message. `Ok(None)` at a clean end of stream.
pub fn read_message<R: Read>(r: &mut R) -> io::Result<Option<(NodeId, Message)>> {
    let mut prefix = [0u8; PREFIX_LEN];
    match r.read_exact(&mut prefix) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }

    let magic = u32::from_le_bytes(prefix[0..4].try_into().expect("four bytes"));
    if magic != MAGIC {
        return Err(invalid("not a minicask raft stream"));
    }
    let len = u32::from_le_bytes(prefix[4..8].try_into().expect("four bytes"));
    if !(12..=MAX_FRAME).contains(&len) {
        return Err(invalid("implausible frame length"));
    }
    let crc = u32::from_le_bytes(prefix[8..12].try_into().expect("four bytes"));
    let from = u64::from_le_bytes(prefix[12..20].try_into().expect("eight bytes"));

    let mut body = vec![0u8; (len - 12) as usize];
    r.read_exact(&mut body)?;
    if crc32_parts(&[&body]) != crc {
        return Err(invalid("checksum mismatch on a raft message"));
    }

    let message = decode(&body).ok_or_else(|| invalid("malformed raft message"))?;
    Ok(Some((from, message)))
}

fn encode(message: &Message) -> Vec<u8> {
    let mut out = Vec::new();
    match message {
        Message::PreVote {
            term,
            last_log_index,
            last_log_term,
        } => {
            out.push(PRE_VOTE);
            put_u64(&mut out, &[*term, *last_log_index, *last_log_term]);
        }
        Message::PreVoteReply { term, granted } => {
            out.push(PRE_VOTE_REPLY);
            put_u64(&mut out, &[*term]);
            out.push(u8::from(*granted));
        }
        Message::RequestVote {
            term,
            last_log_index,
            last_log_term,
        } => {
            out.push(REQUEST_VOTE);
            put_u64(&mut out, &[*term, *last_log_index, *last_log_term]);
        }
        Message::RequestVoteReply { term, granted } => {
            out.push(REQUEST_VOTE_REPLY);
            put_u64(&mut out, &[*term]);
            out.push(u8::from(*granted));
        }
        Message::AppendEntries {
            term,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } => {
            out.push(APPEND_ENTRIES);
            put_u64(
                &mut out,
                &[
                    *term,
                    *prev_log_index,
                    *prev_log_term,
                    *leader_commit,
                    entries.len() as u64,
                ],
            );
            for entry in entries {
                put_u64(&mut out, &[entry.term, entry.index]);
                match &entry.command {
                    Command::Noop => {
                        out.push(ENTRY_NOOP);
                        put_u64(&mut out, &[0]);
                    }
                    Command::Data(bytes) => {
                        out.push(ENTRY_DATA);
                        put_u64(&mut out, &[bytes.len() as u64]);
                        out.extend_from_slice(bytes);
                    }
                }
            }
        }
        Message::InstallSnapshot {
            term,
            last_index,
            last_term,
            offset,
            data,
            done,
        } => {
            out.push(INSTALL_SNAPSHOT);
            put_u64(
                &mut out,
                &[*term, *last_index, *last_term, *offset, data.len() as u64],
            );
            out.extend_from_slice(data);
            out.push(u8::from(*done));
        }
        Message::InstallSnapshotReply {
            term,
            last_index,
            next_offset,
            done,
        } => {
            out.push(INSTALL_SNAPSHOT_REPLY);
            put_u64(&mut out, &[*term, *last_index, *next_offset]);
            out.push(u8::from(*done));
        }
        Message::AppendEntriesReply {
            term,
            success,
            match_index,
            conflict_index,
            conflict_term,
        } => {
            out.push(APPEND_ENTRIES_REPLY);
            put_u64(&mut out, &[*term, *match_index, *conflict_index]);
            out.push(u8::from(*success));
            match conflict_term {
                Some(t) => {
                    out.push(1);
                    put_u64(&mut out, &[*t]);
                }
                None => {
                    out.push(0);
                    put_u64(&mut out, &[0]);
                }
            }
        }
    }
    out
}

fn decode(body: &[u8]) -> Option<Message> {
    let mut r = Reader { body, at: 0 };
    let tag = r.u8()?;
    let message = match tag {
        PRE_VOTE => Message::PreVote {
            term: r.u64()?,
            last_log_index: r.u64()?,
            last_log_term: r.u64()?,
        },
        PRE_VOTE_REPLY => Message::PreVoteReply {
            term: r.u64()?,
            granted: r.bool()?,
        },
        REQUEST_VOTE => Message::RequestVote {
            term: r.u64()?,
            last_log_index: r.u64()?,
            last_log_term: r.u64()?,
        },
        REQUEST_VOTE_REPLY => Message::RequestVoteReply {
            term: r.u64()?,
            granted: r.bool()?,
        },
        APPEND_ENTRIES => {
            let term = r.u64()?;
            let prev_log_index = r.u64()?;
            let prev_log_term = r.u64()?;
            let leader_commit = r.u64()?;
            let count = r.u64()?;
            // Guard against a count that would allocate the world before a
            // single entry has been read.
            if count > body.len() as u64 {
                return None;
            }
            let mut entries = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let term = r.u64()?;
                let index = r.u64()?;
                let kind = r.u8()?;
                let len = r.u64()?;
                let command = match kind {
                    ENTRY_NOOP => Command::Noop,
                    ENTRY_DATA => Command::Data(r.bytes(len)?.to_vec()),
                    _ => return None,
                };
                entries.push(Entry {
                    term,
                    index,
                    command,
                });
            }
            Message::AppendEntries {
                term,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            }
        }
        INSTALL_SNAPSHOT => {
            let term = r.u64()?;
            let last_index = r.u64()?;
            let last_term = r.u64()?;
            let offset = r.u64()?;
            let len = r.u64()?;
            let data = r.bytes(len)?.to_vec();
            Message::InstallSnapshot {
                term,
                last_index,
                last_term,
                offset,
                data,
                done: r.bool()?,
            }
        }
        INSTALL_SNAPSHOT_REPLY => Message::InstallSnapshotReply {
            term: r.u64()?,
            last_index: r.u64()?,
            next_offset: r.u64()?,
            done: r.bool()?,
        },
        APPEND_ENTRIES_REPLY => {
            let term = r.u64()?;
            let match_index = r.u64()?;
            let conflict_index = r.u64()?;
            let success = r.bool()?;
            let has_term = r.bool()?;
            let conflict = r.u64()?;
            Message::AppendEntriesReply {
                term,
                success,
                match_index,
                conflict_index,
                conflict_term: has_term.then_some(conflict),
            }
        }
        _ => return None,
    };
    // A frame with anything left over is one this code does not understand.
    (r.at == body.len()).then_some(message)
}

fn put_u64(out: &mut Vec<u8>, values: &[u64]) {
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
}

struct Reader<'a> {
    body: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn bytes(&mut self, len: u64) -> Option<&'a [u8]> {
        let len = usize::try_from(len).ok()?;
        let end = self.at.checked_add(len)?;
        let slice = self.body.get(self.at..end)?;
        self.at = end;
        Some(slice)
    }

    fn u8(&mut self) -> Option<u8> {
        Some(self.bytes(1)?[0])
    }

    fn bool(&mut self) -> Option<bool> {
        match self.u8()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.bytes(8)?.try_into().ok()?))
    }
}

fn invalid(detail: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, detail)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(message: Message) {
        let mut wire = Vec::new();
        write_message(&mut wire, 7, &message).unwrap();
        let (from, decoded) = read_message(&mut &wire[..]).unwrap().unwrap();
        assert_eq!(from, 7);
        assert_eq!(decoded, message);
    }

    #[test]
    fn snapshot_messages_round_trip() {
        round_trip(Message::InstallSnapshot {
            term: 4,
            last_index: 900,
            last_term: 3,
            offset: 1 << 20,
            data: b"\r\n\0binary snapshot bytes\xff".to_vec(),
            done: false,
        });
        round_trip(Message::InstallSnapshot {
            term: 4,
            last_index: 900,
            last_term: 3,
            offset: 0,
            data: Vec::new(),
            done: true,
        });
        round_trip(Message::InstallSnapshotReply {
            term: 4,
            last_index: 900,
            next_offset: 12345,
            done: false,
        });
        round_trip(Message::InstallSnapshotReply {
            term: 4,
            last_index: 900,
            next_offset: 0,
            done: true,
        });
    }

    #[test]
    fn every_message_round_trips() {
        round_trip(Message::PreVote {
            term: 9,
            last_log_index: 4,
            last_log_term: 3,
        });
        round_trip(Message::PreVoteReply {
            term: 9,
            granted: true,
        });
        round_trip(Message::PreVoteReply {
            term: 9,
            granted: false,
        });
        round_trip(Message::RequestVote {
            term: 9,
            last_log_index: 4,
            last_log_term: 3,
        });
        round_trip(Message::RequestVoteReply {
            term: 9,
            granted: true,
        });
        round_trip(Message::RequestVoteReply {
            term: 9,
            granted: false,
        });
        round_trip(Message::AppendEntries {
            term: 2,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            leader_commit: 0,
        });
        round_trip(Message::AppendEntries {
            term: 5,
            prev_log_index: 2,
            prev_log_term: 4,
            entries: vec![
                Entry {
                    term: 5,
                    index: 3,
                    command: Command::Noop,
                },
                Entry {
                    term: 5,
                    index: 4,
                    command: Command::Data(b"\r\n\0binary\xff".to_vec()),
                },
                Entry {
                    term: 5,
                    index: 5,
                    command: Command::Data(Vec::new()),
                },
            ],
            leader_commit: 3,
        });
        round_trip(Message::AppendEntriesReply {
            term: 5,
            success: true,
            match_index: 4,
            conflict_index: 0,
            conflict_term: None,
        });
        round_trip(Message::AppendEntriesReply {
            term: 5,
            success: false,
            match_index: 0,
            conflict_index: 2,
            conflict_term: Some(3),
        });
    }

    #[test]
    fn several_messages_share_a_stream() {
        let mut wire = Vec::new();
        for term in 1..=3 {
            write_message(
                &mut wire,
                term,
                &Message::RequestVoteReply {
                    term,
                    granted: true,
                },
            )
            .unwrap();
        }
        let mut cursor = &wire[..];
        for expected in 1..=3 {
            let (from, _) = read_message(&mut cursor).unwrap().unwrap();
            assert_eq!(from, expected);
        }
        assert_eq!(read_message(&mut cursor).unwrap(), None, "clean end");
    }

    #[test]
    fn a_flipped_bit_is_caught() {
        let mut wire = Vec::new();
        write_message(
            &mut wire,
            1,
            &Message::AppendEntries {
                term: 1,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![Entry {
                    term: 1,
                    index: 1,
                    command: Command::Data(b"value".to_vec()),
                }],
                leader_commit: 0,
            },
        )
        .unwrap();
        let last = wire.len() - 1;
        wire[last] ^= 0b0000_0100;

        let err = read_message(&mut &wire[..]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn junk_is_rejected_at_the_first_byte() {
        let err = read_message(&mut &b"GET / HTTP/1.1\r\n\r\n\r\n"[..]).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_truncated_frame_is_not_a_clean_end() {
        let mut wire = Vec::new();
        write_message(
            &mut wire,
            1,
            &Message::RequestVote {
                term: 1,
                last_log_index: 0,
                last_log_term: 0,
            },
        )
        .unwrap();
        wire.truncate(wire.len() - 3);
        assert!(read_message(&mut &wire[..]).is_err());
    }
}
