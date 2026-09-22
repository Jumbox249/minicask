//! Raft's durable state, on disk.
//!
//! Raft is only safe if a node's term, its vote and the entries it has
//! acknowledged are on the platter before it replies. [`MemStorage`] is
//! enough to run the deterministic tests; this is what a real node uses.
//!
//! Two files per node:
//!
//! ```text
//! raft/
//!   hard-state   term and vote, rewritten in place, 20 bytes
//!   entries      the log, append-only, one record per entry
//! ```
//!
//! The entries file reuses the store's own record format, so the framing,
//! the checksum and the torn-tail handling are the same code that the
//! single-node store has already been tested on. An entry's key is its
//! index and term; its value is the command, with a tombstone flag
//! standing in for the no-op that carries none.
//!
//! [`MemStorage`]: super::MemStorage

use super::log::{Command, Entry, HardState, Storage};
use crate::crc::crc32_parts;
use crate::error::{Error, Result};
use crate::record::{self, Header, HEADER_LEN};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const HARD_STATE_LEN: usize = 20;
/// `voted_for` is an `Option<NodeId>`, and this stands in for `None`. A
/// real node id of `u64::MAX` is not a thing anyone should configure.
const NO_VOTE: u64 = u64::MAX;

/// A node's consensus state, held on disk and cached in memory.
///
/// Every write is fsynced before it returns. That is not a tunable: a node
/// that acknowledges an entry it has not durably stored can lose a
/// committed write, which is the one thing consensus exists to prevent.
pub struct DiskStorage {
    dir: PathBuf,
    entries_file: File,
    hard_state: HardState,
    entries: Vec<Entry>,
    /// Where each entry starts in the file, so that a truncation is a
    /// `set_len` rather than a rewrite.
    offsets: Vec<u64>,
    end: u64,
}

impl DiskStorage {
    /// Open, creating the directory and both files if they are not there,
    /// and recovering whatever a previous run left behind.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<DiskStorage> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let hard_state = read_hard_state(&dir.join("hard-state"))?;
        let path = dir.join("entries");
        let mut entries_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let (entries, offsets, end) = recover(&mut entries_file)?;

        Ok(DiskStorage {
            dir,
            entries_file,
            hard_state,
            entries,
            offsets,
            end,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Bytes the log occupies on disk.
    pub fn disk_bytes(&self) -> u64 {
        self.end
    }
}

impl Storage for DiskStorage {
    fn hard_state(&self) -> HardState {
        self.hard_state
    }

    fn save_hard_state(&mut self, state: HardState) -> Result<()> {
        if state == self.hard_state {
            return Ok(());
        }
        let mut body = [0u8; HARD_STATE_LEN - 4];
        body[0..8].copy_from_slice(&state.term.to_le_bytes());
        body[8..16].copy_from_slice(&state.voted_for.unwrap_or(NO_VOTE).to_le_bytes());

        let mut buf = [0u8; HARD_STATE_LEN];
        buf[0..4].copy_from_slice(&crc32_parts(&[&body]).to_le_bytes());
        buf[4..].copy_from_slice(&body);

        // Written to one side and renamed over the other, so a crash
        // leaves either the old state or the new one and never a blend.
        let tmp = self.dir.join("hard-state.tmp");
        let final_path = self.dir.join("hard-state");
        {
            let mut file = File::create(&tmp)?;
            file.write_all(&buf)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp, &final_path)?;
        sync_dir(&self.dir)?;

        self.hard_state = state;
        Ok(())
    }

    fn last_index(&self) -> u64 {
        self.entries.last().map_or(0, |e| e.index)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.entry(index).map(|e| e.term)
    }

    fn entry(&self, index: u64) -> Option<&Entry> {
        if index == 0 {
            return None;
        }
        self.entries.get((index - 1) as usize)
    }

    fn entries_from(&self, index: u64) -> Vec<Entry> {
        if index == 0 || index > self.last_index() {
            return Vec::new();
        }
        self.entries[(index - 1) as usize..].to_vec()
    }

    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        let mut offsets = Vec::with_capacity(entries.len());
        let mut at = self.end;
        for entry in entries {
            debug_assert_eq!(
                entry.index,
                self.last_index() + offsets.len() as u64 + 1,
                "entries must be appended in order with no gaps"
            );
            offsets.push(at);
            let bytes = encode_entry(entry)?;
            at += bytes.len() as u64;
            buf.extend_from_slice(&bytes);
        }

        self.entries_file.seek(SeekFrom::Start(self.end))?;
        self.entries_file.write_all(&buf)?;
        self.entries_file.sync_all()?;

        self.end = at;
        self.entries.extend_from_slice(entries);
        self.offsets.extend_from_slice(&offsets);
        Ok(())
    }

    fn truncate_from(&mut self, index: u64) -> Result<()> {
        if index == 0 || index > self.last_index() {
            // Index 0 would mean discarding the sentinel, which is not a
            // thing; past the end there is nothing to discard.
            if index == 0 {
                self.entries_file.set_len(0)?;
                self.entries_file.sync_all()?;
                self.entries.clear();
                self.offsets.clear();
                self.end = 0;
            }
            return Ok(());
        }
        let at = self.offsets[(index - 1) as usize];
        self.entries_file.set_len(at)?;
        self.entries_file.sync_all()?;
        self.entries.truncate((index - 1) as usize);
        self.offsets.truncate((index - 1) as usize);
        self.end = at;
        Ok(())
    }
}

/// An entry's key is its index and term, so a recovered record describes
/// itself without any separate bookkeeping.
fn entry_key(index: u64, term: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[0..8].copy_from_slice(&index.to_le_bytes());
    key[8..16].copy_from_slice(&term.to_le_bytes());
    key
}

fn encode_entry(entry: &Entry) -> Result<Vec<u8>> {
    let key = entry_key(entry.index, entry.term);
    match &entry.command {
        // A no-op carries nothing, which is exactly what the record
        // format's tombstone flag already means.
        Command::Noop => record::encode(&key, None, record::now_millis()),
        Command::Data(bytes) => record::encode(&key, Some(bytes), record::now_millis()),
    }
}

/// Read the log back, stopping at the first record that a crash could have
/// left half-written and truncating the file there.
fn recover(file: &mut File) -> Result<(Vec<Entry>, Vec<u64>, u64)> {
    let size = file.metadata()?.len();
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(&mut *file);

    let mut entries = Vec::new();
    let mut offsets = Vec::new();
    let mut at = 0u64;

    loop {
        let mut header_buf = [0u8; HEADER_LEN];
        match reader.read_exact(&mut header_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let header = Header::decode(&header_buf);

        // A header whose lengths run past the end of the file is one that
        // was being written when the power went out.
        if header.record_len() > size - at {
            break;
        }

        // Summed as u64, the way `record_len` does it: two u32 lengths can
        // total more than a u32 holds, and a corrupt header is exactly
        // where that would happen. The size check above already refuses a
        // record that runs past the file, so this only matters for a log
        // large enough to get here, but guessing at a length is how a
        // reader turns damage into a panic.
        let body_len = header.key_len as u64 + header.value_len as u64;
        let Ok(body_len) = usize::try_from(body_len) else {
            return Err(Error::Corrupt {
                file_id: 0,
                offset: at,
                detail: "raft log record is larger than this machine can address",
            });
        };
        let mut body = vec![0u8; body_len];
        match reader.read_exact(&mut body) {
            Ok(()) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let (key, value) = body.split_at(header.key_len as usize);

        if !header.verify(key, value) {
            // The record is complete but does not match its checksum, so
            // this is damage rather than an interrupted append. Raft has
            // no way to reconstruct it, and guessing is the one thing a
            // consensus log must never do.
            return Err(Error::Corrupt {
                file_id: 0,
                offset: at,
                detail: "checksum mismatch in the raft log",
            });
        }
        if key.len() != 16 {
            return Err(Error::Corrupt {
                file_id: 0,
                offset: at,
                detail: "raft log entry has a malformed key",
            });
        }

        let index = u64::from_le_bytes(key[0..8].try_into().expect("16 byte key"));
        let term = u64::from_le_bytes(key[8..16].try_into().expect("16 byte key"));
        if index != entries.len() as u64 + 1 {
            return Err(Error::Corrupt {
                file_id: 0,
                offset: at,
                detail: "raft log entries are out of order",
            });
        }

        entries.push(Entry {
            term,
            index,
            command: if header.is_tombstone() {
                Command::Noop
            } else {
                Command::Data(value.to_vec())
            },
        });
        offsets.push(at);
        at += header.record_len();
    }

    drop(reader);
    if at != size {
        // Drop the half-written tail so the next append starts clean.
        file.set_len(at)?;
        file.sync_all()?;
    }
    Ok((entries, offsets, at))
}

fn read_hard_state(path: &Path) -> Result<HardState> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(HardState::default()),
        Err(e) => return Err(e.into()),
    };
    let mut buf = [0u8; HARD_STATE_LEN];
    match file.read_exact(&mut buf) {
        Ok(()) => {}
        // A torn state file means the rename never completed, so the
        // previous state is what is still on disk under the real name. An
        // empty or short file can only be a fresh one.
        Err(e) if e.kind() == ErrorKind::UnexpectedEof => return Ok(HardState::default()),
        Err(e) => return Err(e.into()),
    }

    let expected = u32::from_le_bytes(buf[0..4].try_into().expect("four bytes"));
    if crc32_parts(&[&buf[4..]]) != expected {
        return Err(Error::Corrupt {
            file_id: 0,
            offset: 0,
            detail: "checksum mismatch in the raft hard state",
        });
    }
    let term = u64::from_le_bytes(buf[4..12].try_into().expect("eight bytes"));
    let voted = u64::from_le_bytes(buf[12..20].try_into().expect("eight bytes"));
    Ok(HardState {
        term,
        voted_for: (voted != NO_VOTE).then_some(voted),
    })
}

/// A rename is only durable once the directory holding it is. Windows has
/// no equivalent call and does not need one.
#[cfg(unix)]
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
fn sync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::log::StorageExt;
    use super::*;

    struct Dir(PathBuf);

    impl Dir {
        fn new(label: &str) -> Dir {
            let path = std::env::temp_dir().join(format!(
                "minicask-raft-{label}-{}-{:?}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            let _ = std::fs::remove_dir_all(&path);
            Dir(path)
        }
    }

    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn entries(spec: &[(u64, u64)]) -> Vec<Entry> {
        spec.iter()
            .map(|&(index, term)| Entry {
                term,
                index,
                command: Command::Data(format!("command-{index}").into_bytes()),
            })
            .collect()
    }

    #[test]
    fn a_fresh_directory_is_empty() {
        let dir = Dir::new("fresh");
        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.last_index(), 0);
        assert_eq!(storage.hard_state(), HardState::default());
    }

    #[test]
    fn the_term_and_vote_survive_a_reopen() {
        let dir = Dir::new("hard-state");
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage
                .save_hard_state(HardState {
                    term: 7,
                    voted_for: Some(3),
                })
                .unwrap();
        }
        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.hard_state().term, 7);
        assert_eq!(storage.hard_state().voted_for, Some(3));
    }

    #[test]
    fn a_vote_of_none_is_not_a_vote_for_node_zero() {
        let dir = Dir::new("no-vote");
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage
                .save_hard_state(HardState {
                    term: 4,
                    voted_for: None,
                })
                .unwrap();
        }
        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.hard_state().voted_for, None);
    }

    #[test]
    fn entries_survive_a_reopen() {
        let dir = Dir::new("entries");
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage.append(&entries(&[(1, 1), (2, 1), (3, 2)])).unwrap();
            storage
                .append(&[Entry {
                    term: 2,
                    index: 4,
                    command: Command::Noop,
                }])
                .unwrap();
        }
        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.last_index(), 4);
        assert_eq!(storage.last_term(), 2);
        assert_eq!(storage.term_at(2), Some(1));
        assert_eq!(
            storage.entry(3).unwrap().command,
            Command::Data(b"command-3".to_vec())
        );
        assert_eq!(
            storage.entry(4).unwrap().command,
            Command::Noop,
            "a no-op must not come back as an empty command"
        );
    }

    #[test]
    fn an_empty_command_is_not_a_noop() {
        let dir = Dir::new("empty-command");
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage
                .append(&[Entry {
                    term: 1,
                    index: 1,
                    command: Command::Data(Vec::new()),
                }])
                .unwrap();
        }
        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.entry(1).unwrap().command, Command::Data(Vec::new()));
    }

    #[test]
    fn a_truncation_shrinks_the_file_and_sticks() {
        let dir = Dir::new("truncate");
        let size_before;
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage
                .append(&entries(&[(1, 1), (2, 1), (3, 1), (4, 1)]))
                .unwrap();
            size_before = storage.disk_bytes();
            storage.truncate_from(3).unwrap();
            assert_eq!(storage.last_index(), 2);
            assert!(storage.disk_bytes() < size_before);

            // The log stays usable, with the conflicting tail replaced.
            storage.append(&entries(&[(3, 5)])).unwrap();
            assert_eq!(storage.term_at(3), Some(5));
        }
        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.last_index(), 3);
        assert_eq!(storage.term_at(3), Some(5));
    }

    #[test]
    fn a_half_written_entry_is_dropped_at_startup() {
        let dir = Dir::new("torn-tail");
        let good_size;
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage.append(&entries(&[(1, 1), (2, 1)])).unwrap();
            good_size = storage.disk_bytes();
        }
        // A header with no body, which is what an interrupted append
        // leaves behind.
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(dir.0.join("entries"))
                .unwrap();
            let bytes = encode_entry(&entries(&[(3, 1)])[0]).unwrap();
            file.write_all(&bytes[..HEADER_LEN + 4]).unwrap();
            file.sync_all().unwrap();
        }

        let mut storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.last_index(), 2, "the torn entry should be gone");
        assert_eq!(
            storage.disk_bytes(),
            good_size,
            "the file should have been truncated back"
        );
        // And the log is writable again, at the index that was lost.
        storage.append(&entries(&[(3, 2)])).unwrap();
        assert_eq!(storage.term_at(3), Some(2));
    }

    /// A header claiming lengths far past the end of the file is what a
    /// crash mid-header leaves, not a record. It must be treated as a torn
    /// tail rather than believed and acted on.
    #[test]
    fn an_absurd_length_is_not_believed() {
        let dir = Dir::new("absurd-length");
        let good_size;
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage.append(&entries(&[(1, 1)])).unwrap();
            good_size = storage.disk_bytes();
        }
        {
            // key_len and value_len both at u32::MAX, which also happens to
            // overflow a u32 when summed.
            let mut header = [0u8; HEADER_LEN];
            header[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
            header[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
            let mut file = OpenOptions::new()
                .append(true)
                .open(dir.0.join("entries"))
                .unwrap();
            file.write_all(&header).unwrap();
            file.sync_all().unwrap();
        }

        let mut storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.last_index(), 1, "the real entry should survive");
        assert_eq!(storage.disk_bytes(), good_size, "the tail should be gone");
        storage.append(&entries(&[(2, 1)])).unwrap();
        assert_eq!(storage.last_index(), 2);
    }

    #[test]
    fn a_flipped_bit_is_reported_rather_than_guessed_at() {
        let dir = Dir::new("bit-rot");
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage.append(&entries(&[(1, 1), (2, 1)])).unwrap();
        }
        // Corrupt the body of the first record, leaving its length intact
        // so that the damage is not mistaken for a torn tail.
        {
            let path = dir.0.join("entries");
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[HEADER_LEN + 2] ^= 0b0100_0000;
            std::fs::write(&path, &bytes).unwrap();
        }
        match DiskStorage::open(&dir.0) {
            Err(Error::Corrupt { detail, .. }) => {
                assert!(detail.contains("checksum"), "unexpected detail: {detail}");
            }
            Err(e) => panic!("expected a corruption error, got {e}"),
            Ok(_) => panic!("corruption was accepted as valid state"),
        }
    }

    #[test]
    fn a_corrupt_hard_state_is_reported() {
        let dir = Dir::new("bad-hard-state");
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage
                .save_hard_state(HardState {
                    term: 9,
                    voted_for: Some(2),
                })
                .unwrap();
        }
        {
            let path = dir.0.join("hard-state");
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[5] ^= 0b0000_1000;
            std::fs::write(&path, &bytes).unwrap();
        }
        assert!(
            matches!(DiskStorage::open(&dir.0), Err(Error::Corrupt { .. })),
            "a damaged term or vote must not be treated as a fresh node"
        );
    }
}
