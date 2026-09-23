//! Raft's durable state, on disk.
//!
//! Raft is only safe if a node's term, its vote and the entries it has
//! acknowledged are on the platter before it replies. [`MemStorage`] is
//! enough to run the deterministic tests; this is what a real node uses.
//!
//! Three files per node:
//!
//! ```text
//! raft/
//!   hard-state   term and vote, replaced whole, 20 bytes
//!   entries      the log after the snapshot, append-only, one record per entry
//!   snapshot     the state machine as of some index, replaced whole
//! ```
//!
//! The entries file reuses the store's own record format, so the framing,
//! the checksum and the torn-tail handling are the same code that the
//! single-node store has already been tested on. An entry's key is its
//! index and term; its value is the command, with a tombstone flag
//! standing in for the no-op that carries none. A membership entry has one
//! more byte on its key, saying so, and the members as its value.
//!
//! The snapshot file begins with a marker, then a checksum over everything
//! after it, the index and term it covers, the membership as of that
//! index, and the data:
//!
//! ```text
//! "MCS2" | crc32 | index u64 | term u64 | members_len u32 | members | data
//! ```
//!
//! A file without the marker is from before membership could change: a
//! checksum, the index and term, and the data straight after. Both are
//! read; only the first is written.
//!
//! A snapshot is never held in memory whole. One being written, whether
//! taken here or arriving from a leader, goes into a `snapshot.part-N`
//! file beside the real one and is renamed over it once complete and
//! synced, so a crash part way through leaves the old snapshot in place
//! and a stray part file that the next open deletes.
//!
//! Taking a snapshot touches two files, and no filesystem changes two
//! files at once. The snapshot is written first and the entries file
//! rewritten second, so a crash in between leaves a snapshot beside a log
//! that still holds the entries it covers. Opening a directory reconciles
//! the two by the same rule taking the snapshot applies, which makes
//! finishing the job on the next start the same as having finished it.
//!
//! [`MemStorage`]: super::MemStorage

use super::log::{
    decode_members, encode_members, Command, Entry, EntryLog, HardState, Member, SnapshotMeta,
    SnapshotSink, Storage,
};
use crate::crc::{crc32_parts, Crc32};
use crate::error::{Error, Result};
use crate::record::{self, Header, HEADER_LEN};
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const HARD_STATE_LEN: usize = 20;
/// `voted_for` is an `Option<NodeId>`, and this stands in for `None`. A
/// real node id of `u64::MAX` is not a thing anyone should configure.
const NO_VOTE: u64 = u64::MAX;

/// The first four bytes of a snapshot file that records its membership.
const SNAPSHOT_MAGIC: [u8; 4] = *b"MCS2";
/// The header of a snapshot file from before that: a checksum, the index
/// and term, and nothing else.
const OLD_SNAPSHOT_HEADER_LEN: u64 = 4 + 8 + 8;
/// `members_len` when the snapshot records no membership.
const NO_MEMBERS: u32 = u32::MAX;
/// The extra key byte that marks an entry as a membership entry.
const CONFIG_ENTRY: u8 = 1;

/// A node's consensus state, held on disk and cached in memory.
///
/// Every write is fsynced before it returns. That is not a tunable: a node
/// that acknowledges an entry it has not durably stored can lose a
/// committed write, which is the one thing consensus exists to prevent.
pub struct DiskStorage {
    dir: PathBuf,
    entries_file: File,
    hard_state: HardState,
    log: EntryLog,
    /// Where each held entry starts in the entries file, so that a
    /// truncation is a `set_len` rather than a rewrite.
    offsets: Vec<u64>,
    end: u64,
    snapshot_len: u64,
    /// Where the snapshot's data begins in its file, past the header.
    snapshot_data_at: u64,
    snapshot_members: Option<Vec<Member>>,
    /// Numbers the part files of snapshots being written, so that two at
    /// once, one taken here and one arriving, never share a file.
    next_part: u64,
}

impl DiskStorage {
    /// Open, creating the directory and files if they are not there, and
    /// recovering whatever a previous run left behind.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<DiskStorage> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let hard_state = read_hard_state(&dir.join("hard-state"))?;
        let SnapshotHeader {
            meta: base,
            members: snapshot_members,
            data_at: snapshot_data_at,
            data_len: snapshot_len,
        } = read_snapshot_header(&dir)?;
        remove_stray_parts(&dir)?;

        let mut entries_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(dir.join("entries"))?;
        let (entries, offsets, end) = recover(&mut entries_file)?;

        let mut storage = DiskStorage {
            dir,
            entries_file,
            hard_state,
            log: EntryLog::default(),
            offsets,
            end,
            snapshot_len,
            snapshot_data_at,
            snapshot_members,
            next_part: 0,
        };

        match entries.first().map(|e| e.index) {
            // The log starts exactly where the snapshot ends: the usual case.
            None => storage.log = EntryLog::new(base, entries),
            Some(first) if first == base.index + 1 => storage.log = EntryLog::new(base, entries),
            // Entries the snapshot already covers. Either a crash came
            // between writing the snapshot and rewriting the log, or the
            // snapshot was installed from a leader over a log that had
            // diverged from it. Apply the rule that taking the snapshot
            // would have, then finish the rewrite it was cut short of.
            Some(first) if first <= base.index => {
                let mut log = EntryLog::new(
                    SnapshotMeta {
                        index: first - 1,
                        term: 0,
                    },
                    entries,
                );
                // The stand-in base above is never consulted: compacting
                // to a real snapshot replaces it.
                log.compact(base);
                storage.log = log;
                storage.rewrite_entries()?;
            }
            // A hole between the snapshot and the first entry after it.
            // Neither order of writes can produce that, so it is damage.
            Some(_) => {
                return Err(Error::Corrupt {
                    file_id: 0,
                    offset: 0,
                    detail: "the raft log does not begin where the snapshot ends",
                })
            }
        }
        Ok(storage)
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Bytes the log occupies on disk, not counting the snapshot.
    pub fn disk_bytes(&self) -> u64 {
        self.end
    }

    /// Replace the entries file with just the entries the log still holds.
    ///
    /// Built beside the old file and renamed over it, so a crash leaves one
    /// or the other. The new file's handle takes over before the rename:
    /// Windows will not replace a file that is still open.
    fn rewrite_entries(&mut self) -> Result<()> {
        let mut buf = Vec::new();
        let mut offsets = Vec::with_capacity(self.log.entries().len());
        for entry in self.log.entries() {
            offsets.push(buf.len() as u64);
            buf.extend_from_slice(&encode_entry(entry)?);
        }

        let tmp = self.dir.join("entries.tmp");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(&buf)?;
        file.sync_all()?;

        self.entries_file = file;
        std::fs::rename(&tmp, self.dir.join("entries"))?;
        crate::log::sync_dir(&self.dir)?;

        self.offsets = offsets;
        self.end = buf.len() as u64;
        Ok(())
    }
}

/// A snapshot on its way to disk: a part file with room left at the front
/// for the header, whose checksum is only known once the last byte is in.
pub struct DiskSnapshot {
    meta: SnapshotMeta,
    members: Option<Vec<Member>>,
    /// Everything in the header after the checksum, which the checksum
    /// was fed first.
    header: Vec<u8>,
    /// `None` once installed, which is how `Drop` knows to leave it be.
    file: Option<BufWriter<File>>,
    path: PathBuf,
    crc: Crc32,
    written: u64,
}

impl Write for DiskSnapshot {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let file = self.file.as_mut().expect("written after install");
        let n = file.write(buf)?;
        self.crc.update(&buf[..n]);
        self.written += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.as_mut().expect("flushed after install").flush()
    }
}

impl SnapshotSink for DiskSnapshot {
    fn meta(&self) -> SnapshotMeta {
        self.meta
    }

    fn written(&self) -> u64 {
        self.written
    }
}

impl Drop for DiskSnapshot {
    /// A snapshot abandoned part way, because a newer one overtook it or
    /// its leader moved on, takes its part file with it.
    fn drop(&mut self) {
        if self.file.take().is_some() {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

impl DiskSnapshot {
    /// Fill in the header, sync, and hand back the finished part file's
    /// path, closed, ready to be renamed.
    fn finish(mut self) -> Result<PathBuf> {
        let mut file = self
            .file
            .take()
            .expect("finished twice")
            .into_inner()
            .map_err(|e| e.into_error())?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&SNAPSHOT_MAGIC)?;
        file.write_all(&self.crc.finish().to_le_bytes())?;
        file.write_all(&self.header)?;
        file.sync_all()?;
        Ok(std::mem::take(&mut self.path))
    }
}

/// The part of a snapshot's header that follows the checksum.
fn snapshot_header(meta: SnapshotMeta, members: Option<&[Member]>) -> Vec<u8> {
    let mut header = Vec::new();
    header.extend_from_slice(&meta.index.to_le_bytes());
    header.extend_from_slice(&meta.term.to_le_bytes());
    match members {
        Some(members) => {
            let encoded = encode_members(members);
            header.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
            header.extend_from_slice(&encoded);
        }
        None => header.extend_from_slice(&NO_MEMBERS.to_le_bytes()),
    }
    header
}

impl Storage for DiskStorage {
    type Sink = DiskSnapshot;

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

        crate::log::write_atomically(&self.dir, "hard-state", &buf)?;

        self.hard_state = state;
        Ok(())
    }

    fn last_index(&self) -> u64 {
        self.log.last_index()
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        self.log.term_at(index)
    }

    fn entry(&self, index: u64) -> Option<&Entry> {
        self.log.entry(index)
    }

    fn entries_from(&self, index: u64) -> Vec<Entry> {
        self.log.entries_from(index)
    }

    fn append(&mut self, entries: &[Entry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut buf = Vec::new();
        let mut offsets = Vec::with_capacity(entries.len());
        let mut at = self.end;
        for entry in entries {
            offsets.push(at);
            let bytes = encode_entry(entry)?;
            at += bytes.len() as u64;
            buf.extend_from_slice(&bytes);
        }

        self.entries_file.seek(SeekFrom::Start(self.end))?;
        self.entries_file.write_all(&buf)?;
        self.entries_file.sync_all()?;

        self.end = at;
        for entry in entries {
            self.log.push(entry.clone());
        }
        self.offsets.extend_from_slice(&offsets);
        Ok(())
    }

    fn truncate_from(&mut self, index: u64) -> Result<()> {
        let keep = self.log.truncate_from(index);
        if keep >= self.offsets.len() {
            return Ok(());
        }
        let at = self.offsets[keep];
        self.entries_file.set_len(at)?;
        self.entries_file.sync_all()?;
        self.offsets.truncate(keep);
        self.end = at;
        Ok(())
    }

    fn snapshot_meta(&self) -> SnapshotMeta {
        self.log.base()
    }

    fn snapshot_members(&self) -> Option<Vec<Member>> {
        self.snapshot_members.clone()
    }

    fn snapshot_len(&self) -> u64 {
        self.snapshot_len
    }

    fn read_snapshot(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if offset >= self.snapshot_len {
            return Ok(Vec::new());
        }
        let len = (len as u64).min(self.snapshot_len - offset) as usize;
        let mut file = File::open(self.dir.join("snapshot"))?;
        file.seek(SeekFrom::Start(self.snapshot_data_at + offset))?;
        let mut buf = vec![0u8; len];
        file.read_exact(&mut buf)?;
        Ok(buf)
    }

    fn snapshot_reader(&self) -> Result<Box<dyn Read + Send>> {
        let mut file = File::open(self.dir.join("snapshot"))?;
        file.seek(SeekFrom::Start(self.snapshot_data_at))?;
        Ok(Box::new(
            BufReader::with_capacity(64 * 1024, file).take(self.snapshot_len),
        ))
    }

    fn new_snapshot(
        &mut self,
        meta: SnapshotMeta,
        members: Option<Vec<Member>>,
    ) -> Result<DiskSnapshot> {
        self.next_part += 1;
        let path = self.dir.join(format!("snapshot.part-{}", self.next_part));
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        let header = snapshot_header(meta, members.as_deref());
        // Room for the marker, the checksum and the header, which are
        // written last, once the checksum is known.
        file.write_all(&vec![0u8; 8 + header.len()])?;
        let mut crc = Crc32::new();
        crc.update(&header);
        Ok(DiskSnapshot {
            meta,
            members,
            header,
            file: Some(BufWriter::with_capacity(64 * 1024, file)),
            path,
            crc,
            written: 0,
        })
    }

    fn install_snapshot(&mut self, sink: DiskSnapshot) -> Result<()> {
        let meta = sink.meta;
        if meta.index <= self.log.base().index {
            // Dropping it removes its part file.
            return Ok(());
        }
        let len = sink.written;
        let data_at = 8 + sink.header.len() as u64;
        let members = sink.members.clone();

        // The snapshot first, so that the moment the old entries are gone
        // there is already something durable standing in for them.
        let part = sink.finish()?;
        std::fs::rename(&part, self.dir.join("snapshot"))?;
        crate::log::sync_dir(&self.dir)?;
        self.snapshot_len = len;
        self.snapshot_data_at = data_at;
        self.snapshot_members = members;

        // Then the log, cut down to what follows the snapshot.
        self.log.compact(meta);
        self.rewrite_entries()
    }
}

/// Part files are snapshots that were being written when the process
/// stopped. None of them was installed, so none of them is needed.
fn remove_stray_parts(dir: &Path) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // `snapshot.tmp` is where earlier versions wrote a snapshot before
        // renaming it into place.
        if name.starts_with("snapshot.part-") || name == "snapshot.tmp" {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
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
        Command::Config(members) => {
            let mut key = key.to_vec();
            key.push(CONFIG_ENTRY);
            record::encode(&key, Some(&encode_members(members)), record::now_millis())
        }
    }
}

/// What a snapshot file's header says.
#[derive(Debug, Default)]
struct SnapshotHeader {
    meta: SnapshotMeta,
    members: Option<Vec<Member>>,
    /// Where the data begins in the file.
    data_at: u64,
    data_len: u64,
}

/// What the snapshot file covers and how big its data is, checking the
/// whole file against its checksum on the way.
///
/// No file means no snapshot. A file that is there but wrong is not the
/// same thing at all: the entries it stood in for may already be gone, so
/// treating it as absent would quietly throw committed writes away.
///
/// The file is read through once to check it, a piece at a time, since it
/// is as large as the state machine.
fn read_snapshot_header(dir: &Path) -> Result<SnapshotHeader> {
    let file = match File::open(dir.join("snapshot")) {
        Ok(file) => file,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(SnapshotHeader::default()),
        Err(e) => return Err(e.into()),
    };
    let corrupt = |detail| Error::Corrupt {
        file_id: 0,
        offset: 0,
        detail,
    };
    let short = |e: io::Error| {
        if e.kind() == ErrorKind::UnexpectedEof {
            corrupt("the raft snapshot is shorter than its header")
        } else {
            e.into()
        }
    };
    let mut reader = BufReader::with_capacity(64 * 1024, file);
    let mut first = [0u8; 4];
    reader.read_exact(&mut first).map_err(short)?;

    let mut crc = Crc32::new();
    let (expected, index, term, members, data_at) = if first == SNAPSHOT_MAGIC {
        let mut fixed = [0u8; 4 + 8 + 8 + 4];
        reader.read_exact(&mut fixed).map_err(short)?;
        crc.update(&fixed[4..]);
        let expected = u32::from_le_bytes(fixed[0..4].try_into().expect("four bytes"));
        let index = u64::from_le_bytes(fixed[4..12].try_into().expect("eight bytes"));
        let term = u64::from_le_bytes(fixed[12..20].try_into().expect("eight bytes"));
        let members_len = u32::from_le_bytes(fixed[20..24].try_into().expect("four bytes"));
        let (members, extra) = if members_len == NO_MEMBERS {
            (None, 0)
        } else {
            let mut encoded = vec![0u8; members_len as usize];
            reader.read_exact(&mut encoded).map_err(short)?;
            crc.update(&encoded);
            let members = decode_members(&encoded)
                .ok_or_else(|| corrupt("the raft snapshot's membership is malformed"))?;
            (Some(members), members_len as u64)
        };
        (expected, index, term, members, 4 + 24 + extra)
    } else {
        let mut rest = [0u8; OLD_SNAPSHOT_HEADER_LEN as usize - 4];
        reader.read_exact(&mut rest).map_err(short)?;
        crc.update(&rest);
        let expected = u32::from_le_bytes(first);
        let index = u64::from_le_bytes(rest[0..8].try_into().expect("eight bytes"));
        let term = u64::from_le_bytes(rest[8..16].try_into().expect("eight bytes"));
        (expected, index, term, None, OLD_SNAPSHOT_HEADER_LEN)
    };

    let mut len = 0u64;
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                crc.update(&buf[..n]);
                len += n as u64;
            }
            Err(e) if e.kind() == ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    if crc.finish() != expected {
        return Err(corrupt("checksum mismatch in the raft snapshot"));
    }
    Ok(SnapshotHeader {
        meta: SnapshotMeta { index, term },
        members,
        data_at,
        data_len: len,
    })
}

/// Read the log back, stopping at the first record that a crash could have
/// left half-written and truncating the file there.
///
/// The entries need not start at index 1, since a snapshot may have taken
/// the ones before them, but they must follow each other without a gap.
fn recover(file: &mut File) -> Result<(Vec<Entry>, Vec<u64>, u64)> {
    let size = file.metadata()?.len();
    file.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::new(&mut *file);

    let mut entries: Vec<Entry> = Vec::new();
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
        let config = match key.len() {
            16 => false,
            17 if key[16] == CONFIG_ENTRY => true,
            _ => {
                return Err(Error::Corrupt {
                    file_id: 0,
                    offset: at,
                    detail: "raft log entry has a malformed key",
                })
            }
        };

        let index = u64::from_le_bytes(key[0..8].try_into().expect("16 byte key"));
        let term = u64::from_le_bytes(key[8..16].try_into().expect("16 byte key"));
        if let Some(last) = entries.last() {
            if index != last.index + 1 {
                return Err(Error::Corrupt {
                    file_id: 0,
                    offset: at,
                    detail: "raft log entries are out of order",
                });
            }
        } else if index == 0 {
            return Err(Error::Corrupt {
                file_id: 0,
                offset: at,
                detail: "raft log entry has index 0",
            });
        }

        let command = if config {
            Command::Config(decode_members(value).ok_or(Error::Corrupt {
                file_id: 0,
                offset: at,
                detail: "raft log membership entry is malformed",
            })?)
        } else if header.is_tombstone() {
            Command::Noop
        } else {
            Command::Data(value.to_vec())
        };
        entries.push(Entry {
            term,
            index,
            command,
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

    // -- snapshots ------------------------------------------------------

    fn meta(index: u64, term: u64) -> SnapshotMeta {
        SnapshotMeta { index, term }
    }

    #[test]
    fn a_snapshot_survives_a_reopen() {
        let dir = Dir::new("snap-reopen");
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage
                .append(&entries(&[(1, 1), (2, 1), (3, 2), (4, 2)]))
                .unwrap();
            storage.save_snapshot(meta(3, 2), b"the state").unwrap();
        }
        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.snapshot_meta(), meta(3, 2));
        assert_eq!(storage.first_index(), 4);
        assert_eq!(storage.last_index(), 4);
        assert_eq!(storage.term_at(3), Some(2));
        assert_eq!(storage.term_at(2), None);
        assert_eq!(storage.snapshot_len(), 9);
        assert_eq!(storage.read_snapshot(0, 100).unwrap(), b"the state");
        assert_eq!(storage.read_snapshot(4, 3).unwrap(), b"sta");
    }

    #[test]
    fn compaction_shrinks_the_entries_file() {
        let dir = Dir::new("snap-shrink");
        let mut storage = DiskStorage::open(&dir.0).unwrap();
        let many: Vec<(u64, u64)> = (1..=50).map(|i| (i, 1)).collect();
        storage.append(&entries(&many)).unwrap();
        let before = storage.disk_bytes();

        storage.save_snapshot(meta(45, 1), b"").unwrap();
        assert!(
            storage.disk_bytes() < before / 5,
            "the entries file still holds what the snapshot covers"
        );
        // And it is still an ordinary, appendable log.
        storage.append(&entries(&[(51, 1)])).unwrap();
        storage.truncate_from(51).unwrap();
        storage.append(&entries(&[(51, 2)])).unwrap();
        drop(storage);

        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.first_index(), 46);
        assert_eq!(storage.last_index(), 51);
        assert_eq!(storage.term_at(51), Some(2));
    }

    /// The crash the ordering exists for: the snapshot reached disk, but
    /// the entries file was never cut down. Opening must finish the job.
    #[test]
    fn a_crash_between_snapshot_and_rewrite_is_finished_on_open() {
        let dir = Dir::new("snap-crash");
        let old_entries;
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage
                .append(&entries(&[(1, 1), (2, 1), (3, 2), (4, 2), (5, 2)]))
                .unwrap();
            old_entries = std::fs::read(dir.0.join("entries")).unwrap();
            storage.save_snapshot(meta(3, 2), b"s").unwrap();
        }
        // Put the uncut log back, as if the rewrite never happened.
        std::fs::write(dir.0.join("entries"), &old_entries).unwrap();

        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.first_index(), 4);
        assert_eq!(storage.last_index(), 5, "the agreeing tail is kept");
        assert_eq!(storage.entry(3), None);
        assert!(
            storage.disk_bytes() < old_entries.len() as u64,
            "opening should have finished cutting the file down"
        );
    }

    /// The same interruption, for a snapshot installed from a leader whose
    /// log disagreed with this one. None of the old log survives.
    #[test]
    fn an_interrupted_install_over_a_diverged_log_discards_it_on_open() {
        let dir = Dir::new("snap-diverged");
        let old_entries;
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage
                .append(&entries(&[(1, 1), (2, 1), (3, 1), (4, 1)]))
                .unwrap();
            old_entries = std::fs::read(dir.0.join("entries")).unwrap();
            // The leader's snapshot says index 3 was term 5, not term 1.
            storage.save_snapshot(meta(3, 5), b"theirs").unwrap();
        }
        std::fs::write(dir.0.join("entries"), &old_entries).unwrap();

        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.last_index(), 3);
        assert_eq!(storage.term_at(3), Some(5));
        assert_eq!(
            storage.entry(4),
            None,
            "an entry after the divergence came back from the dead"
        );
    }

    #[test]
    fn a_damaged_snapshot_is_reported_not_ignored() {
        let dir = Dir::new("snap-damaged");
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage.append(&entries(&[(1, 1), (2, 1)])).unwrap();
            storage.save_snapshot(meta(2, 1), b"important").unwrap();
        }
        let path = dir.0.join("snapshot");
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0b0000_0001;
        std::fs::write(&path, &bytes).unwrap();

        assert!(
            matches!(DiskStorage::open(&dir.0), Err(Error::Corrupt { .. })),
            "the entries it stood for are gone, so it cannot be treated as absent"
        );
    }

    #[test]
    fn a_gap_after_the_snapshot_is_reported() {
        let dir = Dir::new("snap-gap");
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage.append(&entries(&[(1, 1), (2, 1)])).unwrap();
            storage.save_snapshot(meta(2, 1), b"").unwrap();
        }
        // An entries file that starts at 5, leaving 3 and 4 unaccounted for.
        let mut storage = DiskStorage::open(&dir.0).unwrap();
        storage.log = EntryLog::new(meta(4, 1), Vec::new());
        storage.append(&entries(&[(5, 1)])).unwrap();
        drop(storage);

        assert!(matches!(
            DiskStorage::open(&dir.0),
            Err(Error::Corrupt { .. })
        ));
    }

    #[test]
    fn an_older_snapshot_is_ignored() {
        let dir = Dir::new("snap-older");
        let mut storage = DiskStorage::open(&dir.0).unwrap();
        storage.append(&entries(&[(1, 1), (2, 1), (3, 1)])).unwrap();
        storage.save_snapshot(meta(2, 1), b"newer").unwrap();
        storage.save_snapshot(meta(1, 1), b"older").unwrap();
        assert_eq!(storage.snapshot_meta(), meta(2, 1));
        assert_eq!(storage.read_snapshot(0, 10).unwrap(), b"newer");
    }

    fn members(ids: &[u64]) -> Vec<Member> {
        ids.iter()
            .map(|&id| Member {
                id,
                context: format!("node-{id}").into_bytes(),
                learner: false,
            })
            .collect()
    }

    /// A membership entry survives a reopen like any other.
    #[test]
    fn a_membership_entry_survives_a_reopen() {
        let dir = Dir::new("config-entry");
        let config = Entry {
            term: 2,
            index: 2,
            command: Command::Config(members(&[1, 2, 3, 4])),
        };
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage.append(&entries(&[(1, 1)])).unwrap();
            storage.append(std::slice::from_ref(&config)).unwrap();
            storage.append(&entries(&[(3, 2)])).unwrap();
        }
        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.entry(2), Some(&config));
        assert_eq!(storage.last_index(), 3);
    }

    /// The membership as of a snapshot is part of the snapshot, since the
    /// entry that set it may be among those the snapshot replaced.
    #[test]
    fn a_snapshot_keeps_its_membership() {
        let dir = Dir::new("snap-members");
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage.append(&entries(&[(1, 1), (2, 1)])).unwrap();
            let mut sink = storage
                .new_snapshot(meta(2, 1), Some(members(&[1, 3, 5])))
                .unwrap();
            sink.write_all(b"state").unwrap();
            storage.install_snapshot(sink).unwrap();
            assert_eq!(storage.snapshot_members(), Some(members(&[1, 3, 5])));
        }
        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.snapshot_members(), Some(members(&[1, 3, 5])));
        assert_eq!(storage.read_snapshot(0, 100).unwrap(), b"state");
    }

    /// A snapshot file from before membership could change is still read,
    /// and reports no membership rather than a wrong one.
    #[test]
    fn a_snapshot_in_the_old_format_still_opens() {
        let dir = Dir::new("snap-old-format");
        std::fs::create_dir_all(&dir.0).unwrap();
        let mut body = Vec::new();
        body.extend_from_slice(&7u64.to_le_bytes());
        body.extend_from_slice(&2u64.to_le_bytes());
        body.extend_from_slice(b"old state");
        let mut file = crc32_parts(&[&body]).to_le_bytes().to_vec();
        file.extend_from_slice(&body);
        std::fs::write(dir.0.join("snapshot"), &file).unwrap();

        let storage = DiskStorage::open(&dir.0).unwrap();
        assert_eq!(storage.snapshot_meta(), meta(7, 2));
        assert_eq!(storage.snapshot_members(), None);
        assert_eq!(storage.read_snapshot(0, 100).unwrap(), b"old state");
        let mut back = Vec::new();
        storage
            .snapshot_reader()
            .unwrap()
            .read_to_end(&mut back)
            .unwrap();
        assert_eq!(back, b"old state");
    }

    fn part_files(dir: &Path) -> Vec<String> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("snapshot.part-"))
            .collect()
    }

    /// A snapshot that is started and then given up on, because its leader
    /// moved on or a newer one overtook it, takes its part file with it.
    #[test]
    fn an_abandoned_snapshot_leaves_nothing_behind() {
        let dir = Dir::new("snap-abandoned");
        let mut storage = DiskStorage::open(&dir.0).unwrap();
        let mut sink = storage.new_snapshot(meta(3, 1), None).unwrap();
        sink.write_all(b"half a snapshot").unwrap();
        assert_eq!(part_files(&dir.0).len(), 1);
        drop(sink);
        assert!(part_files(&dir.0).is_empty(), "the part file outlived it");
        assert_eq!(storage.snapshot_meta(), SnapshotMeta::default());
    }

    /// What a crash in the middle of writing one leaves behind. It was
    /// never installed, so it goes, and the snapshot beside it stays.
    #[test]
    fn a_part_file_left_by_a_crash_is_cleared_on_open() {
        let dir = Dir::new("snap-stray");
        {
            let mut storage = DiskStorage::open(&dir.0).unwrap();
            storage.append(&entries(&[(1, 1), (2, 1)])).unwrap();
            storage.save_snapshot(meta(2, 1), b"kept").unwrap();
            let mut sink = storage.new_snapshot(meta(9, 1), None).unwrap();
            sink.write_all(b"interrupted").unwrap();
            sink.flush().unwrap();
            // A crash: nothing gets to run the sink's cleanup.
            std::mem::forget(sink);
        }
        assert_eq!(part_files(&dir.0).len(), 1);

        let storage = DiskStorage::open(&dir.0).unwrap();
        assert!(part_files(&dir.0).is_empty());
        assert_eq!(storage.snapshot_meta(), meta(2, 1));
        assert_eq!(storage.read_snapshot(0, 10).unwrap(), b"kept");
    }

    /// Reading the snapshot back as a stream sees exactly what was written,
    /// however it was written.
    #[test]
    fn a_snapshot_written_in_pieces_reads_back_whole() {
        let dir = Dir::new("snap-pieces");
        let mut storage = DiskStorage::open(&dir.0).unwrap();
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 253) as u8).collect();
        let mut sink = storage.new_snapshot(meta(4, 2), None).unwrap();
        for piece in data.chunks(7_777) {
            sink.write_all(piece).unwrap();
        }
        storage.install_snapshot(sink).unwrap();
        drop(storage);

        // Reopening checks the checksum over the whole file.
        let storage = DiskStorage::open(&dir.0).unwrap();
        let mut back = Vec::new();
        storage
            .snapshot_reader()
            .unwrap()
            .read_to_end(&mut back)
            .unwrap();
        assert_eq!(back, data);
    }
}
