//! The store itself: an in-memory index over a set of append-only files.

use crate::error::{Error, Result};
use crate::log::{
    self, data_file_path, list_data_files, LogWriter, ScanOutcome, Scanner, SyncPolicy,
};
use crate::record::{self, HEADER_LEN};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

/// Where a key's newest record lives. This is the only thing held in memory
/// per key, so the index stays small even when values are large.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Location {
    pub file_id: u64,
    pub offset: u64,
    pub len: u32,
    pub tstamp: u64,
}

/// Tunables, all with defaults that are safe rather than fast.
#[derive(Debug, Clone, Copy)]
pub struct Options {
    /// Durability of each write. Defaults to `EveryWrite`.
    pub sync: SyncPolicy,
    /// Roll over to a new data file once the active one passes this size.
    pub max_file_bytes: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            sync: SyncPolicy::EveryWrite,
            max_file_bytes: 64 * 1024 * 1024,
        }
    }
}

/// A snapshot of how much of the data on disk is still worth keeping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub keys: usize,
    pub files: usize,
    /// Bytes belonging to records the index still points at.
    pub live_bytes: u64,
    /// Bytes occupied by data files, live or not.
    pub disk_bytes: u64,
}

impl Stats {
    /// What a compaction would hand back to the filesystem.
    pub fn reclaimable_bytes(&self) -> u64 {
        self.disk_bytes.saturating_sub(self.live_bytes)
    }

    /// Reclaimable share of the data files, 0.0 to 1.0.
    pub fn fragmentation(&self) -> f64 {
        if self.disk_bytes == 0 {
            0.0
        } else {
            self.reclaimable_bytes() as f64 / self.disk_bytes as f64
        }
    }
}

/// What a compaction did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactReport {
    pub files_before: usize,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

impl CompactReport {
    pub fn reclaimed_bytes(&self) -> u64 {
        self.bytes_before.saturating_sub(self.bytes_after)
    }
}

/// An embedded key/value store backed by append-only files.
///
/// Reads are one hash lookup plus one read from the right offset. Writes
/// append to the newest file and update the index. Nothing is ever mutated
/// in place, which is what makes recovery after a crash a matter of replaying
/// the log rather than repairing it.
pub struct Store {
    dir: PathBuf,
    keydir: HashMap<Vec<u8>, Location>,
    readers: HashMap<u64, File>,
    writer: LogWriter,
    opts: Options,
    live_bytes: u64,
    disk_bytes: u64,
}

impl Store {
    /// Open (or create) a store in `dir` with default options.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        Store::open_with(dir, Options::default())
    }

    /// Open (or create) a store, replaying every data file to rebuild the
    /// index.
    ///
    /// If the newest file ends in a half-written record, which is what a
    /// crash mid-append leaves behind, the partial tail is truncated away and
    /// everything before it is kept. Corruption anywhere else is reported
    /// rather than silently repaired.
    pub fn open_with<P: AsRef<Path>>(dir: P, opts: Options) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let file_ids = list_data_files(&dir)?;
        let newest = file_ids.last().copied();

        let mut keydir: HashMap<Vec<u8>, Location> = HashMap::new();
        let mut readers = HashMap::new();
        let mut live_bytes = 0u64;
        let mut disk_bytes = 0u64;

        for &file_id in &file_ids {
            let mut scanner = Scanner::open(&dir, file_id)?;
            loop {
                match scanner.next_record()? {
                    ScanOutcome::Record(rec) => {
                        disk_bytes += rec.len as u64;
                        if let Some(old) = keydir.remove(&rec.key) {
                            live_bytes -= old.len as u64;
                        }
                        if !rec.header.is_tombstone() {
                            live_bytes += rec.len as u64;
                            keydir.insert(
                                rec.key,
                                Location {
                                    file_id,
                                    offset: rec.offset,
                                    len: rec.len,
                                    tstamp: rec.header.tstamp,
                                },
                            );
                        }
                    }
                    ScanOutcome::Eof => break,
                    ScanOutcome::Torn { offset, detail } => {
                        // Only the file that was being appended to can end
                        // mid-record. Anything else means real damage.
                        if Some(file_id) != newest {
                            return Err(Error::Corrupt {
                                file_id,
                                offset,
                                detail,
                            });
                        }
                        truncate_at(&dir, file_id, offset)?;
                        break;
                    }
                }
            }
            readers.insert(file_id, File::open(data_file_path(&dir, file_id))?);
        }

        let active_id = newest.unwrap_or(1);
        let writer = LogWriter::open(&dir, active_id)?;
        readers
            .entry(active_id)
            .or_insert(File::open(data_file_path(&dir, active_id))?);

        Ok(Store {
            dir,
            keydir,
            readers,
            writer,
            opts,
            live_bytes,
            disk_bytes,
        })
    }

    /// Store a value, replacing any previous one for this key.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let sync = self.opts.sync == SyncPolicy::EveryWrite;
        self.put_with(key, value, sync)
    }

    /// Store a value without waiting for it to reach the disk, whatever the
    /// sync policy. It is visible to reads at once, and durable after the
    /// next [`sync`](Store::sync).
    ///
    /// For a caller that writes many records and needs them durable as a
    /// batch rather than one at a time: one fsync for the lot instead of
    /// one each.
    pub fn put_deferred(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_with(key, value, false)
    }

    fn put_with(&mut self, key: &[u8], value: &[u8], sync: bool) -> Result<()> {
        let bytes = record::encode(key, Some(value), record::now_millis())?;
        self.roll_if_needed(bytes.len() as u64)?;
        let (offset, len) = self.writer.append(&bytes, sync)?;

        self.disk_bytes += len as u64;
        if let Some(old) = self.keydir.remove(key) {
            self.live_bytes -= old.len as u64;
        }
        self.live_bytes += len as u64;
        self.keydir.insert(
            key.to_vec(),
            Location {
                file_id: self.writer.file_id,
                offset,
                len,
                tstamp: record::now_millis(),
            },
        );
        Ok(())
    }

    /// Fetch a value. `Ok(None)` means the key is not there; an error means
    /// the record that should be there could not be read or did not match its
    /// checksum.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(loc) = self.keydir.get(key) else {
            return Ok(None);
        };
        read_value(&self.readers, loc).map(Some)
    }

    /// Remove a key. Returns whether it was there to begin with.
    ///
    /// Deletion appends a tombstone rather than erasing anything, so the
    /// space comes back at the next compaction, not immediately.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let sync = self.opts.sync == SyncPolicy::EveryWrite;
        self.delete_with(key, sync)
    }

    /// Remove a key without waiting for the tombstone to reach the disk.
    /// See [`put_deferred`](Store::put_deferred).
    pub fn delete_deferred(&mut self, key: &[u8]) -> Result<bool> {
        self.delete_with(key, false)
    }

    fn delete_with(&mut self, key: &[u8], sync: bool) -> Result<bool> {
        if !self.keydir.contains_key(key) {
            return Ok(false);
        }
        let bytes = record::encode(key, None, record::now_millis())?;
        self.roll_if_needed(bytes.len() as u64)?;
        let (_, len) = self.writer.append(&bytes, sync)?;

        self.disk_bytes += len as u64;
        if let Some(old) = self.keydir.remove(key) {
            self.live_bytes -= old.len as u64;
        }
        Ok(true)
    }

    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.keydir.contains_key(key)
    }

    /// Every live key, in no particular order.
    pub fn keys(&self) -> impl Iterator<Item = &[u8]> {
        self.keydir.keys().map(|k| k.as_slice())
    }

    pub fn len(&self) -> usize {
        self.keydir.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keydir.is_empty()
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Force everything written so far onto the disk.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.sync()
    }

    pub fn stats(&self) -> Stats {
        Stats {
            keys: self.keydir.len(),
            files: self.readers.len(),
            live_bytes: self.live_bytes,
            disk_bytes: self.disk_bytes,
        }
    }

    /// Rewrite the live records into a single new data file and delete the
    /// old ones.
    ///
    /// The merged file takes an id higher than everything it replaces, so a
    /// crash partway through leaves the old files untouched and the next
    /// startup simply ignores the incomplete merge.
    pub fn compact(&mut self) -> Result<CompactReport> {
        let before = self.stats();
        let old_ids: Vec<u64> = self.readers.keys().copied().collect();
        let merged_id = old_ids.iter().copied().max().unwrap_or(0) + 1;

        // Reading in file order keeps the disk head moving one way.
        let mut entries: Vec<(Vec<u8>, Location)> =
            self.keydir.iter().map(|(k, v)| (k.clone(), *v)).collect();
        entries.sort_unstable_by_key(|(_, loc)| (loc.file_id, loc.offset));

        // Merging is one long sequential write, so there is nothing to gain
        // from an fsync per record: a single sync once the whole merged file
        // is written is the barrier that matters, and it has to happen before
        // any of the originals are unlinked.
        let mut writer = LogWriter::open(&self.dir, merged_id)?;
        let mut merged: HashMap<Vec<u8>, Location> = HashMap::with_capacity(entries.len());
        let mut merged_bytes = 0u64;

        for (key, loc) in entries {
            let file = self.readers.get(&loc.file_id).ok_or(Error::Corrupt {
                file_id: loc.file_id,
                offset: loc.offset,
                detail: "index points at a data file that is not open",
            })?;
            // Copying the encoded bytes verbatim keeps each record's original
            // checksum and timestamp intact.
            let bytes = log::read_at(file, loc.offset, loc.len)?;
            let (offset, len) = writer.append(&bytes, false)?;
            merged_bytes += len as u64;
            merged.insert(
                key,
                Location {
                    file_id: merged_id,
                    offset,
                    len,
                    tstamp: loc.tstamp,
                },
            );
        }
        writer.sync()?;

        // The merged file is durable now, so the originals are safe to drop.
        for id in &old_ids {
            self.readers.remove(id);
            let path = data_file_path(&self.dir, *id);
            if path.exists() {
                std::fs::remove_file(path)?;
            }
        }

        self.readers
            .insert(merged_id, File::open(data_file_path(&self.dir, merged_id))?);
        self.keydir = merged;
        self.writer = writer;
        self.live_bytes = merged_bytes;
        self.disk_bytes = merged_bytes;

        Ok(CompactReport {
            files_before: before.files,
            bytes_before: before.disk_bytes,
            bytes_after: merged_bytes,
        })
    }

    /// The store as it stands right now, readable without the store.
    ///
    /// This works because nothing on disk is ever overwritten: a write
    /// after this point appends a new record somewhere else and leaves the
    /// one the view points at where it was. The view keeps a copy of the
    /// index, so it costs memory per key but none per value, and its own
    /// file handles, so it can be read on another thread while the store
    /// carries on taking writes.
    ///
    /// The one thing that would pull records out from under it is a
    /// [`compact`](Store::compact), which deletes the files they are in.
    /// Deleting a file that is open is allowed everywhere this runs (Rust
    /// opens files on Windows with delete sharing), and the view's handles
    /// keep the data readable until they close, but the caller that owns
    /// the store is still better off not compacting while a view is out.
    pub(crate) fn view(&self) -> Result<StoreView> {
        let mut entries: Vec<(Vec<u8>, Location)> =
            self.keydir.iter().map(|(k, v)| (k.clone(), *v)).collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let mut readers = HashMap::with_capacity(self.readers.len());
        for &id in self.readers.keys() {
            readers.insert(id, File::open(data_file_path(&self.dir, id))?);
        }
        Ok(StoreView { entries, readers })
    }

    /// Start a new data file once the active one has grown past the limit.
    fn roll_if_needed(&mut self, incoming: u64) -> Result<()> {
        if self.writer.offset == 0 || self.writer.offset + incoming <= self.opts.max_file_bytes {
            return Ok(());
        }
        // Whatever was written to the file being sealed without a sync gets
        // one now: `sync` only ever reaches the active file, so after this
        // point nothing would.
        self.writer.sync()?;
        let next_id = self.writer.file_id + 1;
        self.writer = LogWriter::open(&self.dir, next_id)?;
        self.readers
            .insert(next_id, File::open(data_file_path(&self.dir, next_id))?);
        Ok(())
    }
}

/// A frozen copy of a store's index, and the files it points into. See
/// [`Store::view`].
pub(crate) struct StoreView {
    /// In key order, so that walking a view is deterministic.
    entries: Vec<(Vec<u8>, Location)>,
    readers: HashMap<u64, File>,
}

impl StoreView {
    /// Every live key and its value, in key order, one value in memory at
    /// a time.
    pub(crate) fn for_each(&self, mut f: impl FnMut(&[u8], &[u8]) -> Result<()>) -> Result<()> {
        for (key, loc) in &self.entries {
            let value = read_value(&self.readers, loc)?;
            f(key, &value)?;
        }
        Ok(())
    }
}

/// Read and verify the value a location points at.
///
/// The index was built from records that passed their checksums, but the
/// bytes can have changed on disk since. A header that no longer describes
/// the record it heads is reported as corruption, never trusted to slice
/// with.
fn read_value(readers: &HashMap<u64, File>, loc: &Location) -> Result<Vec<u8>> {
    let corrupt = |detail| Error::Corrupt {
        file_id: loc.file_id,
        offset: loc.offset,
        detail,
    };
    let file = readers
        .get(&loc.file_id)
        .ok_or(corrupt("index points at a data file that is not open"))?;

    let bytes = log::read_at(file, loc.offset, loc.len)?;
    let header_bytes: &[u8; HEADER_LEN] = bytes
        .get(..HEADER_LEN)
        .and_then(|h| h.try_into().ok())
        .ok_or(corrupt("record shorter than a header"))?;
    let header = record::Header::decode(header_bytes);
    if header.record_len() != bytes.len() as u64 {
        return Err(corrupt("record header does not match its length"));
    }
    let key_end = HEADER_LEN + header.key_len as usize;
    let value = bytes[key_end..].to_vec();
    if !header.verify(&bytes[HEADER_LEN..key_end], &value) {
        return Err(corrupt("checksum mismatch on read"));
    }
    Ok(value)
}

/// Cut a half-written record off the end of a data file.
fn truncate_at(dir: &Path, file_id: u64, offset: u64) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(data_file_path(dir, file_id))?;
    file.set_len(offset)?;
    file.sync_all()?;
    Ok(())
}
