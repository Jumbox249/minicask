//! The store itself: an in-memory index over a set of append-only files.

use crate::error::{Error, Result};
use crate::log::{
    self, data_file_path, list_data_files, LogWriter, Reach, ScanOutcome, Scanner, SyncPolicy,
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
    /// How much of each sealed file had reached the disk when it was
    /// sealed. Sealing syncs, so this is all of it; it is kept so that
    /// [`simulate_power_cut`](Store::simulate_power_cut) can tell.
    sealed_synced: HashMap<u64, u64>,
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
            sealed_synced: HashMap::new(),
        })
    }

    pub fn options(&self) -> Options {
        self.opts
    }

    /// Where appends have reached: the active file's id and the offset in
    /// it. Points compare in the order they were written, which is what a
    /// caller doing its own group commit needs to know whether a sync
    /// covered a write.
    pub(crate) fn append_point(&self) -> (u64, u64) {
        (self.writer.file_id, self.writer.offset)
    }

    /// A handle that can fsync everything appended so far without holding
    /// the store, and the point it will have made durable once it has.
    ///
    /// Everything in files before the active one is durable already,
    /// because sealing a file syncs it, so syncing the active file is
    /// enough.
    pub(crate) fn sync_handle(&mut self) -> Result<SyncHandle> {
        // The handle sees only what the kernel has, so the buffer goes to
        // the kernel first. That is one write, and the fsync that follows
        // runs without the store.
        self.writer.flush()?;
        Ok(SyncHandle {
            file: self.writer.try_clone_file()?,
            point: self.append_point(),
        })
    }

    /// Record that a [`SyncHandle`] made everything up to `point` durable,
    /// so that the store's own account of what is on the disk, which
    /// [`is_synced`](Store::is_synced) and a simulated power cut go by, is
    /// not behind the truth.
    pub(crate) fn note_synced(&mut self, point: (u64, u64)) {
        if point.0 == self.writer.file_id && point.1 > self.writer.synced {
            self.writer.synced = point.1;
        }
    }

    /// Whether everything written has reached the disk.
    pub(crate) fn is_synced(&self) -> bool {
        self.writer.synced == self.writer.offset
    }

    /// Throw away everything that has not been synced, which is what a
    /// power cut does, and close the store.
    ///
    /// This is for tests of the ordering rules that durability depends on,
    /// which cannot otherwise be observed without pulling a plug. A write
    /// the store has only handed to the kernel is gone afterwards, exactly
    /// as it would be.
    #[doc(hidden)]
    pub fn simulate_power_cut(mut self) -> Result<()> {
        let mut cut: Vec<(u64, u64)> = self
            .sealed_synced
            .iter()
            .map(|(&id, &synced)| (id, synced))
            .collect();
        cut.push((self.writer.file_id, self.writer.synced));
        let dir = self.dir.clone();
        // The writer's buffer is memory, and goes with the power. Closing
        // normally would flush it.
        self.writer.discard_buffer();
        drop(self);
        for (id, synced) in cut {
            let path = data_file_path(&dir, id);
            if path.exists() {
                let file = OpenOptions::new().write(true).open(path)?;
                // Only ever shorter: what never reached the file cannot be
                // on the disk, whatever the store believed it had synced.
                let len = file.metadata()?.len().min(synced);
                file.set_len(len)?;
            }
        }
        Ok(())
    }

    /// How far the sync policy says an ordinary write has to get.
    fn reach(&self) -> Reach {
        match self.opts.sync {
            SyncPolicy::EveryWrite => Reach::Disk,
            SyncPolicy::OsCache => Reach::Kernel,
        }
    }

    /// Store a value, replacing any previous one for this key.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let reach = self.reach();
        self.put_with(key, value, reach)
    }

    /// Store a value without waiting for it to go anywhere, whatever the
    /// sync policy. It is visible to reads at once, and durable after the
    /// next [`sync`](Store::sync); until then not even a crash of the
    /// process is survived, since it may not have reached the kernel.
    ///
    /// For a caller that writes many records and needs them durable as a
    /// batch rather than one at a time: one fsync for the lot instead of
    /// one each, and not even a system call for most of them.
    pub fn put_deferred(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put_with(key, value, Reach::Buffer)
    }

    fn put_with(&mut self, key: &[u8], value: &[u8], reach: Reach) -> Result<()> {
        let bytes = record::encode(key, Some(value), record::now_millis())?;
        self.roll_if_needed(bytes.len() as u64)?;
        let (offset, len) = self.writer.append(&bytes, reach)?;

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
        if loc.file_id == self.writer.file_id {
            if let Some(bytes) = self.writer.buffered(loc.offset, loc.len) {
                return decode_value(bytes, loc).map(Some);
            }
        }
        read_value(&self.readers, loc).map(Some)
    }

    /// Remove a key. Returns whether it was there to begin with.
    ///
    /// Deletion appends a tombstone rather than erasing anything, so the
    /// space comes back at the next compaction, not immediately.
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        let reach = self.reach();
        self.delete_with(key, reach)
    }

    /// Remove a key without waiting for the tombstone to go anywhere. See
    /// [`put_deferred`](Store::put_deferred).
    pub fn delete_deferred(&mut self, key: &[u8]) -> Result<bool> {
        self.delete_with(key, Reach::Buffer)
    }

    fn delete_with(&mut self, key: &[u8], reach: Reach) -> Result<bool> {
        if !self.keydir.contains_key(key) {
            return Ok(false);
        }
        let bytes = record::encode(key, None, record::now_millis())?;
        self.roll_if_needed(bytes.len() as u64)?;
        let (_, len) = self.writer.append(&bytes, reach)?;

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
        // The merge reads records back out of the files, so none of them
        // can still be sitting in the writer's buffer.
        self.writer.flush()?;
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
            let (offset, len) = writer.append(&bytes, Reach::Buffer)?;
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
            self.sealed_synced.remove(id);
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
    pub(crate) fn view(&mut self) -> Result<StoreView> {
        // The view reads the files through handles of its own, so anything
        // still buffered has to be in them first.
        self.writer.flush()?;
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
        self.sealed_synced
            .insert(self.writer.file_id, self.writer.synced);
        let next_id = self.writer.file_id + 1;
        self.writer = LogWriter::open(&self.dir, next_id)?;
        self.readers
            .insert(next_id, File::open(data_file_path(&self.dir, next_id))?);
        Ok(())
    }
}

/// See [`Store::sync_handle`].
pub(crate) struct SyncHandle {
    file: File,
    point: (u64, u64),
}

impl SyncHandle {
    /// Fsync, and return the point that is now durable.
    pub(crate) fn sync(self) -> Result<(u64, u64)> {
        self.file.sync_data()?;
        Ok(self.point)
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
    decode_value(&log::read_at(file, loc.offset, loc.len)?, loc)
}

/// Check a record's bytes and take its value out of them.
fn decode_value(bytes: &[u8], loc: &Location) -> Result<Vec<u8>> {
    let corrupt = |detail| Error::Corrupt {
        file_id: loc.file_id,
        offset: loc.offset,
        detail,
    };
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
