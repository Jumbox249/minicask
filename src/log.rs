//! Data files: append-only logs named `0000000001.log` inside the store
//! directory, plus a forward scanner used to rebuild state at startup.

use crate::error::Result;
use crate::record::{Header, HEADER_LEN};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// How hard the store tries to get bytes onto the physical disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// `fsync` after every write. Survives a power cut, costs a disk round
    /// trip per put.
    EveryWrite,
    /// Hand bytes to the kernel and move on. Survives the process being
    /// killed, but not the machine losing power.
    OsCache,
}

pub fn data_file_path(dir: &Path, file_id: u64) -> PathBuf {
    dir.join(format!("{file_id:010}.log"))
}

/// Replace `dir/name` with `bytes` so that a crash leaves either the old
/// contents or the new ones and never a blend: written beside the target,
/// fsynced, then renamed over it.
pub fn write_atomically(dir: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let tmp = dir.join(format!("{name}.tmp"));
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, dir.join(name))?;
    sync_dir(dir)
}

/// A rename is only durable once the directory holding it is. Windows has
/// no equivalent call and does not need one.
#[cfg(unix)]
pub fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(not(unix))]
pub fn sync_dir(_dir: &Path) -> Result<()> {
    Ok(())
}

/// Every data file in the directory, oldest first.
pub fn list_data_files(dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("log") {
            continue;
        }
        if let Some(id) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| s.parse::<u64>().ok())
        {
            ids.push(id);
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// How far an append has to get before it returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// The writer's own buffer. Visible to the store's reads at once, and
    /// handed to the kernel at the next flush or sync, or once the buffer
    /// passes [`MAX_PENDING`]. Until then it survives neither a crash nor
    /// a power cut.
    Buffer,
    /// The kernel, which survives the process being killed.
    Kernel,
    /// The disk, which survives the power going.
    Disk,
}

/// Buffered appends are handed to the kernel once they reach this much, so
/// a long run of them, a snapshot being restored say, holds a bounded
/// amount in memory.
pub const MAX_PENDING: usize = 1024 * 1024;

/// The append end of the active data file.
pub struct LogWriter {
    file: File,
    pub file_id: u64,
    /// Everything appended, including what is still buffered.
    pub offset: u64,
    /// How much has been handed to the kernel. Everything after it is in
    /// `pending`.
    written: u64,
    /// How much of the file is known to be on the disk rather than only in
    /// the kernel's cache. Everything present at open counts, since
    /// whatever survived to be read back is by definition on the disk.
    pub synced: u64,
    /// Appends that have not been handed to the kernel yet.
    ///
    /// This is what lets a group commit batch anything at all on NTFS,
    /// which holds a write to a file until a flush of that same file has
    /// finished: measured, a 64-byte append took 0.8 microseconds alone
    /// and half a millisecond beside a flush. Writing into the kernel
    /// while another thread fsyncs would put every writer behind every
    /// fsync, one at a time. Buffered here instead, they cost nothing
    /// while the fsync runs, and go to the kernel together afterwards.
    pending: Vec<u8>,
}

impl LogWriter {
    pub fn open(dir: &Path, file_id: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(data_file_path(dir, file_id))?;
        let offset = file.metadata()?.len();
        Ok(LogWriter {
            file,
            file_id,
            offset,
            written: offset,
            synced: offset,
            pending: Vec::new(),
        })
    }

    /// Append one encoded record, take it as far as `reach` says, and
    /// return where it landed.
    pub fn append(&mut self, bytes: &[u8], reach: Reach) -> Result<(u64, u32)> {
        let offset = self.offset;
        if reach != Reach::Buffer && self.pending.is_empty() {
            // Nothing buffered to go first, so no need to copy this through
            // the buffer on its way to the kernel.
            self.file.write_all(bytes)?;
            self.offset += bytes.len() as u64;
            self.written = self.offset;
            if reach == Reach::Disk {
                self.file.sync_data()?;
                self.synced = self.offset;
            }
            return Ok((offset, bytes.len() as u32));
        }
        self.pending.extend_from_slice(bytes);
        self.offset += bytes.len() as u64;
        match reach {
            Reach::Buffer if self.pending.len() < MAX_PENDING => {}
            Reach::Buffer | Reach::Kernel => self.flush()?,
            Reach::Disk => self.sync()?,
        }
        Ok((offset, bytes.len() as u32))
    }

    /// Hand everything buffered to the kernel, in one write.
    pub fn flush(&mut self) -> Result<()> {
        if !self.pending.is_empty() {
            self.file.write_all(&self.pending)?;
            self.pending.clear();
            self.written = self.offset;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.flush()?;
        self.file.sync_data()?;
        self.synced = self.offset;
        Ok(())
    }

    /// The bytes of a record that is still buffered, if this one is.
    pub fn buffered(&self, offset: u64, len: u32) -> Option<&[u8]> {
        let start = usize::try_from(offset.checked_sub(self.written)?).ok()?;
        self.pending.get(start..start.checked_add(len as usize)?)
    }

    /// Forget what is buffered, as a crash would. For
    /// [`Store::simulate_power_cut`](crate::Store::simulate_power_cut).
    pub fn discard_buffer(&mut self) {
        self.pending.clear();
    }

    /// A second handle on the file, which can fsync it without borrowing
    /// the writer. It only sees what has been flushed.
    pub fn try_clone_file(&self) -> Result<File> {
        Ok(self.file.try_clone()?)
    }
}

impl Drop for LogWriter {
    /// A writer closed normally hands its buffer to the kernel, so that
    /// buffering is never the difference between a clean shutdown keeping
    /// a write and losing it.
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

/// One record recovered by [`Scanner`].
pub struct Scanned {
    pub offset: u64,
    pub len: u32,
    pub header: Header,
    pub key: Vec<u8>,
}

/// What the scanner found when it asked for the next record.
pub enum ScanOutcome {
    Record(Scanned),
    /// The file ended exactly on a record boundary, which is what a clean
    /// shutdown looks like.
    Eof,
    /// The file ends mid-record, or the last record fails its checksum. This
    /// is what a crash looks like, and it is always the tail of the newest
    /// file.
    Torn {
        offset: u64,
        detail: &'static str,
    },
}

/// Walks a data file from the start, verifying every record as it goes.
pub struct Scanner {
    reader: BufReader<File>,
    offset: u64,
}

impl Scanner {
    /// Scan from `offset`, which must be where a record starts: the start
    /// of the file, or the end of what a hint already describes.
    pub fn open_at(dir: &Path, file_id: u64, offset: u64) -> Result<Self> {
        let mut file = File::open(data_file_path(dir, file_id))?;
        file.seek(SeekFrom::Start(offset))?;
        Ok(Scanner {
            reader: BufReader::with_capacity(64 * 1024, file),
            offset,
        })
    }

    pub fn next_record(&mut self) -> Result<ScanOutcome> {
        let start = self.offset;
        let mut header_buf = [0u8; HEADER_LEN];
        match read_full(&mut self.reader, &mut header_buf)? {
            0 => return Ok(ScanOutcome::Eof),
            n if n < HEADER_LEN => {
                return Ok(ScanOutcome::Torn {
                    offset: start,
                    detail: "file ends inside a record header",
                })
            }
            _ => {}
        }

        let header = Header::decode(&header_buf);
        let mut key = vec![0u8; header.key_len as usize];
        let mut value = vec![0u8; header.value_len as usize];
        if read_full(&mut self.reader, &mut key)? < key.len()
            || read_full(&mut self.reader, &mut value)? < value.len()
        {
            return Ok(ScanOutcome::Torn {
                offset: start,
                detail: "file ends inside a record payload",
            });
        }
        if !header.verify(&key, &value) {
            return Ok(ScanOutcome::Torn {
                offset: start,
                detail: "checksum mismatch",
            });
        }

        let len = header.record_len();
        self.offset += len;
        Ok(ScanOutcome::Record(Scanned {
            offset: start,
            len: len as u32,
            header,
            key,
        }))
    }
}

/// Reads one record's bytes out of a data file that is already open.
///
/// A positional read, never a seek followed by a read. The two-step version
/// moves a cursor that every reader of the handle shares, so two threads
/// reading at once could each land on the other's offset. Reading at an
/// explicit offset leaves nothing shared, which is what lets the store
/// answer reads from several threads behind a read lock.
pub fn read_at(file: &File, offset: u64, len: u32) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; len as usize];
    read_exact_at(file, &mut buf, offset)?;
    Ok(buf)
}

#[cfg(unix)]
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    std::os::unix::fs::FileExt::read_exact_at(file, buf, offset)
}

/// Windows has no `read_exact_at`, only `seek_read`, which may return
/// short. It does move the handle's cursor, but it reads from the offset it
/// is given rather than from the cursor, so concurrent calls do not
/// interfere.
#[cfg(windows)]
fn read_exact_at(file: &File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "data file ends before the record does",
                ))
            }
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Like `read_exact`, but reports a short read instead of failing, so the
/// caller can tell a torn tail apart from a real I/O error.
fn read_full<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    Ok(filled)
}
