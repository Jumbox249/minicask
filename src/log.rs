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

/// The append end of the active data file.
pub struct LogWriter {
    file: File,
    pub file_id: u64,
    pub offset: u64,
    sync: SyncPolicy,
}

impl LogWriter {
    pub fn open(dir: &Path, file_id: u64, sync: SyncPolicy) -> Result<Self> {
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
            sync,
        })
    }

    /// Append one encoded record and return where it landed.
    ///
    /// The write goes straight to the kernel rather than into a user-space
    /// buffer, so a reader opening the same file sees the record immediately
    /// even when `SyncPolicy::OsCache` is in force.
    pub fn append(&mut self, bytes: &[u8]) -> Result<(u64, u32)> {
        let offset = self.offset;
        self.file.write_all(bytes)?;
        if self.sync == SyncPolicy::EveryWrite {
            self.file.sync_data()?;
        }
        self.offset += bytes.len() as u64;
        Ok((offset, bytes.len() as u32))
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    /// Change the durability policy of an open writer. Compaction uses this
    /// to merge under `OsCache` and fsync once at the end, then hand the
    /// writer back with the store's real policy restored.
    pub fn set_sync(&mut self, sync: SyncPolicy) {
        self.sync = sync;
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
    pub fn open(dir: &Path, file_id: u64) -> Result<Self> {
        let file = File::open(data_file_path(dir, file_id))?;
        Ok(Scanner {
            reader: BufReader::with_capacity(64 * 1024, file),
            offset: 0,
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
pub fn read_at(file: &File, offset: u64, len: u32) -> Result<Vec<u8>> {
    let mut handle = file;
    handle.seek(SeekFrom::Start(offset))?;
    let mut buf = vec![0u8; len as usize];
    handle.read_exact(&mut buf)?;
    Ok(buf)
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
