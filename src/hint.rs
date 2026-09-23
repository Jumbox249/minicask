//! Hint files: the index of a data file, so that opening the store reads a
//! few bytes per record instead of every value.
//!
//! Without them, opening a store reads every byte of every data file to
//! rebuild the index, and the time that takes grows with the data rather
//! than with the number of keys. A hint holds just what the index needs
//! from each record, and lets the scan skip the rest.
//!
//! A hint describes a prefix of its data file, `data_len` bytes of it.
//! Data files only ever grow, so a prefix stays true, and whatever was
//! appended after the hint was written is scanned as before. That makes one
//! format cover both a sealed file, whose hint is the whole of it, and the
//! file a compaction has just written, which becomes the active file and
//! keeps growing.
//!
//! ```text
//!  0 ..  4   crc32 of everything after it
//!  4 .. 12   data_len: how much of the data file this describes
//! 12 ..      one entry per record, in file order:
//!              offset u64 | len u32 | tstamp u64 | flags u8 | key_len u32 | key
//! ```
//!
//! A hint is only ever written for bytes that are already synced, and it is
//! written beside its data file and renamed into place, so it is never
//! ahead of the data. Anything wrong with one, a bad checksum, a length the
//! data file does not have, an entry that runs past it, is a reason to
//! ignore it and scan the file, never an error: a hint only saves work.

use crate::crc::crc32_parts;
use crate::error::Result;
use crate::record::FLAG_TOMBSTONE;
use std::path::{Path, PathBuf};

const HEADER_LEN: usize = 4 + 8;
const ENTRY_FIXED: usize = 8 + 4 + 8 + 1 + 4;

/// What the index needs from one record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HintEntry {
    pub key: Vec<u8>,
    pub offset: u64,
    pub len: u32,
    pub tstamp: u64,
    pub tombstone: bool,
}

pub(crate) fn hint_path(dir: &Path, file_id: u64) -> PathBuf {
    dir.join(format!("{file_id:010}.hint"))
}

fn hint_name(file_id: u64) -> String {
    format!("{file_id:010}.hint")
}

/// Every hint file in the directory, by the data file it describes.
pub(crate) fn list(dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("hint") {
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
    Ok(ids)
}

/// Write the hint for the first `data_len` bytes of a data file, which
/// hold exactly `entries`. The caller has synced those bytes already.
pub(crate) fn write(dir: &Path, file_id: u64, data_len: u64, entries: &[HintEntry]) -> Result<()> {
    let mut body = Vec::with_capacity(8 + entries.len() * (ENTRY_FIXED + 16));
    body.extend_from_slice(&data_len.to_le_bytes());
    for entry in entries {
        body.extend_from_slice(&entry.offset.to_le_bytes());
        body.extend_from_slice(&entry.len.to_le_bytes());
        body.extend_from_slice(&entry.tstamp.to_le_bytes());
        body.push(if entry.tombstone { FLAG_TOMBSTONE } else { 0 });
        body.extend_from_slice(&(entry.key.len() as u32).to_le_bytes());
        body.extend_from_slice(&entry.key);
    }
    let mut file = Vec::with_capacity(4 + body.len());
    file.extend_from_slice(&crc32_parts(&[&body]).to_le_bytes());
    file.extend_from_slice(&body);
    crate::log::write_atomically(dir, &hint_name(file_id), &file)
}

/// Read a data file's hint, if it has one worth believing, given that the
/// data file is `file_len` bytes long now. Returns the entries and how much
/// of the file they cover.
pub(crate) fn load(dir: &Path, file_id: u64, file_len: u64) -> Option<(Vec<HintEntry>, u64)> {
    let bytes = std::fs::read(hint_path(dir, file_id)).ok()?;
    if bytes.len() < HEADER_LEN {
        return None;
    }
    let crc = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
    if crc32_parts(&[&bytes[4..]]) != crc {
        return None;
    }
    let data_len = u64::from_le_bytes(bytes[4..12].try_into().ok()?);
    // The data file is shorter than what the hint describes: whatever
    // happened to it, the hint no longer tells its truth.
    if data_len > file_len {
        return None;
    }

    let mut entries = Vec::new();
    let mut at = HEADER_LEN;
    let mut covered = 0u64;
    while at < bytes.len() {
        let fixed = bytes.get(at..at + ENTRY_FIXED)?;
        let offset = u64::from_le_bytes(fixed[0..8].try_into().ok()?);
        let len = u32::from_le_bytes(fixed[8..12].try_into().ok()?);
        let tstamp = u64::from_le_bytes(fixed[12..20].try_into().ok()?);
        let flags = fixed[20];
        let key_len = u32::from_le_bytes(fixed[21..25].try_into().ok()?) as usize;
        at += ENTRY_FIXED;
        let key = bytes.get(at..at.checked_add(key_len)?)?.to_vec();
        at += key_len;
        // Entries tile the file from the start, one record after another.
        // Anything else is not a hint this code wrote.
        if offset != covered {
            return None;
        }
        covered = offset.checked_add(len as u64)?;
        entries.push(HintEntry {
            key,
            offset,
            len,
            tstamp,
            tombstone: flags & FLAG_TOMBSTONE != 0,
        });
    }
    (covered == data_len).then_some((entries, data_len))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "minicask-hint-{label}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn entries() -> Vec<HintEntry> {
        vec![
            HintEntry {
                key: b"alpha".to_vec(),
                offset: 0,
                len: 40,
                tstamp: 7,
                tombstone: false,
            },
            HintEntry {
                key: b"\x00\xff".to_vec(),
                offset: 40,
                len: 23,
                tstamp: 8,
                tombstone: true,
            },
        ]
    }

    #[test]
    fn a_hint_round_trips() {
        let dir = temp("round-trip");
        write(&dir, 3, 63, &entries()).unwrap();
        assert_eq!(load(&dir, 3, 63), Some((entries(), 63)));
        // A file that has grown since is still described up to 63.
        assert_eq!(load(&dir, 3, 1000), Some((entries(), 63)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_hint_for_more_than_the_file_holds_is_ignored() {
        let dir = temp("too-long");
        write(&dir, 3, 63, &entries()).unwrap();
        assert_eq!(load(&dir, 3, 62), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_damaged_hint_is_ignored() {
        let dir = temp("damaged");
        write(&dir, 3, 63, &entries()).unwrap();
        let path = hint_path(&dir, 3);
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(load(&dir, 3, 63), None);
        std::fs::write(&path, &bytes[..bytes.len() - 3]).unwrap();
        assert_eq!(load(&dir, 3, 63), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn entries_that_do_not_tile_the_file_are_not_a_hint() {
        let dir = temp("gap");
        let mut gappy = entries();
        gappy[1].offset = 41;
        write(&dir, 3, 64, &gappy).unwrap();
        assert_eq!(load(&dir, 3, 64), None);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn no_hint_is_no_hint() {
        let dir = temp("missing");
        assert_eq!(load(&dir, 3, 63), None);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
