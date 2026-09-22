#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

static COUNTER: AtomicU32 = AtomicU32::new(0);

/// A scratch directory that cleans itself up. Saves pulling in `tempfile`
/// just for the test suite.
pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new(label: &str) -> TempDir {
        let n = COUNTER.fetch_add(1, Ordering::SeqCst);
        let path =
            std::env::temp_dir().join(format!("minicask-{label}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp dir");
        TempDir { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn str(&self) -> &str {
        self.path.to_str().expect("utf-8 temp path")
    }

    /// Data files in the directory, oldest first.
    pub fn data_files(&self) -> Vec<PathBuf> {
        let mut logs: Vec<PathBuf> = std::fs::read_dir(&self.path)
            .expect("read temp dir")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("log"))
            .collect();
        logs.sort();
        logs
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

/// Flip one bit at `offset` in a file, simulating bit rot.
pub fn corrupt_byte(path: &Path, offset: u64) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .expect("open for corruption");
    file.seek(SeekFrom::Start(offset)).expect("seek");
    let mut byte = [0u8; 1];
    file.read_exact(&mut byte).expect("read byte");
    byte[0] ^= 0b0100_0000;
    file.seek(SeekFrom::Start(offset)).expect("seek back");
    file.write_all(&byte).expect("write byte");
    file.sync_all().expect("sync");
}
