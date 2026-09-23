//! Hint files: opening a store from each data file's index instead of
//! reading every value, and falling back to a scan whenever a hint cannot
//! be believed.

mod common;

use common::TempDir;
use minicask::{Options, Store};

fn small_files() -> Options {
    Options {
        max_file_bytes: 1024,
        ..Options::default()
    }
}

/// Writes across many files, with overwrites and deletes that land in
/// later files than the records they replace.
fn fill(store: &mut Store) {
    for i in 0..300 {
        store
            .put(format!("key-{i:03}").as_bytes(), &[b'v'; 50])
            .unwrap();
    }
    for i in (0..300).step_by(3) {
        store
            .put(format!("key-{i:03}").as_bytes(), b"overwritten")
            .unwrap();
    }
    for i in (1..300).step_by(3) {
        assert!(store.delete(format!("key-{i:03}").as_bytes()).unwrap());
    }
}

fn check(store: &Store) {
    assert_eq!(store.len(), 200);
    for i in 0..300 {
        let got = store.get(format!("key-{i:03}").as_bytes()).unwrap();
        let want = match i % 3 {
            0 => Some(b"overwritten".to_vec()),
            1 => None,
            _ => Some(vec![b'v'; 50]),
        };
        assert_eq!(got, want, "key-{i:03}");
    }
}

fn hints(dir: &TempDir) -> Vec<std::path::PathBuf> {
    let mut hints: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("hint"))
        .collect();
    hints.sort();
    hints
}

fn active_len(dir: &TempDir) -> u64 {
    std::fs::metadata(dir.data_files().last().unwrap())
        .unwrap()
        .len()
}

/// Every sealed file gets a hint, and opening reads the hints and scans
/// only the active file, with the same result as a scan of everything,
/// tombstones in later files included.
#[test]
fn a_store_opens_from_its_hints() {
    let dir = TempDir::new("hints-open");
    {
        let mut store = Store::open_with(dir.path(), small_files()).unwrap();
        fill(&mut store);
        check(&store);
    }
    let files = dir.data_files().len();
    assert!(files > 10, "expected many data files, got {files}");
    assert_eq!(hints(&dir).len(), files - 1, "one hint per sealed file");

    let store = Store::open_with(dir.path(), small_files()).unwrap();
    assert_eq!(
        store.bytes_scanned_at_open(),
        active_len(&dir),
        "only the active file should have been scanned"
    );
    check(&store);

    let verified = Store::open_with(
        dir.path(),
        Options {
            verify_on_open: true,
            ..small_files()
        },
    )
    .unwrap();
    assert_eq!(verified.stats(), store.stats());
}

/// A hint that has been damaged is not believed, and neither is one that
/// is missing. The file is scanned instead, and a good hint written for
/// next time.
#[test]
fn a_damaged_or_missing_hint_is_rebuilt_from_a_scan() {
    let dir = TempDir::new("hints-damaged");
    {
        let mut store = Store::open_with(dir.path(), small_files()).unwrap();
        fill(&mut store);
    }
    let all = hints(&dir);
    let mut bytes = std::fs::read(&all[0]).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x40;
    std::fs::write(&all[0], &bytes).unwrap();
    std::fs::remove_file(&all[1]).unwrap();

    let store = Store::open_with(dir.path(), small_files()).unwrap();
    check(&store);
    assert!(
        store.bytes_scanned_at_open() > active_len(&dir),
        "the damaged and missing hints should have cost a scan"
    );
    drop(store);

    assert_eq!(
        hints(&dir).len(),
        all.len(),
        "both hints were written again"
    );
    let store = Store::open_with(dir.path(), small_files()).unwrap();
    assert_eq!(store.bytes_scanned_at_open(), active_len(&dir));
    check(&store);
}

/// A compaction leaves one file, still the active one, and a hint for
/// what is in it. Reopening scans nothing, and writes made after the
/// compaction are scanned from where the hint stops.
#[test]
fn a_compacted_store_opens_from_the_merged_files_hint() {
    let dir = TempDir::new("hints-compacted");
    {
        let mut store = Store::open_with(dir.path(), small_files()).unwrap();
        fill(&mut store);
        store.compact().unwrap();
    }
    assert_eq!(dir.data_files().len(), 1);
    assert_eq!(hints(&dir).len(), 1, "the old hints went with their files");
    {
        let store = Store::open(dir.path()).unwrap();
        assert_eq!(store.bytes_scanned_at_open(), 0);
        check(&store);
    }

    let mut store = Store::open(dir.path()).unwrap();
    store.put(b"after", b"compaction").unwrap();
    drop(store);
    let store = Store::open(dir.path()).unwrap();
    assert!(store.bytes_scanned_at_open() > 0);
    assert!(store.bytes_scanned_at_open() < 100, "only the new record");
    assert_eq!(store.get(b"after").unwrap(), Some(b"compaction".to_vec()));
    assert_eq!(store.len(), 201);

    // The hinted prefix and the scanned rest add up to the file exactly,
    // with nothing counted twice.
    let verified = Store::open_with(
        dir.path(),
        Options {
            verify_on_open: true,
            ..Options::default()
        },
    )
    .unwrap();
    assert_eq!(store.stats(), verified.stats());
    assert_eq!(store.stats().disk_bytes, active_len(&dir));
}

/// A hint left behind for a data file that is gone, by a crash while a
/// compaction was deleting what it replaced, is cleared away.
#[test]
fn a_hint_without_its_data_file_is_removed() {
    let dir = TempDir::new("hints-orphan");
    {
        let mut store = Store::open_with(dir.path(), small_files()).unwrap();
        fill(&mut store);
    }
    let orphan = dir.path().join("0000009999.hint");
    std::fs::copy(&hints(&dir)[0], &orphan).unwrap();
    let store = Store::open_with(dir.path(), small_files()).unwrap();
    assert!(!orphan.exists());
    check(&store);
}
