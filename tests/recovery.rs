//! What happens when the process dies mid-write, and what happens when the
//! bytes on disk stop matching their checksums.

mod common;

use caskdb::{Error, Options, Store};
use common::{corrupt_byte, TempDir};
use std::io::Write;
use std::process::Command;

/// The headline claim: kill the writer with no chance to clean up, and every
/// record it finished is still there afterwards.
#[test]
fn a_killed_writer_loses_nothing_it_finished() {
    let dir = TempDir::new("kill-9");
    let status = Command::new(env!("CARGO_BIN_EXE_caskdb-crash-writer"))
        .arg(dir.str())
        .arg("500")
        .status()
        .expect("run the crash writer");

    assert!(
        !status.success(),
        "the helper is supposed to abort, not exit cleanly"
    );
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(6), "expected SIGABRT");
    }

    // No clean shutdown ran, and the file ends in a half-written record.
    let mut store = Store::open(dir.path()).expect("recover after a crash");
    assert_eq!(store.len(), 500);
    for i in 0..500 {
        assert_eq!(
            store.get(format!("key-{i}").as_bytes()).unwrap(),
            Some(format!("value-{i}").into_bytes()),
            "record {i} did not survive the crash"
        );
    }

    // And the store is writable again rather than merely readable.
    store.put(b"after-the-crash", b"fine").unwrap();
    drop(store);

    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.len(), 501);
    assert_eq!(
        store.get(b"after-the-crash").unwrap(),
        Some(b"fine".to_vec())
    );
}

/// The same situation reached directly: a record header with no body behind
/// it is discarded, and everything before it is kept.
#[test]
fn a_torn_tail_is_truncated_not_fatal() {
    let dir = TempDir::new("torn-tail");
    {
        let mut store = Store::open(dir.path()).unwrap();
        for i in 0..20 {
            store.put(format!("key-{i}").as_bytes(), b"intact").unwrap();
        }
    }

    let active = dir.data_files().pop().unwrap();
    let good_len = std::fs::metadata(&active).unwrap().len();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&active)
        .unwrap();
    file.write_all(&[0x00, 0x11, 0x22, 0x33, 0x44]).unwrap();
    file.sync_all().unwrap();
    drop(file);

    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.len(), 20);
    assert_eq!(store.get(b"key-19").unwrap(), Some(b"intact".to_vec()));
    assert_eq!(
        std::fs::metadata(&active).unwrap().len(),
        good_len,
        "the partial record should have been cut off"
    );
}

/// Damage in a file that was already closed is not a torn write, so it is
/// reported instead of being quietly discarded.
#[test]
fn corruption_in_a_sealed_file_is_reported() {
    let dir = TempDir::new("sealed-corruption");
    {
        // A tiny file limit forces a rollover, so file 1 gets sealed.
        let mut store = Store::open_with(
            dir.path(),
            Options {
                max_file_bytes: 200,
                ..Options::default()
            },
        )
        .unwrap();
        for i in 0..40 {
            store
                .put(
                    format!("key-{i}").as_bytes(),
                    format!("value-{i}").as_bytes(),
                )
                .unwrap();
        }
    }

    let files = dir.data_files();
    assert!(files.len() > 1, "expected the store to roll over");
    corrupt_byte(&files[0], 25);

    match Store::open(dir.path()) {
        Err(Error::Corrupt { file_id, .. }) => assert_eq!(file_id, 1),
        Err(other) => panic!("expected a corruption error, got: {other}"),
        Ok(_) => panic!("expected a corruption error, but the store opened cleanly"),
    }
}

/// Bit rot under a live store is caught on read rather than handed back as
/// if it were the real value.
#[test]
fn a_flipped_bit_is_caught_on_read() {
    let dir = TempDir::new("bit-rot");
    let mut store = Store::open(dir.path()).unwrap();
    store.put(b"alpha", b"untouched").unwrap();
    store.sync().unwrap();

    // Byte 21 is the first byte of the key, just past the header.
    let active = dir.data_files().pop().unwrap();
    corrupt_byte(&active, 24);

    match store.get(b"alpha") {
        Err(Error::Corrupt { detail, .. }) => {
            assert!(detail.contains("checksum"), "unexpected detail: {detail}")
        }
        Err(other) => panic!("expected a checksum failure, got: {other}"),
        Ok(value) => panic!("corrupt record was served as {value:?}"),
    }
}

/// An empty directory is a valid, empty store.
#[test]
fn a_fresh_directory_opens_clean() {
    let dir = TempDir::new("fresh");
    let store = Store::open(dir.path().join("does-not-exist-yet")).unwrap();
    assert!(store.is_empty());
    assert_eq!(store.stats().disk_bytes, 0);
}
