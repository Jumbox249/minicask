//! Space only comes back when you ask for it. These tests check that it does,
//! and that nothing live is lost on the way.

mod common;

use minicask::{Options, Store};
use common::TempDir;

#[test]
fn compaction_reclaims_overwritten_records() {
    let dir = TempDir::new("compact-overwrites");
    let mut store = Store::open(dir.path()).unwrap();

    for round in 0..100 {
        for key in ["alpha", "beta", "gamma"] {
            store
                .put(key.as_bytes(), format!("round-{round}").as_bytes())
                .unwrap();
        }
    }

    let before = store.stats();
    assert_eq!(before.keys, 3);
    assert!(
        before.fragmentation() > 0.9,
        "300 writes for 3 keys should be mostly garbage, got {:.2}",
        before.fragmentation()
    );

    let report = store.compact().unwrap();
    assert!(report.reclaimed_bytes() > 0);

    let after = store.stats();
    assert_eq!(after.keys, 3);
    assert_eq!(after.files, 1, "compaction should leave one data file");
    assert_eq!(after.live_bytes, after.disk_bytes);
    assert!(after.disk_bytes < before.disk_bytes / 50);

    // The surviving values are the newest ones, before and after a reopen.
    for key in ["alpha", "beta", "gamma"] {
        assert_eq!(
            store.get(key.as_bytes()).unwrap(),
            Some(b"round-99".to_vec())
        );
    }
    drop(store);

    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.len(), 3);
    assert_eq!(store.get(b"alpha").unwrap(), Some(b"round-99".to_vec()));
}

#[test]
fn deleted_keys_do_not_come_back_after_a_compaction() {
    let dir = TempDir::new("compact-deletes");
    let mut store = Store::open(dir.path()).unwrap();

    for i in 0..200 {
        store
            .put(format!("key-{i}").as_bytes(), b"payload")
            .unwrap();
    }
    for i in 0..200 {
        if i % 2 == 0 {
            assert!(store.delete(format!("key-{i}").as_bytes()).unwrap());
        }
    }

    store.compact().unwrap();
    drop(store);

    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.len(), 100);
    assert_eq!(
        store.get(b"key-0").unwrap(),
        None,
        "tombstoned key resurfaced"
    );
    assert_eq!(store.get(b"key-1").unwrap(), Some(b"payload".to_vec()));
}

#[test]
fn the_store_keeps_working_after_a_compaction() {
    let dir = TempDir::new("compact-then-write");
    let mut store = Store::open(dir.path()).unwrap();

    store.put(b"before", b"1").unwrap();
    store.compact().unwrap();
    store.put(b"after", b"2").unwrap();
    store.delete(b"before").unwrap();
    store.put(b"after", b"3").unwrap();
    drop(store);

    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.len(), 1);
    assert_eq!(store.get(b"after").unwrap(), Some(b"3".to_vec()));
    assert_eq!(store.get(b"before").unwrap(), None);
}

#[test]
fn compacting_an_empty_store_is_harmless() {
    let dir = TempDir::new("compact-empty");
    let mut store = Store::open(dir.path()).unwrap();
    let report = store.compact().unwrap();
    assert_eq!(report.bytes_after, 0);
    assert!(store.is_empty());

    store.put(b"still", b"works").unwrap();
    assert_eq!(store.get(b"still").unwrap(), Some(b"works".to_vec()));
}

#[test]
fn the_active_file_rolls_over_once_it_is_full() {
    let dir = TempDir::new("rollover");
    let mut store = Store::open_with(
        dir.path(),
        Options {
            max_file_bytes: 1024,
            ..Options::default()
        },
    )
    .unwrap();

    for i in 0..300 {
        store
            .put(
                format!("key-{i}").as_bytes(),
                format!("value-{i}").as_bytes(),
            )
            .unwrap();
    }

    assert!(
        dir.data_files().len() > 5,
        "expected several data files, found {}",
        dir.data_files().len()
    );
    for path in dir.data_files() {
        let len = std::fs::metadata(&path).unwrap().len();
        assert!(len <= 1024 + 64, "{path:?} grew to {len} bytes");
    }

    // Reading spans every file, not just the active one.
    assert_eq!(store.get(b"key-0").unwrap(), Some(b"value-0".to_vec()));
    assert_eq!(store.get(b"key-299").unwrap(), Some(b"value-299".to_vec()));

    store.compact().unwrap();
    assert_eq!(dir.data_files().len(), 1);
    assert_eq!(store.len(), 300);
    assert_eq!(store.get(b"key-150").unwrap(), Some(b"value-150".to_vec()));
}
