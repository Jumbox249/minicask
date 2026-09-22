mod common;

use common::TempDir;
use minicask::{Error, Store};

#[test]
fn put_then_get() {
    let dir = TempDir::new("put-then-get");
    let mut store = Store::open(dir.path()).unwrap();

    store.put(b"language", b"rust").unwrap();
    assert_eq!(store.get(b"language").unwrap(), Some(b"rust".to_vec()));
    assert!(store.contains_key(b"language"));
    assert_eq!(store.len(), 1);
}

#[test]
fn missing_keys_are_none_not_errors() {
    let dir = TempDir::new("missing");
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.get(b"nothing here").unwrap(), None);
    assert!(store.is_empty());
}

#[test]
fn the_newest_write_wins() {
    let dir = TempDir::new("overwrite");
    let mut store = Store::open(dir.path()).unwrap();

    for value in ["one", "two", "three"] {
        store.put(b"counter", value.as_bytes()).unwrap();
    }
    assert_eq!(store.get(b"counter").unwrap(), Some(b"three".to_vec()));
    assert_eq!(store.len(), 1, "overwrites must not add index entries");
}

#[test]
fn delete_reports_whether_the_key_was_there() {
    let dir = TempDir::new("delete");
    let mut store = Store::open(dir.path()).unwrap();

    store.put(b"doomed", b"value").unwrap();
    assert!(store.delete(b"doomed").unwrap());
    assert_eq!(store.get(b"doomed").unwrap(), None);
    assert!(
        !store.delete(b"doomed").unwrap(),
        "second delete is a no-op"
    );
    assert!(!store.delete(b"never existed").unwrap());
}

#[test]
fn an_empty_value_is_not_a_deletion() {
    let dir = TempDir::new("empty-value");
    let mut store = Store::open(dir.path()).unwrap();

    store.put(b"blank", b"").unwrap();
    assert_eq!(
        store.get(b"blank").unwrap(),
        Some(Vec::new()),
        "a zero-length value must stay distinguishable from a tombstone"
    );

    store.delete(b"blank").unwrap();
    assert_eq!(store.get(b"blank").unwrap(), None);
}

#[test]
fn everything_survives_a_reopen() {
    let dir = TempDir::new("reopen");
    {
        let mut store = Store::open(dir.path()).unwrap();
        for i in 0..250 {
            store
                .put(
                    format!("key-{i}").as_bytes(),
                    format!("value-{i}").as_bytes(),
                )
                .unwrap();
        }
        store.delete(b"key-7").unwrap();
        store.put(b"key-8", b"replaced").unwrap();
        store.sync().unwrap();
    }

    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.len(), 249);
    assert_eq!(store.get(b"key-0").unwrap(), Some(b"value-0".to_vec()));
    assert_eq!(
        store.get(b"key-7").unwrap(),
        None,
        "tombstone must be replayed"
    );
    assert_eq!(store.get(b"key-8").unwrap(), Some(b"replaced".to_vec()));
    assert_eq!(store.get(b"key-249").unwrap(), Some(b"value-249".to_vec()));
}

#[test]
fn keys_and_values_are_arbitrary_bytes() {
    let dir = TempDir::new("binary");
    let key: &[u8] = &[0x00, 0xFF, 0x1B, b'\n', 0x80];
    let value: &[u8] = &[0xDE, 0xAD, 0x00, 0xBE, 0xEF];

    {
        let mut store = Store::open(dir.path()).unwrap();
        store.put(key, value).unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.get(key).unwrap().as_deref(), Some(value));
}

#[test]
fn values_can_be_large() {
    let dir = TempDir::new("large");
    let value = vec![b'x'; 4 * 1024 * 1024];

    {
        let mut store = Store::open(dir.path()).unwrap();
        store.put(b"big", &value).unwrap();
    }
    let store = Store::open(dir.path()).unwrap();
    assert_eq!(store.get(b"big").unwrap(), Some(value));
}

#[test]
fn empty_keys_are_rejected() {
    let dir = TempDir::new("empty-key");
    let mut store = Store::open(dir.path()).unwrap();
    assert!(matches!(store.put(b"", b"x"), Err(Error::InvalidKey(_))));
}

#[test]
fn keys_lists_only_live_entries() {
    let dir = TempDir::new("keys");
    let mut store = Store::open(dir.path()).unwrap();
    store.put(b"a", b"1").unwrap();
    store.put(b"b", b"2").unwrap();
    store.put(b"c", b"3").unwrap();
    store.delete(b"b").unwrap();

    let mut keys: Vec<Vec<u8>> = store.keys().map(|k| k.to_vec()).collect();
    keys.sort();
    assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);
}
