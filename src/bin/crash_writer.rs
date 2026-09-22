//! Test helper: writes records, scribbles a half-finished one onto the end of
//! the active data file, and then kills itself with `abort` so that no
//! destructor, flush or clean shutdown ever runs.
//!
//! This is what an interrupted append really looks like on disk, and
//! `tests/recovery.rs` uses it to prove the store survives one.

use caskdb::{Options, Store, SyncPolicy};
use std::io::Write;

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args.next().expect("usage: crash-writer <dir> <count>");
    let count: usize = args
        .next()
        .expect("usage: crash-writer <dir> <count>")
        .parse()
        .expect("count must be a number");

    let mut store = Store::open_with(
        &dir,
        Options {
            sync: SyncPolicy::EveryWrite,
            ..Options::default()
        },
    )
    .expect("open store");

    for i in 0..count {
        store
            .put(
                format!("key-{i}").as_bytes(),
                format!("value-{i}").as_bytes(),
            )
            .expect("put");
    }
    drop(store);

    // Nine bytes of a record header that will never get the rest of its body.
    let active = newest_data_file(&dir);
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(active)
        .expect("reopen active data file");
    file.write_all(&[0xAB; 9]).expect("write partial record");
    file.sync_all().expect("sync partial record");

    std::process::abort();
}

fn newest_data_file(dir: &str) -> std::path::PathBuf {
    let mut logs: Vec<_> = std::fs::read_dir(dir)
        .expect("read store dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("log"))
        .collect();
    logs.sort();
    logs.pop().expect("at least one data file")
}
