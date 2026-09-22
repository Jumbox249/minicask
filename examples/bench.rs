//! A rough throughput check. Not a scientific benchmark, just enough to show
//! where the time goes and to catch a change that makes things ten times
//! slower.
//!
//! Run with: `cargo run --release --example bench`

use caskdb::{Options, Store, SyncPolicy};
use std::time::Instant;

const VALUE_SIZE: usize = 100;

fn main() {
    let dir = std::env::temp_dir().join(format!("caskdb-bench-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    println!("caskdb benchmark, {VALUE_SIZE} byte values\n");
    println!("{:<34} {:>12} {:>14}", "operation", "count", "ops/sec");
    println!("{}", "-".repeat(62));

    bench_writes(
        &dir.join("fsync"),
        20_000,
        SyncPolicy::EveryWrite,
        "put (fsync every write)",
    );
    bench_writes(
        &dir.join("cached"),
        200_000,
        SyncPolicy::OsCache,
        "put (os cache)",
    );
    bench_reads(&dir.join("cached"), 200_000);
    bench_startup(&dir.join("cached"));
    bench_compaction(&dir.join("cached"));

    let _ = std::fs::remove_dir_all(&dir);
}

fn bench_writes(dir: &std::path::Path, count: usize, sync: SyncPolicy, label: &str) {
    let mut store = Store::open_with(
        dir,
        Options {
            sync,
            ..Options::default()
        },
    )
    .expect("open");
    let value = vec![b'v'; VALUE_SIZE];

    let start = Instant::now();
    for i in 0..count {
        store
            .put(format!("key-{i:09}").as_bytes(), &value)
            .expect("put");
    }
    store.sync().expect("sync");
    report(label, count, start.elapsed().as_secs_f64());
}

fn bench_reads(dir: &std::path::Path, count: usize) {
    let store = Store::open(dir).expect("open");
    let keys = store.len();
    let mut rng = XorShift::new(0x5EED_1234_ABCD_9876);

    let start = Instant::now();
    let mut bytes = 0usize;
    for _ in 0..count {
        let i = rng.next() as usize % keys;
        let value = store
            .get(format!("key-{i:09}").as_bytes())
            .expect("get")
            .expect("key should exist");
        bytes += value.len();
    }
    report("get (random key)", count, start.elapsed().as_secs_f64());
    assert_eq!(bytes, count * VALUE_SIZE);
}

fn bench_startup(dir: &std::path::Path) {
    let start = Instant::now();
    let store = Store::open(dir).expect("open");
    let elapsed = start.elapsed();
    println!(
        "{:<34} {:>12} {:>14}",
        "open (replay every record)",
        store.len(),
        format!("{:.0} rec/s", store.len() as f64 / elapsed.as_secs_f64())
    );
}

fn bench_compaction(dir: &std::path::Path) {
    let mut store = Store::open(dir).expect("open");
    // Overwrite a tenth of the keys so there is something to reclaim.
    let value = vec![b'w'; VALUE_SIZE];
    let count = store.len() / 10;
    for i in 0..count {
        store
            .put(format!("key-{i:09}").as_bytes(), &value)
            .expect("put");
    }

    let start = Instant::now();
    let report = store.compact().expect("compact");
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "{:<34} {:>12} {:>14}",
        "compact",
        store.len(),
        format!("{:.0} rec/s", store.len() as f64 / elapsed)
    );
    println!(
        "\nreclaimed {:.1} MiB of {:.1} MiB in {:.2}s",
        report.reclaimed_bytes() as f64 / (1024.0 * 1024.0),
        report.bytes_before as f64 / (1024.0 * 1024.0),
        elapsed
    );
}

fn report(label: &str, count: usize, seconds: f64) {
    println!(
        "{:<34} {:>12} {:>14.0}",
        label,
        count,
        count as f64 / seconds
    );
}

/// A tiny deterministic RNG, so the benchmark needs no dependencies and gives
/// the same access pattern on every run.
struct XorShift(u64);

impl XorShift {
    fn new(seed: u64) -> Self {
        XorShift(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}
