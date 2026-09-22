# caskdb

[![CI](https://github.com/Jumbox249/caskdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Jumbox249/caskdb/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An embedded key/value store for Rust, built the way [Bitcask](https://riak.com/assets/bitcask-intro.pdf) is: every write appends to a log file, and an in-memory index remembers where each key's newest record lives.

No dependencies. Not even for the CRC.

```rust
use caskdb::Store;

let mut store = Store::open("./my-data")?;
store.put(b"language", b"rust")?;
assert_eq!(store.get(b"language")?, Some(b"rust".to_vec()));
store.delete(b"language")?;
```

## Why append-only

Nothing on disk is ever overwritten in place. That single constraint buys three things:

- **A read is one hash lookup and one seek.** The index holds a file id, an offset and a length, so the store never searches for anything.
- **A write is one append.** There is no read-modify-write cycle that can be interrupted halfway through leaving a page half old and half new.
- **Recovery is a replay, not a repair.** Restarting after a crash means scanning the log forward. There is no fsck step, because there is no structure on disk that can be left inconsistent.

The price is paid in two places: every key lives in memory, and space from overwritten records only comes back when you compact.

## On-disk format

One directory, a set of numbered data files, and the newest one is the only one open for writing.

```
my-data/
  0000000001.log   sealed
  0000000002.log   sealed
  0000000003.log   active, appends land here
```

Each record is fixed-width up front so a reader needs exactly one 21-byte read to know how far it extends:

```
 0 ..  4   crc32 of every byte after it
 4 .. 12   timestamp, ms since the unix epoch
12 .. 16   key length
16 .. 20   value length
20 .. 21   flags, bit 0 = tombstone
21 ..      key bytes, then value bytes
```

A delete appends a tombstone rather than erasing anything, which is what keeps the file append-only even when data goes away. A zero-length value is still a value, and stays distinguishable from a tombstone.

## What survives what

| Failure | Result |
| --- | --- |
| Process killed mid-append (`kill -9`, panic, abort) | Every completed record is kept. The half-written tail is truncated at startup. |
| Power cut, `SyncPolicy::EveryWrite` (default) | Every record that returned `Ok` is on the disk. |
| Power cut, `SyncPolicy::OsCache` | Records may be lost from the tail. What remains is never torn or partially applied. |
| A bit flips inside a record | Caught by the checksum, reported as `Error::Corrupt`, never returned as data. |

The distinction the last two rows draw is deliberate. Losing the newest writes is a durability choice you opt into for speed. Returning a value that is not what was written is a correctness bug, and no policy enables it.

A torn tail is only accepted on the newest file, since that is the only one that could have been mid-append. Damage anywhere else is real corruption and is reported instead of being quietly discarded.

## Compaction

Overwrites and deletes leave dead records behind. `compact()` rewrites the live records into a single new file and unlinks the originals.

```rust
let stats = store.stats();
if stats.fragmentation() > 0.5 {
    let report = store.compact()?;
    println!("reclaimed {} bytes", report.reclaimed_bytes());
}
```

The merged file takes an id higher than everything it replaces, and the originals are unlinked only after it has been fsynced. A crash partway through leaves the old files untouched, and the next startup ignores the incomplete merge.

## Benchmarks

`cargo run --release --example bench`, 100-byte values, on a 4-core Xeon at 2.8GHz with an ordinary virtualised disk. Absolute numbers depend entirely on the hardware; the ratios are the interesting part.

| Operation | Throughput |
| --- | --- |
| `put`, fsync on every write | 4,572 ops/sec |
| `put`, OS cache | 488,697 ops/sec |
| `get`, random key | 490,689 ops/sec |
| startup replay | 1,271,554 records/sec |
| compaction | 361,122 records/sec |

The 100x gap between the two write rows is the fsync, not the store. It is the cost of the default guarantee, and `SyncPolicy::OsCache` is there for callers who would rather have the speed.

Compaction originally ran at 4,087 records/sec because the merge inherited the store's fsync-per-write policy. Since the merge is one long sequential write and the only barrier that matters is a single sync before the originals are unlinked, batching it made compaction 88 times faster with no change to the guarantee.

## Command line

```console
$ cargo install --path .
$ caskdb --dir ./data put greeting "hello world"
$ caskdb --dir ./data get greeting
hello world
$ caskdb --dir ./data stats
keys         1
data files   1
live bytes   40
disk bytes   40
reclaimable  0 (0.0%)
```

`get` and `del` exit with status 1 on a missing key, so they compose with shell scripts.

## Testing

```console
$ cargo test
```

27 tests, including the three that matter:

- **`a_killed_writer_loses_nothing_it_finished`** spawns a real child process that writes 500 records, scribbles a header with no body onto the end of the file, then calls `abort()`. No destructor runs, no buffer is flushed, the kernel takes the process out with `SIGABRT`. The test then reopens the store and checks all 500 records, and that it is still writable afterwards.
- **`corruption_in_a_sealed_file_is_reported`** flips a bit in a file that was already closed and asserts the store refuses to open rather than pretending.
- **`deleted_keys_do_not_come_back_after_a_compaction`** guards the ordering rule that makes compaction safe.

## Layout

```
src/crc.rs      CRC-32, table built at compile time
src/record.rs   the on-disk record format
src/log.rs      data files, the append writer, the recovery scanner
src/store.rs    the index, the read path, recovery, compaction
src/bin/        the CLI, and the crash-test helper
```

## What it does not do

Worth being straight about, since each of these is a design choice rather than an oversight:

- **Keys must fit in memory.** The index is a `HashMap`, so memory scales with key count, not data size.
- **Single process, single thread.** There is no file lock and no internal synchronisation. Two `Store` instances on one directory will corrupt each other.
- **Startup reads every byte.** Recovery verifies the checksum of each record, which means replay is proportional to data size rather than key count. Bitcask solves this with hint files, which would be the next thing to build.
- **Compaction is stop-the-world.** It blocks until the merge finishes.
- **No range scans.** A hash index cannot answer ordered queries.

## License

MIT
