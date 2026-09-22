# minicask

[![CI](https://github.com/Jumbox249/minicask/actions/workflows/ci.yml/badge.svg)](https://github.com/Jumbox249/minicask/actions/workflows/ci.yml)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An embedded key/value store for Rust, built the way [Bitcask](https://riak.com/assets/bitcask-intro.pdf) is: every write appends to a log file, and an in-memory index remembers where each key's newest record lives.

No dependencies. Not even for the CRC.

```rust
use minicask::Store;

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
$ minicask --dir ./data put greeting "hello world"
$ minicask --dir ./data get greeting
hello world
$ minicask --dir ./data stats
keys         1
data files   1
live bytes   40
disk bytes   40
reclaimable  0 (0.0%)
```

`get` and `del` exit with status 1 on a missing key, so they compose with shell scripts.

## Redis-compatible server

`minicask-server` puts the store behind a TCP port speaking RESP, the Redis wire protocol. The official `redis-cli` and the ordinary client libraries connect to it without knowing the difference. Verified against `redis-cli` 7.4.11, which needs no flags and no shim:

```console
$ minicask-server --dir ./data
listening on 127.0.0.1:6379
```

```console
$ redis-cli SET greeting "hello world"
OK
$ redis-cli GET greeting
"hello world"
$ redis-cli MSET user:1 ann user:2 bob
OK
$ redis-cli KEYS 'user:*'
1) "user:1"
2) "user:2"
$ redis-cli DBSIZE
(integer) 3
```

The commands it answers: `GET`, `SET` (with `NX` and `XX`), `MGET`, `MSET`, `DEL`, `EXISTS`, `KEYS`, `DBSIZE`, `FLUSHDB`, `PING`, `ECHO`, `SELECT 0`, `QUIT`, and enough of `COMMAND` and `CLIENT` for clients to finish their handshake. `SET` with an expiry is refused with a syntax error rather than accepted and forgotten, since the store has no clock to honour it with.

Pipelining works, including `redis-cli --pipe`: replies to a batch of commands go out in one write. Inline commands (`GET greeting` on a bare line) work too, so `telnet` and `nc` are enough to poke at it. A protocol error closes that one connection and no other.

Concurrency is a mutex. The store is single-threaded by design, so each connection gets a thread and each command takes the lock for exactly one read or one append. That is the simplest correct thing, and it means the server's write throughput is the store's: 4.5k/sec with fsync on every write, 100x that with `--no-fsync`.

`src/resp.rs` is both halves of the protocol, and `minicask::resp::read_reply` is what the test suite uses as a client.

## Raft

A single server is one disk and one power supply. `minicask::raft` is leader election and log replication across a set of nodes, so that a majority surviving is enough.

The consensus layer is a state machine and nothing else. It owns no threads, opens no sockets and reads no clock:

```rust
use minicask::raft::{Config, MemStorage, Node};

let mut node = Node::new(1, vec![1, 2, 3], Config::default(), MemStorage::new());
let actions = node.tick()?;        // time passes
let actions = node.step(2, msg)?;  // a message arrives
// each hands back the messages to send; the caller owns the sockets
```

That shape is the point rather than a matter of taste. Consensus bugs are timing bugs, and a timing bug is only reproducible if the test owns the timing. `tests/raft.rs` drives whole clusters through network partitions, leader kills and restarts with no sleeps and no threads, so a failure reproduces exactly, at the same tick, every run.

What it implements: elections with randomised timeouts, log replication with the term-matching induction, the up-to-date check that decides a vote, conflict backoff by term rather than one entry per round trip, and the no-op a leader appends on taking office so that entries from earlier terms become committable.

### What the tests actually prove

Nineteen cluster scenarios and thirteen protocol tests, including the ones a naive implementation passes and should not:

- **`a_stale_candidate_cannot_win_even_with_the_highest_term`** isolates a node until it has missed six committed entries and campaigned its term far above everyone else's, then heals the network with nothing in flight and makes it stand for election. Its term is high enough to depose the leader. Its log is not good enough to replace it, and the votes have to say so.
- **`a_deposed_leader_steps_down_and_drops_its_orphan_entries`** feeds commands to a leader that has been cut off from everyone, then heals and requires those entries to be overwritten rather than applied.
- **`commands_survive_relentless_churn`** runs twelve rounds of commit-then-break-something, checking after every round that no two nodes disagree about any index.
- **`a_stale_append_does_not_shorten_the_log`** delivers a late duplicate that mentions fewer entries than the follower holds, and requires the extra ones to survive. Deleting what a message merely failed to mention is the classic way to lose a committed entry.

A test suite that passes proves nothing on its own, so the safety rules were checked by breaking them on purpose. Removing the up-to-date check from voting, truncating the log on every append, trusting the leader's commit index, or allowing two votes in one term each fail the suite.

One mutation does *not* fail it, which is worth saying plainly: dropping the rule that a leader may only commit entries from its own term. That rule is unreachable here, because `next_index` is set before the leader appends its no-op, so every `AppendEntries` it ever sends includes that no-op, and a successful reply therefore always reports a match at or past it. The check stays as defence in depth for the day that stops being true.

### What it does not do yet

- **Storage is in memory.** The `Storage` trait is the seam, and the test harness models a crash honestly by dropping the node and keeping the storage. Backing it with the append-only files this repository already has is the next piece of work.
- **Nothing is wired to the store.** The consensus layer moves opaque bytes; making `SET` and `DEL` into replicated commands is what turns this into a replicated database rather than a Raft implementation sitting beside one.
- **No snapshots**, so a log grows forever and a node that falls far enough behind is caught up an entry at a time.
- **Fixed membership.** Adding or removing a node means restarting the cluster.

## Testing

```console
$ cargo test
```

78 tests, including the three that matter:

- **`a_killed_writer_loses_nothing_it_finished`** spawns a real child process that writes 500 records, scribbles a header with no body onto the end of the file, then calls `abort()`. No destructor runs, no buffer is flushed, the kernel takes the process out with `SIGABRT`. The test then reopens the store and checks all 500 records, and that it is still writable afterwards.
- **`corruption_in_a_sealed_file_is_reported`** flips a bit in a file that was already closed and asserts the store refuses to open rather than pretending.
- **`deleted_keys_do_not_come_back_after_a_compaction`** guards the ordering rule that makes compaction safe.

`tests/raft.rs` is a deterministic cluster harness, described above. `tests/server.rs` starts the real `minicask-server` binary and talks to it over a socket: pipelining, inline commands, binary-safe values, eight clients writing at once, a protocol error that must not affect other connections, and a `kill` followed by a restart on the same directory.

## Layout

```
src/crc.rs      CRC-32, table built at compile time
src/record.rs   the on-disk record format
src/log.rs      data files, the append writer, the recovery scanner
src/store.rs    the index, the read path, recovery, compaction
src/resp.rs     the Redis wire protocol, both directions
src/server.rs   the TCP server and its command table
src/raft/       consensus: the log, the RPCs, the state machine
src/bin/        the CLI, the server, and the crash-test helper
```

## What it does not do

Worth being straight about, since each of these is a design choice rather than an oversight:

- **Keys must fit in memory.** The index is a `HashMap`, so memory scales with key count, not data size.
- **Single process, single thread.** There is no file lock and no internal synchronisation. Two `Store` instances on one directory will corrupt each other. The server puts one store behind one mutex, which is why it has one.
- **Startup reads every byte.** Recovery verifies the checksum of each record, which means replay is proportional to data size rather than key count. Bitcask solves this with hint files, which would be the next thing to build.
- **Compaction is stop-the-world.** It blocks until the merge finishes.
- **No range scans.** A hash index cannot answer ordered queries.

## License

MIT
