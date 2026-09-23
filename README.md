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
  0000000001.hint  its index: each record's key, offset, length and timestamp
  0000000002.log   sealed
  0000000002.hint
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

## Hint files

Rebuilding the index means reading every data file, and without help that means every byte of every value, so startup time grows with the data rather than with the number of keys. A hint file is a data file's index: for each record, its key, where it is, how long it is, its timestamp and whether it is a tombstone, and none of the value. It is written when a file is sealed, and after a compaction for the merged file, so opening a store reads the hints and skips the values.

A hint describes a prefix of its data file, a stated number of bytes of it. Data files only grow, so a prefix stays true, and whatever was appended after the hint was written is scanned as before. That one rule covers a sealed file, whose hint is the whole of it, and a freshly compacted file, which is still the active one and keeps growing. A hint is only written for bytes already synced, and written beside its data file and renamed into place, so it is never ahead of the data. One that is damaged, describes more than the file holds, or does not tile the file record by record is ignored and the file scanned, never an error, since a hint only ever saves work. A sealed file found without one gets one written, so upgrading a store costs one ordinary open. How much a hint saves depends on how big the values are, since values are what it skips: with the benchmark's 100-byte values, opening 200,000 records went from 1.8 to 2.75 million records a second, and building the index is most of what is left.

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

The server rows were measured separately, on a Windows machine whose disk manages about 400 fsyncs a second, with each client sending one `SET` and waiting for its reply before the next, against a store that syncs every write:

| Server `SET`, fsync every write | Before group commit | After |
| --- | --- | --- |
| 1 client | 408 ops/sec | 410 ops/sec |
| 16 clients | 423 ops/sec | 3,300 ops/sec |

One client is still one fsync per write, since each write waits for its own before the next is sent; that is the guarantee, and batching cannot change it. Sixteen clients share them. The same buffer made compaction's merge about twice as fast on that machine, 217,000 to 458,000 records a second, since the merged file's appends now go to the kernel a megabyte at a time.

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

Each connection gets a thread, and the store sits behind a read-write lock. Reads share it: a read is a hash lookup and a positional read of the file, which moves no cursor another reader depends on, so any number run at once. Writes take it alone.

Writes are group-committed. A write appends under the lock and lets go of it before the disk has the bytes, and its reply is held until an fsync has covered it. Whoever needs an fsync first runs one, outside the lock, and every write that landed before it started is made durable by that one call. So many clients writing at once share fsyncs instead of queueing for one each, and a pipelined batch from one client waits once. Replies are held in the server's own buffer rather than a `BufWriter`, which would send whenever it filled, possibly ahead of the disk. If the disk refuses an fsync, the server says so and hangs up rather than acknowledge writes it cannot vouch for.

The first version of this batched nothing on Windows: 1,600 writes from 16 clients took 1,571 fsyncs. NTFS holds a write to a file until a flush of that same file has finished, and measured, a 64-byte append that takes under a microsecond alone took half a millisecond beside a flush. So while one thread fsynced, every other writer sat inside its append, and each fsync covered about one write. The fix is a small buffer in the writer: a deferred write goes into memory, the thread about to fsync hands the whole buffer to the kernel in one write and lets go of the lock, and the next batch fills the buffer, touching no file, while the fsync runs. Reads of a record still in the buffer are answered from it, and it is handed to the kernel past 1 MiB, so a long run of deferred writes holds a bounded amount. The same 1,600 writes now take 200 fsyncs.

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

What it implements: elections with randomised timeouts, log replication with the term-matching induction, the up-to-date check that decides a vote, conflict backoff by term rather than one entry per round trip, the no-op a leader appends on taking office so that entries from earlier terms become committable, pre-vote, check-quorum, snapshots, read-index reads on any node, and membership changes one node at a time.

### What the tests actually prove

Thirty-nine cluster scenarios and fifty-three protocol tests, including the ones a naive implementation passes and should not:

- **`a_stale_candidate_cannot_win_even_with_the_highest_term`** isolates a node until it has missed six committed entries and campaigned its term far above everyone else's, then heals the network with nothing in flight and makes it stand for election. Its term is high enough to depose the leader. Its log is not good enough to replace it, and the votes have to say so.
- **`a_deposed_leader_steps_down_and_drops_its_orphan_entries`** feeds commands to a leader that has been cut off from everyone, then heals and requires those entries to be overwritten rather than applied.
- **`commands_survive_relentless_churn`** runs twelve rounds of commit-then-break-something, checking after every round that no two nodes disagree about any index.
- **`a_stale_append_does_not_shorten_the_log`** delivers a late duplicate that mentions fewer entries than the follower holds, and requires the extra ones to survive. Deleting what a message merely failed to mention is the classic way to lose a committed entry.

A test suite that passes proves nothing on its own, so every safety rule here was checked by breaking it on purpose and confirming the suite noticed. Ninety-two broken variants, each failing at least one test: removing the up-to-date check from voting, truncating the log on every append, trusting the leader's commit index, allowing two votes in one term, an off-by-one in the quorum, committing an earlier term's entry on a replica count, not standing down when cut off, not recording contact with peers, granting a pre-vote to a node that is behind, ignoring the log when answering one, dropping the leader lease, counting pre-vote replies from any round, not echoing the proposed term on a grant, sending a lagging follower its whole backlog in one message, waiting for a heartbeat between catch-up batches, accepting a command too large to replicate, re-applying the log to the store on restart, trusting a damaged applied index, and eleven ways of getting snapshots wrong: restoring by writing without deleting, rewriting unchanged values on every restore, not finishing an interrupted restore, sending entries the leader no longer has, refusing appends that start inside a snapshot, keeping a log that disagrees with an installed snapshot, splicing pieces from two leaders, accepting a piece at the wrong offset, snapshotting unapplied state, forgetting on restart that a snapshot is committed, and continuing a replaced snapshot from an old offset. Then nine more once snapshots were streamed: keeping stale keys that sort before a snapshot key, keeping ones after the last, believing a snapshot out of order, letting two snapshot jobs out at once, installing a compaction a newer snapshot overtook, leaving an abandoned part file behind, keeping stray part files across a restart, leaving the data out of the snapshot's checksum, and reading through a shared file cursor. Eight for the read index: counting a reply to any round, reading at a new leader's bare commit index, keeping a read alive after its leader loses office, keeping a forwarded read alive after the leader changes, not releasing followers' reads when a round is answered, answering a follower before its round is confirmed, a follower not echoing the round, and answering a confirmed read before the store has applied that far. Twelve for group commit: not syncing a file as it is sealed, not syncing the store before the applied index, writing the index before the sync, replying before the disk has the write, one fsync per write, two batches proposed at once, reads ignoring the write buffer, compacting, taking a view or taking a sync handle without flushing it, syncing without flushing it, and dropping it on close. Ten for hint files: never reading them, rescanning from the start after one, believing one longer than its file, believing one with gaps, not writing one when a file is sealed or compacted, leaving old ones behind after a compaction, keeping ones whose file is gone, not rebuilding a missing one, and reading them when asked to verify. And fourteen for membership: learners getting a vote, promoting a learner before it has caught up, never promoting one, dropping a node being removed before it hears, a follower ignoring membership entries, keeping a membership whose entry was truncated, changing two voters at once, starting a change while the last is uncommitted, changing before the new leader's no-op commits, a non-member standing, counting anyone's vote, a removed leader staying on, a snapshot forgetting the membership, and an installed snapshot not bringing it. And six for compacting the store off the lock: overwriting keys written while the merge ran, keeping an unfinished merge across a restart, running two at once, miscounting the disk afterwards, compacting under the lock anyway, and giving the merge the new active file's id. And four for restoring off the lock: keeping the old files, installing a newer snapshot over the one being read, restoring under the lock anyway, and taking writes while a restore is out.

The first attempt at this suite caught none of the subtle ones. Every scenario passed against three deliberately broken implementations, because the scenarios never built the interleavings those rules exist for. The rules that cannot be reached through ordinary operation — a leader committing an earlier term's entry is the clearest — are now tested against the state they guard, directly, rather than hoped for through a cluster.

### Standing down

Plain Raft never tells a leader it has been cut off. It keeps the title until it hears a later term, and it cannot hear one from the wrong side of a partition, so it goes on answering reads from a store the majority has long since moved past. Two rules close that:

- **Check quorum.** A leader that cannot account for a majority within one election timeout stands itself down. It keeps its term, since nothing has been decided; it only stops claiming an office it can no longer do the job of.
- **A new leader waits before it reads.** Winning an election is not enough. A new leader holds every committed entry, by the rule that decides a vote, but it does not yet know *which* of them are committed — a follower learns that from the leader's next message, and the old leader may have died before sending one. Committing the no-op from its own term settles it, and until then the node will not answer a read.

The second of those was found by a test that failed only when the suite ran in parallel: a `GET` for a write that had already been acknowledged came back empty, in the window between an election being won and the backlog being applied.

Check quorum narrows the window in which a deposed leader answers reads, but it does not close it: until the timeout fires, the old leader still believes it leads, and the majority may already have elected someone else and accepted writes it knows nothing about. A read index closes it.

### Reads that cannot be stale

A read records the commit index, then waits for a majority to answer a round of heartbeats sent *after* it began. Every message a leader sends carries its current round and every reply echoes it, so a reply to an earlier message, which says nothing about the moment the read began, is not counted. If a majority answers, no other leader can have been elected in between, and a store applied up to the recorded index holds every acknowledged write. It costs one round trip and nothing in the log. A leader that has not yet committed its no-op records the no-op's index rather than its own commit index, which it does not yet know to be complete.

A follower asks the leader for the index instead of recording its own, waits for its store to apply that far, and then answers the read itself. So reads are spread across the cluster rather than all landing on the leader, and a read on a follower still sees a write the leader acknowledged a moment earlier. The wait matters: the leader's answer often arrives before the follower has applied the entries it covers, and answering then would miss exactly the writes the index exists to include.

One test cuts a leader off from the majority, starts a read on it straight away, lets the others elect a replacement and commit a write, and requires the read never to be confirmed; another reads from both followers straight after each of twenty writes on the leader, over TCP, and requires every read to see it.

### Asking before standing

A node that has been cut off spends the partition timing out. Raising its term each time it does costs nothing while it is away and a great deal when it returns: its term now leads the cluster's, so a leader that is doing its job perfectly well has to stand down, and an election is held that the returning node was never going to win.

So it asks first. `PreVote` is the hypothetical — *if* I stood in the next term, would you have me? — and it moves nobody's term, in either direction. Only once a majority has said yes does a node spend a term and stand for real. Two rules make the answer worth having:

- **A node that is hearing from a leader says no**, to canvassers of either kind. It owes the sitting leader the rest of its lease.
- **The log still decides.** Pre-vote is not a way around the rule that keeps a stale node out. It applies the same test one step earlier and refuses on the same grounds.

A refusal carries the refuser's own term, which is how a node that really has fallen behind finds out. A grant echoes the term that was asked about, so a late yes from an earlier round cannot be counted towards this one.

The effect is that an isolated node's term does not move at all while it is away. One test holds a node out for three hundred ticks, heals the network, and then requires the leader's term to be exactly what it was before.

### Catching up in pieces

A follower that has been away is behind by however much the cluster wrote while it was gone. Sending that as one message fails twice over: the backlog goes out again in full on every heartbeat until it is acknowledged, and a backlog bigger than the 64 MiB frame limit can never be sent at all, so the follower never catches up.

So each `AppendEntries` carries about `max_append_bytes` of entries (1 MiB by default), and an acknowledged batch is followed at once by the next rather than waiting for the heartbeat, which makes catch-up one batch per round trip. A batch always holds at least one entry, however large, so a single big entry cannot stall replication; `max_entry_bytes` (16 MiB) is the hard ceiling, and a `SET` past it is refused before it reaches the log, since once there it would block everything behind it. A cluster node will not start with limits that could add up to more than a frame.

### Snapshots

Without them the log only ever grows, and a node that has been gone long enough is caught up by replaying everything the cluster has ever written.

Every 10,000 applied entries (`--snapshot-every`), a node writes out its store as of the last applied index and discards the log that index covers. The snapshot is the store's own record format, one record per live key in key order, so it is checksummed record by record and two stores holding the same data produce the same bytes. The store is compacted at the same moment when more than half of it is dead records, since in a cluster nothing else ever would, and that runs off the lock too. The active file is sealed and writing moves to a new one two ids on, leaving the id between them for the merge. The merge copies live records out of files that will not change again, is written under another name, synced and renamed into place; only then is the lock taken to point each key at its copy, and only if the key still points where it did when the merge began. A write made meanwhile lands in a file that replays after the merge, so it wins either way, before or after a crash.

A follower that needs entries the leader has already folded away is sent the snapshot instead, in pieces of `max_append_bytes`, each acknowledged before the next. A piece that goes missing or arrives twice is answered with how far the follower has actually got, and the leader carries on from there. Pieces from two leaders are never spliced together, even for the same snapshot: they describe the same state, but nothing promises they are the same bytes.

Installing one is where the obvious approach is wrong. Writing the snapshot's keys into the store is not enough, because a key this node still holds but the cluster deleted while it was away is simply *absent* from the snapshot, and would survive. So the store is made exactly the snapshot: everything it does not contain is deleted first. There is a test that deletes a key while a follower is cut off, compacts past the delete so it can only arrive by snapshot, and requires the key to be gone.

None of it holds the store in memory. A snapshot is written from a point-in-time view of the store: a copy of its index as of one applied entry, which costs memory per key but none per value. Because nothing in the store is ever overwritten, a write that lands after the view was taken appends somewhere new and leaves the records the view points at where they were, so the view can be read on a thread of its own while the node carries on. The node's lock is held only to take the view and, at the end, to install the result. Sending reads the snapshot file a piece at a time, a follower writes each piece to disk as it arrives, and a restore streams the snapshot back one record at a time, walking it in step with the store's own keys in sorted order so it can find the stale ones without holding either side in memory.

Restoring one runs off the lock as well. The snapshot is streamed into a new file in the slot left by sealing the active one, and finishing makes the store's index exactly that file and deletes every older one, so whatever the old files held that the snapshot lacks goes with them. Until then the node takes part in consensus but applies nothing, since nothing after the snapshot can be applied before it, and so it answers no reads. It ignores any newer snapshot the leader sends meanwhile: the restore is reading the current snapshot's file, and on Windows with some toolchains a file that is open cannot be renamed over. Ignoring a message is always safe in Raft, and the leader sends it again. A crash before finishing leaves the new file beside the old ones, but the applied index has not moved, so the next start restores again.

Taking a snapshot changes two files, and a crash can land between them. The snapshot is always written before the log is cut down, and opening a directory finishes whichever half was interrupted by the same rule that taking the snapshot applies: if the log agrees with the snapshot at its last entry, the entries after it are kept, and otherwise none of them are. Restoring the store is interruptible too, because the store's applied index only moves once the restore is on disk; a crash part way leaves the snapshot ahead of the store, and the next start restores it again.

### Changing who is in the cluster

Membership changes one node at a time, which is what makes it safe without a joint consensus: any majority of the old members and any majority of the new ones share a node, so the two can never elect a leader each. A membership is an entry in the log holding the whole list, and each node uses the newest one in its log as soon as it has it, committed or not, which is what the one-at-a-time argument needs. So it has to stop using one the moment the log loses it, too: a leader cut off with an uncommitted change loses the entry to the majority's log, and everyone who had it goes back to the membership before.

Two more rules. A change is refused while another is uncommitted, since two in flight can add up to more than one node. And a leader may not propose one until it has committed an entry of its own term, without which a change it proposes can race one an earlier leader left behind; that is the bug in the original single-server algorithm, fixed the way the dissertation's errata fixes it. A leader that removes itself sees the change through, then stands down.

A new node is started with `--join` and the other nodes' addresses. It then has no membership at all, and stands for nothing, until a leader adds it. `RAFT.ADD` adds it as a learner: sent the log like any follower, by entries or by snapshot, but with no vote, so the cluster's majorities do not depend on a node that is still being sent the whole store. Once it holds everything committed, the leader promotes it to a voter on its own, which is an ordinary one-voter change. Adding and dropping learners moves no majority, so only voters are held to one change at a time:

```console
$ minicask-cluster --id 4 --dir ./n4 --raft 127.0.0.1:7004 --client 127.0.0.1:6004 --join \
      --peer 1@127.0.0.1:7001,127.0.0.1:6001 --peer 2@127.0.0.1:7002,127.0.0.1:6002 \
      --peer 3@127.0.0.1:7003,127.0.0.1:6003
$ redis-cli -p 6003 RAFT.ADD 4 127.0.0.1:7004 127.0.0.1:6004
OK
$ redis-cli -p 6003 RAFT.REMOVE 1
OK
```

A member's addresses travel in its membership entry, so the others learn how to reach a new node from the log. A snapshot records the membership as of its index, since the entry that set it may be one the snapshot replaced.

While a removal is uncommitted, the leader goes on sending the log to the node being removed, so that the entry removing it reaches it; with that entry in its log it knows it is out, stops standing, and reports `voters:` without itself. If it misses the entry anyway, by being down at the time, pre-vote and the leader lease keep it harmless: nobody who is hearing from a leader says yes. Either way it still has to be shut down; nothing stops the process for you.

### What it does not do yet

- **A removed node has to be stopped by hand.** It knows it is out, stops standing, and harms nothing, but the process keeps running.

## A replicated store

`minicask-cluster` is the store with consensus underneath it. Three of them tolerate any one dying; five tolerate two.

```console
$ minicask-cluster --id 1 --dir ./n1 --raft 127.0.0.1:7001 --client 127.0.0.1:6001       --peer 2@127.0.0.1:7002,127.0.0.1:6002 --peer 3@127.0.0.1:7003,127.0.0.1:6003
node 1 listening raft=127.0.0.1:7001 client=127.0.0.1:6001
```

Clients are still `redis-cli`. A write is not acknowledged until a majority has it on disk:

```console
$ redis-cli -p 6003 SET language rust
OK
$ redis-cli -p 6001 GET language          # a follower, and it still sees the write
"rust"
$ redis-cli -p 6001 SET language go       # but writes go to the leader
MOVED 0 127.0.0.1:6003
$ redis-cli -p 6003 RAFT
id:3
role:leader
term:1
leader:3
commit_index:3
applied_index:3
last_index:3
snapshot_index:0
keys:1
members:1,2,3
voters:1,2,3
```

Kill node 3 and the other two elect a replacement in well under a second, with every acknowledged write intact. Start it again and it comes back off its own disk, learns it is behind, and is caught up by the new leader.

`SET` and `DEL` become `Op::Put` and `Op::Delete`, encoded in the store's own record format, so a command on the wire is checksummed and a delete is the same tombstone the store has always understood. The consensus layer never looks inside one.

Writes go to the leader, and a follower answers one with `MOVED`, which Redis clients already know how to follow. Reads are answered wherever they arrive, through the read index above, so every node is read capacity and none of them can hand back a value a later read contradicts.

### Where the durability lives

```text
n1/
  data/            the store, exactly as a single node writes it
    applied-index  how far through the log the store has got
  raft/
    hard-state     term and vote, written to one side and renamed over the other
    entries        the log after the snapshot, append-only, same record format as the store
    snapshot       the store as of some index, checksummed, replaced whole
    snapshot.part-N  one being written or received; renamed into place, or deleted on the next start
```

Raft is only safe if a node's term, its vote and the entries it has acknowledged are on the platter before it replies, so every one of those writes is fsynced. That is not a tunable. `--no-fsync` exists for a single node that has chosen speed over a power cut; a node that acknowledges what it has not stored can lose a committed write, which is the one thing consensus exists to prevent.

The log reuses the store's record format, which means its framing, its checksum and its torn-tail recovery are the code the single-node store has already been tested on. A half-written entry is dropped at startup; a flipped bit inside a complete one is reported rather than guessed at.

Writes are batched on the way into the log. Appending means an fsync, under the node's lock, so proposing one write at a time would cap a node at one write per fsync however many clients it has. Instead each write joins a queue, and whoever finds nobody proposing takes the whole queue and proposes it as one batch: one append, one fsync, one message to each follower. Applying committed entries to the store is batched the same way, with one sync of the store for everything a step applied, before the applied index is moved.

A restarted node relearns its commit index from its snapshot onwards, so every committed entry after the snapshot is handed to it again. `applied-index` is what stops it writing those into its store a second time on each restart: entries up to it are skipped. The store is synced before the index is written, never after, so the index cannot claim writes a power cut took away. It is only ever an optimisation, which is why a missing or damaged one is not an error: it falls back to zero, the store is restored from the snapshot, and the entries after it are replayed, which lands in the same state.

## Testing

```console
$ cargo test
```

262 tests, including the three that matter:

- **`a_killed_writer_loses_nothing_it_finished`** spawns a real child process that writes 500 records, scribbles a header with no body onto the end of the file, then calls `abort()`. No destructor runs, no buffer is flushed, the kernel takes the process out with `SIGABRT`. The test then reopens the store and checks all 500 records, and that it is still writable afterwards.
- **`corruption_in_a_sealed_file_is_reported`** flips a bit in a file that was already closed and asserts the store refuses to open rather than pretending when asked to verify at open, and that opened from the file's hint it reports the damaged record when it is read, never handing it back.
- **`deleted_keys_do_not_come_back_after_a_compaction`** guards the ordering rule that makes compaction safe.

`tests/raft.rs` is a deterministic cluster harness, described above. `tests/replicated.rs` runs three replicas on real files through partitions, leader kills and restarts, checking after every round that no two of them hold different data. `tests/cluster.rs` starts three actual `minicask-cluster` processes and does it over TCP, including reads on followers and a fourth node joining while another leaves. `tests/hints.rs` opens stores from their hint files, and from damaged, missing and orphaned ones. `tests/server.rs` starts the real `minicask-server` binary and talks to it over a socket: pipelining, inline commands, binary-safe values, eight clients writing at once, a protocol error that must not affect other connections, and a `kill` followed by a restart on the same directory.

## Layout

```
src/crc.rs      CRC-32, table built at compile time
src/record.rs   the on-disk record format
src/log.rs      data files, the append writer, the recovery scanner
src/store.rs    the index, the read path, recovery, compaction
src/hint.rs     hint files: a data file's index, so opening skips the values
src/resp.rs     the Redis wire protocol, both directions
src/server.rs   the TCP server and its command table
src/raft/       consensus: the log, snapshots, the RPCs, the state machine, the wire
src/replicated.rs  committed entries applied to the store
src/cluster.rs  a replicated node as a running process
src/bin/        the CLI, the server, and the crash-test helper
```

## What it does not do

Worth being straight about, since each of these is a design choice rather than an oversight:

- **Keys must fit in memory.** The index is a `HashMap`, so memory scales with key count, not data size.
- **Single process.** There is no file lock. Two `Store` instances on one directory will corrupt each other. Within one process, reads can share a `Store` across threads and writes need it alone, which is what the server's read-write lock is for.
- **One store is still one disk.** `minicask-cluster` is the answer to that, and it is a different set of trade-offs rather than a strictly better one: every write costs a network round trip and a majority of fsyncs.
- **A hint is trusted, not checked.** Opening from a hint does not verify each record's checksum the way a scan does, so rot inside a hinted file is found when the value is read rather than at startup. It is still found: every read checks its record.
- **Compaction is stop-the-world.** It blocks until the merge finishes.
- **No range scans.** A hash index cannot answer ordered queries.

## License

MIT
