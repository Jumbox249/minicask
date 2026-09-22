//! A replicated store, end to end.
//!
//! These are real stores on real files with a real Raft log on disk, driven
//! a tick at a time so that partitions, leader kills and restarts happen at
//! exactly the moment each test says they do. A restart here reopens both
//! files from scratch, so the durability of the consensus log is part of
//! what is being tested rather than something taken on trust.

mod common;

use common::TempDir;
use minicask::raft::{Action, Config, DiskStorage, Node, NodeId};
use minicask::{ReplicatedStore, Store};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

type Replica = ReplicatedStore<DiskStorage>;

struct InFlight {
    from: NodeId,
    to: NodeId,
    message: minicask::raft::Message,
}

struct Cluster {
    ids: Vec<NodeId>,
    nodes: HashMap<NodeId, Option<Replica>>,
    dirs: HashMap<NodeId, (PathBuf, PathBuf)>,
    config: Config,
    inflight: Vec<InFlight>,
    severed: HashSet<(NodeId, NodeId)>,
    _root: TempDir,
}

impl Cluster {
    fn new(size: u64) -> Cluster {
        let root = TempDir::new("replicated");
        let ids: Vec<NodeId> = (1..=size).collect();
        let config = Config::default();
        let mut nodes = HashMap::new();
        let mut dirs = HashMap::new();

        for &id in &ids {
            let raft_dir = root.path().join(format!("node-{id}/raft"));
            let store_dir = root.path().join(format!("node-{id}/data"));
            dirs.insert(id, (raft_dir.clone(), store_dir.clone()));
            nodes.insert(id, Some(open(id, &ids, config, &raft_dir, &store_dir)));
        }

        Cluster {
            ids,
            nodes,
            dirs,
            config,
            inflight: Vec::new(),
            severed: HashSet::new(),
            _root: root,
        }
    }

    // -- driving --------------------------------------------------------

    fn tick(&mut self) {
        let mut next = Vec::new();
        for m in std::mem::take(&mut self.inflight) {
            if self.severed.contains(&(m.from, m.to)) {
                continue;
            }
            if let Some(Some(node)) = self.nodes.get_mut(&m.to) {
                let to = m.to;
                let actions = node.step(m.from, m.message).expect("step");
                queue(&mut next, to, actions);
            }
        }
        for id in self.ids.clone() {
            if let Some(Some(node)) = self.nodes.get_mut(&id) {
                let actions = node.tick().expect("tick");
                queue(&mut next, id, actions);
            }
        }
        self.inflight.extend(next);
    }

    fn tick_n(&mut self, n: u64) {
        for _ in 0..n {
            self.tick();
        }
    }

    fn run_until(&mut self, what: &str, mut done: impl FnMut(&Cluster) -> bool) {
        for _ in 0..600 {
            if done(self) {
                return;
            }
            self.tick();
        }
        panic!("gave up after 600 ticks waiting for {what}");
    }

    // -- faults ---------------------------------------------------------

    fn partition(&mut self, groups: &[&[NodeId]]) {
        self.severed.clear();
        for (i, group) in groups.iter().enumerate() {
            for (j, other) in groups.iter().enumerate() {
                if i != j {
                    for &a in *group {
                        for &b in *other {
                            self.severed.insert((a, b));
                        }
                    }
                }
            }
        }
    }

    fn heal(&mut self) {
        self.severed.clear();
    }

    /// Drop the node entirely, closing both files. Nothing but what
    /// reached the disk comes back.
    fn kill(&mut self, id: NodeId) {
        self.nodes.insert(id, None);
        self.inflight.retain(|m| m.to != id);
    }

    /// Reopen from disk, the way the process would on restart.
    fn restart(&mut self, id: NodeId) {
        self.kill(id);
        let (raft_dir, store_dir) = self.dirs[&id].clone();
        let node = open(id, &self.ids, self.config, &raft_dir, &store_dir);
        self.nodes.insert(id, Some(node));
    }

    // -- inspection -----------------------------------------------------

    fn node(&self, id: NodeId) -> &Replica {
        self.nodes
            .get(&id)
            .and_then(|n| n.as_ref())
            .unwrap_or_else(|| panic!("node {id} is not running"))
    }

    fn running(&self) -> impl Iterator<Item = &Replica> {
        self.ids.iter().filter_map(|id| self.nodes[id].as_ref())
    }

    fn leader(&self) -> Option<NodeId> {
        let leaders: Vec<NodeId> = self
            .running()
            .filter(|n| n.is_leader())
            .map(|n| n.id())
            .collect();
        match leaders.as_slice() {
            [only] => Some(*only),
            _ => None,
        }
    }

    /// The one leader inside `group`. Deliberately not built on
    /// `leader()`: during a partition a stranded old leader still believes
    /// it leads, so the cluster has two, and this has to ignore the one on
    /// the other side of the cut.
    fn leader_in(&self, group: &[NodeId]) -> Option<NodeId> {
        let found: Vec<NodeId> = self
            .running()
            .filter(|n| n.is_leader() && group.contains(&n.id()))
            .map(|n| n.id())
            .collect();
        match found.as_slice() {
            [only] => Some(*only),
            _ => None,
        }
    }

    /// Tick until someone other than `avoid` is leading.
    fn poll_leader_other_than(&mut self, avoid: NodeId) -> NodeId {
        self.run_until("a replacement leader", |c| {
            c.leader().is_some_and(|id| id != avoid)
        });
        self.leader().expect("a replacement leader")
    }

    fn put(&mut self, id: NodeId, key: &[u8], value: &[u8]) -> u64 {
        match self.nodes.get_mut(&id) {
            Some(Some(node)) => {
                let accepted = node.put(key, value).expect("the leader accepts");
                queue(&mut self.inflight, id, accepted.actions);
                accepted.index
            }
            _ => panic!("node {id} is not running"),
        }
    }

    fn delete(&mut self, id: NodeId, key: &[u8]) -> u64 {
        match self.nodes.get_mut(&id) {
            Some(Some(node)) => {
                let accepted = node.delete(key).expect("the leader accepts");
                queue(&mut self.inflight, id, accepted.actions);
                accepted.index
            }
            _ => panic!("node {id} is not running"),
        }
    }

    /// Wait for every running node to have applied up to the leader.
    fn settle(&mut self) {
        let target = self
            .running()
            .map(|n| n.node().last_index())
            .max()
            .expect("a running node");
        self.run_until("every node to apply", |c| {
            c.running().all(|n| n.applied_index() >= target)
        });
    }

    /// Every key and value a node holds.
    fn contents(&self, id: NodeId) -> Vec<(Vec<u8>, Vec<u8>)> {
        let node = self.node(id);
        let mut keys: Vec<Vec<u8>> = node.store().keys().map(<[u8]>::to_vec).collect();
        keys.sort();
        keys.into_iter()
            .map(|k| {
                let value = node.get(&k).expect("read").expect("a live key has a value");
                (k, value)
            })
            .collect()
    }

    /// The property the whole thing exists for: every node holds the same
    /// data.
    fn assert_all_agree(&self) {
        let ids: Vec<NodeId> = self.running().map(|n| n.id()).collect();
        let Some(&first) = ids.first() else { return };
        let expected = self.contents(first);
        for &id in &ids[1..] {
            assert_eq!(
                self.contents(id),
                expected,
                "node {id} holds different data from node {first}"
            );
        }
    }
}

fn open(id: NodeId, ids: &[NodeId], config: Config, raft_dir: &Path, store_dir: &Path) -> Replica {
    let storage = DiskStorage::open(raft_dir).expect("open the raft log");
    let store = Store::open(store_dir).expect("open the store");
    ReplicatedStore::new(Node::new(id, ids.to_vec(), config, storage), store)
}

fn queue(out: &mut Vec<InFlight>, from: NodeId, actions: Vec<Action>) {
    for Action::Send { to, message } in actions {
        out.push(InFlight { from, to, message });
    }
}

fn pairs(spec: &[(&str, &str)]) -> Vec<(Vec<u8>, Vec<u8>)> {
    spec.iter()
        .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
        .collect()
}

// -- the basics ---------------------------------------------------------

#[test]
fn a_write_reaches_every_node() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");

    c.put(leader, b"language", b"rust");
    c.put(leader, b"answer", b"42");
    c.settle();

    for id in 1..=3 {
        assert_eq!(
            c.node(id).get(b"language").unwrap(),
            Some(b"rust".to_vec()),
            "node {id} is missing the write"
        );
    }
    c.assert_all_agree();
}

#[test]
fn a_write_is_not_visible_before_it_commits() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");

    // Accepted by the leader, but not yet acknowledged by anyone else.
    let index = c.put(leader, b"k", b"v");
    assert_eq!(
        c.node(leader).get(b"k").unwrap(),
        None,
        "a proposal took effect before it was committed"
    );
    c.run_until("the write to apply", |c| {
        c.node(leader).applied_index() >= index
    });
    assert_eq!(c.node(leader).get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn deletes_replicate_too() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");

    c.put(leader, b"keep", b"1");
    c.put(leader, b"drop", b"2");
    c.settle();
    c.delete(leader, b"drop");
    c.settle();

    for id in 1..=3 {
        assert_eq!(c.node(id).get(b"drop").unwrap(), None, "node {id}");
        assert_eq!(c.node(id).get(b"keep").unwrap(), Some(b"1".to_vec()));
    }
    c.assert_all_agree();
}

#[test]
fn a_follower_will_not_take_a_write() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let follower = (1..=3).find(|&id| id != leader).unwrap();
    c.run_until("the follower to learn who leads", |c| {
        c.node(follower).leader() == Some(leader)
    });

    match c.nodes.get_mut(&follower) {
        Some(Some(node)) => {
            let err = node.put(b"k", b"v").unwrap_err();
            assert!(
                err.to_string().contains(&format!("try node {leader}")),
                "expected a redirect, got: {err}"
            );
        }
        _ => panic!("the follower is not running"),
    }
    // And nothing was written anywhere.
    c.tick_n(10);
    assert_eq!(c.node(follower).get(b"k").unwrap(), None);
}

// -- failure ------------------------------------------------------------

#[test]
fn data_survives_the_leader_dying() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let first = c.leader().expect("a leader");

    c.put(first, b"before", b"1");
    c.settle();

    c.kill(first);
    c.run_until("a replacement", |c| c.leader().is_some());
    let second = c.leader().expect("a new leader");
    assert_ne!(second, first);

    c.put(second, b"after", b"2");
    c.settle();

    for node in c.running() {
        let id = node.id();
        assert_eq!(
            node.get(b"before").unwrap(),
            Some(b"1".to_vec()),
            "node {id}"
        );
        assert_eq!(
            node.get(b"after").unwrap(),
            Some(b"2".to_vec()),
            "node {id}"
        );
    }
    c.assert_all_agree();
}

#[test]
fn a_minority_cannot_change_anything() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    c.put(leader, b"k", b"original");
    c.settle();

    // Strand the leader with one follower.
    let others: Vec<NodeId> = (1..=5).filter(|&id| id != leader).collect();
    let minority = [leader, others[0]];
    let majority = [others[1], others[2], others[3]];
    c.partition(&[&minority, &majority]);

    c.put(leader, b"k", b"doomed");
    c.tick_n(120);
    assert_eq!(
        c.node(leader).get(b"k").unwrap(),
        Some(b"original".to_vec()),
        "a leader without a majority applied a write"
    );

    // The majority elects someone and makes a real change.
    c.run_until("the majority to elect", |c| {
        c.leader_in(&majority).is_some()
    });
    let new_leader = c.leader_in(&majority).expect("a leader");
    c.put(new_leader, b"k", b"real");
    c.run_until("the majority to apply", |c| {
        majority
            .iter()
            .all(|&id| c.node(id).get(b"k").unwrap() == Some(b"real".to_vec()))
    });

    // On healing, the stranded leader's write is discarded, not applied.
    c.heal();
    c.settle();
    for id in 1..=5 {
        assert_eq!(
            c.node(id).get(b"k").unwrap(),
            Some(b"real".to_vec()),
            "node {id} kept a write that never had a majority"
        );
    }
    c.assert_all_agree();
}

// -- durability ---------------------------------------------------------

#[test]
fn a_restarted_node_reloads_its_data_from_disk() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");

    for i in 0..5u8 {
        c.put(leader, &[b'k', b'0' + i], &[b'v', b'0' + i]);
    }
    c.settle();

    let follower = (1..=3).find(|&id| id != leader).unwrap();
    let before = c.contents(follower);
    c.restart(follower);

    assert_eq!(
        c.contents(follower),
        before,
        "the store did not come back off the disk"
    );
    c.settle();
    c.assert_all_agree();
}

#[test]
fn the_whole_cluster_can_be_bounced() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    c.put(leader, b"durable", b"yes");
    c.put(leader, b"transient", b"no");
    c.settle();
    c.delete(leader, b"transient");
    c.settle();

    for id in 1..=3 {
        c.restart(id);
    }
    c.run_until("a leader after the bounce", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    c.put(leader, b"after", b"bounce");
    c.settle();

    for id in 1..=3 {
        assert_eq!(
            c.contents(id),
            pairs(&[("after", "bounce"), ("durable", "yes")]),
            "node {id} came back wrong"
        );
    }
    c.assert_all_agree();
}

#[test]
fn a_node_that_missed_everything_catches_up_on_rejoining() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let behind = (1..=3).find(|&id| id != leader).unwrap();
    let rest: Vec<NodeId> = (1..=3).filter(|&id| id != behind).collect();

    c.partition(&[&rest, &[behind]]);
    for i in 0..20u8 {
        c.put(leader, &[b'k', i], &[b'v', i]);
    }
    c.run_until("the majority to apply", |c| {
        rest.iter()
            .all(|&id| c.node(id).applied_index() >= c.node(leader).node().last_index())
    });
    assert!(
        c.node(behind).len() < 20,
        "the isolated node should have missed the writes"
    );

    c.heal();
    c.settle();
    assert_eq!(c.contents(behind), c.contents(leader));
    c.assert_all_agree();
}

// -- churn --------------------------------------------------------------

#[test]
fn the_data_stays_consistent_under_churn() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());

    let mut expected: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    for round in 0..12u8 {
        c.run_until("a leader", |c| c.leader().is_some());
        let leader = c.leader().expect("a leader");

        // A write, an overwrite of an older key, and sometimes a delete.
        let key = vec![b'k', round];
        let value = vec![b'v', round];
        c.put(leader, &key, &value);
        expected.insert(key.clone(), value);

        if round >= 2 {
            let older = vec![b'k', round - 2];
            let updated = vec![b'u', round];
            c.put(leader, &older, &updated);
            expected.insert(older, updated);
        }
        if round % 4 == 3 {
            let victim = vec![b'k', round - 1];
            c.delete(leader, &victim);
            expected.remove(&victim);
        }
        c.run_until("the round to apply", |c| {
            c.node(leader).applied_index() >= c.node(leader).node().last_index()
        });

        match round % 3 {
            0 => {
                c.kill(leader);
                c.run_until("a replacement", |c| c.leader().is_some());
                c.restart(leader);
            }
            1 => {
                let group: Vec<NodeId> = (1..=5).filter(|&id| id != leader).collect();
                c.partition(&[&[leader], &group]);
                c.tick_n(40);
                c.heal();
            }
            _ => {
                let victim = (1..=5).find(|&id| id != leader).unwrap();
                c.restart(victim);
                c.tick_n(20);
            }
        }
        c.run_until("the cluster to settle", |c| c.leader().is_some());
    }

    c.settle();
    c.assert_all_agree();

    let mut wanted: Vec<(Vec<u8>, Vec<u8>)> = expected.into_iter().collect();
    wanted.sort();
    let leader = c.leader().expect("a leader");
    assert_eq!(
        c.contents(leader),
        wanted,
        "the replicated data does not match what was written"
    );
}

/// A leader that has only just won cannot answer reads yet, and must not
/// pretend otherwise.
///
/// It holds every committed entry, by the rule that decides a vote, but it
/// does not yet know which of them are committed: a follower learns that
/// from the leader's next message, and the old leader can die before
/// sending one. Answering in that window returns nothing for a write that
/// was already acknowledged.
#[test]
fn a_brand_new_leader_does_not_answer_reads_until_it_has_caught_up() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let first = c.leader().expect("a leader");

    // A write that commits on the leader. The followers have it on disk,
    // which is what made it commit, but have not been told so yet.
    let index = c.put(first, b"acknowledged", b"yes");
    c.run_until("the leader to apply it", |c| {
        c.node(first).applied_index() >= index
    });

    c.kill(first);
    let second = c.poll_leader_other_than(first);

    // The moment it takes office it must not claim to be able to serve.
    assert!(
        !c.node(second).ready_to_serve() || c.node(second).get(b"acknowledged").unwrap().is_some(),
        "a leader called itself ready while still missing an acknowledged write"
    );

    // Once it is ready, the write is there. This is the property that
    // matters: readiness and the data arrive together, never apart.
    c.run_until("the new leader to become ready", |c| {
        c.node(second).ready_to_serve()
    });
    assert_eq!(
        c.node(second).get(b"acknowledged").unwrap(),
        Some(b"yes".to_vec()),
        "a ready leader was missing a write that had been acknowledged"
    );
}

/// Every tick between winning and being ready must hold the same line.
#[test]
fn readiness_never_runs_ahead_of_the_data() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());

    let mut committed: Vec<Vec<u8>> = Vec::new();
    for round in 0..4u8 {
        let leader = c.leader().expect("a leader");
        let key = vec![b'k', round];
        let index = c.put(leader, &key, b"yes");
        c.run_until("the write to apply on the leader", |c| {
            c.node(leader).applied_index() >= index
        });
        committed.push(key);

        c.kill(leader);
        let next = c.poll_leader_other_than(leader);

        // Check on every single tick, not just once it has settled.
        for _ in 0..60 {
            if c.node(next).ready_to_serve() {
                for key in &committed {
                    assert_eq!(
                        c.node(next).get(key).unwrap(),
                        Some(b"yes".to_vec()),
                        "node {next} said it was ready while missing {key:?}"
                    );
                }
            }
            c.tick();
        }
        c.restart(leader);
        c.run_until("the cluster to settle", |c| c.leader().is_some());
    }
}

// -- restarts do not rewrite history ------------------------------------

impl Cluster {
    fn store_bytes(&self, id: NodeId) -> u64 {
        self.node(id).store().stats().disk_bytes
    }

    fn store_dir(&self, id: NodeId) -> PathBuf {
        self.dirs[&id].1.clone()
    }
}

/// The bug this guards against: a restarted node relearns its commit index
/// from zero, gets every committed entry handed to it again, and appends
/// the whole history to its store a second time. The data comes out right,
/// because the writes are idempotent, but the disk grows by the full size
/// of the log on every restart.
#[test]
fn a_restart_does_not_rewrite_the_store() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    for i in 0..20u8 {
        c.put(leader, &[b'k', i], &[b'v'; 64]);
    }
    c.settle();

    let follower = (1..=3).find(|&id| id != leader).unwrap();
    let before = c.store_bytes(follower);
    let contents = c.contents(follower);

    for _ in 0..3 {
        c.restart(follower);
        // Long enough for the leader to tell it the commit index again,
        // which is the moment the old code replayed everything.
        c.tick_n(40);
        c.settle();
    }

    assert_eq!(
        c.store_bytes(follower),
        before,
        "restarting grew the store, so committed entries were applied again"
    );
    assert_eq!(c.contents(follower), contents);
    c.assert_all_agree();
}

/// And for the whole cluster at once, where every node relearns the commit
/// index from whichever of them wins the next election.
#[test]
fn bouncing_the_cluster_does_not_rewrite_any_store() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    for i in 0..10u8 {
        c.put(leader, &[b'k', i], &[b'v'; 64]);
    }
    c.settle();
    let before: Vec<u64> = (1..=3).map(|id| c.store_bytes(id)).collect();

    for _ in 0..2 {
        for id in 1..=3 {
            c.restart(id);
        }
        c.run_until("a leader after the bounce", |c| c.leader().is_some());
        c.settle();
    }

    // Each new term adds a no-op to the log, but a no-op writes nothing to
    // the store, so the stores must not have moved.
    let after: Vec<u64> = (1..=3).map(|id| c.store_bytes(id)).collect();
    assert_eq!(after, before, "a full bounce rewrote the stores");
    c.assert_all_agree();
}

#[test]
fn the_applied_index_survives_a_restart() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    c.put(leader, b"a", b"1");
    c.put(leader, b"b", b"2");
    c.settle();

    let follower = (1..=3).find(|&id| id != leader).unwrap();
    let applied = c.node(follower).applied_index();
    assert!(applied > 0);

    c.restart(follower);
    assert_eq!(
        c.node(follower).applied_index(),
        applied,
        "the store forgot how far through the log it had got"
    );
}

/// The index only saves work, so losing it, or finding it damaged, must
/// cost a replay and nothing else.
#[test]
fn a_missing_or_damaged_applied_index_only_costs_a_replay() {
    for damage in ["missing", "garbage", "flipped bit"] {
        let mut c = Cluster::new(3);
        c.run_until("a leader", |c| c.leader().is_some());
        let leader = c.leader().expect("a leader");
        for i in 0..5u8 {
            c.put(leader, &[b'k', i], &[b'v', i]);
        }
        c.delete(leader, &[b'k', 2]);
        c.settle();

        let follower = (1..=3).find(|&id| id != leader).unwrap();
        let expected = c.contents(follower);
        let file = c.store_dir(follower).join("applied-index");

        c.kill(follower);
        match damage {
            "missing" => std::fs::remove_file(&file).expect("remove the index"),
            "garbage" => std::fs::write(&file, b"not an index").expect("write junk"),
            _ => {
                let mut bytes = std::fs::read(&file).expect("read the index");
                bytes[6] ^= 0b0001_0000;
                std::fs::write(&file, &bytes).expect("write it back");
            }
        }
        c.restart(follower);
        assert_eq!(
            c.node(follower).applied_index(),
            0,
            "{damage}: an index that cannot be trusted must not be used"
        );

        c.settle();
        assert_eq!(
            c.contents(follower),
            expected,
            "{damage}: the replay did not reproduce the store"
        );
        c.assert_all_agree();
    }
}
