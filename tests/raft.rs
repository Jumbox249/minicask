//! A whole Raft cluster under a microscope.
//!
//! Every node in here runs in this thread, driven one tick at a time, with
//! the harness deciding which messages get through. Nothing sleeps and
//! nothing races, so a scenario that fails once fails every time with the
//! same tick count — which is the only way consensus bugs are tractable.
//!
//! The harness can do the three things that break naive implementations:
//! cut the network in two, kill a leader outright, and restart a node onto
//! the state it had persisted.

use minicask::raft::{
    Action, Command, Config, Entry, MemStorage, Message, Node, NodeId, ReadRequest, ReadState,
    Role, Storage,
};
use std::collections::{HashMap, HashSet};

/// A node that is either running or has been killed. A killed node keeps
/// its storage, which is what makes a restart mean something.
enum Slot {
    Running(Box<Node<MemStorage>>),
    Down(MemStorage),
}

struct InFlight {
    from: NodeId,
    to: NodeId,
    message: Message,
}

struct Cluster {
    slots: HashMap<NodeId, Slot>,
    ids: Vec<NodeId>,
    config: Config,
    inflight: Vec<InFlight>,
    /// Links cut in both directions. A partition is expressed as the set of
    /// ordered pairs that cannot deliver.
    severed: HashSet<(NodeId, NodeId)>,
    /// Everything ever committed, per node, so that divergence between two
    /// state machines is caught rather than inferred.
    applied: HashMap<NodeId, Vec<Entry>>,
    ticks: u64,
}

impl Cluster {
    fn new(size: u64) -> Cluster {
        Cluster::with_config(size, Config::default())
    }

    fn with_config(size: u64, config: Config) -> Cluster {
        let ids: Vec<NodeId> = (1..=size).collect();
        let slots = ids
            .iter()
            .map(|&id| {
                let node = Node::new(id, ids.clone(), config, MemStorage::new());
                (id, Slot::Running(Box::new(node)))
            })
            .collect();
        Cluster {
            slots,
            applied: ids.iter().map(|&id| (id, Vec::new())).collect(),
            ids,
            config,
            inflight: Vec::new(),
            severed: HashSet::new(),
            ticks: 0,
        }
    }

    // -- driving --------------------------------------------------------

    /// Deliver what is in flight, then give every live node a tick. A round
    /// trip therefore takes two ticks, which is realistic enough and keeps
    /// causality obvious.
    fn tick(&mut self) {
        self.ticks += 1;
        let mut next = Vec::new();

        for m in std::mem::take(&mut self.inflight) {
            if self.severed.contains(&(m.from, m.to)) {
                continue; // dropped on the floor, as a partition does
            }
            let to = m.to;
            if let Some(Slot::Running(node)) = self.slots.get_mut(&to) {
                let actions = node.step(m.from, m.message).expect("step");
                queue(&mut next, to, actions);
            }
        }

        for id in self.ids.clone() {
            if let Some(Slot::Running(node)) = self.slots.get_mut(&id) {
                let actions = node.tick().expect("tick");
                queue(&mut next, id, actions);
            }
        }

        self.inflight.extend(next);
        self.collect_applied();
    }

    fn tick_n(&mut self, n: u64) {
        for _ in 0..n {
            self.tick();
        }
    }

    /// Tick until `done` holds, up to a generous bound. The bound exists so
    /// a broken implementation fails with a message instead of hanging.
    fn run_until(&mut self, what: &str, mut done: impl FnMut(&Cluster) -> bool) {
        for _ in 0..500 {
            if done(self) {
                return;
            }
            self.tick();
        }
        panic!("gave up after 500 ticks waiting for {what}");
    }

    /// Record newly committed entries so that any disagreement between two
    /// nodes about what was committed at a given index is caught.
    fn collect_applied(&mut self) {
        for id in self.ids.clone() {
            if let Some(Slot::Running(node)) = self.slots.get_mut(&id) {
                let new = node.take_committed();
                let log = self.applied.get_mut(&id).expect("known node");
                for entry in new {
                    let position = (entry.index - 1) as usize;
                    if let Some(existing) = log.get(position) {
                        assert_eq!(
                            existing, &entry,
                            "node {id} applied two different entries at index {}",
                            entry.index
                        );
                    } else {
                        assert_eq!(
                            position,
                            log.len(),
                            "node {id} applied index {} out of order",
                            entry.index
                        );
                        log.push(entry);
                    }
                }
            }
        }
    }

    // -- faults ---------------------------------------------------------

    /// Split the cluster so that no message crosses between the groups.
    fn partition(&mut self, groups: &[&[NodeId]]) {
        self.severed.clear();
        for (i, group) in groups.iter().enumerate() {
            for (j, other) in groups.iter().enumerate() {
                if i == j {
                    continue;
                }
                for &a in *group {
                    for &b in *other {
                        self.severed.insert((a, b));
                    }
                }
            }
        }
    }

    fn heal(&mut self) {
        self.severed.clear();
    }

    /// Kill a node. Volatile state is lost; the log and the vote are not.
    /// Messages already in flight to it are discarded, as they would be.
    fn kill(&mut self, id: NodeId) {
        let slot = self.slots.remove(&id).expect("known node");
        let storage = match slot {
            Slot::Running(node) => node.into_storage(),
            Slot::Down(storage) => storage,
        };
        self.slots.insert(id, Slot::Down(storage));
        self.inflight.retain(|m| m.to != id);
    }

    /// Bring a node back on the state it had persisted.
    fn restart(&mut self, id: NodeId) {
        let slot = self.slots.remove(&id).expect("known node");
        let storage = match slot {
            Slot::Down(storage) => storage,
            Slot::Running(node) => node.into_storage(),
        };
        let node = Node::new(id, self.ids.clone(), self.config, storage);
        self.slots.insert(id, Slot::Running(Box::new(node)));
    }

    // -- inspection -----------------------------------------------------

    fn node(&self, id: NodeId) -> &Node<MemStorage> {
        match self.slots.get(&id) {
            Some(Slot::Running(node)) => node,
            _ => panic!("node {id} is not running"),
        }
    }

    fn running(&self) -> impl Iterator<Item = &Node<MemStorage>> {
        self.ids.iter().filter_map(|id| match self.slots.get(id) {
            Some(Slot::Running(node)) => Some(&**node),
            _ => None,
        })
    }

    fn leaders(&self) -> Vec<NodeId> {
        self.running()
            .filter(|n| n.is_leader())
            .map(|n| n.id())
            .collect()
    }

    /// The one leader, if the cluster has settled on exactly one.
    fn leader(&self) -> Option<NodeId> {
        match self.leaders().as_slice() {
            [only] => Some(*only),
            _ => None,
        }
    }

    fn leader_in_group(&self, group: &[NodeId]) -> Option<NodeId> {
        let found: Vec<NodeId> = self
            .leaders()
            .into_iter()
            .filter(|id| group.contains(id))
            .collect();
        match found.as_slice() {
            [only] => Some(*only),
            _ => None,
        }
    }

    /// Make a node stand for election right now.
    fn campaign(&mut self, id: NodeId) {
        match self.slots.get_mut(&id) {
            Some(Slot::Running(node)) => {
                let actions = node.campaign().expect("campaign");
                queue(&mut self.inflight, id, actions);
            }
            _ => panic!("node {id} is not running"),
        }
    }

    /// Throw away everything in flight, as a network that was down for a
    /// while would have.
    fn drop_inflight(&mut self) {
        self.inflight.clear();
    }

    /// Make a node ask whether an election would be worth holding, the way
    /// an ordinary timeout would.
    fn pre_vote(&mut self, id: NodeId) {
        match self.slots.get_mut(&id) {
            Some(Slot::Running(node)) => {
                let actions = node.pre_vote().expect("pre-vote");
                queue(&mut self.inflight, id, actions);
            }
            _ => panic!("node {id} is not running"),
        }
    }

    /// Run a node's election timer down without letting it act, so that it
    /// no longer owes a sitting leader its loyalty.
    fn expire_lease(&mut self, id: NodeId) {
        match self.slots.get_mut(&id) {
            Some(Slot::Running(node)) => node.expire_election_timer(),
            _ => panic!("node {id} is not running"),
        }
    }

    fn propose(&mut self, id: NodeId, command: &[u8]) -> u64 {
        let index = match self.slots.get_mut(&id) {
            Some(Slot::Running(node)) => {
                let accepted = node.propose(command.to_vec()).expect("leader accepts");
                queue(&mut self.inflight, id, accepted.actions);
                accepted.index
            }
            _ => panic!("node {id} is not running"),
        };
        index
    }

    fn read_index(&mut self, id: NodeId) -> ReadRequest {
        match self.slots.get_mut(&id) {
            Some(Slot::Running(node)) => {
                let (request, actions) = node.read_index();
                queue(&mut self.inflight, id, actions);
                request
            }
            _ => panic!("node {id} is not running"),
        }
    }

    fn read_state(&self, id: NodeId, request: &ReadRequest) -> ReadState {
        self.node(id).read_state(request)
    }

    /// The data commands a node has applied, in order, with the leaders'
    /// no-op entries filtered out.
    fn applied_data(&self, id: NodeId) -> Vec<Vec<u8>> {
        self.applied[&id]
            .iter()
            .filter_map(|e| match &e.command {
                Command::Data(bytes) => Some(bytes.clone()),
                Command::Noop => None,
            })
            .collect()
    }

    fn committed_on(&self, id: NodeId) -> u64 {
        self.node(id).commit_index()
    }

    /// The core safety property: no two nodes disagree about what is in the
    /// log at any index they have both committed.
    fn assert_logs_agree(&self) {
        let logs: Vec<(NodeId, &Vec<Entry>)> = self.applied.iter().map(|(&k, v)| (k, v)).collect();
        for (a, left) in &logs {
            for (b, right) in &logs {
                if a >= b {
                    continue;
                }
                for (i, (l, r)) in left.iter().zip(right.iter()).enumerate() {
                    assert_eq!(
                        l,
                        r,
                        "nodes {a} and {b} applied different entries at index {}",
                        i + 1
                    );
                }
            }
        }
    }
}

fn queue(out: &mut Vec<InFlight>, from: NodeId, actions: Vec<Action>) {
    for Action::Send { to, message } in actions {
        out.push(InFlight { from, to, message });
    }
}

// -- elections ----------------------------------------------------------

#[test]
fn a_cluster_elects_exactly_one_leader() {
    let mut c = Cluster::new(3);
    assert_eq!(c.leaders(), vec![], "nobody leads before the first timeout");

    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");

    // Everyone agrees, and nobody else thinks they are leading.
    c.tick_n(10);
    assert_eq!(c.leaders(), vec![leader]);
    for node in c.running() {
        assert_eq!(node.leader(), Some(leader), "node {} disagrees", node.id());
        assert_eq!(node.term(), c.node(leader).term());
    }
}

#[test]
fn a_leader_keeps_its_followers_quiet() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let term = c.node(leader).term();

    // Heartbeats alone should hold the term steady well past several
    // election timeouts.
    c.tick_n(200);
    assert_eq!(c.leader(), Some(leader), "the leader was unseated");
    assert_eq!(c.node(leader).term(), term, "an unnecessary election ran");
}

#[test]
fn a_single_node_cluster_elects_itself() {
    let mut c = Cluster::new(1);
    c.run_until("a leader", |c| c.leader().is_some());
    assert_eq!(c.leader(), Some(1));

    let index = c.propose(1, b"alone");
    c.tick_n(2);
    assert_eq!(c.committed_on(1), index, "its own majority commits at once");
}

// -- replication --------------------------------------------------------

#[test]
fn commands_reach_every_follower() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");

    for command in [b"one".as_slice(), b"two", b"three"] {
        c.propose(leader, command);
    }
    let last = c.node(leader).last_index();

    c.run_until("the cluster to commit", |c| {
        c.running().all(|n| n.commit_index() >= last)
    });

    for id in [1, 2, 3] {
        assert_eq!(
            c.applied_data(id),
            vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()],
            "node {id} applied the wrong commands"
        );
    }
    c.assert_logs_agree();
}

#[test]
fn a_follower_refuses_commands() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let follower = [1, 2, 3].into_iter().find(|&id| id != leader).unwrap();
    // A follower only knows where to redirect once a heartbeat has reached
    // it, which is a tick or two after the election.
    c.run_until("the follower to hear from the leader", |c| {
        c.node(follower).leader() == Some(leader)
    });

    match c.slots.get_mut(&follower) {
        Some(Slot::Running(node)) => {
            let err = node.propose(b"nope".to_vec()).unwrap_err();
            let message = err.to_string();
            assert!(
                message.contains(&format!("try node {leader}")),
                "a follower should redirect to the leader, said: {message}"
            );
        }
        _ => panic!("follower is not running"),
    }
}

// -- leader failure -----------------------------------------------------

#[test]
fn killing_the_leader_elects_another() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let first = c.leader().expect("a leader");
    let term = c.node(first).term();

    c.propose(first, b"before the fall");
    c.run_until("the first command to commit", |c| {
        c.running().all(|n| n.commit_index() >= 2)
    });

    c.kill(first);
    c.run_until("a replacement", |c| c.leader().is_some());
    let second = c.leader().expect("a new leader");

    assert_ne!(second, first, "the dead node cannot still be leading");
    assert!(
        c.node(second).term() > term,
        "a new leader must run in a later term"
    );

    // The survivors are still a majority, so the cluster still works.
    c.propose(second, b"after the fall");
    let last = c.node(second).last_index();
    c.run_until("the new leader to commit", |c| {
        c.running().all(|n| n.commit_index() >= last)
    });

    for node in c.running() {
        let id = node.id();
        assert_eq!(
            c.applied_data(id),
            vec![b"before the fall".to_vec(), b"after the fall".to_vec()],
            "node {id} lost or invented a command across the handover"
        );
    }
    c.assert_logs_agree();
}

#[test]
fn a_committed_entry_survives_every_leader_change() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());

    let mut expected: Vec<Vec<u8>> = Vec::new();
    // Kill the leader three times over, committing either side of each
    // handover. Any entry acknowledged as committed must outlive all of it.
    for round in 0..3u8 {
        let leader = c.leader().expect("a leader");
        let command = vec![b'a' + round];
        c.propose(leader, &command);
        expected.push(command);

        let last = c.node(leader).last_index();
        c.run_until("a majority to store the command", |c| {
            c.running().filter(|n| n.last_index() >= last).count() >= 3
        });
        c.run_until("the leader to commit", |c| c.committed_on(leader) >= last);

        c.kill(leader);
        c.run_until("a replacement", |c| c.leader().is_some());
        c.restart(leader);
        c.run_until("the cluster to settle", |c| {
            c.leader().is_some() && c.running().count() == 5
        });
    }

    let leader = c.leader().expect("a leader");
    let last = c.node(leader).last_index();
    c.run_until("everyone to catch up", |c| {
        c.running().all(|n| n.commit_index() >= last)
    });

    for id in 1..=5 {
        assert_eq!(
            c.applied_data(id),
            expected,
            "node {id} does not have every committed command"
        );
    }
    c.assert_logs_agree();
}

// -- partitions ---------------------------------------------------------

#[test]
fn a_minority_cannot_elect_a_leader() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());

    // Two nodes on their own. They will time out and campaign forever,
    // raising their terms, but two votes is not three.
    c.partition(&[&[1, 2], &[3, 4, 5]]);
    c.tick_n(200);

    assert_eq!(
        c.leader_in_group(&[1, 2]),
        None,
        "a minority elected a leader"
    );
    assert!(
        c.leader_in_group(&[3, 4, 5]).is_some(),
        "the majority should still have one"
    );
}

#[test]
fn a_minority_cannot_commit() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");

    // Strand the leader with one follower. It does not know it yet, so it
    // will accept commands it can never commit.
    let others: Vec<NodeId> = (1..=5).filter(|&id| id != leader).collect();
    let stranded = [leader, others[0]];
    let majority = [others[1], others[2], others[3]];
    c.partition(&[&stranded, &majority]);

    let index = c.propose(leader, b"doomed");
    c.tick_n(100);
    assert!(
        c.committed_on(leader) < index,
        "a leader without a majority committed anyway"
    );

    // The majority side, meanwhile, carries on.
    c.run_until("the majority to elect", |c| {
        c.leader_in_group(&majority).is_some()
    });
    let new_leader = c.leader_in_group(&majority).expect("a leader");
    c.propose(new_leader, b"real");
    let last = c.node(new_leader).last_index();
    c.run_until("the majority to commit", |c| {
        majority.iter().all(|&id| c.committed_on(id) >= last)
    });
    c.assert_logs_agree();
}

#[test]
fn a_deposed_leader_steps_down_and_drops_its_orphan_entries() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());
    let old = c.leader().expect("a leader");

    c.propose(old, b"agreed");
    c.run_until("the first command to commit", |c| {
        c.running().all(|n| n.commit_index() >= 2)
    });

    // Isolate the leader completely, then feed it commands it cannot
    // replicate. They reach its own log and nowhere else.
    let rest: Vec<NodeId> = (1..=5).filter(|&id| id != old).collect();
    c.partition(&[&[old], &rest]);
    for command in [b"orphan-1".as_slice(), b"orphan-2"] {
        c.propose(old, command);
    }
    c.tick_n(50);
    assert!(c.node(old).last_index() > 2, "the orphans should be local");

    // The others elect someone and make real progress.
    c.run_until("a new leader", |c| c.leader_in_group(&rest).is_some());
    let new = c.leader_in_group(&rest).expect("a new leader");
    c.propose(new, b"real");
    let last = c.node(new).last_index();
    c.run_until("the majority to commit", |c| {
        rest.iter().all(|&id| c.committed_on(id) >= last)
    });

    // Heal. The old leader must step down and have its orphans overwritten.
    c.heal();
    c.run_until("the old leader to rejoin", |c| {
        c.node(old).role() == Role::Follower && c.committed_on(old) >= last
    });

    assert_eq!(c.node(old).role(), Role::Follower);
    assert_eq!(
        c.applied_data(old),
        vec![b"agreed".to_vec(), b"real".to_vec()],
        "the orphaned entries were applied or the real one was lost"
    );
    c.assert_logs_agree();
}

#[test]
fn a_healed_partition_reconciles_every_log() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");

    // Cut two nodes off, commit without them, then let them back.
    let lagging: Vec<NodeId> = (1..=5).filter(|&id| id != leader).take(2).collect();
    let connected: Vec<NodeId> = (1..=5).filter(|&id| !lagging.contains(&id)).collect();
    c.partition(&[&connected, &lagging]);

    for i in 0..5u8 {
        c.propose(leader, &[b'0' + i]);
    }
    let last = c.node(leader).last_index();
    c.run_until("the connected side to commit", |c| {
        connected.iter().all(|&id| c.committed_on(id) >= last)
    });
    for &id in &lagging {
        assert!(c.committed_on(id) < last, "node {id} should be behind");
    }

    c.heal();
    c.run_until("the stragglers to catch up", |c| {
        c.running().all(|n| n.commit_index() >= last)
    });

    let expected = c.applied_data(leader);
    for id in 1..=5 {
        assert_eq!(c.applied_data(id), expected, "node {id} did not reconcile");
    }
    c.assert_logs_agree();
}

#[test]
fn a_far_behind_follower_catches_up() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let behind = (1..=3).find(|&id| id != leader).unwrap();

    // Hold one node out for a long run of entries, so the leader has to
    // rewind a long way when it comes back.
    let rest: Vec<NodeId> = (1..=3).filter(|&id| id != behind).collect();
    c.partition(&[&rest, &[behind]]);
    for i in 0..40u8 {
        c.propose(leader, &[i]);
    }
    let last = c.node(leader).last_index();
    c.run_until("the majority to commit", |c| c.committed_on(leader) >= last);
    assert!(
        c.node(behind).last_index() < 2,
        "the isolated node should have missed the whole run, has {}",
        c.node(behind).last_index()
    );

    c.heal();
    c.run_until("the straggler to catch up", |c| {
        c.committed_on(behind) >= last
    });
    assert_eq!(c.applied_data(behind), c.applied_data(leader));
    c.assert_logs_agree();
}

// -- persistence --------------------------------------------------------

#[test]
fn a_restarted_node_remembers_its_term_and_vote() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let follower = (1..=3).find(|&id| id != leader).unwrap();

    let term = c.node(follower).term();
    let vote = c.node(follower).storage().hard_state().voted_for;
    assert!(vote.is_some(), "a follower in a settled term has voted");

    c.kill(follower);
    c.restart(follower);

    let state = c.node(follower).storage().hard_state();
    assert_eq!(state.term, term, "the term was forgotten");
    assert_eq!(state.voted_for, vote, "the vote was forgotten");
    assert_eq!(
        c.node(follower).role(),
        Role::Follower,
        "a node comes back as a follower whatever it was"
    );
}

#[test]
fn a_restarted_node_still_has_its_log() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let follower = (1..=3).find(|&id| id != leader).unwrap();

    for command in [b"one".as_slice(), b"two"] {
        c.propose(leader, command);
    }
    let last = c.node(leader).last_index();
    c.run_until("everyone to commit", |c| {
        c.running().all(|n| n.commit_index() >= last)
    });

    c.kill(follower);
    c.restart(follower);
    assert_eq!(
        c.node(follower).last_index(),
        last,
        "the log did not survive the restart"
    );
    // The commit index is volatile and relearned from the leader.
    c.run_until("the leader to bring it back up to date", |c| {
        c.committed_on(follower) >= last
    });
    assert_eq!(c.applied_data(follower), c.applied_data(leader));
}

#[test]
fn the_whole_cluster_can_be_bounced() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    c.propose(leader, b"survive me");
    let last = c.node(leader).last_index();
    c.run_until("everyone to commit", |c| {
        c.running().all(|n| n.commit_index() >= last)
    });
    let before = c.applied_data(leader);

    for id in 1..=3 {
        c.kill(id);
    }
    c.tick_n(5);
    assert_eq!(c.leaders(), vec![], "nothing runs while everything is down");
    for id in 1..=3 {
        c.restart(id);
    }

    c.run_until("a leader after the bounce", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    c.propose(leader, b"and after");
    let last = c.node(leader).last_index();
    c.run_until("everyone to commit again", |c| {
        c.running().all(|n| n.commit_index() >= last)
    });

    let mut expected = before;
    expected.push(b"and after".to_vec());
    for id in 1..=3 {
        assert_eq!(c.applied_data(id), expected, "node {id} lost history");
    }
    c.assert_logs_agree();
}

// -- churn --------------------------------------------------------------

#[test]
fn commands_survive_relentless_churn() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());

    let mut accepted: Vec<Vec<u8>> = Vec::new();
    // Twelve rounds of: propose, wait for it to commit, then break
    // something. Each round ends with whatever was committed having to
    // still be there.
    for round in 0..12u8 {
        let Some(leader) = c.leader() else {
            c.run_until("a leader", |c| c.leader().is_some());
            continue;
        };
        let command = format!("cmd-{round}").into_bytes();
        c.propose(leader, &command);
        let last = c.node(leader).last_index();
        c.run_until("the round to commit", |c| c.committed_on(leader) >= last);
        accepted.push(command);

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
                c.kill(victim);
                c.tick_n(20);
                c.restart(victim);
            }
        }
        c.run_until("the cluster to settle", |c| c.leader().is_some());
        c.assert_logs_agree();
    }

    let leader = c.leader().expect("a leader");
    let last = c.node(leader).last_index();
    c.run_until("a final catch-up", |c| {
        c.running().all(|n| n.commit_index() >= last)
    });

    for id in 1..=5 {
        let applied = c.applied_data(id);
        for command in &accepted {
            assert!(
                applied.contains(command),
                "node {id} is missing {}, which was committed",
                String::from_utf8_lossy(command)
            );
        }
    }
    c.assert_logs_agree();
}

// -- election safety ----------------------------------------------------

/// The scenario the up-to-date check exists for. A node that missed a run
/// of committed entries comes back with a high term, because it has been
/// campaigning alone the whole time, and stands for election at the one
/// moment nothing else is in flight. Its term is high enough to depose the
/// leader; its log is not good enough to replace it.
#[test]
fn a_stale_candidate_cannot_win_even_with_the_highest_term() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let stale = (1..=5).find(|&id| id != leader).unwrap();
    let rest: Vec<NodeId> = (1..=5).filter(|&id| id != stale).collect();

    // Cut it off and commit without it.
    c.partition(&[&rest, &[stale]]);
    let mut expected = Vec::new();
    for i in 0..6u8 {
        let command = vec![b'a' + i];
        c.propose(leader, &command);
        expected.push(command);
    }
    let last = c.node(leader).last_index();
    c.run_until("the majority to commit", |c| {
        rest.iter().all(|&id| c.committed_on(id) >= last)
    });
    c.tick_n(120);

    // Drive its term above the cluster's on purpose. Left alone it would
    // not get there, because the pre-vote keeps a node that cannot win
    // from raising its term at all; `campaign` is the way past that, and
    // the point here is what happens to a stale node that *has* somehow
    // ended up in front.
    for _ in 0..12 {
        c.campaign(stale);
        c.tick_n(2);
    }
    let stale_term = c.node(stale).term();
    assert!(
        stale_term > c.node(leader).term(),
        "the isolated node should have outrun the cluster's term"
    );
    assert!(
        c.node(stale).last_index() < last,
        "the isolated node should be missing entries"
    );

    // Heal with nothing in flight, so no heartbeat can reach it first, and
    // let it campaign into a cluster that will step down to its term.
    c.heal();
    c.drop_inflight();
    c.campaign(stale);
    c.tick_n(10);

    assert!(
        !c.node(stale).is_leader(),
        "a node missing committed entries won an election"
    );

    // And the cluster still recovers, on a leader that has the history.
    c.run_until("the cluster to settle", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    assert_ne!(leader, stale);
    let last = c.node(leader).last_index();
    c.run_until("everyone to catch up", |c| {
        c.running().all(|n| n.commit_index() >= last)
    });
    for id in 1..=5 {
        let applied = c.applied_data(id);
        for command in &expected {
            assert!(
                applied.contains(command),
                "node {id} lost {}, which was committed",
                String::from_utf8_lossy(command)
            );
        }
    }
    c.assert_logs_agree();
}

/// Two candidates in the same term must not both win, however the votes
/// arrive. Forcing them to campaign together is the tightest version of it.
#[test]
fn two_candidates_in_one_term_cannot_both_win() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());

    for _ in 0..20 {
        c.drop_inflight();
        // Two nodes stand at once, from the same term, with identical logs.
        c.campaign(1);
        c.campaign(2);
        c.tick_n(6);
        let leaders = c.leaders();
        assert!(
            leaders.len() <= 1,
            "term {} elected more than one leader: {leaders:?}",
            c.node(1).term()
        );
    }
}

/// A leader that has been away must not resume giving orders. Its term is
/// stale, so its own followers tell it so and it stands down.
#[test]
fn a_returning_leader_does_not_resume_command() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());
    let old = c.leader().expect("a leader");
    let rest: Vec<NodeId> = (1..=5).filter(|&id| id != old).collect();

    c.partition(&[&[old], &rest]);
    c.run_until("the majority to move on", |c| {
        c.leader_in_group(&rest).is_some()
    });
    let new = c.leader_in_group(&rest).expect("a new leader");
    c.propose(new, b"while you were out");
    let last = c.node(new).last_index();
    c.run_until("the majority to commit", |c| {
        rest.iter().all(|&id| c.committed_on(id) >= last)
    });

    // It has heard nothing from anyone, and cannot: it is on the wrong
    // side of the cut. It has to work out on its own that it is finished,
    // or it would go on answering reads from a store the majority has
    // already moved past.
    c.run_until("the cut-off leader to stand down unprompted", |c| {
        c.node(old).role() != Role::Leader
    });
    c.heal();
    c.run_until("the old leader to stand down", |c| {
        c.node(old).role() == Role::Follower
    });
    assert_eq!(c.leaders(), vec![new], "there should be exactly one leader");
    c.assert_logs_agree();
}

/// A leader hears nothing from a partition it is on the wrong side of, so
/// nothing will ever tell it that it has been replaced. Left alone it would
/// keep answering reads from a store the majority has moved past.
#[test]
fn a_leader_cut_off_from_the_majority_stands_down_on_its_own() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let rest: Vec<NodeId> = (1..=5).filter(|&id| id != leader).collect();

    c.partition(&[&[leader], &rest]);
    c.run_until("the cut-off leader to give up", |c| {
        c.node(leader).role() != Role::Leader
    });

    // It keeps its term: nothing has been decided, it has only stopped
    // claiming an office it can no longer do the job of.
    assert_eq!(c.node(leader).leader(), None);
}

/// The other half of that: a leader with a majority must not stand itself
/// down, however long it runs.
#[test]
fn a_leader_with_a_majority_stays_in_office() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let term = c.node(leader).term();

    // Lose one node, which still leaves four of five.
    let victim = (1..=5).find(|&id| id != leader).unwrap();
    c.kill(victim);
    c.tick_n(300);

    assert_eq!(c.leader(), Some(leader), "a healthy leader stood down");
    assert_eq!(c.node(leader).term(), term, "an unnecessary election ran");
}

/// And a leader that loses its majority and gets it back carries on.
#[test]
fn a_leader_that_stands_down_can_be_re_elected() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    c.propose(leader, b"before");
    c.run_until("the write to commit", |c| {
        c.running().all(|n| n.commit_index() >= 2)
    });

    let rest: Vec<NodeId> = (1..=3).filter(|&id| id != leader).collect();
    c.partition(&[&[leader], &rest]);
    c.run_until("the cut-off leader to give up", |c| {
        c.node(leader).role() != Role::Leader
    });

    c.heal();
    c.run_until("the cluster to settle", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    c.propose(leader, b"after");
    let last = c.node(leader).last_index();
    c.run_until("the cluster to commit again", |c| {
        c.running().all(|n| n.commit_index() >= last)
    });
    for id in 1..=3 {
        assert_eq!(
            c.applied_data(id),
            vec![b"before".to_vec(), b"after".to_vec()],
            "node {id}"
        );
    }
    c.assert_logs_agree();
}

// -- pre-vote -----------------------------------------------------------

/// The reason pre-vote exists. A node that has been cut off spends the
/// partition timing out, and on returning it must not cost the cluster an
/// election it was never going to win.
#[test]
fn a_node_returning_from_a_partition_does_not_disturb_the_leader() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let away = (1..=5).find(|&id| id != leader).unwrap();
    let rest: Vec<NodeId> = (1..=5).filter(|&id| id != away).collect();

    let term = c.node(leader).term();
    c.partition(&[&rest, &[away]]);
    c.propose(leader, b"while you were out");
    let last = c.node(leader).last_index();
    c.run_until("the majority to commit", |c| {
        rest.iter().all(|&id| c.committed_on(id) >= last)
    });
    // Long enough for many election timeouts to come and go.
    c.tick_n(300);

    c.heal();
    c.run_until("the returning node to catch up", |c| {
        c.committed_on(away) >= last
    });

    assert_eq!(
        c.node(leader).term(),
        term,
        "the returning node forced an election"
    );
    assert_eq!(c.leader(), Some(leader), "the leader was unseated");
    assert_eq!(c.node(away).role(), Role::Follower);
    c.assert_logs_agree();
}

/// The mechanism behind that: a node with nobody to ask never raises its
/// term, so it has nothing to disturb anyone with when it gets back.
#[test]
fn a_node_with_nobody_to_ask_does_not_raise_its_term() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let away = (1..=5).find(|&id| id != leader).unwrap();
    let rest: Vec<NodeId> = (1..=5).filter(|&id| id != away).collect();

    c.partition(&[&rest, &[away]]);
    let term = c.node(away).term();
    c.tick_n(300);

    assert_eq!(
        c.node(away).term(),
        term,
        "an isolated node raised its term with nobody to elect it"
    );
    assert_eq!(
        c.node(away).role(),
        Role::PreCandidate,
        "it should be stuck asking, not standing"
    );
}

/// A node that is hearing from a healthy leader refuses to canvass for
/// anyone else, which is what keeps a sitting leader in place.
#[test]
fn a_followers_loyalty_lasts_as_long_as_the_leader_is_heard_from() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let follower = (1..=3).find(|&id| id != leader).unwrap();
    c.run_until("the follower to settle", |c| {
        c.node(follower).leader() == Some(leader)
    });

    // The third node asks, repeatedly, while the leader is perfectly fine.
    let other = (1..=3).find(|&id| id != leader && id != follower).unwrap();
    let term = c.node(leader).term();
    for _ in 0..20 {
        c.pre_vote(other);
        c.tick_n(3);
    }

    assert_eq!(c.leader(), Some(leader), "a healthy leader was unseated");
    assert_eq!(c.node(leader).term(), term, "an election was forced");
}

/// But loyalty is not blind. Once the leader really has gone, the same
/// question has to be answered differently or nothing would ever recover.
#[test]
fn loyalty_expires_when_the_leader_does() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");

    c.kill(leader);
    c.run_until("a replacement", |c| c.leader().is_some());
    assert_ne!(c.leader(), Some(leader));
}

/// Pre-vote must not become a way round the up-to-date rule.
#[test]
fn a_stale_node_is_refused_at_the_pre_vote() {
    let mut c = Cluster::new(5);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let stale = (1..=5).find(|&id| id != leader).unwrap();
    let rest: Vec<NodeId> = (1..=5).filter(|&id| id != stale).collect();

    c.partition(&[&rest, &[stale]]);
    for i in 0..6u8 {
        c.propose(leader, &[b'a' + i]);
    }
    let last = c.node(leader).last_index();
    c.run_until("the majority to commit", |c| {
        rest.iter().all(|&id| c.committed_on(id) >= last)
    });

    // Heal with nothing in flight and let the stale node ask first, before
    // any heartbeat can reach it. Even with every lease expired, its log
    // is not good enough and the answer has to be no.
    c.heal();
    c.drop_inflight();
    for id in &rest {
        c.expire_lease(*id);
    }
    let before = c.node(stale).term();
    c.pre_vote(stale);
    c.tick_n(6);

    // It may well be a follower by now, because the leader's next
    // heartbeat reaches it and it accepts. What it must never have been is
    // a candidate: nobody told it an election was worth holding, so its
    // term never moved and it never stood.
    assert!(
        matches!(c.node(stale).role(), Role::PreCandidate | Role::Follower),
        "a stale node was told an election would be worth holding, and stood: {:?}",
        c.node(stale).role()
    );
    assert_eq!(
        c.node(stale).term(),
        before,
        "a refused pre-vote must leave the term where it was"
    );
}

// -- message size -------------------------------------------------------

/// A config whose budget fits about two of the test's entries, and whose
/// heartbeat is slow enough that waiting for it would be obvious.
fn small_batches() -> Config {
    Config {
        heartbeat_ticks: 8,
        election_timeout_min: 30,
        election_timeout_max: 40,
        max_append_bytes: 200,
        max_entry_bytes: 4096,
    }
}

/// Every `AppendEntries` in flight either fits the budget or is a single
/// entry, which is the only case allowed to exceed it.
fn assert_messages_within(c: &Cluster, budget: usize) {
    for m in &c.inflight {
        if let Message::AppendEntries { entries, .. } = &m.message {
            let bytes: usize = entries.iter().map(|e| e.encoded_len()).sum();
            assert!(
                entries.len() <= 1 || bytes <= budget,
                "a message to node {} carried {} entries, {bytes} bytes, over a {budget} byte budget",
                m.to,
                entries.len()
            );
        }
    }
}

/// The failure this exists for: a follower far enough behind that its
/// backlog would not fit in one message. It has to be caught up in pieces,
/// and no piece may be larger than the budget.
#[test]
fn a_far_behind_follower_is_caught_up_in_bounded_pieces() {
    let config = small_batches();
    let mut c = Cluster::with_config(3, config);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let behind = (1..=3).find(|&id| id != leader).unwrap();
    let rest: Vec<NodeId> = (1..=3).filter(|&id| id != behind).collect();

    c.partition(&[&rest, &[behind]]);
    for i in 0..60u8 {
        c.propose(leader, &[i; 50]);
        assert_messages_within(&c, config.max_append_bytes);
        c.tick();
    }
    let last = c.node(leader).last_index();
    c.run_until("the majority to commit", |c| c.committed_on(leader) >= last);

    c.heal();
    for _ in 0..500 {
        assert_messages_within(&c, config.max_append_bytes);
        if c.committed_on(behind) >= last {
            break;
        }
        c.tick();
    }
    assert_eq!(c.committed_on(behind), last, "the follower never caught up");
    assert_eq!(c.applied_data(behind), c.applied_data(leader));
    c.assert_logs_agree();
}

/// Catching up must move at a batch per round trip, not a batch per
/// heartbeat. Sixty entries at two a message is thirty batches; waiting
/// for an eight-tick heartbeat each time would take well over two hundred
/// ticks, while carrying straight on takes about sixty.
#[test]
fn catching_up_does_not_wait_for_heartbeats() {
    let config = small_batches();
    let mut c = Cluster::with_config(3, config);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let behind = (1..=3).find(|&id| id != leader).unwrap();
    let rest: Vec<NodeId> = (1..=3).filter(|&id| id != behind).collect();

    c.partition(&[&rest, &[behind]]);
    for i in 0..60u8 {
        c.propose(leader, &[i; 50]);
        c.tick();
    }
    let last = c.node(leader).last_index();
    c.run_until("the majority to commit", |c| c.committed_on(leader) >= last);

    c.heal();
    let mut ticks = 0;
    while c.committed_on(behind) < last {
        c.tick();
        ticks += 1;
        assert!(
            ticks < 150,
            "catch-up is waiting on the heartbeat: {ticks} ticks and counting"
        );
    }
}

/// A command no message could carry is refused up front, because once in
/// the log it would block everything behind it.
#[test]
fn a_command_too_large_to_replicate_is_refused() {
    let mut c = Cluster::with_config(3, small_batches());
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("a leader");
    let before = c.node(leader).last_index();

    match c.slots.get_mut(&leader) {
        Some(Slot::Running(node)) => {
            let err = node.propose(vec![0; 5000]).unwrap_err();
            assert!(
                matches!(err, minicask::raft::ProposeError::TooLarge { .. }),
                "expected TooLarge, got {err}"
            );
        }
        _ => panic!("the leader is not running"),
    }
    assert_eq!(c.node(leader).last_index(), before, "nothing was appended");
}

// -- reads --------------------------------------------------------------

/// The case the read index exists for. A leader cut off from the others
/// goes on believing it leads until check-quorum catches up with it, and
/// in the meantime the majority elects someone else and commits writes it
/// knows nothing about. A read started on it in that window must never be
/// confirmed, or it would be answered from a store that is already stale.
#[test]
fn a_leader_cut_off_from_the_majority_never_confirms_a_read() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let old = c.leader().expect("leader");
    let before = c.propose(old, b"before");
    c.run_until("the write to commit everywhere", |c| {
        c.ids.iter().all(|&id| c.committed_on(id) >= before)
    });

    let others: Vec<NodeId> = c.ids.iter().copied().filter(|&id| id != old).collect();
    c.partition(&[&[old], &others]);
    let read = c.read_index(old);
    assert!(c.node(old).is_leader(), "it has not noticed yet");

    let mut replaced = false;
    for _ in 0..500 {
        c.tick();
        assert_ne!(
            c.read_state(old, &read),
            ReadState::Ready(before),
            "a leader with no majority confirmed a read"
        );
        assert!(!matches!(c.read_state(old, &read), ReadState::Ready(_)));
        if !replaced {
            if let Some(new) = c.leader_in_group(&others) {
                // The write the stale leader could have missed.
                c.propose(new, b"after");
                replaced = true;
            }
        }
        if replaced && !c.node(old).is_leader() {
            break;
        }
    }
    assert!(replaced, "the majority never elected a new leader");
    assert!(!c.node(old).is_leader(), "check-quorum never stood it down");
    assert_eq!(c.read_state(old, &read), ReadState::Failed);
}

/// A read on a follower is confirmed by the leader, and its index covers a
/// write the leader acknowledged before the read began, whatever the
/// follower itself has heard about that write.
#[test]
fn a_follower_read_covers_every_write_acknowledged_before_it() {
    let mut c = Cluster::new(3);
    c.run_until("a leader", |c| c.leader().is_some());
    let leader = c.leader().expect("leader");
    let follower = c.ids.iter().copied().find(|&id| id != leader).expect("one");

    for round in 0..10u8 {
        let index = c.propose(leader, &[b'w', round]);
        c.run_until("the leader to commit", |c| c.committed_on(leader) >= index);

        let read = c.read_index(follower);
        c.run_until("the read to be answered", |c| {
            c.read_state(follower, &read) != ReadState::Pending
        });
        match c.read_state(follower, &read) {
            ReadState::Ready(at) => assert!(
                at >= index,
                "round {round}: a read after write {index} was told to wait only for {at}"
            ),
            other => panic!("round {round}: the read was not confirmed: {other:?}"),
        }
    }
}
