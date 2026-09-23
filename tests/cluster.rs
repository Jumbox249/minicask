//! Three real `minicask-cluster` processes, talked to over TCP.
//!
//! The deterministic tests in `tests/replicated.rs` prove the consensus
//! logic. These prove the thing actually runs: that three processes find
//! each other, elect a leader, replicate a write to disk on all of them,
//! survive their leader being killed, and let it back in afterwards.
//!
//! Real processes mean real timing, so everything here polls against a
//! deadline rather than asserting on the first look.

mod common;

use common::TempDir;
use minicask::resp::{self, Reply};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Elections take 10 to 20 ticks, so a short tick keeps these quick.
const TICK_MS: u64 = 20;
const PATIENCE: Duration = Duration::from_secs(20);

/// A port nothing in this test process has been given before.
///
/// Asking the OS for port 0 and letting it go is not enough on its own: the
/// port goes back in the pool the moment it is released, and the next ask,
/// from this test or one running beside it, can get the same one. Two
/// nodes handed the same port means one of them cannot bind, and the
/// cluster never forms. So every port handed out is remembered, and a
/// repeat is asked for again. Another process on the machine could still
/// take one in the moment before the node binds it, but nothing in here
/// can.
fn free_port() -> u16 {
    static GIVEN: Mutex<Vec<u16>> = Mutex::new(Vec::new());
    let mut given = GIVEN.lock().unwrap_or_else(|e| e.into_inner());
    loop {
        let port = TcpListener::bind("127.0.0.1:0")
            .expect("bind an ephemeral port")
            .local_addr()
            .expect("read the port")
            .port();
        if !given.contains(&port) {
            given.push(port);
            return port;
        }
    }
}

struct Node {
    id: u64,
    child: Option<Child>,
    raft_addr: String,
    client_addr: String,
    dir: String,
    /// Extra command-line flags, the same for every node.
    extra: Vec<String>,
}

struct Cluster {
    nodes: Vec<Node>,
    _root: TempDir,
}

impl Cluster {
    fn start(size: u64) -> Cluster {
        Cluster::start_with(size, &[])
    }

    fn start_with(size: u64, extra: &[&str]) -> Cluster {
        let root = TempDir::new("cluster");
        let nodes: Vec<Node> = (1..=size)
            .map(|id| Node {
                id,
                child: None,
                raft_addr: format!("127.0.0.1:{}", free_port()),
                client_addr: format!("127.0.0.1:{}", free_port()),
                dir: root
                    .path()
                    .join(format!("node-{id}"))
                    .to_str()
                    .expect("utf-8 path")
                    .to_string(),
                extra: extra.iter().map(|s| s.to_string()).collect(),
            })
            .collect();

        // Built before anything is started, so that if a node fails to
        // come up the ones already running are killed by `Drop` as the
        // panic unwinds, instead of outliving the test.
        let mut cluster = Cluster { nodes, _root: root };
        for i in 0..cluster.nodes.len() {
            spawn(&mut cluster.nodes, i);
        }
        cluster
    }

    fn client_addr(&self, id: u64) -> &str {
        &self.node(id).client_addr
    }

    fn node(&self, id: u64) -> &Node {
        self.nodes.iter().find(|n| n.id == id).expect("known node")
    }

    fn ids(&self) -> Vec<u64> {
        self.nodes.iter().map(|n| n.id).collect()
    }

    fn running(&self) -> Vec<u64> {
        self.nodes
            .iter()
            .filter(|n| n.child.is_some())
            .map(|n| n.id)
            .collect()
    }

    /// One field of a node's `RAFT` reply, or `None` if it cannot be
    /// reached at all.
    fn field(&self, id: u64, name: &str) -> Option<String> {
        let reply = self.try_command(id, &[b"RAFT"])?;
        let Reply::Bulk(bytes) = reply else {
            return None;
        };
        String::from_utf8_lossy(&bytes)
            .lines()
            .find_map(|line| line.strip_prefix(&format!("{name}:")).map(str::to_string))
    }

    fn try_command(&self, id: u64, args: &[&[u8]]) -> Option<Reply> {
        let stream = TcpStream::connect(self.client_addr(id)).ok()?;
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .ok()?;
        let mut client = Client {
            reader: BufReader::new(stream.try_clone().ok()?),
            stream,
        };
        Some(client.cmd(args))
    }

    fn command(&self, id: u64, args: &[&[u8]]) -> Reply {
        self.try_command(id, args)
            .unwrap_or_else(|| panic!("could not reach node {id}"))
    }

    /// The single node that says it is leading, once exactly one does.
    fn leader(&self) -> Option<u64> {
        let leaders: Vec<u64> = self
            .running()
            .into_iter()
            .filter(|&id| self.field(id, "role").as_deref() == Some("leader"))
            .collect();
        match leaders.as_slice() {
            [only] => Some(*only),
            _ => None,
        }
    }

    fn await_leader(&self) -> u64 {
        self.poll("a leader", || self.leader())
    }

    /// Poll until `f` produces something, or give up loudly.
    fn poll<T>(&self, what: &str, mut f: impl FnMut() -> Option<T>) -> T {
        let deadline = Instant::now() + PATIENCE;
        loop {
            if let Some(value) = f() {
                return value;
            }
            if Instant::now() > deadline {
                panic!("gave up after {PATIENCE:?} waiting for {what}");
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn kill(&mut self, id: u64) {
        let node = self.nodes.iter_mut().find(|n| n.id == id).expect("known");
        if let Some(mut child) = node.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    fn restart(&mut self, id: u64) {
        self.kill(id);
        let index = self.nodes.iter().position(|n| n.id == id).expect("known");
        spawn(&mut self.nodes, index);
    }

    /// Start a new node that joins the running cluster rather than
    /// starting one of its own. It knows where the others are, from its
    /// flags; they learn where it is when it is added.
    fn join(&mut self, id: u64) {
        let dir = self.nodes[0].dir.replace("node-1", &format!("node-{id}"));
        let mut extra = self.nodes[0].extra.clone();
        extra.push("--join".to_string());
        self.nodes.push(Node {
            id,
            child: None,
            raft_addr: format!("127.0.0.1:{}", free_port()),
            client_addr: format!("127.0.0.1:{}", free_port()),
            dir,
            extra,
        });
        let index = self.nodes.len() - 1;
        spawn(&mut self.nodes, index);
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for node in &mut self.nodes {
            if let Some(mut child) = node.child.take() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

fn spawn(nodes: &mut [Node], index: usize) {
    let me = &nodes[index];
    let (id, dir, raft_addr, client_addr) = (
        me.id,
        me.dir.clone(),
        me.raft_addr.clone(),
        me.client_addr.clone(),
    );

    let mut command = Command::new(env!("CARGO_BIN_EXE_minicask-cluster"));
    command
        .args(["--id", &id.to_string()])
        .args(["--dir", &dir])
        .args(["--raft", &raft_addr])
        .args(["--client", &client_addr])
        .args(["--tick-ms", &TICK_MS.to_string()])
        .args(&nodes[index].extra);
    for peer in nodes.iter().filter(|n| n.id != id) {
        command.args([
            "--peer",
            &format!("{}@{},{}", peer.id, peer.raft_addr, peer.client_addr),
        ]);
    }

    let mut child = command
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn a cluster node");

    // The node announces both addresses once it is listening, so this is
    // also the signal that it is ready to be connected to.
    let mut line = String::new();
    let read = BufReader::new(child.stdout.take().expect("stdout is piped")).read_line(&mut line);
    if read.is_err() || !line.contains("listening") {
        // Not tracked by the cluster yet, so nothing else would kill it.
        let _ = child.kill();
        let _ = child.wait();
        panic!("node {id} did not start; its first line was {line:?}");
    }

    nodes[index].child = Some(child);
}

struct Client {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Client {
    fn cmd(&mut self, args: &[&[u8]]) -> Reply {
        Reply::Array(args.iter().map(|a| Reply::Bulk(a.to_vec())).collect())
            .write_to(&mut self.stream)
            .expect("send a command");
        self.stream.flush().expect("flush");
        resp::read_reply(&mut self.reader).expect("read a reply")
    }
}

fn bulk(s: &str) -> Reply {
    Reply::Bulk(s.as_bytes().to_vec())
}

#[test]
fn three_processes_elect_one_leader() {
    let c = Cluster::start(3);
    let leader = c.await_leader();

    // And everyone agrees who it is, in the same term.
    let term = c.field(leader, "term").expect("a term");
    for id in c.ids() {
        c.poll(&format!("node {id} to follow node {leader}"), || {
            (c.field(id, "leader").as_deref() == Some(&leader.to_string())
                && c.field(id, "term").as_deref() == Some(&term))
            .then_some(())
        });
    }
}

#[test]
fn a_write_reaches_every_node_on_disk() {
    let c = Cluster::start(3);
    let leader = c.await_leader();

    assert_eq!(
        c.command(leader, &[b"SET", b"language", b"rust"]),
        Reply::ok()
    );
    assert_eq!(c.command(leader, &[b"GET", b"language"]), bulk("rust"));

    // Every node, leader or not, ends up holding the entry. `keys` comes
    // from each node's own store rather than from the leader.
    for id in c.ids() {
        c.poll(&format!("node {id} to hold the write"), || {
            (c.field(id, "keys").as_deref() == Some("1")).then_some(())
        });
    }
}

#[test]
fn a_follower_redirects_writes_to_the_leader() {
    let c = Cluster::start(3);
    let leader = c.await_leader();
    let follower = c.ids().into_iter().find(|&id| id != leader).expect("one");
    let leader_addr = c.client_addr(leader).to_string();

    // Redis clients already know how to read `MOVED`.
    match c.command(follower, &[b"SET", b"k", b"v"]) {
        Reply::Error(e) => assert_eq!(e, format!("MOVED 0 {leader_addr}")),
        other => panic!("expected a redirect from the follower, got {other:?}"),
    }
}

/// A follower answers reads itself, and a read there sees a write the
/// leader acknowledged a moment earlier, even though nothing told the
/// follower to wait for it. That is the read index at work: the follower
/// asks the leader how far a read has to see, then waits until its own
/// store has applied that far.
#[test]
fn a_follower_serves_reads_that_see_every_acknowledged_write() {
    let c = Cluster::start(3);
    let leader = c.await_leader();
    let followers: Vec<u64> = c.ids().into_iter().filter(|&id| id != leader).collect();

    for round in 0..20 {
        let value = format!("v{round}");
        assert_eq!(
            c.command(leader, &[b"SET", b"k", value.as_bytes()]),
            Reply::ok()
        );
        for &id in &followers {
            assert_eq!(
                c.command(id, &[b"GET", b"k"]),
                bulk(&value),
                "node {id} served a stale read in round {round}"
            );
        }
    }
}

#[test]
fn the_cluster_survives_its_leader_being_killed() {
    let mut c = Cluster::start(3);
    let first = c.await_leader();

    assert_eq!(c.command(first, &[b"SET", b"before", b"yes"]), Reply::ok());
    c.kill(first);

    let second = c.poll("a replacement leader", || {
        c.leader().filter(|&id| id != first)
    });

    // The committed write survived, and the survivors still accept new
    // ones, because two of three is a majority.
    assert_eq!(c.command(second, &[b"GET", b"before"]), bulk("yes"));
    assert_eq!(c.command(second, &[b"SET", b"after", b"yes"]), Reply::ok());
    assert_eq!(c.command(second, &[b"DBSIZE"]), Reply::Integer(2));
}

#[test]
fn a_restarted_node_catches_up_on_what_it_missed() {
    let mut c = Cluster::start(3);
    let first = c.await_leader();
    assert_eq!(c.command(first, &[b"SET", b"before", b"yes"]), Reply::ok());

    c.kill(first);
    let second = c.poll("a replacement leader", || {
        c.leader().filter(|&id| id != first)
    });
    // A write the dead node knows nothing about.
    assert_eq!(
        c.command(second, &[b"SET", b"while-away", b"yes"]),
        Reply::ok()
    );

    c.restart(first);
    // It comes back on the state it persisted, learns it is behind, and is
    // caught up by the leader.
    c.poll("the restarted node to catch up", || {
        (c.field(first, "keys").as_deref() == Some("2")
            && c.field(first, "role").as_deref() == Some("follower"))
        .then_some(())
    });
    assert_eq!(c.command(second, &[b"DBSIZE"]), Reply::Integer(2));
}

#[test]
fn a_minority_refuses_to_serve() {
    let mut c = Cluster::start(3);
    let leader = c.await_leader();
    assert_eq!(c.command(leader, &[b"SET", b"k", b"v"]), Reply::ok());

    // Kill two of three. The survivor cannot reach a majority, so it must
    // stop answering rather than serve data it can no longer vouch for.
    let doomed: Vec<u64> = c.ids().into_iter().filter(|&id| id != leader).collect();
    for id in &doomed {
        c.kill(*id);
    }

    c.poll("the survivor to give up leadership", || {
        match c.try_command(leader, &[b"GET", b"k"])? {
            Reply::Error(e) if e.starts_with("CLUSTERDOWN") || e.starts_with("MOVED") => Some(()),
            _ => None,
        }
    });

    // And it recovers the moment a majority exists again.
    c.restart(doomed[0]);
    let leader = c.await_leader();
    assert_eq!(c.command(leader, &[b"GET", b"k"]), bulk("v"));
}

/// Snapshots across real processes: a node down long enough that the
/// others compact past everything it holds comes back, is sent the state
/// over TCP, and does not resurrect a key deleted while it was away.
#[test]
fn a_node_down_past_the_compacted_log_is_caught_up_by_snapshot() {
    let mut c = Cluster::start_with(3, &["--snapshot-every", "5"]);
    let leader = c.await_leader();
    assert_eq!(c.command(leader, &[b"SET", b"doomed", b"yes"]), Reply::ok());

    let away = c.ids().into_iter().find(|&id| id != leader).expect("one");
    c.poll("the doomed key to reach every node", || {
        (c.field(away, "keys").as_deref() == Some("1")).then_some(())
    });
    c.kill(away);

    assert_eq!(c.command(leader, &[b"DEL", b"doomed"]), Reply::ok());
    for i in 0..30 {
        let key = format!("k{i}");
        assert_eq!(
            c.command(leader, &[b"SET", key.as_bytes(), b"v"]),
            Reply::ok()
        );
    }
    let compacted: u64 = c
        .field(leader, "snapshot_index")
        .and_then(|v| v.parse().ok())
        .expect("a snapshot index");
    assert!(compacted >= 25, "the leader never compacted: {compacted}");

    c.restart(away);
    c.poll("the returning node to catch up", || {
        (c.field(away, "keys").as_deref() == Some("30")).then_some(())
    });
    let installed: u64 = c
        .field(away, "snapshot_index")
        .and_then(|v| v.parse().ok())
        .expect("a snapshot index");
    assert!(
        installed > 0,
        "it caught up without ever holding a snapshot"
    );

    // Only the leader answers reads, so ask it to confirm the key is gone
    // everywhere by checking every node holds exactly the thirty.
    assert_eq!(
        c.command(leader, &[b"EXISTS", b"doomed"]),
        Reply::Integer(0)
    );
    for id in c.ids() {
        assert_eq!(c.field(id, "keys").as_deref(), Some("30"), "node {id}");
    }
}

/// Membership over real processes: a fourth node joins a running cluster
/// with nothing, is added, catches up and serves reads; then one of the
/// originals is removed and shut down, and the cluster carries on.
#[test]
fn a_node_can_join_a_running_cluster_and_another_can_leave() {
    let mut c = Cluster::start(3);
    let leader = c.await_leader();
    assert_eq!(c.command(leader, &[b"SET", b"before", b"1"]), Reply::ok());

    c.join(4);
    // Until it is added it is nobody's follower and stands for nothing.
    assert_eq!(c.field(4, "members").as_deref(), Some(""));
    let (raft, client) = (c.node(4).raft_addr.clone(), c.node(4).client_addr.clone());
    assert_eq!(
        c.command(
            leader,
            &[b"RAFT.ADD", b"4", raft.as_bytes(), client.as_bytes()]
        ),
        Reply::ok()
    );
    c.poll("the new node to catch up", || {
        (c.field(4, "keys").as_deref() == Some("1")
            && c.field(4, "members").as_deref() == Some("1,2,3,4")
            && c.field(4, "voters").as_deref() == Some("1,2,3,4"))
        .then_some(())
    });
    assert_eq!(c.command(4, &[b"GET", b"before"]), bulk("1"));

    let gone = c
        .ids()
        .into_iter()
        .find(|&id| id != leader && id != 4)
        .expect("an original follower");
    assert_eq!(
        c.command(leader, &[b"RAFT.REMOVE", gone.to_string().as_bytes()]),
        Reply::ok()
    );
    c.kill(gone);

    assert_eq!(c.command(leader, &[b"SET", b"after", b"2"]), Reply::ok());
    let expected: Vec<String> = [1, 2, 3, 4]
        .iter()
        .filter(|&&id| id != gone)
        .map(|id| id.to_string())
        .collect();
    let expected = expected.join(",");
    for id in [leader, 4] {
        c.poll(&format!("node {id} to hold both writes"), || {
            (c.field(id, "keys").as_deref() == Some("2")
                && c.field(id, "members").as_deref() == Some(expected.as_str()))
            .then_some(())
        });
    }
}
