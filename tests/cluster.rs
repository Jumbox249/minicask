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
use std::time::{Duration, Instant};

/// Elections take 10 to 20 ticks, so a short tick keeps these quick.
const TICK_MS: u64 = 20;
const PATIENCE: Duration = Duration::from_secs(20);

/// Ask the OS for a free port and immediately hand it back. There is a
/// window before the node binds it, which nothing else here is racing for.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read the port")
        .port()
}

struct Node {
    id: u64,
    child: Option<Child>,
    raft_addr: String,
    client_addr: String,
    dir: String,
}

struct Cluster {
    nodes: Vec<Node>,
    _root: TempDir,
}

impl Cluster {
    fn start(size: u64) -> Cluster {
        let root = TempDir::new("cluster");
        let mut nodes: Vec<Node> = (1..=size)
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
            })
            .collect();

        for i in 0..nodes.len() {
            spawn(&mut nodes, i);
        }
        Cluster { nodes, _root: root }
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
        .args(["--tick-ms", &TICK_MS.to_string()]);
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
    BufReader::new(child.stdout.take().expect("stdout is piped"))
        .read_line(&mut line)
        .expect("read the listening line");
    assert!(
        line.contains("listening"),
        "unexpected first line from node {id}: {line:?}"
    );

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
fn a_follower_redirects_to_the_leader() {
    let c = Cluster::start(3);
    let leader = c.await_leader();
    let follower = c.ids().into_iter().find(|&id| id != leader).expect("one");
    let leader_addr = c.client_addr(leader).to_string();

    // Redis clients already know how to read `MOVED`.
    for command in [
        [b"SET".as_slice(), b"k", b"v"].as_slice(),
        [b"GET".as_slice(), b"k"].as_slice(),
    ] {
        match c.command(follower, command) {
            Reply::Error(e) => assert_eq!(e, format!("MOVED 0 {leader_addr}")),
            other => panic!("expected a redirect from the follower, got {other:?}"),
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
