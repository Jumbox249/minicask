//! `minicask-cluster`: one node of a replicated store.
//!
//! Three of these make a cluster that survives any one of them dying.
//!
//! ```console
//! $ minicask-cluster --id 1 --dir ./n1 --raft 127.0.0.1:7001 --client 127.0.0.1:6001 \
//!       --peer 2@127.0.0.1:7002,127.0.0.1:6002 \
//!       --peer 3@127.0.0.1:7003,127.0.0.1:6003
//! ```

use minicask::raft::Config;
use minicask::{ClusterConfig, ClusterNode, Peer};
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

const USAGE: &str = "\
minicask-cluster - one node of a replicated key/value store

USAGE:
    minicask-cluster --id N --dir PATH --raft ADDR --client ADDR [--peer SPEC]...

OPTIONS:
    --id N              This node's id, unique in the cluster
    --dir PATH          Directory for this node's data and consensus log
    --raft ADDR         Address to listen on for the other nodes
    --client ADDR       Address to listen on for Redis clients
    --peer SPEC         A peer, as ID@RAFT_ADDR,CLIENT_ADDR; repeat per peer
    --tick-ms N         Milliseconds per consensus tick (default: 50)
    --snapshot-every N  Applied entries between snapshots, 0 for never
                        (default: 10000)
    -h, --help          Print this message

A cluster of three tolerates one node being down; a cluster of five
tolerates two. An even number buys nothing over the odd number below it.
";

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mut id = None;
    let mut dir = None;
    let mut raft_addr = None;
    let mut client_addr = None;
    let mut peers = Vec::new();
    let mut tick_ms = 50u64;
    let mut snapshot_every = minicask::DEFAULT_SNAPSHOT_EVERY;

    while let Some(arg) = args.next() {
        let mut value = |what: &str| match args.next() {
            Some(v) => Ok(v),
            None => Err(format!("{what} needs a value")),
        };
        let result = match arg.as_str() {
            "--id" => value("--id").and_then(|v| {
                v.parse::<u64>()
                    .map(|n| id = Some(n))
                    .map_err(|_| "--id must be a number".to_string())
            }),
            "--dir" => value("--dir").map(|v| dir = Some(PathBuf::from(v))),
            "--raft" => value("--raft").map(|v| raft_addr = Some(v)),
            "--client" => value("--client").map(|v| client_addr = Some(v)),
            "--tick-ms" => value("--tick-ms").and_then(|v| {
                v.parse::<u64>()
                    .map(|n| tick_ms = n)
                    .map_err(|_| "--tick-ms must be a number".to_string())
            }),
            "--snapshot-every" => value("--snapshot-every").and_then(|v| {
                v.parse::<u64>()
                    .map(|n| snapshot_every = n)
                    .map_err(|_| "--snapshot-every must be a number".to_string())
            }),
            "--peer" => value("--peer").and_then(|v| parse_peer(&v).map(|p| peers.push(p))),
            "-h" | "--help" => {
                print!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            other => Err(format!("unknown option: {other}")),
        };
        if let Err(message) = result {
            return fail(&message);
        }
    }

    let (Some(id), Some(dir), Some(raft_addr), Some(client_addr)) =
        (id, dir, raft_addr, client_addr)
    else {
        eprintln!("minicask-cluster: --id, --dir, --raft and --client are all required");
        print!("{USAGE}");
        return ExitCode::from(2);
    };

    if peers.iter().any(|p: &Peer| p.id == id) {
        return fail("a node cannot list itself as a peer");
    }

    let config = ClusterConfig {
        id,
        peers,
        tick_ms,
        raft: Config::default(),
        snapshot_every,
    };

    let node = match ClusterNode::bind(
        &dir.join("raft"),
        &dir.join("data"),
        &raft_addr,
        &client_addr,
        config,
    ) {
        Ok(node) => node,
        Err(err) => return fail(&format!("cannot start: {err}")),
    };

    // Announce both real addresses, so a caller that asked for port 0 can
    // find out what it got.
    match (node.raft_addr(), node.client_addr()) {
        (Ok(raft), Ok(client)) => println!("node {id} listening raft={raft} client={client}"),
        (Err(e), _) | (_, Err(e)) => return fail(&format!("cannot read listening address: {e}")),
    }
    let _ = std::io::stdout().flush();

    match node.run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => fail(&format!("accept failed: {err}")),
    }
}

/// `ID@RAFT_ADDR,CLIENT_ADDR`
fn parse_peer(spec: &str) -> Result<Peer, String> {
    let bad = || format!("--peer wants ID@RAFT_ADDR,CLIENT_ADDR, got {spec:?}");
    let (id, addrs) = spec.split_once('@').ok_or_else(bad)?;
    let (raft_addr, client_addr) = addrs.split_once(',').ok_or_else(bad)?;
    if raft_addr.is_empty() || client_addr.is_empty() {
        return Err(bad());
    }
    Ok(Peer {
        id: id.parse().map_err(|_| bad())?,
        raft_addr: raft_addr.to_string(),
        client_addr: client_addr.to_string(),
    })
}

fn fail(message: &str) -> ExitCode {
    eprintln!("minicask-cluster: {message}");
    ExitCode::from(2)
}
