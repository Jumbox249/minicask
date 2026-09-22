//! The server as a client sees it: a real `minicask-server` process, spoken
//! to over TCP in the Redis protocol.

mod common;

use common::TempDir;
use minicask::resp::{self, Reply};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::process::{Child, Command, Stdio};

/// A running server process. Killed when dropped.
struct ServerProcess {
    child: Child,
    addr: String,
}

impl ServerProcess {
    fn start(dir: &TempDir) -> ServerProcess {
        let mut child = Command::new(env!("CARGO_BIN_EXE_minicask-server"))
            .args(["--dir", dir.str(), "--port", "0"])
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn the server");

        // The server announces its address as its first line of output.
        let mut line = String::new();
        BufReader::new(child.stdout.take().expect("stdout is piped"))
            .read_line(&mut line)
            .expect("read the listening line");
        let addr = line
            .trim()
            .strip_prefix("listening on ")
            .unwrap_or_else(|| panic!("unexpected first line from server: {line:?}"))
            .to_string();

        ServerProcess { child, addr }
    }

    fn connect(&self) -> Client {
        let stream = TcpStream::connect(&self.addr).expect("connect to server");
        Client {
            reader: BufReader::new(stream.try_clone().expect("clone socket")),
            stream,
        }
    }

    fn kill(mut self) {
        self.child.kill().expect("kill server");
        self.child.wait().expect("reap server");
        // Skip the drop-time kill; the process is already gone.
        std::mem::forget(self);
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Client {
    stream: TcpStream,
    reader: BufReader<TcpStream>,
}

impl Client {
    fn send(&mut self, args: &[&[u8]]) {
        let command = Reply::Array(args.iter().map(|a| Reply::Bulk(a.to_vec())).collect());
        command.write_to(&mut self.stream).expect("send command");
    }

    fn recv(&mut self) -> Reply {
        resp::read_reply(&mut self.reader).expect("read reply")
    }

    fn cmd(&mut self, args: &[&[u8]]) -> Reply {
        self.send(args);
        self.recv()
    }

    fn raw(&mut self, bytes: &[u8]) {
        self.stream.write_all(bytes).expect("write raw bytes");
    }
}

fn bulk(s: &str) -> Reply {
    Reply::Bulk(s.as_bytes().to_vec())
}

fn simple(s: &str) -> Reply {
    Reply::Simple(s.to_string())
}

#[test]
fn ping_and_echo() {
    let dir = TempDir::new("server-ping");
    let server = ServerProcess::start(&dir);
    let mut c = server.connect();

    assert_eq!(c.cmd(&[b"PING"]), simple("PONG"));
    assert_eq!(c.cmd(&[b"ping", b"hi"]), bulk("hi"));
    assert_eq!(c.cmd(&[b"ECHO", b"there"]), bulk("there"));
}

#[test]
fn set_get_del_exists() {
    let dir = TempDir::new("server-crud");
    let server = ServerProcess::start(&dir);
    let mut c = server.connect();

    assert_eq!(c.cmd(&[b"GET", b"k"]), Reply::Null);
    assert_eq!(c.cmd(&[b"EXISTS", b"k"]), Reply::Integer(0));
    assert_eq!(c.cmd(&[b"SET", b"k", b"v1"]), Reply::ok());
    assert_eq!(c.cmd(&[b"GET", b"k"]), bulk("v1"));
    assert_eq!(c.cmd(&[b"SET", b"k", b"v2"]), Reply::ok());
    assert_eq!(c.cmd(&[b"GET", b"k"]), bulk("v2"));
    assert_eq!(
        c.cmd(&[b"EXISTS", b"k", b"missing", b"k"]),
        Reply::Integer(2)
    );
    assert_eq!(c.cmd(&[b"DEL", b"k", b"missing"]), Reply::Integer(1));
    assert_eq!(c.cmd(&[b"GET", b"k"]), Reply::Null);
    assert_eq!(c.cmd(&[b"DEL", b"k"]), Reply::Integer(0));
}

#[test]
fn values_are_binary_safe() {
    let dir = TempDir::new("server-binary");
    let server = ServerProcess::start(&dir);
    let mut c = server.connect();

    let value = b"line\r\nbreak\0nul\xff\xfe";
    assert_eq!(c.cmd(&[b"SET", b"bin", value]), Reply::ok());
    assert_eq!(c.cmd(&[b"GET", b"bin"]), Reply::Bulk(value.to_vec()));

    // An empty value is a value, not a missing key.
    assert_eq!(c.cmd(&[b"SET", b"empty", b""]), Reply::ok());
    assert_eq!(c.cmd(&[b"GET", b"empty"]), Reply::Bulk(Vec::new()));
}

#[test]
fn set_nx_and_xx() {
    let dir = TempDir::new("server-nx-xx");
    let server = ServerProcess::start(&dir);
    let mut c = server.connect();

    assert_eq!(c.cmd(&[b"SET", b"k", b"a", b"XX"]), Reply::Null);
    assert_eq!(c.cmd(&[b"SET", b"k", b"a", b"NX"]), Reply::ok());
    assert_eq!(c.cmd(&[b"SET", b"k", b"b", b"nx"]), Reply::Null);
    assert_eq!(c.cmd(&[b"SET", b"k", b"c", b"XX"]), Reply::ok());
    assert_eq!(c.cmd(&[b"GET", b"k"]), bulk("c"));

    // Expiry is not something the store can promise, so it is refused.
    assert!(matches!(
        c.cmd(&[b"SET", b"k", b"d", b"EX", b"10"]),
        Reply::Error(e) if e.starts_with("ERR syntax")
    ));
    assert_eq!(c.cmd(&[b"GET", b"k"]), bulk("c"));
}

#[test]
fn mget_mset_keys_dbsize_flushdb() {
    let dir = TempDir::new("server-multi");
    let server = ServerProcess::start(&dir);
    let mut c = server.connect();

    assert_eq!(
        c.cmd(&[b"MSET", b"user:1", b"ann", b"user:2", b"bob", b"order:1", b"x"]),
        Reply::ok()
    );
    assert_eq!(c.cmd(&[b"DBSIZE"]), Reply::Integer(3));
    assert_eq!(
        c.cmd(&[b"MGET", b"user:2", b"nope", b"user:1"]),
        Reply::Array(vec![bulk("bob"), Reply::Null, bulk("ann")])
    );
    assert_eq!(
        c.cmd(&[b"KEYS", b"user:*"]),
        Reply::Array(vec![bulk("user:1"), bulk("user:2")])
    );
    assert_eq!(
        c.cmd(&[b"KEYS", b"*"]),
        Reply::Array(vec![bulk("order:1"), bulk("user:1"), bulk("user:2")])
    );
    assert_eq!(c.cmd(&[b"FLUSHDB"]), Reply::ok());
    assert_eq!(c.cmd(&[b"DBSIZE"]), Reply::Integer(0));
    assert_eq!(c.cmd(&[b"KEYS", b"*"]), Reply::Array(vec![]));
}

#[test]
fn errors_look_like_redis_errors() {
    let dir = TempDir::new("server-errors");
    let server = ServerProcess::start(&dir);
    let mut c = server.connect();

    assert_eq!(
        c.cmd(&[b"NOSUCH", b"x"]),
        Reply::Error("ERR unknown command 'nosuch'".to_string())
    );
    assert_eq!(
        c.cmd(&[b"GET"]),
        Reply::Error("ERR wrong number of arguments for 'get' command".to_string())
    );
    assert_eq!(
        c.cmd(&[b"SET", b"only-key"]),
        Reply::Error("ERR wrong number of arguments for 'set' command".to_string())
    );
    // The connection is still usable after an error.
    assert_eq!(c.cmd(&[b"PING"]), simple("PONG"));
}

#[test]
fn pipelined_commands_are_answered_in_order() {
    let dir = TempDir::new("server-pipeline");
    let server = ServerProcess::start(&dir);
    let mut c = server.connect();

    c.send(&[b"SET", b"a", b"1"]);
    c.send(&[b"SET", b"b", b"2"]);
    c.send(&[b"GET", b"a"]);
    c.send(&[b"GET", b"b"]);
    c.send(&[b"DBSIZE"]);

    assert_eq!(c.recv(), Reply::ok());
    assert_eq!(c.recv(), Reply::ok());
    assert_eq!(c.recv(), bulk("1"));
    assert_eq!(c.recv(), bulk("2"));
    assert_eq!(c.recv(), Reply::Integer(2));
}

#[test]
fn inline_commands_work_for_telnet_users() {
    let dir = TempDir::new("server-inline");
    let server = ServerProcess::start(&dir);
    let mut c = server.connect();

    c.raw(b"SET greeting hello\r\n");
    assert_eq!(c.recv(), Reply::ok());
    c.raw(b"GET greeting\n");
    assert_eq!(c.recv(), bulk("hello"));
    c.raw(b"\r\nPING\r\n");
    assert_eq!(c.recv(), simple("PONG"));
}

#[test]
fn a_protocol_error_ends_only_that_connection() {
    let dir = TempDir::new("server-proto-error");
    let server = ServerProcess::start(&dir);
    let mut bad = server.connect();

    bad.raw(b"*1\r\n:notabulk\r\n");
    assert!(matches!(
        bad.recv(),
        Reply::Error(e) if e.starts_with("ERR Protocol error")
    ));
    assert!(
        resp::read_reply(&mut bad.reader).is_err(),
        "the server should have hung up"
    );

    let mut good = server.connect();
    assert_eq!(good.cmd(&[b"PING"]), simple("PONG"));
}

#[test]
fn concurrent_clients_do_not_lose_writes() {
    let dir = TempDir::new("server-concurrent");
    let server = ServerProcess::start(&dir);
    const CLIENTS: usize = 8;
    const WRITES: usize = 50;

    let handles: Vec<_> = (0..CLIENTS)
        .map(|i| {
            let mut c = server.connect();
            std::thread::spawn(move || {
                for j in 0..WRITES {
                    let key = format!("c{i}:{j}");
                    let value = format!("{i}-{j}");
                    assert_eq!(
                        c.cmd(&[b"SET", key.as_bytes(), value.as_bytes()]),
                        Reply::ok()
                    );
                }
            })
        })
        .collect();
    for handle in handles {
        handle.join().expect("client thread");
    }

    let mut c = server.connect();
    assert_eq!(
        c.cmd(&[b"DBSIZE"]),
        Reply::Integer((CLIENTS * WRITES) as i64)
    );
    assert_eq!(c.cmd(&[b"GET", b"c3:17"]), bulk("3-17"));
}

#[test]
fn data_survives_a_server_restart() {
    let dir = TempDir::new("server-restart");
    let server = ServerProcess::start(&dir);
    let mut c = server.connect();
    assert_eq!(c.cmd(&[b"SET", b"durable", b"yes"]), Reply::ok());
    assert_eq!(c.cmd(&[b"SET", b"gone", b"soon"]), Reply::ok());
    assert_eq!(c.cmd(&[b"DEL", b"gone"]), Reply::Integer(1));
    drop(c);
    server.kill();

    let server = ServerProcess::start(&dir);
    let mut c = server.connect();
    assert_eq!(c.cmd(&[b"GET", b"durable"]), bulk("yes"));
    assert_eq!(c.cmd(&[b"GET", b"gone"]), Reply::Null);
    assert_eq!(c.cmd(&[b"DBSIZE"]), Reply::Integer(1));
}
