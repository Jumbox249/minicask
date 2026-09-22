//! `minicask-server`: the store behind a Redis-compatible TCP port.

use minicask::{Options, Server, Store, SyncPolicy};
use std::io::Write;
use std::process::ExitCode;

const USAGE: &str = "\
minicask-server - serve a store over the Redis protocol

USAGE:
    minicask-server [--dir PATH] [--bind ADDR] [--port N] [--no-fsync]

OPTIONS:
    --dir PATH          Store directory (default: ./minicask-data)
    --bind ADDR         Interface to listen on (default: 127.0.0.1)
    --port N            Port to listen on (default: 6379, 0 picks a free one)
    --no-fsync          Trade power-cut durability for speed
    -h, --help          Print this message
";

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mut dir = String::from("./minicask-data");
    let mut bind = String::from("127.0.0.1");
    let mut port: u16 = 6379;
    let mut opts = Options::default();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dir" => match args.next() {
                Some(path) => dir = path,
                None => return fail("--dir needs a path"),
            },
            "--bind" => match args.next() {
                Some(addr) => bind = addr,
                None => return fail("--bind needs an address"),
            },
            "--port" => match args.next().and_then(|p| p.parse().ok()) {
                Some(n) => port = n,
                None => return fail("--port needs a number from 0 to 65535"),
            },
            "--no-fsync" => opts.sync = SyncPolicy::OsCache,
            "-h" | "--help" => {
                print!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            other => return fail(&format!("unknown option: {other}")),
        }
    }

    let store = match Store::open_with(&dir, opts) {
        Ok(store) => store,
        Err(err) => return fail(&format!("cannot open {dir}: {err}")),
    };
    let server = match Server::bind((bind.as_str(), port), store) {
        Ok(server) => server,
        Err(err) => return fail(&format!("cannot listen on {bind}:{port}: {err}")),
    };

    // Announce the real address so a caller that asked for port 0 can find
    // out what it got. Flushed, because there may not be a terminal.
    match server.local_addr() {
        Ok(addr) => println!("listening on {addr}"),
        Err(err) => return fail(&format!("cannot read listening address: {err}")),
    }
    let _ = std::io::stdout().flush();

    match server.run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => fail(&format!("accept failed: {err}")),
    }
}

fn fail(message: &str) -> ExitCode {
    eprintln!("minicask-server: {message}");
    ExitCode::from(2)
}
