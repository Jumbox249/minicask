//! A thin command line wrapper so the store can be poked at without writing
//! any Rust.

use minicask::{Options, Store, SyncPolicy};
use std::process::ExitCode;

const USAGE: &str = "\
minicask - an append-only key/value store

USAGE:
    minicask [--dir PATH] [--no-fsync] <COMMAND> [ARGS]

COMMANDS:
    put <key> <value>   Store a value
    get <key>           Print a value, or exit 1 if the key is missing
    del <key>           Delete a key
    keys                List every live key
    stats               Show key count, disk usage and fragmentation
    compact             Rewrite live records and drop the rest

OPTIONS:
    --dir PATH          Store directory (default: ./minicask-data)
    --no-fsync          Trade power-cut durability for speed
    -h, --help          Print this message
";

fn main() -> ExitCode {
    match run() {
        Ok(code) => code,
        Err(err) => {
            eprintln!("minicask: {err}");
            ExitCode::from(2)
        }
    }
}

fn run() -> minicask::Result<ExitCode> {
    let mut args = std::env::args().skip(1).peekable();
    let mut dir = String::from("./minicask-data");
    let mut opts = Options::default();

    while let Some(arg) = args.peek() {
        match arg.as_str() {
            "--dir" => {
                args.next();
                match args.next() {
                    Some(path) => dir = path,
                    None => return fail("--dir needs a path"),
                }
            }
            "--no-fsync" => {
                args.next();
                opts.sync = SyncPolicy::OsCache;
            }
            "-h" | "--help" => {
                print!("{USAGE}");
                return Ok(ExitCode::SUCCESS);
            }
            _ => break,
        }
    }

    let Some(command) = args.next() else {
        print!("{USAGE}");
        return Ok(ExitCode::from(2));
    };

    let mut store = Store::open_with(&dir, opts)?;
    match command.as_str() {
        "put" => match (args.next(), args.next()) {
            (Some(key), Some(value)) => {
                store.put(key.as_bytes(), value.as_bytes())?;
                store.sync()?;
                Ok(ExitCode::SUCCESS)
            }
            _ => fail("put needs a key and a value"),
        },
        "get" => match args.next() {
            Some(key) => match store.get(key.as_bytes())? {
                Some(value) => {
                    println!("{}", String::from_utf8_lossy(&value));
                    Ok(ExitCode::SUCCESS)
                }
                None => {
                    eprintln!("minicask: no such key: {key}");
                    Ok(ExitCode::FAILURE)
                }
            },
            None => fail("get needs a key"),
        },
        "del" => match args.next() {
            Some(key) => {
                let existed = store.delete(key.as_bytes())?;
                store.sync()?;
                if existed {
                    Ok(ExitCode::SUCCESS)
                } else {
                    eprintln!("minicask: no such key: {key}");
                    Ok(ExitCode::FAILURE)
                }
            }
            None => fail("del needs a key"),
        },
        "keys" => {
            let mut keys: Vec<String> = store
                .keys()
                .map(|k| String::from_utf8_lossy(k).into_owned())
                .collect();
            keys.sort();
            for key in keys {
                println!("{key}");
            }
            Ok(ExitCode::SUCCESS)
        }
        "stats" => {
            let stats = store.stats();
            println!("keys         {}", stats.keys);
            println!("data files   {}", stats.files);
            println!("live bytes   {}", stats.live_bytes);
            println!("disk bytes   {}", stats.disk_bytes);
            println!(
                "reclaimable  {} ({:.1}%)",
                stats.reclaimable_bytes(),
                stats.fragmentation() * 100.0
            );
            Ok(ExitCode::SUCCESS)
        }
        "compact" => {
            let report = store.compact()?;
            println!(
                "merged {} file(s), {} bytes -> {} bytes, reclaimed {}",
                report.files_before,
                report.bytes_before,
                report.bytes_after,
                report.reclaimed_bytes()
            );
            Ok(ExitCode::SUCCESS)
        }
        other => {
            eprintln!("minicask: unknown command: {other}");
            print!("{USAGE}");
            Ok(ExitCode::from(2))
        }
    }
}

fn fail(message: &str) -> minicask::Result<ExitCode> {
    eprintln!("minicask: {message}");
    Ok(ExitCode::from(2))
}
