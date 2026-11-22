//! Serving a store over the Redis protocol and talking to it over TCP.
//!
//! The same server runs from the command line with
//! `cargo run --release -- serve`, after which `redis-cli` works against it.
//!
//! Run with `cargo run --example resp_server`.

use lsm_rust::{RespServer, SharedStorage};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};

fn read_reply(reader: &mut BufReader<TcpStream>) -> std::io::Result<String> {
    let mut line = String::new();
    reader.read_line(&mut line)?;
    let line = line.trim_end().to_string();
    match line.chars().next() {
        Some('$') if line != "$-1" => {
            let mut payload = String::new();
            reader.read_line(&mut payload)?;
            Ok(format!("\"{}\"", payload.trim_end()))
        }
        Some('$') => Ok("(nil)".to_string()),
        Some('*') => {
            let count: usize = line[1..].parse().unwrap_or(0);
            let items = (0..count)
                .map(|_| read_reply(reader))
                .collect::<std::io::Result<Vec<_>>>()?;
            Ok(format!("[{}]", items.join(", ")))
        }
        _ => Ok(line),
    }
}

fn command(
    stream: &mut TcpStream,
    reader: &mut BufReader<TcpStream>,
    args: &[&str],
) -> std::io::Result<String> {
    let mut request = format!("*{}\r\n", args.len());
    for arg in args {
        request.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    stream.write_all(request.as_bytes())?;
    read_reply(reader)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db = SharedStorage::new(dir.path(), false)?;
    let server = RespServer::spawn(db, TcpListener::bind("127.0.0.1:0")?)?;
    println!("listening on {}", server.local_addr());

    let mut stream = TcpStream::connect(server.local_addr())?;
    let mut reader = BufReader::new(stream.try_clone()?);

    for args in [
        vec!["SET", "user:1", "Jane"],
        vec!["GET", "user:1"],
        vec!["SET", "session", "token", "EX", "60"],
        vec!["TTL", "session"],
        vec!["KEYS", "user:*"],
        vec!["DEL", "user:1"],
        vec!["GET", "user:1"],
    ] {
        let reply = command(&mut stream, &mut reader, &args)?;
        println!("{:<28} -> {reply}", args.join(" "));
    }
    Ok(())
}
