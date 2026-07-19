//! A minimal RESP (Redis Serialization Protocol) front end.
//!
//! Exposes the store over TCP speaking RESP2, so standard Redis clients
//! (`redis-cli`, client libraries) can talk to it:
//!
//! ```text
//! $ redis-cli -p 6379
//! 127.0.0.1:6379> SET name "Jane"
//! OK
//! 127.0.0.1:6379> GET name
//! "Jane"
//! 127.0.0.1:6379> KEYS user:*
//! ```
//!
//! Supported commands: `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`,
//! `KEYS <prefix>*` (trailing-star globs only), and `QUIT`. One thread per
//! connection; all state lives in a [`SharedStorage`].

mod metrics;
pub use metrics::MetricsServer;

use crate::storage::SharedStorage;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

/// Maximum accepted bulk-string / array sizes, to bound memory per request.
const MAX_BULK_LEN: i64 = 64 * 1024 * 1024; // 64MB
const MAX_ARRAY_LEN: i64 = 1024 * 1024;

/// A running RESP server. Dropping the handle stops the accept loop and
/// waits for it to exit; connections already being served finish their
/// current command.
pub struct RespServer {
    stop: Arc<AtomicBool>,
    local_addr: std::net::SocketAddr,
    accept_thread: Option<JoinHandle<()>>,
}

impl RespServer {
    /// Start serving `storage` on `listener` in background threads.
    pub fn spawn(storage: SharedStorage, listener: TcpListener) -> io::Result<Self> {
        let local_addr = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));

        let accept_stop = Arc::clone(&stop);
        let accept_thread = thread::spawn(move || {
            for stream in listener.incoming() {
                if accept_stop.load(Ordering::Relaxed) {
                    break;
                }
                match stream {
                    Ok(stream) => {
                        let storage = storage.clone();
                        thread::spawn(move || {
                            let _ = handle_connection(stream, storage);
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(RespServer {
            stop,
            local_addr,
            accept_thread: Some(accept_thread),
        })
    }

    /// The address the server is listening on (useful with port 0).
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
    }

    /// Block the current thread until the server stops (runs forever unless
    /// another thread drops/stops the handle). Used by the CLI.
    pub fn join(mut self) {
        if let Some(thread) = self.accept_thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for RespServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // Wake the blocking accept() with a throwaway connection
        let _ = TcpStream::connect(self.local_addr);
        if let Some(thread) = self.accept_thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle_connection(stream: TcpStream, storage: SharedStorage) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);

    loop {
        let Some(command) = read_command(&mut reader)? else {
            return Ok(()); // clean disconnect
        };
        if command.is_empty() {
            write_error(&mut writer, "empty command")?;
            continue;
        }

        let name = String::from_utf8_lossy(&command[0]).to_ascii_uppercase();
        match name.as_str() {
            "PING" => match command.len() {
                1 => write_simple(&mut writer, "PONG")?,
                2 => write_bulk(&mut writer, Some(&command[1]))?,
                _ => write_wrong_args(&mut writer, "ping")?,
            },
            "ECHO" => match command.len() {
                2 => write_bulk(&mut writer, Some(&command[1]))?,
                _ => write_wrong_args(&mut writer, "echo")?,
            },
            "SET" => match command.len() {
                3 => match storage.put(command[1].clone(), command[2].clone()) {
                    Ok(()) => write_simple(&mut writer, "OK")?,
                    Err(e) => write_error(&mut writer, &e.to_string())?,
                },
                _ => write_wrong_args(&mut writer, "set")?,
            },
            "GET" => match command.len() {
                2 => match storage.get(&command[1]) {
                    Ok(value) => write_bulk(&mut writer, value.as_deref())?,
                    Err(e) => write_error(&mut writer, &e.to_string())?,
                },
                _ => write_wrong_args(&mut writer, "get")?,
            },
            "DEL" => {
                if command.len() < 2 {
                    write_wrong_args(&mut writer, "del")?;
                } else {
                    let mut removed = 0i64;
                    let mut failed = None;
                    for key in &command[1..] {
                        match storage.get(key) {
                            Ok(Some(_)) => match storage.delete(key) {
                                Ok(()) => removed += 1,
                                Err(e) => {
                                    failed = Some(e);
                                    break;
                                }
                            },
                            Ok(None) => {}
                            Err(e) => {
                                failed = Some(e);
                                break;
                            }
                        }
                    }
                    match failed {
                        Some(e) => write_error(&mut writer, &e.to_string())?,
                        None => write_integer(&mut writer, removed)?,
                    }
                }
            }
            "EXISTS" => {
                if command.len() < 2 {
                    write_wrong_args(&mut writer, "exists")?;
                } else {
                    let mut found = 0i64;
                    let mut failed = None;
                    for key in &command[1..] {
                        match storage.get(key) {
                            Ok(Some(_)) => found += 1,
                            Ok(None) => {}
                            Err(e) => {
                                failed = Some(e);
                                break;
                            }
                        }
                    }
                    match failed {
                        Some(e) => write_error(&mut writer, &e.to_string())?,
                        None => write_integer(&mut writer, found)?,
                    }
                }
            }
            "KEYS" => match command.len() {
                2 => match parse_keys_pattern(&command[1]) {
                    KeysPattern::Prefix(prefix) => match storage.scan_prefix(&prefix) {
                        Ok(entries) => {
                            let keys: Vec<_> = entries.into_iter().map(|(k, _)| k).collect();
                            write_array_of_bulk(&mut writer, &keys)?;
                        }
                        Err(e) => write_error(&mut writer, &e.to_string())?,
                    },
                    KeysPattern::Exact(key) => match storage.get(&key) {
                        Ok(Some(_)) => write_array_of_bulk(&mut writer, &[key])?,
                        Ok(None) => write_array_of_bulk(&mut writer, &[])?,
                        Err(e) => write_error(&mut writer, &e.to_string())?,
                    },
                    KeysPattern::Unsupported => write_error(
                        &mut writer,
                        "only 'prefix*' or exact patterns are supported",
                    )?,
                },
                _ => write_wrong_args(&mut writer, "keys")?,
            },
            // redis-cli sends COMMAND DOCS on connect; an empty reply keeps
            // it happy without implementing introspection
            "COMMAND" => write_array_of_bulk(&mut writer, &[])?,
            "QUIT" => {
                write_simple(&mut writer, "OK")?;
                writer.flush()?;
                return Ok(());
            }
            _ => write_error(&mut writer, &format!("unknown command '{}'", name))?,
        }
        writer.flush()?;
    }
}

/// How a KEYS pattern is interpreted: `prefix*` scans by prefix, a pattern
/// without any glob characters matches exactly one key. Other glob forms
/// are not supported.
enum KeysPattern {
    Prefix(Vec<u8>),
    Exact(Vec<u8>),
    Unsupported,
}

fn parse_keys_pattern(pattern: &[u8]) -> KeysPattern {
    let has_glob = |s: &[u8]| s.iter().any(|b| matches!(b, b'*' | b'?' | b'[' | b']'));
    match pattern {
        [head @ .., b'*'] if !has_glob(head) => KeysPattern::Prefix(head.to_vec()),
        _ if !has_glob(pattern) => KeysPattern::Exact(pattern.to_vec()),
        _ => KeysPattern::Unsupported,
    }
}

/// Read one RESP command. Supports the array-of-bulk-strings form used by
/// all Redis clients, plus space-separated inline commands for telnet use.
/// Returns `None` on a clean EOF between commands.
fn read_command(reader: &mut impl BufRead) -> io::Result<Option<Vec<Vec<u8>>>> {
    let Some(line) = read_line(reader)? else {
        return Ok(None);
    };
    if line.is_empty() {
        return Ok(Some(Vec::new()));
    }

    if line[0] != b'*' {
        // Inline command: split on spaces
        return Ok(Some(
            line.split(|&b| b == b' ')
                .filter(|part| !part.is_empty())
                .map(|part| part.to_vec())
                .collect(),
        ));
    }

    let count = parse_int(&line[1..])?;
    if !(0..=MAX_ARRAY_LEN).contains(&count) {
        return Err(protocol_error("array length out of range"));
    }

    let mut parts = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let Some(header) = read_line(reader)? else {
            return Err(protocol_error("unexpected EOF inside command"));
        };
        if header.first() != Some(&b'$') {
            return Err(protocol_error("expected bulk string"));
        }
        let len = parse_int(&header[1..])?;
        if !(0..=MAX_BULK_LEN).contains(&len) {
            return Err(protocol_error("bulk length out of range"));
        }
        let mut buf = vec![0u8; len as usize + 2]; // payload + CRLF
        reader.read_exact(&mut buf)?;
        buf.truncate(len as usize);
        parts.push(buf);
    }
    Ok(Some(parts))
}

/// Read a CRLF-terminated line (without the terminator). `None` on EOF
/// before any bytes were read.
fn read_line(reader: &mut impl BufRead) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    let n = reader.read_until(b'\n', &mut line)?;
    if n == 0 {
        return Ok(None);
    }
    while matches!(line.last(), Some(b'\n') | Some(b'\r')) {
        line.pop();
    }
    Ok(Some(line))
}

fn parse_int(bytes: &[u8]) -> io::Result<i64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| protocol_error("invalid integer"))
}

fn protocol_error(msg: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("RESP protocol: {}", msg),
    )
}

fn write_simple(w: &mut impl Write, s: &str) -> io::Result<()> {
    write!(w, "+{}\r\n", s)
}

fn write_error(w: &mut impl Write, msg: &str) -> io::Result<()> {
    // RESP errors are single-line
    write!(w, "-ERR {}\r\n", msg.replace(['\r', '\n'], " "))
}

fn write_wrong_args(w: &mut impl Write, cmd: &str) -> io::Result<()> {
    write!(
        w,
        "-ERR wrong number of arguments for '{}' command\r\n",
        cmd
    )
}

fn write_integer(w: &mut impl Write, n: i64) -> io::Result<()> {
    write!(w, ":{}\r\n", n)
}

fn write_bulk(w: &mut impl Write, data: Option<&[u8]>) -> io::Result<()> {
    match data {
        Some(data) => {
            write!(w, "${}\r\n", data.len())?;
            w.write_all(data)?;
            w.write_all(b"\r\n")
        }
        None => w.write_all(b"$-1\r\n"), // nil
    }
}

fn write_array_of_bulk(w: &mut impl Write, items: &[Vec<u8>]) -> io::Result<()> {
    write!(w, "*{}\r\n", items.len())?;
    for item in items {
        write_bulk(w, Some(item))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_keys_pattern() {
        assert!(matches!(
            parse_keys_pattern(b"user:*"),
            KeysPattern::Prefix(p) if p == b"user:"
        ));
        assert!(matches!(
            parse_keys_pattern(b"*"),
            KeysPattern::Prefix(p) if p.is_empty()
        ));
        assert!(matches!(
            parse_keys_pattern(b"exact"),
            KeysPattern::Exact(k) if k == b"exact"
        ));
        assert!(matches!(
            parse_keys_pattern(b"a*b"),
            KeysPattern::Unsupported
        ));
        assert!(matches!(
            parse_keys_pattern(b"a?c"),
            KeysPattern::Unsupported
        ));
    }
}
