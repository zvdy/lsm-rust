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
use std::time::Duration;

/// Maximum accepted bulk-string / array sizes, to bound memory per request.
const MAX_BULK_LEN: i64 = 64 * 1024 * 1024; // 64MB
const MAX_ARRAY_LEN: i64 = 1024 * 1024;

/// Cap on the summed payload of one command.
///
/// The limits above bound each element on its own, which leaves the total
/// unbounded: an array may declare a million elements of 64 MB each, and the
/// server accumulates every one of them before the command is dispatched.
/// Twice [`MAX_BULK_LEN`] leaves room for a maximal value plus its key and
/// framing — every legitimate command fits, and nothing accumulates without
/// limit.
const MAX_COMMAND_BYTES: usize = 2 * MAX_BULK_LEN as usize;

/// How much to reserve up front for an array's elements.
///
/// The declared element count is a claim, not evidence; reserving for a
/// million elements costs tens of megabytes before a single one has arrived.
/// Ordinary commands are a handful of elements, so reserving for those and
/// letting the vector grow past them costs nothing measurable.
const ARRAY_RESERVE: usize = 16;

/// Longest protocol *line* accepted before the connection is failed.
///
/// The limits above bound what a client may declare, but they are read from a
/// line that has to be buffered first. Without a bound of its own, a client
/// that never sends a newline grows that buffer without limit — no command is
/// ever dispatched, so nothing else gets the chance to reject it. Bulk payloads
/// are read by length rather than by line, so a 64 MB value is unaffected by
/// this; it bounds only the framing. Redis caps its inline buffer at the same
/// 64 KiB.
pub(super) const MAX_LINE_LEN: usize = 64 * 1024;

/// How much of an unrecognised command name is quoted back in the error.
/// Echoing it whole turns any oversized input into an equally oversized reply.
const MAX_ECHOED_NAME: usize = 64;

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
    pub fn spawn(storage: SharedStorage, listener: TcpListener) -> crate::Result<Self> {
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
        let command = match read_command(&mut reader) {
            Ok(Some(command)) => command,
            Ok(None) => return Ok(()), // clean disconnect
            // Malformed framing. Say so before hanging up: the stream is now
            // at an unknown offset, so carrying on would misparse whatever
            // follows, but closing silently leaves the client guessing.
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                write_error(&mut writer, &format!("ERR Protocol error: {e}"))?;
                writer.flush()?;
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        if command.is_empty() {
            write_error(&mut writer, "empty command")?;
            continue;
        }

        let name = String::from_utf8_lossy(&command[0]).to_ascii_uppercase();
        // Bounded copy for error messages, so a bad command cannot be
        // reflected back at its own size.
        let quoted: String = name.chars().take(MAX_ECHOED_NAME).collect();
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
            // SET key value [EX seconds | PX milliseconds]
            "SET" => match parse_set(&command) {
                Ok(None) => write_wrong_args(&mut writer, "set")?,
                Err(message) => write_error(&mut writer, message)?,
                Ok(Some(ttl)) => {
                    let result = match ttl {
                        Some(ttl) => {
                            storage.put_with_ttl(command[1].clone(), command[2].clone(), ttl)
                        }
                        None => storage.put(command[1].clone(), command[2].clone()),
                    };
                    match result {
                        Ok(()) => write_simple(&mut writer, "OK")?,
                        Err(e) => write_error(&mut writer, &e.to_string())?,
                    }
                }
            },
            // TTL key -> remaining whole seconds, -1 no deadline, -2 no key
            "TTL" => match command.len() {
                2 => match storage.expiry(&command[1]) {
                    Ok(None) => write_integer(&mut writer, -2)?,
                    Ok(Some(None)) => write_integer(&mut writer, -1)?,
                    Ok(Some(Some(deadline))) => {
                        let now = lsm_now_ms();
                        // Round up, so a key with any time left never reports 0
                        // seconds while still being readable.
                        let remaining = deadline.saturating_sub(now).div_ceil(1000);
                        write_integer(&mut writer, remaining as i64)?
                    }
                    Err(e) => write_error(&mut writer, &e.to_string())?,
                },
                _ => write_wrong_args(&mut writer, "ttl")?,
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
            _ => write_error(&mut writer, &format!("unknown command '{}'", quoted))?,
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
    read_command_within(reader, MAX_COMMAND_BYTES)
}

/// The body of [`read_command`], with the per-command byte budget named so it
/// can be exercised at a size a test can afford to send.
fn read_command_within(
    reader: &mut impl BufRead,
    budget: usize,
) -> io::Result<Option<Vec<Vec<u8>>>> {
    let Some(line) = read_bounded_line(reader)? else {
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

    let mut parts = Vec::with_capacity((count as usize).min(ARRAY_RESERVE));
    let mut total = 0usize;
    for _ in 0..count {
        let Some(header) = read_bounded_line(reader)? else {
            return Err(protocol_error("unexpected EOF inside command"));
        };
        if header.first() != Some(&b'$') {
            return Err(protocol_error("expected bulk string"));
        }
        let len = parse_int(&header[1..])?;
        if !(0..=MAX_BULK_LEN).contains(&len) {
            return Err(protocol_error("bulk length out of range"));
        }
        total = total.saturating_add(len as usize);
        if total > budget {
            return Err(protocol_error("command too large"));
        }
        parts.push(read_bulk(reader, len as usize)?);
    }
    Ok(Some(parts))
}

/// Read a bulk string of `len` bytes plus its trailing CRLF.
///
/// The buffer grows as the payload arrives rather than being sized from the
/// declared length: that length is a claim by the client, and sizing the
/// allocation from it lets fifteen bytes of input reserve sixty-four
/// megabytes for as long as the sender cares to stall.
fn read_bulk<R: BufRead>(reader: &mut R, len: usize) -> io::Result<Vec<u8>> {
    let want = len + 2; // payload + CRLF
    let mut buf = Vec::new();
    let read = {
        use std::io::Read;
        (&mut *reader).take(want as u64).read_to_end(&mut buf)?
    };
    if read != want {
        return Err(protocol_error("unexpected EOF inside bulk string"));
    }
    // The declared length and the terminator have to agree. Consuming those
    // two bytes without looking at them lets a client whose length is wrong be
    // silently misparsed rather than told.
    if !buf.ends_with(b"\r\n") {
        return Err(protocol_error("bulk string is not CRLF-terminated"));
    }
    buf.truncate(len);
    Ok(buf)
}

/// Read a CRLF-terminated line (without the terminator). `None` on EOF
/// before any bytes were read.
/// Read one line, refusing to buffer more than [`MAX_LINE_LEN`] bytes.
///
/// Shared by both front ends. A line has to be buffered before anything can
/// inspect it, so any limit expressed in terms of the line's *contents* is
/// downstream of the memory it takes to get there — which is why this is one
/// function rather than one per protocol.
///
/// Trailing `\r` and `\n` are stripped, so an empty result is a blank line.
pub(super) fn read_bounded_line<R: BufRead>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    // Read one byte past the limit so that hitting it is distinguishable from
    // a line that merely ends exactly at it.
    let mut limited = std::io::Read::take(&mut *reader, MAX_LINE_LEN as u64 + 1);
    let n = limited.read_until(b'\n', &mut line)?;
    if n == 0 {
        return Ok(None);
    }
    if n > MAX_LINE_LEN {
        return Err(protocol_error("line too long"));
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

/// Parse the optional expiry of a `SET`.
///
/// `Ok(None)` means the arity is wrong; `Ok(Some(None))` a plain set;
/// `Ok(Some(Some(ttl)))` a set with a deadline; `Err` a malformed option,
/// reported to the client rather than silently ignored — a client asking for
/// an expiry must never be told OK for a key that will live for ever.
fn parse_set(command: &[Vec<u8>]) -> Result<Option<Option<Duration>>, &'static str> {
    match command.len() {
        3 => Ok(Some(None)),
        5 => {
            let unit = command[3].to_ascii_uppercase();
            let amount = std::str::from_utf8(&command[4])
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|n| *n > 0)
                .ok_or("ERR invalid expire time in 'set' command")?;
            match unit.as_slice() {
                b"EX" => Ok(Some(Some(Duration::from_secs(amount)))),
                b"PX" => Ok(Some(Some(Duration::from_millis(amount)))),
                _ => Err("ERR syntax error"),
            }
        }
        _ => Ok(None),
    }
}

/// Wall-clock now, in Unix milliseconds.
fn lsm_now_ms() -> u64 {
    crate::version::now_ms()
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

    fn read(raw: &[u8]) -> io::Result<Option<Vec<Vec<u8>>>> {
        read_command(&mut BufReader::new(raw))
    }

    #[test]
    fn a_well_formed_command_parses() {
        let parts = read(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n")
            .unwrap()
            .unwrap();
        assert_eq!(parts, vec![b"SET".to_vec(), b"k".to_vec(), b"v".to_vec()]);
    }

    #[test]
    fn an_empty_bulk_string_is_allowed() {
        // Zero length is a real value, not a malformed one: `SET k ""`.
        let parts = read(b"*2\r\n$1\r\nk\r\n$0\r\n\r\n").unwrap().unwrap();
        assert_eq!(parts, vec![b"k".to_vec(), Vec::new()]);
    }

    #[test]
    fn a_bulk_string_must_be_followed_by_its_terminator() {
        // A three-byte payload that is not followed by CRLF — here the next
        // command begins immediately. Reading `len + 2` bytes and discarding
        // the last two unexamined swallowed the "*1" that starts the second
        // command, leaving the parser aligned on nothing in particular and
        // every command after it misread. Rejecting is the only safe answer:
        // once the stream is off by two bytes there is nothing to resynchronise
        // against.
        let err = read(b"*1\r\n$3\r\nabc*1\r\n$4\r\nPING\r\n").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("CRLF"),
            "the client should be told which rule it broke: {err}"
        );
    }

    #[test]
    fn a_truncated_bulk_string_is_an_error_not_a_short_value() {
        let err = read(b"*1\r\n$64\r\nshort").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_commands_elements_are_bounded_in_total() {
        // Each element is individually within every limit; their sum is not.
        // The budget is a parameter here only so the test can afford to send
        // the bytes — in production it is `MAX_COMMAND_BYTES`.
        let mut raw = b"*4\r\n".to_vec();
        for _ in 0..4 {
            raw.extend_from_slice(b"$8\r\naaaaaaaa\r\n");
        }
        // 4 x 8 bytes against a 24-byte budget: the fourth element trips it.
        let err = read_command_within(&mut BufReader::new(&raw[..]), 24).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("too large"), "{err}");

        // ... and the same command fits when the budget allows it.
        let parts = read_command_within(&mut BufReader::new(&raw[..]), 32)
            .unwrap()
            .unwrap();
        assert_eq!(parts.len(), 4);
    }
}
