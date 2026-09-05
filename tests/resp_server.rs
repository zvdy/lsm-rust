//! End-to-end tests for the RESP server: a real TCP client speaking the
//! Redis protocol against a server backed by a temporary store.

use lsm_rust::{RespServer, SharedStorage};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use tempfile::TempDir;

struct Client {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl Client {
    fn connect(addr: std::net::SocketAddr) -> Self {
        let stream = TcpStream::connect(addr).unwrap();
        Client {
            reader: BufReader::new(stream.try_clone().unwrap()),
            writer: stream,
        }
    }

    /// Send a command as a RESP array of bulk strings.
    fn send(&mut self, parts: &[&[u8]]) {
        let mut msg = format!("*{}\r\n", parts.len()).into_bytes();
        for part in parts {
            msg.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            msg.extend_from_slice(part);
            msg.extend_from_slice(b"\r\n");
        }
        self.writer.write_all(&msg).unwrap();
    }

    fn read_line(&mut self) -> String {
        let mut line = String::new();
        self.reader.read_line(&mut line).unwrap();
        line.trim_end().to_string()
    }

    /// Read one full RESP reply, flattened to strings for assertions.
    fn read_reply(&mut self) -> Vec<String> {
        let line = self.read_line();
        match line.chars().next().unwrap() {
            '+' | '-' | ':' => vec![line],
            '$' => {
                let len: i64 = line[1..].parse().unwrap();
                if len < 0 {
                    return vec!["nil".to_string()];
                }
                let mut buf = vec![0u8; len as usize + 2];
                self.reader.read_exact(&mut buf).unwrap();
                buf.truncate(len as usize);
                vec![String::from_utf8(buf).unwrap()]
            }
            '*' => {
                let count: usize = line[1..].parse().unwrap();
                let mut items = Vec::new();
                for _ in 0..count {
                    items.extend(self.read_reply());
                }
                items
            }
            other => panic!("unexpected RESP type: {}", other),
        }
    }
}

fn start_server() -> (TempDir, RespServer) {
    let temp_dir = TempDir::new().unwrap();
    let storage = SharedStorage::new(temp_dir.path(), false).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let server = RespServer::spawn(storage, listener).unwrap();
    (temp_dir, server)
}

#[test]
fn ping_set_get_del_roundtrip() {
    let (_temp_dir, server) = start_server();
    let mut client = Client::connect(server.local_addr());

    client.send(&[b"PING"]);
    assert_eq!(client.read_reply(), vec!["+PONG"]);

    client.send(&[b"SET", b"name", b"Jane Doe"]);
    assert_eq!(client.read_reply(), vec!["+OK"]);

    client.send(&[b"GET", b"name"]);
    assert_eq!(client.read_reply(), vec!["Jane Doe"]);

    client.send(&[b"EXISTS", b"name", b"missing", b"name"]);
    assert_eq!(client.read_reply(), vec![":2"]);

    client.send(&[b"DEL", b"name", b"missing"]);
    assert_eq!(client.read_reply(), vec![":1"]);

    client.send(&[b"GET", b"name"]);
    assert_eq!(client.read_reply(), vec!["nil"]);
}

#[test]
fn keys_prefix_scan() {
    let (_temp_dir, server) = start_server();
    let mut client = Client::connect(server.local_addr());

    for key in ["user:1", "user:2", "other:1"] {
        client.send(&[b"SET", key.as_bytes(), b"v"]);
        assert_eq!(client.read_reply(), vec!["+OK"]);
    }

    client.send(&[b"KEYS", b"user:*"]);
    assert_eq!(client.read_reply(), vec!["user:1", "user:2"]);

    client.send(&[b"KEYS", b"user:1"]);
    assert_eq!(client.read_reply(), vec!["user:1"]);

    client.send(&[b"KEYS", b"nope"]);
    assert_eq!(client.read_reply(), Vec::<String>::new());

    // Unsupported glob forms are rejected, not silently wrong
    client.send(&[b"KEYS", b"a*b"]);
    assert!(client.read_reply()[0].starts_with("-ERR"));
}

#[test]
fn protocol_errors_and_unknown_commands() {
    let (_temp_dir, server) = start_server();
    let mut client = Client::connect(server.local_addr());

    client.send(&[b"NOSUCHCMD"]);
    assert!(client.read_reply()[0].starts_with("-ERR unknown command"));

    client.send(&[b"GET"]); // wrong arity
    assert!(client.read_reply()[0].starts_with("-ERR wrong number of arguments"));

    // Inline (telnet-style) commands also work
    client.writer.write_all(b"PING hello\r\n").unwrap();
    assert_eq!(client.read_reply(), vec!["hello"]);
}

#[test]
fn concurrent_clients_and_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let addr;
    {
        let storage = SharedStorage::new(temp_dir.path(), false).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server = RespServer::spawn(storage, listener).unwrap();
        addr = server.local_addr();

        // Several clients writing disjoint keys at once
        let handles: Vec<_> = (0..4)
            .map(|c| {
                std::thread::spawn(move || {
                    let mut client = Client::connect(addr);
                    for i in 0..50 {
                        let key = format!("c{}:key{}", c, i);
                        client.send(&[b"SET", key.as_bytes(), b"v"]);
                        assert_eq!(client.read_reply(), vec!["+OK"]);
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        let mut client = Client::connect(addr);
        client.send(&[b"KEYS", b"c2:*"]);
        assert_eq!(client.read_reply().len(), 50);

        drop(server); // clean shutdown
    }

    // Data written over the wire survives a restart of the store
    let storage = SharedStorage::new(temp_dir.path(), false).unwrap();
    assert_eq!(
        storage.get(&b"c0:key0".to_vec()).unwrap(),
        Some(b"v".to_vec())
    );
}

#[test]
fn set_with_px_expires_the_key() {
    let (_temp_dir, server) = start_server();
    let mut client = Client::connect(server.local_addr());

    // Only the "gone" direction is timed. Asserting the key is still there
    // inside a short window would race the write itself — every put fsyncs,
    // and that can outlast a deadline measured in milliseconds on a slow
    // machine. `set_with_ex_is_readable_before_its_deadline` covers presence
    // with a deadline nothing can outrun.
    client.send(&[b"SET", b"quick", b"v", b"PX", b"1"]);
    assert_eq!(client.read_reply(), vec!["+OK"]);

    std::thread::sleep(std::time::Duration::from_millis(250));

    client.send(&[b"GET", b"quick"]);
    assert_eq!(client.read_reply(), vec!["nil"]);
    // EXISTS must agree with GET rather than reporting the hidden version.
    client.send(&[b"EXISTS", b"quick"]);
    assert_eq!(client.read_reply(), vec![":0"]);
    client.send(&[b"TTL", b"quick"]);
    assert_eq!(
        client.read_reply(),
        vec![":-2"],
        "expired reads as no such key"
    );
}

#[test]
fn set_with_ex_is_readable_before_its_deadline() {
    let (_temp_dir, server) = start_server();
    let mut client = Client::connect(server.local_addr());

    client.send(&[b"SET", b"slow", b"v", b"EX", b"3600"]);
    assert_eq!(client.read_reply(), vec!["+OK"]);
    client.send(&[b"GET", b"slow"]);
    assert_eq!(client.read_reply(), vec!["v"]);
    client.send(&[b"EXISTS", b"slow"]);
    assert_eq!(client.read_reply(), vec![":1"]);
}

#[test]
fn ttl_reports_redis_style_states() {
    let (_temp_dir, server) = start_server();
    let mut client = Client::connect(server.local_addr());

    client.send(&[b"TTL", b"missing"]);
    assert_eq!(client.read_reply(), vec![":-2"], "no such key");

    client.send(&[b"SET", b"permanent", b"v"]);
    client.read_reply();
    client.send(&[b"TTL", b"permanent"]);
    assert_eq!(client.read_reply(), vec![":-1"], "no deadline");

    client.send(&[b"SET", b"expiring", b"v", b"EX", b"100"]);
    assert_eq!(client.read_reply(), vec!["+OK"]);
    client.send(&[b"TTL", b"expiring"]);
    let reply = client.read_reply();
    let seconds: i64 = reply[0][1..].parse().unwrap();
    assert!(
        (95..=100).contains(&seconds),
        "expected roughly 100s left, got {seconds}"
    );
}

#[test]
fn a_malformed_expiry_is_rejected_rather_than_silently_ignored() {
    let (_temp_dir, server) = start_server();
    let mut client = Client::connect(server.local_addr());

    // A client asking for an expiry must never be told OK for a key that
    // would then live for ever.
    client.send(&[b"SET", b"k", b"v", b"EX", b"nonsense"]);
    assert!(client.read_reply()[0].starts_with('-'));

    client.send(&[b"SET", b"k", b"v", b"EX", b"0"]);
    assert!(client.read_reply()[0].starts_with('-'));

    client.send(&[b"SET", b"k", b"v", b"NX", b"5"]);
    assert!(client.read_reply()[0].starts_with('-'), "unknown option");

    client.send(&[b"GET", b"k"]);
    assert_eq!(client.read_reply(), vec!["nil"], "nothing was written");

    // Wrong arity still reports the usual arity error.
    client.send(&[b"SET", b"k", b"v", b"EX"]);
    assert!(client.read_reply()[0].starts_with('-'));
}

#[test]
fn an_over_long_line_is_rejected_rather_than_buffered() {
    // Framing lines have to be buffered before anything can inspect them, so
    // without a bound of their own a client that never sends a newline grows
    // that buffer without limit — the declared-size limits never get a chance
    // to reject it, because no command is ever dispatched.
    let (_temp_dir, server) = start_server();
    let mut client = Client::connect(server.local_addr());

    // Without the bound the server simply waits for a newline that never
    // arrives, so guard with a read timeout: a regression must fail this test
    // rather than hang CI.
    client
        .writer
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();

    client.writer.write_all(&vec![b'A'; 70 * 1024]).unwrap();
    client.writer.flush().unwrap();

    let reply = client.read_reply();
    assert!(
        reply[0].starts_with('-'),
        "expected a protocol error, got {reply:?}"
    );
    assert!(
        reply[0].to_lowercase().contains("too long"),
        "error should say what was wrong: {reply:?}"
    );
    assert!(
        reply[0].len() < 256,
        "the error must not echo the oversized input back: {} bytes",
        reply[0].len()
    );
}

#[test]
fn an_unknown_command_is_not_reflected_back_at_its_own_size() {
    let (_temp_dir, server) = start_server();
    let mut client = Client::connect(server.local_addr());

    let name = vec![b'Z'; 4096];
    client.send(&[&name]);

    let reply = client.read_reply();
    assert!(reply[0].starts_with('-'), "{reply:?}");
    assert!(
        reply[0].len() < 256,
        "unknown-command errors must be bounded, got {} bytes",
        reply[0].len()
    );

    // The connection is still usable: this was a command error, not a framing
    // error, so nothing was closed.
    client.send(&[b"PING"]);
    assert_eq!(client.read_reply(), vec!["+PONG"]);
}

#[test]
fn a_large_bulk_value_still_round_trips() {
    // The line bound must not cap payloads: bulk strings are read by declared
    // length, not by line, so a value far larger than a line still works.
    let (_temp_dir, server) = start_server();
    let mut client = Client::connect(server.local_addr());

    let value = vec![b'v'; 1024 * 1024];
    client.send(&[b"SET", b"big", &value]);
    assert_eq!(client.read_reply(), vec!["+OK"]);

    client.send(&[b"GET", b"big"]);
    let reply = client.read_reply();
    assert_eq!(reply.len(), 1);
    assert_eq!(reply[0].len(), value.len());
    assert!(reply[0].bytes().all(|b| b == b'v'));
}
