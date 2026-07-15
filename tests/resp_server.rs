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
