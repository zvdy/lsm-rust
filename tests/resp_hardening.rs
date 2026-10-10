//! Authentication, connection cap and idle-timeout behaviour of the RESP
//! server, exercised over a real TCP connection.

use lsm_rust::{RespConfig, RespServer, SharedStorage};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;
use tempfile::TempDir;

fn start(config: RespConfig) -> (RespServer, TempDir) {
    let dir = TempDir::new().unwrap();
    let storage = SharedStorage::new(dir.path(), false).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    (
        RespServer::spawn_with(storage, listener, config).unwrap(),
        dir,
    )
}

/// Send one inline command and read one line of reply.
fn roundtrip(stream: &mut TcpStream, reader: &mut BufReader<TcpStream>, cmd: &str) -> String {
    stream.write_all(format!("{cmd}\r\n").as_bytes()).unwrap();
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    line.trim_end().to_string()
}

fn connect(server: &RespServer) -> (TcpStream, BufReader<TcpStream>) {
    let stream = TcpStream::connect(server.local_addr()).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let reader = BufReader::new(stream.try_clone().unwrap());
    (stream, reader)
}

#[test]
fn commands_are_refused_until_auth() {
    let (server, _dir) = start(RespConfig {
        password: Some(b"hunter2".to_vec()),
        ..RespConfig::default()
    });
    let (mut s, mut r) = connect(&server);

    assert!(roundtrip(&mut s, &mut r, "PING").starts_with("-NOAUTH"));
    assert!(roundtrip(&mut s, &mut r, "SET k v").starts_with("-NOAUTH"));
    assert!(roundtrip(&mut s, &mut r, "AUTH wrong").starts_with("-WRONGPASS"));
    assert!(roundtrip(&mut s, &mut r, "GET k").starts_with("-NOAUTH"));
    assert_eq!(roundtrip(&mut s, &mut r, "AUTH hunter2"), "+OK");
    assert_eq!(roundtrip(&mut s, &mut r, "PING"), "+PONG");
    assert_eq!(roundtrip(&mut s, &mut r, "SET k v"), "+OK");
}

#[test]
fn auth_accepts_the_default_user_form() {
    let (server, _dir) = start(RespConfig {
        password: Some(b"hunter2".to_vec()),
        ..RespConfig::default()
    });
    let (mut s, mut r) = connect(&server);
    assert!(roundtrip(&mut s, &mut r, "AUTH root hunter2").starts_with("-"));
    assert_eq!(roundtrip(&mut s, &mut r, "AUTH default hunter2"), "+OK");
}

#[test]
fn auth_without_a_configured_password_is_an_error() {
    let (server, _dir) = start(RespConfig::default());
    let (mut s, mut r) = connect(&server);
    assert!(roundtrip(&mut s, &mut r, "AUTH anything").starts_with("-ERR"));
    assert_eq!(roundtrip(&mut s, &mut r, "PING"), "+PONG");
}

#[test]
fn connections_beyond_the_cap_are_refused() {
    let (server, _dir) = start(RespConfig {
        max_connections: 1,
        ..RespConfig::default()
    });
    let (mut first, mut first_reader) = connect(&server);
    assert_eq!(roundtrip(&mut first, &mut first_reader, "PING"), "+PONG");

    let (_second, mut second_reader) = connect(&server);
    let mut line = String::new();
    second_reader.read_line(&mut line).unwrap();
    assert!(line.starts_with("-ERR max number of clients"), "{line:?}");

    // Releasing the slot admits a new client.
    drop(first_reader);
    drop(first);
    let mut admitted = false;
    for _ in 0..50 {
        let (mut s, mut r) = connect(&server);
        if roundtrip(&mut s, &mut r, "PING") == "+PONG" {
            admitted = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(admitted, "slot was never released");
}

#[test]
fn idle_connections_are_dropped() {
    let (server, _dir) = start(RespConfig {
        idle_timeout: Some(Duration::from_millis(200)),
        ..RespConfig::default()
    });
    let (mut s, _r) = connect(&server);
    std::thread::sleep(Duration::from_millis(600));
    // The server has closed its end: a read sees EOF rather than blocking.
    let mut buf = [0u8; 1];
    let n = s.read(&mut buf).unwrap_or(0);
    assert_eq!(n, 0);
}
