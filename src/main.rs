use std::env;
use std::fs;
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::Duration;

use lsm_rust::{Error, MetricsServer, RespConfig, RespServer, SharedStorage, Storage};

fn main() -> lsm_rust::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let verbose = args.iter().any(|a| a == "-v" || a == "--verbose");

    match args.first().map(String::as_str) {
        Some("serve") => serve(&args[1..], verbose),
        Some("healthcheck") => healthcheck(&args[1..]),
        Some("demo") | None => demo(verbose),
        Some("-v") | Some("--verbose") => demo(verbose),
        Some("help") | Some("--help") | Some("-h") => {
            print_usage();
            Ok(())
        }
        Some(other) => {
            eprintln!("Unknown command: {}", other);
            print_usage();
            std::process::exit(2);
        }
    }
}

fn print_usage() {
    println!("Usage: lsm-rust [COMMAND] [OPTIONS]");
    println!();
    println!("Commands:");
    println!("  demo               Run the scripted demo (default)");
    println!("  serve              Serve the store over the Redis protocol (RESP)");
    println!(
        "  healthcheck        Probe a running server (for container HEALTHCHECK); exit 0 if live"
    );
    println!();
    println!("Options:");
    println!("  -v, --verbose            Verbose engine logging");
    println!("  --addr HOST:PORT         serve: RESP listen address (default 127.0.0.1:6379)");
    println!("  --data DIR               serve: data directory (default ./data)");
    println!("  --metrics-addr HOST:PORT serve: also expose Prometheus /metrics at this address");
    println!("  --requirepass-file PATH  serve: require AUTH with the password in this file");
    println!(
        "  --max-connections N      serve: client connection cap, 0 = unlimited (default 1024)"
    );
    println!("  --idle-timeout-secs N    serve: drop idle clients after N seconds, 0 = never (default 300)");
    println!();
    println!("Environment (flags take precedence): LSM_ADDR, LSM_DATA_DIR, LSM_METRICS_ADDR,");
    println!("  LSM_REQUIREPASS_FILE, LSM_MAX_CONNECTIONS, LSM_IDLE_TIMEOUT_SECS");
}

/// Settings for `serve`. Each comes from `--flag VALUE`, else an environment
/// variable, else a default; flags win so an operator can always override a
/// setting baked into a container image.
struct ServeOptions {
    addr: String,
    data_dir: String,
    metrics_addr: Option<String>,
    password_file: Option<String>,
    max_connections: usize,
    idle_timeout_secs: u64,
}

fn env_nonempty(name: &str) -> Option<String> {
    env::var(name).ok().filter(|v| !v.is_empty())
}

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> lsm_rust::Result<T> {
    match env_nonempty(name) {
        None => Ok(default),
        Some(raw) => raw
            .parse()
            .map_err(|_| Error::InvalidArgument(format!("{name} is not a valid number: {raw}"))),
    }
}

/// `lsm-rust serve [OPTIONS]` — see `print_usage` for the full list.
fn serve(args: &[String], verbose: bool) -> lsm_rust::Result<()> {
    let mut opts = ServeOptions {
        addr: env_nonempty("LSM_ADDR").unwrap_or_else(|| "127.0.0.1:6379".to_string()),
        data_dir: env_nonempty("LSM_DATA_DIR").unwrap_or_else(|| "./data".to_string()),
        metrics_addr: env_nonempty("LSM_METRICS_ADDR"),
        password_file: env_nonempty("LSM_REQUIREPASS_FILE"),
        max_connections: env_parse("LSM_MAX_CONNECTIONS", 1024)?,
        idle_timeout_secs: env_parse("LSM_IDLE_TIMEOUT_SECS", 300)?,
    };

    let mut i = 0;
    while i < args.len() {
        let flag = args[i].as_str();
        i += 1;
        let mut take = |flag: &str| -> lsm_rust::Result<String> {
            let v = args
                .get(i)
                .cloned()
                .ok_or_else(|| Error::InvalidArgument(format!("{flag} needs a value")))?;
            i += 1;
            Ok(v)
        };
        match flag {
            "--addr" => opts.addr = take("--addr")?,
            "--data" => opts.data_dir = take("--data")?,
            "--metrics-addr" => opts.metrics_addr = Some(take("--metrics-addr")?),
            "--requirepass-file" => opts.password_file = Some(take("--requirepass-file")?),
            "--max-connections" => {
                opts.max_connections = take("--max-connections")?
                    .parse()
                    .map_err(|_| Error::InvalidArgument("--max-connections: not a number".into()))?
            }
            "--idle-timeout-secs" => {
                opts.idle_timeout_secs = take("--idle-timeout-secs")?.parse().map_err(|_| {
                    Error::InvalidArgument("--idle-timeout-secs: not a number".into())
                })?
            }
            "-v" | "--verbose" => {}
            other => {
                return Err(Error::InvalidArgument(format!(
                    "unknown serve option: {}",
                    other
                )));
            }
        }
    }

    // The password comes from a file (a mounted Secret), never from argv or an
    // environment variable, both of which leak via `ps`, `/proc` and crash
    // dumps. One trailing newline is stripped so `echo secret > file` works.
    let password = match &opts.password_file {
        None => None,
        Some(path) => {
            let mut raw = fs::read(path).map_err(|e| {
                Error::InvalidArgument(format!("cannot read password file {path}: {e}"))
            })?;
            while matches!(raw.last(), Some(b'\n') | Some(b'\r')) {
                raw.pop();
            }
            if raw.is_empty() {
                return Err(Error::InvalidArgument(format!(
                    "password file {path} is empty"
                )));
            }
            Some(raw)
        }
    };

    let storage = SharedStorage::new(&opts.data_dir, verbose)?;
    let listener = TcpListener::bind(&opts.addr)?;
    println!(
        "lsm-rust serving RESP on {} (data: {}, auth: {})",
        listener.local_addr()?,
        opts.data_dir,
        if password.is_some() {
            "required"
        } else {
            "off"
        }
    );
    if password.is_none() && !listener.local_addr()?.ip().is_loopback() {
        eprintln!(
            "warning: listening on a non-loopback address without authentication; \
             set --requirepass-file or restrict access at the network layer"
        );
    }

    // Optionally expose Prometheus metrics on a separate HTTP port. Keep the
    // handle alive for the lifetime of the server so the thread keeps running.
    let _metrics = match &opts.metrics_addr {
        Some(addr) => {
            let metrics_listener = TcpListener::bind(addr)?;
            let bound = metrics_listener.local_addr()?;
            let server = MetricsServer::spawn(storage.clone(), metrics_listener)?;
            println!("Prometheus metrics on http://{}/metrics", bound);
            Some(server)
        }
        None => None,
    };

    let config = RespConfig {
        password,
        max_connections: opts.max_connections,
        idle_timeout: (opts.idle_timeout_secs > 0)
            .then(|| Duration::from_secs(opts.idle_timeout_secs)),
    };
    let server = RespServer::spawn_with(storage, listener, config)?;

    // Block until SIGTERM/SIGINT, then drop the servers in order so the accept
    // loops stop and in-flight commands finish before the process exits.
    let (tx, rx) = mpsc::channel();
    ctrlc::set_handler(move || {
        let _ = tx.send(());
    })
    .map_err(|e| Error::InvalidArgument(format!("cannot install signal handler: {e}")))?;
    let _ = rx.recv();
    println!("shutting down");
    drop(server);
    Ok(())
}

/// `lsm-rust healthcheck [--addr HOST:PORT]`
///
/// Sends `PING` and exits 0 on any well-formed reply. A `-NOAUTH` reply counts:
/// it proves the server is up and parsing, without the probe needing the
/// password. This exists because the runtime image has no shell or `pgrep`.
fn healthcheck(args: &[String]) -> lsm_rust::Result<()> {
    use std::io::{BufRead, BufReader, Write};
    use std::net::{SocketAddr, TcpStream, ToSocketAddrs};

    let mut addr = env_nonempty("LSM_ADDR").unwrap_or_else(|| "127.0.0.1:6379".to_string());
    if args.first().map(String::as_str) == Some("--addr") {
        addr = args
            .get(1)
            .cloned()
            .ok_or_else(|| Error::InvalidArgument("--addr needs a value".to_string()))?;
    }
    // A wildcard bind address is not connectable; probe loopback on its port.
    let target: SocketAddr = addr
        .to_socket_addrs()?
        .next()
        .map(|a| {
            if a.ip().is_unspecified() {
                SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), a.port())
            } else {
                a
            }
        })
        .ok_or_else(|| Error::InvalidArgument(format!("cannot resolve {addr}")))?;

    let timeout = Duration::from_secs(2);
    let mut stream = TcpStream::connect_timeout(&target, timeout)?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    stream.write_all(b"PING\r\n")?;
    let mut reply = String::new();
    BufReader::new(stream).read_line(&mut reply)?;
    if reply.starts_with("+PONG") || reply.starts_with("-NOAUTH") {
        Ok(())
    } else {
        eprintln!("unhealthy: unexpected reply {reply:?}");
        std::process::exit(1);
    }
}

/// `lsm-rust demo [-v]` — the original scripted example.
fn demo(verbose: bool) -> lsm_rust::Result<()> {
    println!("LSM Tree Database Example");
    if verbose {
        println!("Verbose mode enabled");
    }

    // Clean up any existing data
    let _ = fs::remove_dir_all("./data");
    let mut db = Storage::new("./data", verbose)?;

    // Test 1: Basic Operations
    println!("\n=== Test 1: Basic Operations ===");
    basic_operations_test(&mut db)?;

    // Test 2: Compaction Trigger
    println!("\n=== Test 2: Compaction Test ===");
    compaction_test(&mut db)?;

    Ok(())
}

fn basic_operations_test(db: &mut Storage) -> lsm_rust::Result<()> {
    println!("Inserting initial data...");
    db.put(b"name".to_vec(), b"John Doe".to_vec())?;
    db.put(b"age".to_vec(), b"30".to_vec())?;
    db.put(b"city".to_vec(), b"New York".to_vec())?;

    println!("\nRetrieving data:");
    if let Ok(Some(name)) = db.get(&b"name".to_vec()) {
        println!("name: {}", String::from_utf8_lossy(&name));
    }
    if let Ok(Some(age)) = db.get(&b"age".to_vec()) {
        println!("age: {}", String::from_utf8_lossy(&age));
    }
    if let Ok(Some(city)) = db.get(&b"city".to_vec()) {
        println!("city: {}", String::from_utf8_lossy(&city));
    }

    println!("\nDeleting 'age' entry...");
    db.delete(&b"age".to_vec())?;

    println!("\nTrying to retrieve deleted data:");
    match db.get(&b"age".to_vec()) {
        Ok(Some(_)) => println!("age: still exists"),
        Ok(None) => println!("age: was deleted"),
        Err(e) => println!("Error: {}", e),
    }

    Ok(())
}

fn compaction_test(db: &mut Storage) -> lsm_rust::Result<()> {
    // Helper function to count SST files
    fn count_sst_files() -> lsm_rust::Result<(usize, Vec<String>)> {
        let mut count = 0;
        let mut files = Vec::new();
        for entry in fs::read_dir("./data")? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("sst") {
                count += 1;
                files.push(path.file_name().unwrap().to_string_lossy().to_string());
            }
        }
        Ok((count, files))
    }

    println!("Initial state:");
    let (initial_files, files) = count_sst_files()?;
    println!("SSTable files: {} {:?}", initial_files, files);

    // Write enough data to trigger multiple flushes and compactions
    println!("\nWriting large dataset to trigger compaction...");
    for i in 0..5000 {
        let key = format!("key{:05}", i).into_bytes();
        let value = format!("value{}", i).repeat(100).into_bytes(); // Large values
        db.put(key, value)?;

        if i > 0 && i % 1000 == 0 {
            println!("Inserted {} records", i);
            let (count, files) = count_sst_files()?;
            println!("Current SSTable files: {} {:?}", count, files);
        }
    }

    // Final state
    println!("\nFinal state:");
    let (final_files, files) = count_sst_files()?;
    println!("SSTable files: {} {:?}", final_files, files);

    // Verify data integrity
    println!("\nVerifying data integrity...");
    let test_keys = [0, 1000, 2000, 3000, 4000, 4999];
    for i in test_keys {
        let key = format!("key{:05}", i).into_bytes();
        let expected_value = format!("value{}", i).repeat(100);
        match db.get(&key) {
            Ok(Some(value)) => {
                let got_value = String::from_utf8_lossy(&value);
                if got_value == expected_value {
                    println!("Key {:05}: OK", i);
                } else {
                    println!("Key {:05}: Value mismatch!", i);
                }
            }
            Ok(None) => println!("Key {:05}: Not found!", i),
            Err(e) => println!("Key {:05}: Error: {}", i, e),
        }
    }

    Ok(())
}
