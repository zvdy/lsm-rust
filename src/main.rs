use std::env;
use std::fs;
use std::net::TcpListener;

use lsm_rust::{Error, MetricsServer, RespServer, SharedStorage, Storage};

fn main() -> lsm_rust::Result<()> {
    let args: Vec<String> = env::args().skip(1).collect();
    let verbose = args.iter().any(|a| a == "-v" || a == "--verbose");

    match args.first().map(String::as_str) {
        Some("serve") => serve(&args[1..], verbose),
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
    println!();
    println!("Options:");
    println!("  -v, --verbose            Verbose engine logging");
    println!("  --addr HOST:PORT         serve: RESP listen address (default 127.0.0.1:6379)");
    println!("  --data DIR               serve: data directory (default ./data)");
    println!("  --metrics-addr HOST:PORT serve: also expose Prometheus /metrics at this address");
}

/// `lsm-rust serve [--addr HOST:PORT] [--data DIR] [-v]`
fn serve(args: &[String], verbose: bool) -> lsm_rust::Result<()> {
    let mut addr = "127.0.0.1:6379".to_string();
    let mut data_dir = "./data".to_string();
    let mut metrics_addr: Option<String> = None;

    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--addr" => {
                addr = iter
                    .next()
                    .ok_or_else(|| Error::InvalidArgument("--addr needs a value".to_string()))?
                    .clone();
            }
            "--data" => {
                data_dir = iter
                    .next()
                    .ok_or_else(|| Error::InvalidArgument("--data needs a value".to_string()))?
                    .clone();
            }
            "--metrics-addr" => {
                metrics_addr = Some(
                    iter.next()
                        .ok_or_else(|| {
                            Error::InvalidArgument("--metrics-addr needs a value".to_string())
                        })?
                        .clone(),
                );
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

    let storage = SharedStorage::new(&data_dir, verbose)?;
    let listener = TcpListener::bind(&addr)?;
    println!(
        "lsm-rust serving RESP on {} (data: {})",
        listener.local_addr()?,
        data_dir
    );
    println!("Try: redis-cli -p {}", listener.local_addr()?.port());

    // Optionally expose Prometheus metrics on a separate HTTP port. Keep the
    // handle alive for the lifetime of the server so the thread keeps running.
    let _metrics = match metrics_addr {
        Some(addr) => {
            let metrics_listener = TcpListener::bind(&addr)?;
            let bound = metrics_listener.local_addr()?;
            let server = MetricsServer::spawn(storage.clone(), metrics_listener)?;
            println!("Prometheus metrics on http://{}/metrics", bound);
            Some(server)
        }
        None => None,
    };

    let server = RespServer::spawn(storage, listener)?;
    server.join();
    Ok(())
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
