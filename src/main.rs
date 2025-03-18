use std::env;
use std::fs;
use std::io;

mod memtable;
mod sstable;
mod storage;
mod wal;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

use storage::Storage;

fn main() -> io::Result<()> {
    let verbose = env::args().any(|arg| arg == "-v" || arg == "--verbose");

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

fn basic_operations_test(db: &mut Storage) -> io::Result<()> {
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

fn compaction_test(db: &mut Storage) -> io::Result<()> {
    // Helper function to count SST files
    fn count_sst_files() -> io::Result<(usize, Vec<String>)> {
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
