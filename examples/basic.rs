//! Puts, gets, deletes, ordered scans, atomic batches and snapshots.
//!
//! Run with `cargo run --example basic`.

use lsm_rust::{Storage, WriteBatch};

fn main() -> lsm_rust::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut db = Storage::new(dir.path(), false)?;

    db.put("user:1", "Jane")?;
    db.put("user:2", "John")?;
    db.put("order:1", "book")?;
    println!("user:1 = {:?}", db.get("user:1")?.map(String::from_utf8));

    db.delete("user:2")?;
    println!("user:2 after delete = {:?}", db.get("user:2")?);

    let mut batch = WriteBatch::new();
    batch
        .put("user:3", "Ana")
        .put("user:4", "Bo")
        .delete("order:1");
    db.write_batch(batch)?;

    println!("keys with prefix user:");
    for (key, value) in db.scan_prefix(b"user:")? {
        println!(
            "  {} = {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }

    // A snapshot keeps reading the store as it was, whatever is written later.
    let snapshot = db.snapshot();
    db.put("user:1", "Jane Doe")?;
    println!(
        "user:1 now      = {:?}",
        db.get("user:1")?.map(String::from_utf8)
    );
    println!(
        "user:1 snapshot = {:?}",
        db.get_at(&snapshot, "user:1")?.map(String::from_utf8)
    );

    // Streaming a range reads one block at a time instead of collecting it.
    let mut count = 0;
    for entry in db.scan_iter(b"user:", Some(b"user;"))? {
        entry?;
        count += 1;
    }
    println!("{count} users");

    Ok(())
}
