//! Concurrent read-modify-write with optimistic transactions.
//!
//! Eight threads increment one counter 25 times each. `transaction` reruns the
//! closure when it loses a race, so no increment is lost.
//!
//! Run with `cargo run --example transactions`.

use lsm_rust::SharedStorage;
use std::thread;

fn main() -> lsm_rust::Result<()> {
    let dir = tempfile::tempdir()?;
    let db = SharedStorage::new(dir.path(), false)?;
    db.put("counter", "0")?;

    let workers: Vec<_> = (0..8)
        .map(|_| {
            let db = db.clone();
            thread::spawn(move || -> lsm_rust::Result<()> {
                for _ in 0..25 {
                    db.transaction(1000, |tx| {
                        let current = tx.get("counter")?.unwrap_or_default();
                        let n: u64 = String::from_utf8_lossy(&current).parse().unwrap_or(0);
                        tx.put("counter", (n + 1).to_string());
                        Ok(())
                    })?;
                }
                Ok(())
            })
        })
        .collect();
    for worker in workers {
        worker.join().expect("worker panicked")?;
    }

    let total = db.get("counter")?.unwrap_or_default();
    println!("counter = {}", String::from_utf8_lossy(&total));

    // Driving one by hand: a conflicting commit is reported, not applied.
    let mut mine = db.begin()?;
    mine.get("counter")?;
    db.put("counter", "0")?;
    mine.put("counter", "999");
    match mine.commit() {
        Ok(seq) => println!("committed at {seq}"),
        Err(e) if e.is_retriable() => println!("conflict, would retry: {e}"),
        Err(e) => return Err(e),
    }
    Ok(())
}
