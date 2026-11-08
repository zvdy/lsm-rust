//! Keys that expire.
//!
//! Run with `cargo run --example ttl`.

use lsm_rust::Storage;
use std::thread::sleep;
use std::time::Duration;

fn main() -> lsm_rust::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut db = Storage::new(dir.path(), false)?;

    db.put("permanent", "stays")?;
    db.put_with_ttl("session", "token", Duration::from_secs(3600))?;
    db.put_with_ttl("flash", "gone soon", Duration::from_millis(200))?;

    for key in ["permanent", "session", "flash", "missing"] {
        match db.expiry(key)? {
            None => println!("{key}: no such key"),
            Some(None) => println!("{key}: present, no deadline"),
            Some(Some(at)) => println!("{key}: expires at {at} ms since the epoch"),
        }
    }

    sleep(Duration::from_millis(300));
    println!("flash after 300 ms = {:?}", db.get("flash")?);
    println!("session still there = {}", db.get("session")?.is_some());
    Ok(())
}
