//! Taking a consistent backup and restoring it.
//!
//! A checkpoint directory is a data directory: restoring is just opening it.
//!
//! Run with `cargo run --example checkpoint`.

use lsm_rust::{SharedStorage, Storage, StorageConfig};

fn main() -> lsm_rust::Result<()> {
    let dir = tempfile::tempdir()?;
    let backups = tempfile::tempdir()?;
    let target = backups.path().join("backup-1");

    // A small memtable so the writes reach SSTables rather than only the WAL.
    let config = StorageConfig {
        memtable_size_threshold: 4 * 1024,
        ..StorageConfig::default()
    };
    let db = SharedStorage::with_config(dir.path().join("data"), config)?;
    for i in 0..500 {
        db.put(format!("key:{i:03}"), format!("value {i}"))?;
    }

    let info = db.checkpoint(&target)?;
    println!(
        "checkpoint: {} tables, consistent as of seq {}, hard linked: {}",
        info.tables, info.sequence, info.hard_linked
    );

    // Writes after the checkpoint do not appear in it.
    db.put("key:000", "changed after the backup")?;
    drop(db);

    let restored = Storage::new(&target, false)?;
    println!(
        "restored key:000 = {}",
        String::from_utf8_lossy(&restored.get("key:000")?.unwrap_or_default())
    );
    Ok(())
}
