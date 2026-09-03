//! Checkpoint and restore.
//!
//! Crash recovery is covered by `recovery.rs`: the store already survives a
//! process dying. These tests cover what it could *not* survive — the data
//! itself being destroyed — and assert that a checkpoint taken beforehand
//! brings it back.

use lsm_rust::{Error, SharedStorage, Storage, StorageConfig};
use std::fs;
use std::path::Path;
use tempfile::TempDir;

/// Small thresholds so a modest number of writes produces real SSTables
/// across more than one level.
fn config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 4 * 1024,
        compaction_size_threshold: 16 * 1024,
        block_cache_size: 0,
        ..StorageConfig::default()
    }
}

fn seed(db: &mut Storage, range: std::ops::Range<usize>) {
    for i in range {
        db.put(
            format!("key{:04}", i).into_bytes(),
            format!("value-{:04}", i).repeat(4).into_bytes(),
        )
        .unwrap();
    }
}

fn sst_count(dir: &Path) -> usize {
    fs::read_dir(dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .and_then(|s| s.to_str())
                == Some("sst")
        })
        .count()
}

#[test]
fn a_checkpoint_directory_is_a_readable_store() {
    let src = TempDir::new().unwrap();
    let dst = TempDir::new().unwrap();
    let target = dst.path().join("checkpoint");

    let mut db = Storage::with_config(src.path(), config()).unwrap();
    seed(&mut db, 0..300);
    let info = db.checkpoint(&target).unwrap();

    assert!(info.tables > 0, "expected flushed tables: {info:?}");
    assert_eq!(info.sequence, db.current_sequence());

    // No separate restore call: opening the directory is the restore.
    let restored = Storage::with_config(&target, config()).unwrap();
    for i in 0..300 {
        let key = format!("key{:04}", i).into_bytes();
        assert_eq!(
            restored.get(&key).unwrap(),
            Some(format!("value-{:04}", i).repeat(4).into_bytes()),
            "key {i} missing from the restored store"
        );
    }
}

#[test]
fn a_checkpoint_survives_destruction_of_the_original() {
    let src = TempDir::new().unwrap();
    let dst = TempDir::new().unwrap();
    let target = dst.path().join("checkpoint");

    let mut db = Storage::with_config(src.path(), config()).unwrap();
    seed(&mut db, 0..300);
    db.checkpoint(&target).unwrap();
    drop(db);

    // The disaster the checkpoint exists for: the data directory is gone.
    fs::remove_dir_all(src.path()).unwrap();

    let restored = Storage::with_config(&target, config()).unwrap();
    assert_eq!(
        restored.get(&b"key0150".to_vec()).unwrap(),
        Some(b"value-0150".repeat(4).to_vec())
    );
}

#[test]
fn a_checkpoint_is_unaffected_by_later_writes_and_compaction() {
    let src = TempDir::new().unwrap();
    let dst = TempDir::new().unwrap();
    let target = dst.path().join("checkpoint");

    let mut db = Storage::with_config(src.path(), config()).unwrap();
    seed(&mut db, 0..300);
    let info = db.checkpoint(&target).unwrap();

    // Overwrite everything the checkpoint captured, delete some of it, add
    // more, and force compaction to rewrite the tables it hard-linked.
    for i in 0..300 {
        db.put(format!("key{:04}", i).into_bytes(), b"CHANGED".to_vec())
            .unwrap();
    }
    for i in 0..50 {
        db.delete(&format!("key{:04}", i).into_bytes()).unwrap();
    }
    seed(&mut db, 300..600);
    db.compact_now().unwrap();

    let restored = Storage::with_config(&target, config()).unwrap();

    // The checkpoint still reads as of its own sequence.
    assert_eq!(
        restored.get(&b"key0000".to_vec()).unwrap(),
        Some(b"value-0000".repeat(4).to_vec()),
        "a key deleted after the checkpoint must still be present in it"
    );
    assert_eq!(
        restored.get(&b"key0299".to_vec()).unwrap(),
        Some(b"value-0299".repeat(4).to_vec()),
        "a key overwritten after the checkpoint must hold its old value"
    );
    assert_eq!(
        restored.get(&b"key0400".to_vec()).unwrap(),
        None,
        "a key written after the checkpoint must not appear in it"
    );
    assert_eq!(restored.current_sequence(), info.sequence);

    // And the live store is unharmed by the checkpoint's existence.
    assert_eq!(
        db.get(&b"key0299".to_vec()).unwrap(),
        Some(b"CHANGED".to_vec())
    );
    assert_eq!(db.get(&b"key0000".to_vec()).unwrap(), None);
}

#[test]
fn unflushed_writes_are_captured_through_the_wal() {
    let src = TempDir::new().unwrap();
    let dst = TempDir::new().unwrap();
    let target = dst.path().join("checkpoint");

    let mut db = Storage::with_config(src.path(), config()).unwrap();
    seed(&mut db, 0..300);
    // Sits in the memtable and the WAL, not in any SSTable.
    db.put(b"only-in-memtable".to_vec(), b"present".to_vec())
        .unwrap();

    let info = db.checkpoint(&target).unwrap();
    assert!(
        info.wal_bytes > 0,
        "the WAL should carry the unflushed write: {info:?}"
    );

    let restored = Storage::with_config(&target, config()).unwrap();
    assert_eq!(
        restored.get(&b"only-in-memtable".to_vec()).unwrap(),
        Some(b"present".to_vec())
    );
}

#[test]
fn the_wal_is_copied_not_linked_so_later_writes_cannot_leak_in() {
    let src = TempDir::new().unwrap();
    let dst = TempDir::new().unwrap();
    let target = dst.path().join("checkpoint");

    let mut db = Storage::with_config(src.path(), config()).unwrap();
    db.put(b"before".to_vec(), b"1".to_vec()).unwrap();
    db.checkpoint(&target).unwrap();

    // If the WAL had been hard-linked, this append would land in the
    // checkpoint's WAL too and show up on restore.
    db.put(b"after".to_vec(), b"2".to_vec()).unwrap();

    let restored = Storage::with_config(&target, config()).unwrap();
    assert_eq!(
        restored.get(&b"before".to_vec()).unwrap(),
        Some(b"1".to_vec())
    );
    assert_eq!(
        restored.get(&b"after".to_vec()).unwrap(),
        None,
        "a write made after the checkpoint leaked into it"
    );
}

#[test]
fn tables_are_shared_rather_than_duplicated() {
    let src = TempDir::new().unwrap();
    let dst = TempDir::new().unwrap();
    let target = dst.path().join("checkpoint");

    let mut db = Storage::with_config(src.path(), config()).unwrap();
    seed(&mut db, 0..300);
    let info = db.checkpoint(&target).unwrap();

    assert!(
        info.hard_linked,
        "expected hard links within one filesystem"
    );
    assert_eq!(
        sst_count(&target),
        info.tables,
        "every live table should be captured"
    );
    // The tables are visible in full, but only the WAL was really duplicated.
    assert!(info.table_bytes > 0);
    assert_eq!(info.bytes_duplicated(), info.wal_bytes);
    assert!(
        info.bytes_duplicated() < info.table_bytes,
        "linking should cost far less than the data it captures: {info:?}"
    );
}

#[test]
fn a_populated_target_is_refused() {
    let src = TempDir::new().unwrap();
    let dst = TempDir::new().unwrap();
    let target = dst.path().join("checkpoint");
    fs::create_dir_all(&target).unwrap();
    fs::write(target.join("L0_1.sst"), b"someone else's table").unwrap();

    let mut db = Storage::with_config(src.path(), config()).unwrap();
    seed(&mut db, 0..20);

    let err = db.checkpoint(&target).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
    // Refused without touching what was already there.
    assert_eq!(
        fs::read(target.join("L0_1.sst")).unwrap(),
        b"someone else's table"
    );

    // An empty directory is fine.
    let empty = dst.path().join("empty");
    fs::create_dir_all(&empty).unwrap();
    db.checkpoint(&empty).unwrap();
}

#[test]
fn checkpoints_can_be_chained_and_each_holds_its_own_instant() {
    let src = TempDir::new().unwrap();
    let dst = TempDir::new().unwrap();

    let mut db = Storage::with_config(src.path(), config()).unwrap();
    seed(&mut db, 0..100);
    let first = db.checkpoint(dst.path().join("first")).unwrap();

    seed(&mut db, 100..200);
    let second = db.checkpoint(dst.path().join("second")).unwrap();

    assert!(second.sequence > first.sequence);

    let a = Storage::with_config(&first.path, config()).unwrap();
    let b = Storage::with_config(&second.path, config()).unwrap();

    assert_eq!(a.get(&b"key0150".to_vec()).unwrap(), None);
    assert_eq!(
        b.get(&b"key0150".to_vec()).unwrap(),
        Some(b"value-0150".repeat(4).to_vec())
    );
}

#[test]
fn shared_storage_checkpoints_under_the_write_lock() {
    let src = TempDir::new().unwrap();
    let dst = TempDir::new().unwrap();
    let target = dst.path().join("checkpoint");

    let db = SharedStorage::with_config(src.path(), config()).unwrap();
    for i in 0..300 {
        db.put(
            format!("key{:04}", i).into_bytes(),
            format!("value-{:04}", i).repeat(4).into_bytes(),
        )
        .unwrap();
    }

    let info = db.checkpoint(&target).unwrap();
    assert_eq!(info.sequence, db.current_sequence().unwrap());

    let restored = Storage::with_config(&target, config()).unwrap();
    assert_eq!(
        restored.get(&b"key0200".to_vec()).unwrap(),
        Some(b"value-0200".repeat(4).to_vec())
    );

    // The store keeps working afterwards.
    db.put(b"after".to_vec(), b"ok".to_vec()).unwrap();
    assert_eq!(db.get(&b"after".to_vec()).unwrap(), Some(b"ok".to_vec()));
}

#[test]
fn taking_a_checkpoint_is_counted_in_the_metrics() {
    let src = TempDir::new().unwrap();
    let dst = TempDir::new().unwrap();

    let mut db = Storage::with_config(src.path(), config()).unwrap();
    seed(&mut db, 0..20);
    assert_eq!(db.stats().checkpoints_total, 0);

    db.checkpoint(dst.path().join("one")).unwrap();
    db.checkpoint(dst.path().join("two")).unwrap();

    let stats = db.stats();
    assert_eq!(stats.checkpoints_total, 2);
    assert!(stats.to_prometheus().contains("lsm_checkpoints_total 2"));
}

#[test]
fn the_restored_sequence_matches_the_captured_one() {
    // Regression: the manifest's sequence deliberately lags the in-memory
    // counter — it is written at each flush, and the WAL carries the writes
    // since. Recovery rebuilds the current sequence as `manifest.last_seq +
    // replayed records`, so a checkpoint that stamps the *live* counter into
    // the manifest while also copying that WAL counts those writes twice and
    // restores a store whose sequence has run ahead of its data. The values
    // read back would still be right, but every time-travel coordinate would
    // be off by the number of unflushed writes.
    let src = TempDir::new().unwrap();
    let dst = TempDir::new().unwrap();

    let mut db = Storage::with_config(src.path(), config()).unwrap();
    seed(&mut db, 0..300);

    // Leave writes sitting in the WAL, unflushed, so the lag is non-zero.
    db.put(b"unflushed".to_vec(), b"x".to_vec()).unwrap();

    let target = dst.path().join("checkpoint");
    let info = db.checkpoint(&target).unwrap();
    assert!(info.wal_bytes > 0, "test needs a non-empty WAL: {info:?}");

    let restored = Storage::with_config(&target, config()).unwrap();
    assert_eq!(
        restored.current_sequence(),
        info.sequence,
        "restored sequence must equal the captured one, not overshoot it"
    );

    // And a snapshot at the captured sequence still sees the whole capture.
    let snap = restored.snapshot_at(info.sequence).unwrap();
    assert_eq!(
        restored.get_at(&snap, &b"unflushed".to_vec()).unwrap(),
        Some(b"x".to_vec())
    );
}
