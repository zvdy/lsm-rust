//! Integration tests for time-travel reads: opening a snapshot positioned at
//! a specific historical (persisted) sequence number.

use lsm_rust::{SharedStorage, Storage, StorageConfig};
use tempfile::TempDir;

#[test]
fn snapshot_at_reads_historical_versions() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::new(temp.path(), false).unwrap();

    db.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
    let after_v1 = db.current_sequence();
    db.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
    let after_v2 = db.current_sequence();
    db.put(b"k".to_vec(), b"v3".to_vec()).unwrap();

    // Travel back to each recorded checkpoint
    let s1 = db.snapshot_at(after_v1).unwrap();
    let s2 = db.snapshot_at(after_v2).unwrap();
    assert_eq!(
        db.get_at(&s1, &b"k".to_vec()).unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        db.get_at(&s2, &b"k".to_vec()).unwrap(),
        Some(b"v2".to_vec())
    );
    // The live view still sees the latest
    assert_eq!(db.get(&b"k".to_vec()).unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn snapshot_at_zero_sees_an_empty_store() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::new(temp.path(), false).unwrap();
    db.put(b"k".to_vec(), b"v".to_vec()).unwrap();

    // Sequence 0 predates every write
    let genesis = db.snapshot_at(0).unwrap();
    assert_eq!(db.get_at(&genesis, &b"k".to_vec()).unwrap(), None);
}

#[test]
fn snapshot_at_future_sequence_is_rejected() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::new(temp.path(), false).unwrap();
    db.put(b"k".to_vec(), b"v".to_vec()).unwrap();

    let now = db.current_sequence();
    let err = db.snapshot_at(now + 1).unwrap_err();
    assert!(
        matches!(err, lsm_rust::Error::InvalidArgument(_)),
        "expected InvalidArgument, got {err:?}"
    );
}

#[test]
fn time_travel_sees_deletes_at_the_right_point() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::new(temp.path(), false).unwrap();

    db.put(b"k".to_vec(), b"alive".to_vec()).unwrap();
    let before_delete = db.current_sequence();
    db.delete(&b"k".to_vec()).unwrap();
    let after_delete = db.current_sequence();

    let s_before = db.snapshot_at(before_delete).unwrap();
    let s_after = db.snapshot_at(after_delete).unwrap();
    assert_eq!(
        db.get_at(&s_before, &b"k".to_vec()).unwrap(),
        Some(b"alive".to_vec())
    );
    assert_eq!(db.get_at(&s_after, &b"k".to_vec()).unwrap(), None);
}

#[test]
fn time_travel_survives_restart_via_persisted_sequence() {
    let temp = TempDir::new().unwrap();
    // Keep compaction off the write path so historical versions are retained
    // across the flush that persists them.
    let config = StorageConfig {
        memtable_size_threshold: 4 * 1024,
        inline_compaction: false,
        ..StorageConfig::default()
    };

    let checkpoint;
    {
        let mut db = Storage::with_config(temp.path(), config.clone()).unwrap();
        db.put(b"account".to_vec(), b"100".to_vec()).unwrap();
        checkpoint = db.current_sequence();
        db.put(b"account".to_vec(), b"250".to_vec()).unwrap();
        // Flush both versions to an SSTable (write_versioned keeps every version)
        for i in 0..500 {
            db.put(format!("filler{:04}", i).into_bytes(), vec![b'x'; 64])
                .unwrap();
        }
    }

    // Reopen: the sequence counter is restored from the manifest, so the
    // checkpoint recorded before the restart is still meaningful.
    let db = Storage::with_config(temp.path(), config).unwrap();
    assert!(db.current_sequence() >= checkpoint);
    let snap = db.snapshot_at(checkpoint).unwrap();
    assert_eq!(
        db.get_at(&snap, &b"account".to_vec()).unwrap(),
        Some(b"100".to_vec())
    );
    // Latest value is the newer one
    assert_eq!(db.get(&b"account".to_vec()).unwrap(), Some(b"250".to_vec()));
}

#[test]
fn time_travel_scan_is_consistent() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::new(temp.path(), false).unwrap();

    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
    db.put(b"b".to_vec(), b"1".to_vec()).unwrap();
    let checkpoint = db.current_sequence();
    // Mutate the keyspace after the checkpoint
    db.put(b"a".to_vec(), b"2".to_vec()).unwrap();
    db.delete(&b"b".to_vec()).unwrap();
    db.put(b"c".to_vec(), b"1".to_vec()).unwrap();

    let snap = db.snapshot_at(checkpoint).unwrap();
    let scanned = db.scan_at(&snap, b"a", b"z").unwrap();
    assert_eq!(
        scanned,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"1".to_vec()),
            // c did not exist yet at the checkpoint
        ]
    );
}

#[test]
fn shared_storage_time_travel() {
    let temp = TempDir::new().unwrap();
    let db = SharedStorage::new(temp.path(), false).unwrap();

    db.put(b"k".to_vec(), b"old".to_vec()).unwrap();
    let checkpoint = db.current_sequence().unwrap();
    db.put(b"k".to_vec(), b"new".to_vec()).unwrap();

    let snap = db.snapshot_at(checkpoint).unwrap();
    assert_eq!(
        db.get_at(&snap, &b"k".to_vec()).unwrap(),
        Some(b"old".to_vec())
    );
    assert_eq!(db.get(&b"k".to_vec()).unwrap(), Some(b"new".to_vec()));

    // Future sequence rejected through the shared handle too
    let future = db.current_sequence().unwrap() + 10;
    assert!(db.snapshot_at(future).is_err());
}
