//! Integration tests for MVCC snapshot isolation.
//!
//! A snapshot must observe exactly the state that had committed when it was
//! taken — even as later writes, deletes, flushes, and compactions change the
//! live store underneath it.

use lsm_rust::{SharedStorage, Storage, StorageConfig};
use std::thread;
use tempfile::TempDir;

/// Small thresholds so a handful of writes cross flush and compaction points.
fn small_config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 4 * 1024,
        compaction_size_threshold: 16 * 1024,
        ..StorageConfig::default()
    }
}

/// Write enough filler to force at least one memtable flush.
fn force_flush(storage: &mut Storage, tag: &str) {
    let filler = vec![b'x'; 512];
    for i in 0..12 {
        storage
            .put(
                format!("__filler_{}_{}", tag, i).into_bytes(),
                filler.clone(),
            )
            .unwrap();
    }
}

#[test]
fn snapshot_hides_later_writes_and_deletes() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::new(temp_dir.path(), false).unwrap();

    storage.put(b"a".to_vec(), b"a1".to_vec()).unwrap();
    storage.put(b"b".to_vec(), b"b1".to_vec()).unwrap();

    let snap = storage.snapshot();

    // Mutate everything after the snapshot
    storage.put(b"a".to_vec(), b"a2".to_vec()).unwrap();
    storage.delete(&b"b".to_vec()).unwrap();
    storage.put(b"c".to_vec(), b"c1".to_vec()).unwrap();

    // The snapshot still sees the original committed state
    assert_eq!(
        storage.get_at(&snap, &b"a".to_vec()).unwrap(),
        Some(b"a1".to_vec())
    );
    assert_eq!(
        storage.get_at(&snap, &b"b".to_vec()).unwrap(),
        Some(b"b1".to_vec())
    );
    assert_eq!(storage.get_at(&snap, &b"c".to_vec()).unwrap(), None);

    // Latest reads see the new state
    assert_eq!(storage.get(&b"a".to_vec()).unwrap(), Some(b"a2".to_vec()));
    assert_eq!(storage.get(&b"b".to_vec()).unwrap(), None);
    assert_eq!(storage.get(&b"c".to_vec()).unwrap(), Some(b"c1".to_vec()));
}

#[test]
fn snapshot_taken_before_a_key_exists_never_sees_it() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::new(temp_dir.path(), false).unwrap();

    let snap = storage.snapshot(); // empty store
    storage.put(b"k".to_vec(), b"v".to_vec()).unwrap();

    assert_eq!(storage.get_at(&snap, &b"k".to_vec()).unwrap(), None);
    assert_eq!(storage.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
}

#[test]
fn snapshot_reads_old_value_across_flush_and_compaction() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::with_config(temp_dir.path(), small_config()).unwrap();

    storage.put(b"key".to_vec(), b"original".to_vec()).unwrap();
    let snap = storage.snapshot();

    // Overwrite the key repeatedly, forcing many flushes and at least one
    // level-0 compaction while the snapshot is alive. Its pinned version must
    // survive garbage collection.
    for round in 0..8 {
        storage
            .put(b"key".to_vec(), format!("v{}", round).into_bytes())
            .unwrap();
        force_flush(&mut storage, &format!("r{}", round));
    }

    assert_eq!(
        storage.get_at(&snap, &b"key".to_vec()).unwrap(),
        Some(b"original".to_vec()),
        "snapshot must still read the version pinned at creation"
    );
    assert_eq!(storage.get(&b"key".to_vec()).unwrap(), Some(b"v7".to_vec()));
}

#[test]
fn snapshot_sees_deleted_key_that_was_alive_at_capture() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::with_config(temp_dir.path(), small_config()).unwrap();

    storage.put(b"doomed".to_vec(), b"alive".to_vec()).unwrap();
    let snap = storage.snapshot();

    storage.delete(&b"doomed".to_vec()).unwrap();
    for round in 0..6 {
        force_flush(&mut storage, &format!("d{}", round));
    }

    // The delete (and its GC) must not reach back into the snapshot
    assert_eq!(
        storage.get_at(&snap, &b"doomed".to_vec()).unwrap(),
        Some(b"alive".to_vec())
    );
    assert_eq!(storage.get(&b"doomed".to_vec()).unwrap(), None);
}

#[test]
fn scan_at_snapshot_is_consistent() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::with_config(temp_dir.path(), small_config()).unwrap();

    for i in 0..20 {
        storage
            .put(format!("k{:02}", i).into_bytes(), b"orig".to_vec())
            .unwrap();
    }
    let snap = storage.snapshot();

    // Change the keyspace after the snapshot, with flushes/compaction
    for i in 0..20 {
        if i % 2 == 0 {
            storage.delete(&format!("k{:02}", i).into_bytes()).unwrap();
        } else {
            storage
                .put(format!("k{:02}", i).into_bytes(), b"changed".to_vec())
                .unwrap();
        }
    }
    for round in 0..5 {
        force_flush(&mut storage, &format!("s{}", round));
    }

    // The snapshot scan still returns all 20 original keys with original values
    let snap_view = storage.scan_at(&snap, b"k", b"l").unwrap();
    assert_eq!(snap_view.len(), 20);
    assert!(snap_view.iter().all(|(_, v)| v == b"orig"));
    assert!(snap_view.windows(2).all(|w| w[0].0 < w[1].0)); // sorted

    // The latest scan reflects the mutations: evens gone, odds changed
    let latest: Vec<_> = storage
        .scan(b"k", b"l")
        .unwrap()
        .into_iter()
        .filter(|(k, _)| !k.starts_with(b"__filler"))
        .collect();
    assert_eq!(latest.len(), 10);
    assert!(latest.iter().all(|(_, v)| v == b"changed"));
}

#[test]
fn multiple_snapshots_are_independent() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::new(temp_dir.path(), false).unwrap();

    storage.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
    let s1 = storage.snapshot();
    storage.put(b"k".to_vec(), b"v2".to_vec()).unwrap();
    let s2 = storage.snapshot();
    storage.put(b"k".to_vec(), b"v3".to_vec()).unwrap();

    assert_eq!(
        storage.get_at(&s1, &b"k".to_vec()).unwrap(),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        storage.get_at(&s2, &b"k".to_vec()).unwrap(),
        Some(b"v2".to_vec())
    );
    assert_eq!(storage.get(&b"k".to_vec()).unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn shared_storage_snapshot_is_stable_under_concurrent_writes() {
    let temp_dir = TempDir::new().unwrap();
    let db = SharedStorage::with_config(temp_dir.path(), small_config()).unwrap();

    for i in 0..50 {
        db.put(format!("k{:03}", i).into_bytes(), b"orig".to_vec())
            .unwrap();
    }
    let snap = db.snapshot().unwrap();

    // Hammer the store with concurrent writers while reading the snapshot
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..50 {
                    db.put(
                        format!("k{:03}", i).into_bytes(),
                        format!("w{}", w).into_bytes(),
                    )
                    .unwrap();
                }
            })
        })
        .collect();

    // Concurrently, the snapshot view must stay pinned to "orig"
    for _ in 0..200 {
        for i in [0usize, 17, 49] {
            assert_eq!(
                db.get_at(&snap, &format!("k{:03}", i).into_bytes())
                    .unwrap(),
                Some(b"orig".to_vec())
            );
        }
    }

    for writer in writers {
        writer.join().unwrap();
    }

    // Snapshot unchanged; latest reflects the writers
    assert_eq!(
        db.get_at(&snap, &b"k000".to_vec()).unwrap(),
        Some(b"orig".to_vec())
    );
    assert!(db
        .get(&b"k000".to_vec())
        .unwrap()
        .unwrap()
        .starts_with(b"w"));
}
