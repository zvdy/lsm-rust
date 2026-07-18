//! Integration tests for atomic write batches.

use lsm_rust::{SharedStorage, Storage, StorageConfig, WriteBatch};
use tempfile::TempDir;

#[test]
fn batch_applies_all_operations() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::new(temp_dir.path(), false).unwrap();

    storage.put(b"stale".to_vec(), b"old".to_vec()).unwrap();

    let mut batch = WriteBatch::new();
    batch
        .put(b"a".to_vec(), b"1".to_vec())
        .put(b"b".to_vec(), b"2".to_vec())
        .delete(b"stale".to_vec());
    assert_eq!(batch.len(), 3);
    storage.write_batch(batch).unwrap();

    assert_eq!(storage.get(&b"a".to_vec()).unwrap(), Some(b"1".to_vec()));
    assert_eq!(storage.get(&b"b".to_vec()).unwrap(), Some(b"2".to_vec()));
    assert_eq!(storage.get(&b"stale".to_vec()).unwrap(), None);
}

#[test]
fn empty_batch_is_a_noop() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::new(temp_dir.path(), false).unwrap();
    storage.write_batch(WriteBatch::new()).unwrap();
    assert_eq!(storage.get(&b"nothing".to_vec()).unwrap(), None);
}

#[test]
fn batch_later_op_supersedes_earlier_for_same_key() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::new(temp_dir.path(), false).unwrap();

    let mut batch = WriteBatch::new();
    batch
        .put(b"k".to_vec(), b"first".to_vec())
        .put(b"k".to_vec(), b"second".to_vec());
    storage.write_batch(batch).unwrap();

    assert_eq!(
        storage.get(&b"k".to_vec()).unwrap(),
        Some(b"second".to_vec())
    );
}

#[test]
fn batch_is_atomically_visible_to_snapshots() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::new(temp_dir.path(), false).unwrap();

    storage.put(b"x".to_vec(), b"x0".to_vec()).unwrap();
    let before = storage.snapshot();

    let mut batch = WriteBatch::new();
    batch
        .put(b"x".to_vec(), b"x1".to_vec())
        .put(b"y".to_vec(), b"y1".to_vec());
    storage.write_batch(batch).unwrap();

    let after = storage.snapshot();

    // The pre-batch snapshot sees none of the batch...
    assert_eq!(
        storage.get_at(&before, &b"x".to_vec()).unwrap(),
        Some(b"x0".to_vec())
    );
    assert_eq!(storage.get_at(&before, &b"y".to_vec()).unwrap(), None);
    // ...the post-batch snapshot sees all of it
    assert_eq!(
        storage.get_at(&after, &b"x".to_vec()).unwrap(),
        Some(b"x1".to_vec())
    );
    assert_eq!(
        storage.get_at(&after, &b"y".to_vec()).unwrap(),
        Some(b"y1".to_vec())
    );
}

#[test]
fn batch_survives_restart() {
    let temp_dir = TempDir::new().unwrap();
    {
        let mut storage = Storage::new(temp_dir.path(), false).unwrap();
        let mut batch = WriteBatch::new();
        for i in 0..100 {
            batch.put(
                format!("k{:03}", i).into_bytes(),
                format!("v{}", i).into_bytes(),
            );
        }
        batch.delete(b"k050".to_vec());
        storage.write_batch(batch).unwrap();
    }

    // Reopen from the WAL: the whole batch must have been recovered
    let recovered = Storage::new(temp_dir.path(), false).unwrap();
    assert_eq!(
        recovered.get(&b"k000".to_vec()).unwrap(),
        Some(b"v0".to_vec())
    );
    assert_eq!(
        recovered.get(&b"k099".to_vec()).unwrap(),
        Some(b"v99".to_vec())
    );
    assert_eq!(recovered.get(&b"k050".to_vec()).unwrap(), None);
}

#[test]
fn batch_survives_restart_after_flush() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 4 * 1024,
        ..StorageConfig::default()
    };
    {
        let mut storage = Storage::with_config(temp_dir.path(), config.clone()).unwrap();
        // A batch large enough to flush to an SSTable
        let mut batch = WriteBatch::new();
        for i in 0..500 {
            batch.put(format!("k{:04}", i).into_bytes(), vec![b'v'; 64]);
        }
        storage.write_batch(batch).unwrap();
    }

    let recovered = Storage::with_config(temp_dir.path(), config).unwrap();
    assert_eq!(
        recovered.get(&b"k0000".to_vec()).unwrap(),
        Some(vec![b'v'; 64])
    );
    assert_eq!(
        recovered.get(&b"k0499".to_vec()).unwrap(),
        Some(vec![b'v'; 64])
    );
}

#[test]
fn shared_storage_write_batch() {
    let temp_dir = TempDir::new().unwrap();
    let db = SharedStorage::new(temp_dir.path(), false).unwrap();

    let mut batch = WriteBatch::new();
    batch
        .put(b"a".to_vec(), b"1".to_vec())
        .put(b"b".to_vec(), b"2".to_vec());
    db.write_batch(batch).unwrap();

    assert_eq!(db.get(&b"a".to_vec()).unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(&b"b".to_vec()).unwrap(), Some(b"2".to_vec()));
}
