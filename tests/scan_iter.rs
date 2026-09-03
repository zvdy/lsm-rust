//! Integration tests for streaming range scans.

use lsm_rust::{SharedStorage, Storage, StorageConfig};
use std::fs;
use tempfile::TempDir;

/// Small thresholds so the data really does span several SSTables and levels,
/// which is what exercises the k-way merge.
fn config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 4 * 1024,
        inline_compaction: false,
        block_cache_size: 0,
        ..StorageConfig::default()
    }
}

/// The live entries a fixture wrote, in key order.
type Entries = Vec<(Vec<u8>, Vec<u8>)>;

/// Populate a store spread across many SSTables plus a live memtable.
fn multi_level_store(dir: &TempDir) -> (Storage, Entries) {
    let mut db = Storage::with_config(dir.path(), config()).unwrap();
    let mut expected = Vec::new();
    for i in 0..600 {
        let k = format!("key{i:04}").into_bytes();
        let v = format!("value-{i:04}").repeat(3).into_bytes();
        db.put(k.clone(), v.clone()).unwrap();
        expected.push((k, v));
    }
    // Overwrite some keys so several versions exist across levels.
    for i in (0..600).step_by(7) {
        let k = format!("key{i:04}").into_bytes();
        let v = format!("updated-{i:04}").repeat(3).into_bytes();
        db.put(k.clone(), v.clone()).unwrap();
        expected[i].1 = v;
    }
    // Delete some keys.
    for i in (0..600).step_by(11) {
        db.delete(&format!("key{i:04}").into_bytes()).unwrap();
    }
    let expected: Entries = expected
        .into_iter()
        .enumerate()
        .filter(|(i, _)| !i.is_multiple_of(11))
        .map(|(_, kv)| kv)
        .collect();
    (db, expected)
}

#[test]
fn streaming_scan_matches_the_materializing_scan() {
    let temp = TempDir::new().unwrap();
    let (db, expected) = multi_level_store(&temp);

    let streamed: Vec<_> = db
        .scan_iter(b"key", Some(b"kez"))
        .unwrap()
        .collect::<lsm_rust::Result<Vec<_>>>()
        .unwrap();
    let materialized = db.scan(b"key", b"kez").unwrap();

    assert_eq!(streamed, materialized, "iterator disagrees with scan()");
    assert_eq!(streamed, expected, "wrong contents");
    // Ordered by key, no duplicates.
    assert!(streamed.windows(2).all(|w| w[0].0 < w[1].0));
}

#[test]
fn scan_respects_range_bounds() {
    let temp = TempDir::new().unwrap();
    let (db, _) = multi_level_store(&temp);

    let got: Vec<_> = db
        .scan_iter(b"key0100", Some(b"key0200"))
        .unwrap()
        .map(|e| e.unwrap().0)
        .collect();
    assert!(!got.is_empty());
    assert!(got.iter().all(|k| k.as_slice() >= b"key0100".as_slice()));
    assert!(got.iter().all(|k| k.as_slice() < b"key0200".as_slice()));
    assert_eq!(
        got,
        db.scan(b"key0100", b"key0200")
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect::<Vec<_>>()
    );
}

#[test]
fn unbounded_end_streams_to_the_end() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::with_config(temp.path(), config()).unwrap();
    for i in 0..50 {
        db.put(format!("k{i:03}").into_bytes(), b"v".to_vec())
            .unwrap();
    }
    let count = db.scan_iter(b"k", None).unwrap().count();
    assert_eq!(count, 50);
}

#[test]
fn prefix_iter_matches_prefix_scan() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::with_config(temp.path(), config()).unwrap();
    for i in 0..200 {
        db.put(format!("user:{i:03}").into_bytes(), b"u".to_vec())
            .unwrap();
        db.put(format!("acct:{i:03}").into_bytes(), b"a".to_vec())
            .unwrap();
    }

    let streamed: Vec<_> = db
        .scan_prefix_iter(b"user:")
        .unwrap()
        .collect::<lsm_rust::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(streamed, db.scan_prefix(b"user:").unwrap());
    assert_eq!(streamed.len(), 200);
    assert!(streamed.iter().all(|(k, _)| k.starts_with(b"user:")));
}

#[test]
fn partial_consumption_is_cheap_and_correct() {
    // Taking a handful of entries from a large range must not require reading
    // (or returning) the whole range.
    let temp = TempDir::new().unwrap();
    let (db, expected) = multi_level_store(&temp);

    let first_five: Vec<_> = db
        .scan_iter(b"key", Some(b"kez"))
        .unwrap()
        .take(5)
        .map(|e| e.unwrap())
        .collect();
    assert_eq!(first_five, expected[..5].to_vec());
}

#[test]
fn scan_iter_at_snapshot_ignores_later_writes() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::with_config(temp.path(), config()).unwrap();
    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
    db.put(b"b".to_vec(), b"1".to_vec()).unwrap();

    let snap = db.snapshot();
    db.put(b"a".to_vec(), b"2".to_vec()).unwrap();
    db.delete(&b"b".to_vec()).unwrap();
    db.put(b"c".to_vec(), b"1".to_vec()).unwrap();

    let seen: Vec<_> = db
        .scan_iter_at(&snap, b"a", Some(b"z"))
        .unwrap()
        .map(|e| e.unwrap())
        .collect();
    assert_eq!(
        seen,
        vec![
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"1".to_vec()),
        ]
    );
    // The live view has moved on.
    assert_eq!(
        db.scan(b"a", b"z").unwrap(),
        vec![
            (b"a".to_vec(), b"2".to_vec()),
            (b"c".to_vec(), b"1".to_vec()),
        ]
    );
}

#[test]
fn corruption_surfaces_as_an_error_not_a_panic() {
    // The scan reads blocks lazily, so a corrupted block must come back as an
    // Err item rather than panicking or yielding wrong data.
    let temp = TempDir::new().unwrap();
    {
        let (_db, _) = multi_level_store(&temp);
    }
    let sst = fs::read_dir(temp.path())
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            (p.extension().and_then(|s| s.to_str()) == Some("sst")).then_some(p)
        })
        .max_by_key(|p| fs::metadata(p).unwrap().len())
        .expect("an SSTable");

    let mut bytes = fs::read(&sst).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xFF; // damage the final data block
    fs::write(&sst, &bytes).unwrap();

    let db = Storage::with_config(temp.path(), config()).unwrap();
    let mut saw_error = false;
    for entry in db.scan_iter(b"key", Some(b"kez")).unwrap() {
        if entry.is_err() {
            saw_error = true;
            break;
        }
    }
    assert!(saw_error, "corrupted block was not reported by the scan");
}

#[test]
fn shared_storage_streams_without_materializing() {
    let temp = TempDir::new().unwrap();
    let db = SharedStorage::with_config(temp.path(), config()).unwrap();
    for i in 0..300 {
        db.put(format!("k{i:04}").into_bytes(), b"v".to_vec())
            .unwrap();
    }

    let mut count = 0usize;
    let mut last: Option<Vec<u8>> = None;
    db.scan_for_each(b"k", None, |key, _value| {
        if let Some(prev) = &last {
            assert!(prev < &key, "out of order");
        }
        last = Some(key);
        count += 1;
        Ok(())
    })
    .unwrap();
    assert_eq!(count, 300);

    // Prefix variant, and an early error from the callback propagates.
    let mut seen = 0usize;
    let err = db
        .scan_prefix_for_each(b"k", |_k, _v| {
            seen += 1;
            if seen == 10 {
                Err(std::io::Error::other("stop").into())
            } else {
                Ok(())
            }
        })
        .unwrap_err();
    assert_eq!(seen, 10);
    // The callback's own error reaches the caller unchanged, as `Error::Io`.
    assert!(matches!(err, lsm_rust::Error::Io(_)), "{err:?}");
    assert!(err.to_string().contains("stop"), "{err}");
}
