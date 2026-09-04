//! The memtable's tracked size drives flushing, so it has to stay honest.
//!
//! Every op in a write batch commits at one sequence number, so a batch that
//! writes the same key twice inserts the same `(key, seq)` twice. If the
//! replaced value's bytes are subtracted without the new value's being added,
//! the tracked size falls towards zero while the table keeps growing — and
//! the flush threshold, which is a memory bound, silently stops firing.

use lsm_rust::{Storage, StorageConfig, WriteBatch};
use tempfile::TempDir;

const THRESHOLD: usize = 8 * 1024;

fn config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: THRESHOLD,
        inline_compaction: false,
        block_cache_size: 0,
        ..StorageConfig::default()
    }
}

#[test]
fn batches_that_rewrite_one_key_still_flush() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    // 50 batches × 4 KiB of live data each: far past the 8 KiB threshold.
    for _ in 0..50 {
        let mut batch = WriteBatch::new();
        batch.put(b"k".to_vec(), vec![b'a'; 4096]);
        batch.put(b"k".to_vec(), vec![b'b'; 4096]);
        db.write_batch(batch).unwrap();
    }

    let stats = db.stats();
    assert!(
        stats.flushes_total > 0,
        "200 KiB written against a {THRESHOLD}-byte threshold never flushed: {stats:?}"
    );
    assert!(
        stats.memtable_bytes <= (THRESHOLD * 2) as u64,
        "memtable grew past its threshold unnoticed: {stats:?}"
    );
    // The last write still wins.
    assert_eq!(db.get(&b"k".to_vec()).unwrap(), Some(vec![b'b'; 4096]));
}

#[test]
fn the_reported_size_tracks_what_the_memtable_holds() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(
        dir.path(),
        StorageConfig {
            // Large enough that nothing flushes and the gauge is comparable.
            memtable_size_threshold: 8 * 1024 * 1024,
            ..config()
        },
    )
    .unwrap();

    for i in 0..40 {
        let mut batch = WriteBatch::new();
        // Two writes to one key, then one to a distinct key.
        batch.put(b"hot".to_vec(), vec![b'a'; 1024]);
        batch.put(b"hot".to_vec(), vec![b'b'; 2048]);
        batch.put(format!("cold{i:03}").into_bytes(), vec![b'c'; 512]);
        db.write_batch(batch).unwrap();
    }

    let stats = db.stats();
    assert_eq!(stats.flushes_total, 0, "test needs everything in memory");

    // 40 versions of "hot" at 2048 bytes each, plus 40 cold keys at 512.
    let live_values = 40 * 2048 + 40 * 512;
    assert!(
        stats.memtable_bytes >= live_values as u64,
        "reported {} bytes for at least {live_values} bytes of values: {stats:?}",
        stats.memtable_bytes
    );
}
