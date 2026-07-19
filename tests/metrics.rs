//! Integration tests for the operational metrics / Prometheus endpoint.

use lsm_rust::{SharedStorage, Storage, StorageConfig};
use tempfile::TempDir;

#[test]
fn stats_reflect_operations() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::new(temp.path(), false).unwrap();

    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
    db.put(b"b".to_vec(), b"2".to_vec()).unwrap();
    db.delete(&b"a".to_vec()).unwrap();
    let _ = db.get(&b"b".to_vec()).unwrap();
    let _ = db.get(&b"missing".to_vec()).unwrap();
    let _ = db.scan(b"a", b"z").unwrap();

    let stats = db.stats();
    assert_eq!(stats.puts_total, 2);
    assert_eq!(stats.deletes_total, 1);
    assert_eq!(stats.gets_total, 2);
    assert_eq!(stats.scans_total, 1);
    // Three writes each advance the sequence counter
    assert_eq!(stats.sequence, 3);
    assert!(stats.memtable_entries >= 3);
    assert!(stats.memtable_bytes > 0);
    // Nothing has flushed yet, so there are no SSTables
    assert_eq!(stats.total_sstables(), 0);
}

#[test]
fn stats_report_sstables_after_flush() {
    let temp = TempDir::new().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 4 * 1024,
        ..StorageConfig::default()
    };
    let mut db = Storage::with_config(temp.path(), config).unwrap();

    for i in 0..500 {
        db.put(format!("k{:04}", i).into_bytes(), vec![b'v'; 64])
            .unwrap();
    }

    let stats = db.stats();
    assert!(stats.flushes_total >= 1, "at least one flush should occur");
    assert!(
        stats.total_sstables() >= 1,
        "flushed data should appear as SSTables"
    );
    assert!(stats.total_sstable_bytes() > 0);
    // Prometheus rendering carries a per-level series for whichever level(s)
    // the flushed data ended up on (level 0, or deeper after compaction).
    let text = stats.to_prometheus();
    assert!(text.contains("lsm_sstables{level="));
}

#[test]
fn live_snapshots_are_counted() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::new(temp.path(), false).unwrap();
    db.put(b"k".to_vec(), b"v".to_vec()).unwrap();

    assert_eq!(db.stats().live_snapshots, 0);
    let snap = db.snapshot();
    assert_eq!(db.stats().live_snapshots, 1);
    {
        let _snap2 = db.snapshot();
        assert_eq!(db.stats().live_snapshots, 2);
    }
    assert_eq!(db.stats().live_snapshots, 1);
    drop(snap);
    assert_eq!(db.stats().live_snapshots, 0);
}

#[test]
fn shared_storage_stats() {
    let temp = TempDir::new().unwrap();
    let db = SharedStorage::new(temp.path(), false).unwrap();
    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();

    let stats = db.stats().unwrap();
    assert_eq!(stats.puts_total, 1);
    assert!(stats.to_prometheus().contains("lsm_puts_total 1"));
}
