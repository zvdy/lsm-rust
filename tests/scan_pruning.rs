//! Scans skip SSTables whose key range cannot match.
//!
//! `ScanIter` used to open a cursor over every table in the store, so a narrow
//! scan of a large store paid a cursor, a heap slot and — for tables lying
//! entirely below the scan — a block read that yielded only keys the merge then
//! discarded. The check is exact, comparing the scan's bounds against each
//! table's real first and last keys, so it can never drop a matching key.

use lsm_rust::{Storage, StorageConfig};
use tempfile::TempDir;

/// Small memtable, no automatic compaction: consecutive key ranges land in
/// their own level-0 tables and stay there, which is what gives a scan many
/// tables it can rule out.
fn config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 2 * 1024,
        inline_compaction: false,
        block_cache_size: 0,
        ..StorageConfig::default()
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("key{:05}", i).into_bytes()
}

/// Ascending keys, so each flushed table covers a distinct slice.
fn seeded(dir: &TempDir, count: usize) -> Storage {
    let mut db = Storage::with_config(dir.path(), config()).unwrap();
    for i in 0..count {
        db.put(key(i), vec![b'v'; 64]).unwrap();
    }
    db
}

#[test]
fn a_narrow_scan_skips_the_tables_it_cannot_match() {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir, 2000);

    let tables: u64 = db.stats().levels.iter().map(|l| l.num_sstables).sum();
    assert!(tables > 4, "test needs several tables, got {tables}");

    let before = db.stats().scan_tables_pruned_total;
    let found = db.scan(&key(0), &key(5)).unwrap();
    let pruned = db.stats().scan_tables_pruned_total - before;

    assert_eq!(found.len(), 5);
    assert!(
        pruned > 0,
        "a scan of 5 keys across {tables} tables pruned nothing"
    );
}

#[test]
fn pruning_never_drops_a_matching_key() {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir, 2000);

    // Every window, including ones straddling table boundaries, must return
    // exactly the keys it covers.
    for start in (0..2000).step_by(97) {
        let end = (start + 150).min(2000);
        let found = db.scan(&key(start), &key(end)).unwrap();
        assert_eq!(
            found.len(),
            end - start,
            "window [{start}, {end}) came back short"
        );
        for (offset, (k, _)) in found.iter().enumerate() {
            assert_eq!(k, &key(start + offset));
        }
    }

    // And the full range is still complete.
    assert_eq!(db.scan(&key(0), b"zzz").unwrap().len(), 2000);
}

#[test]
fn every_single_key_window_still_finds_its_key() {
    // The windows above step across table boundaries but never *start* exactly
    // on one. A scan whose inclusive start is precisely some table's largest
    // key is the case an off-by-one in the bound would silently drop, and the
    // only way to be sure none is missed is to try them all.
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir, 800);

    for i in 0..800 {
        let found = db.scan(&key(i), &key(i + 1)).unwrap();
        assert_eq!(found.len(), 1, "single-key window for {i} came back empty");
        assert_eq!(found[0].0, key(i));
    }
}

#[test]
fn a_scan_matching_everything_prunes_nothing() {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir, 800);

    let before = db.stats().scan_tables_pruned_total;
    let found = db.scan(b"", b"zzz").unwrap();
    assert_eq!(found.len(), 800);
    assert_eq!(
        db.stats().scan_tables_pruned_total - before,
        0,
        "no table can be ruled out of a scan that covers the whole store"
    );
}

#[test]
fn a_scan_below_and_above_every_table_matches_nothing() {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir, 800);
    let tables: u64 = db.stats().levels.iter().map(|l| l.num_sstables).sum();

    // Entirely below every table's range.
    let before = db.stats().scan_tables_pruned_total;
    assert!(db.scan(b"aaa", b"aab").unwrap().is_empty());
    assert_eq!(db.stats().scan_tables_pruned_total - before, tables);

    // Entirely above it.
    let before = db.stats().scan_tables_pruned_total;
    assert!(db.scan(b"zzz", b"zzzz").unwrap().is_empty());
    assert_eq!(db.stats().scan_tables_pruned_total - before, tables);
}

#[test]
fn an_unbounded_scan_still_prunes_tables_below_its_start() {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir, 2000);

    let before = db.stats().scan_tables_pruned_total;
    let found: Vec<_> = db
        .scan_iter(&key(1900), None)
        .unwrap()
        .collect::<lsm_rust::Result<Vec<_>>>()
        .unwrap();
    let pruned = db.stats().scan_tables_pruned_total - before;

    assert_eq!(found.len(), 100);
    assert!(pruned > 0, "tables below the start should still be skipped");
}

#[test]
fn prefix_scans_prune_too() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    // Distinct prefixes in ascending order, so each lands in its own tables.
    for prefix in ["aaa", "bbb", "ccc", "ddd"] {
        for i in 0..250 {
            db.put(format!("{prefix}:{:04}", i).into_bytes(), vec![b'v'; 64])
                .unwrap();
        }
    }

    let before = db.stats().scan_tables_pruned_total;
    let found = db.scan_prefix(b"ccc:").unwrap();
    let pruned = db.stats().scan_tables_pruned_total - before;

    assert_eq!(found.len(), 250);
    assert!(found.iter().all(|(k, _)| k.starts_with(b"ccc:")));
    assert!(pruned > 0, "other prefixes' tables should be skipped");
}

#[test]
fn a_snapshot_scan_prunes_and_still_reads_its_own_versions() {
    let dir = TempDir::new().unwrap();
    let mut db = seeded(&dir, 1200);
    let snap = db.snapshot();

    for i in 0..1200 {
        db.put(key(i), b"newer".to_vec()).unwrap();
    }

    let found = db.scan_at(&snap, &key(100), &key(200)).unwrap();
    assert_eq!(found.len(), 100);
    assert!(
        found.iter().all(|(_, v)| v == &vec![b'v'; 64]),
        "the snapshot must still see its own versions"
    );
}

#[test]
fn the_prune_counter_is_exposed_to_prometheus() {
    let dir = TempDir::new().unwrap();
    let db = seeded(&dir, 800);
    db.scan(&key(0), &key(2)).unwrap();

    let stats = db.stats();
    assert!(stats.scan_tables_pruned_total > 0);
    assert!(stats.to_prometheus().contains(&format!(
        "lsm_scan_tables_pruned_total {}",
        stats.scan_tables_pruned_total
    )));
}

#[test]
fn expired_and_deleted_keys_are_unaffected_by_pruning() {
    let dir = TempDir::new().unwrap();
    let mut db = seeded(&dir, 1200);

    for i in (0..1200).step_by(4) {
        db.delete(&key(i)).unwrap();
    }

    // The deletes live in newer tables than the values they shadow, so a
    // pruned scan must still bring the two together.
    let found = db.scan(&key(0), &key(400)).unwrap();
    assert_eq!(found.len(), 400 - 100);
    assert!(found.iter().all(|(k, _)| {
        let i: usize = String::from_utf8_lossy(&k[3..]).parse().unwrap();
        !i.is_multiple_of(4)
    }));
}
