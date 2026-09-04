//! Per-key expiry.
//!
//! Expiry is wall-clock and absolute, so most of these tests set a deadline
//! directly — in the past for "already gone", far in the future for "still
//! here" — and never sleep. A couple of tests do exercise the real clock
//! through `put_with_ttl`, because the conversion from a duration to a
//! deadline is part of what has to work.

use lsm_rust::{SharedStorage, Storage, StorageConfig, WriteBatch};
use std::thread::sleep;
use std::time::Duration;
use tempfile::TempDir;

fn config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 4 * 1024,
        compaction_size_threshold: 16 * 1024,
        block_cache_size: 0,
        ..StorageConfig::default()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// A deadline comfortably in the past.
fn past() -> u64 {
    now_ms() - 60_000
}

/// A deadline comfortably in the future.
fn future() -> u64 {
    now_ms() + 3_600_000
}

#[test]
fn a_key_past_its_deadline_reads_as_absent() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    db.put_with_expiry(b"gone".to_vec(), b"v".to_vec(), past())
        .unwrap();
    db.put_with_expiry(b"here".to_vec(), b"v".to_vec(), future())
        .unwrap();

    assert_eq!(db.get(&b"gone".to_vec()).unwrap(), None);
    assert_eq!(db.get(&b"here".to_vec()).unwrap(), Some(b"v".to_vec()));
}

#[test]
fn expiry_shadows_the_value_it_replaced_rather_than_uncovering_it() {
    // The subtle one. Writing an expiring value over a permanent one must
    // hide the key, not fall through to the older version underneath.
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    db.put(b"k".to_vec(), b"permanent".to_vec()).unwrap();
    db.put_with_expiry(b"k".to_vec(), b"temporary".to_vec(), past())
        .unwrap();

    assert_eq!(
        db.get(&b"k".to_vec()).unwrap(),
        None,
        "the expired write must shadow the permanent value beneath it"
    );
}

#[test]
fn expiry_shadows_across_a_flush_and_compaction() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    db.put(b"k".to_vec(), b"permanent".to_vec()).unwrap();
    // Push the permanent version out to an SSTable first.
    for i in 0..400 {
        db.put(format!("filler{:04}", i).into_bytes(), vec![b'x'; 64])
            .unwrap();
    }
    db.put_with_expiry(b"k".to_vec(), b"temporary".to_vec(), past())
        .unwrap();
    db.compact_now().unwrap();

    assert_eq!(db.get(&b"k".to_vec()).unwrap(), None);
}

/// Small memtable and no automatic compaction, so consecutive writes to the
/// same key land in *separate* level-0 tables and nothing merges them. That is
/// what puts an expired version and the value it replaced in different files —
/// the case where the read path itself, rather than compaction, has to keep
/// the expiry from uncovering what lies beneath.
fn unmerged_tables_config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 2 * 1024,
        inline_compaction: false,
        block_cache_size: 0,
        ..StorageConfig::default()
    }
}

fn flush_with_filler(db: &mut Storage, tag: &str) {
    for i in 0..80 {
        db.put(format!("{tag}{:03}", i).into_bytes(), vec![b'x'; 64])
            .unwrap();
    }
}

#[test]
fn an_expired_version_on_disk_shadows_an_older_one_in_another_table() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), unmerged_tables_config()).unwrap();

    db.put(b"k".to_vec(), b"permanent".to_vec()).unwrap();
    flush_with_filler(&mut db, "a");
    db.put_with_expiry(b"k".to_vec(), b"temporary".to_vec(), past())
        .unwrap();
    flush_with_filler(&mut db, "b");

    let stats = db.stats();
    assert!(
        stats.levels.iter().map(|l| l.num_sstables).sum::<u64>() > 1,
        "test needs the two versions in separate tables: {stats:?}"
    );
    assert_eq!(
        stats.compactions_total, 0,
        "nothing may have merged them first"
    );

    assert_eq!(
        db.get(&b"k".to_vec()).unwrap(),
        None,
        "the expired version uncovered the value it was meant to retire"
    );
    assert_eq!(db.expiry(&b"k".to_vec()).unwrap(), None);
}

#[test]
fn a_scan_over_unmerged_tables_does_not_uncover_an_expired_key() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), unmerged_tables_config()).unwrap();

    db.put(b"k".to_vec(), b"permanent".to_vec()).unwrap();
    flush_with_filler(&mut db, "a");
    db.put_with_expiry(b"k".to_vec(), b"temporary".to_vec(), past())
        .unwrap();
    flush_with_filler(&mut db, "b");

    let found: Vec<_> = db
        .scan(b"k", b"l")
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert!(
        found.is_empty(),
        "the merge fell through an expired version to an older one: {found:?}"
    );
}

#[test]
fn compaction_must_not_resurrect_the_value_an_expiry_retired() {
    // The sharpest failure mode of the whole feature: an expired version
    // cannot simply be dropped when tables are merged, because dropping it
    // uncovers the older version of the same key underneath and brings a
    // value back from the dead. It has to become a tombstone instead.
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), unmerged_tables_config()).unwrap();

    db.put(b"k".to_vec(), b"permanent".to_vec()).unwrap();
    flush_with_filler(&mut db, "a");
    db.put_with_expiry(b"k".to_vec(), b"temporary".to_vec(), past())
        .unwrap();
    flush_with_filler(&mut db, "b");

    assert_eq!(
        db.get(&b"k".to_vec()).unwrap(),
        None,
        "precondition: the key is already hidden before any merge"
    );

    db.compact_now().unwrap();

    let stats = db.stats();
    assert!(
        stats.compactions_total > 0 && stats.compaction_moves_total == 0,
        "the level had expired data, so it must really merge: {stats:?}"
    );
    assert_eq!(
        db.get(&b"k".to_vec()).unwrap(),
        None,
        "compaction resurrected the value the expiry retired"
    );
    assert!(db.scan(b"k", b"l").unwrap().is_empty());
}

#[test]
fn a_deadline_survives_a_restart_without_being_refreshed() {
    let dir = TempDir::new().unwrap();
    let deadline = future();
    {
        let mut db = Storage::with_config(dir.path(), config()).unwrap();
        db.put_with_expiry(b"k".to_vec(), b"v".to_vec(), deadline)
            .unwrap();
        db.put_with_expiry(b"gone".to_vec(), b"v".to_vec(), past())
            .unwrap();
    }

    // Replayed from the WAL, so the deadline had to be written there too.
    let db = Storage::with_config(dir.path(), config()).unwrap();
    assert_eq!(db.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.expiry(&b"k".to_vec()).unwrap(), Some(Some(deadline)));
    assert_eq!(db.get(&b"gone".to_vec()).unwrap(), None);
}

#[test]
fn a_deadline_survives_a_flush_to_disk() {
    let dir = TempDir::new().unwrap();
    let deadline = future();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();
    db.put_with_expiry(b"k".to_vec(), b"v".to_vec(), deadline)
        .unwrap();
    // Force the memtable out to an SSTable, so the deadline round-trips
    // through the v5 entry format rather than staying in memory.
    for i in 0..400 {
        db.put(format!("filler{:04}", i).into_bytes(), vec![b'x'; 64])
            .unwrap();
    }

    assert_eq!(db.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.expiry(&b"k".to_vec()).unwrap(), Some(Some(deadline)));
}

#[test]
fn expiry_reports_the_three_states() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    db.put(b"permanent".to_vec(), b"v".to_vec()).unwrap();
    let deadline = future();
    db.put_with_expiry(b"expiring".to_vec(), b"v".to_vec(), deadline)
        .unwrap();
    db.put_with_expiry(b"expired".to_vec(), b"v".to_vec(), past())
        .unwrap();

    assert_eq!(db.expiry(&b"missing".to_vec()).unwrap(), None);
    assert_eq!(db.expiry(&b"expired".to_vec()).unwrap(), None);
    assert_eq!(db.expiry(&b"permanent".to_vec()).unwrap(), Some(None));
    assert_eq!(
        db.expiry(&b"expiring".to_vec()).unwrap(),
        Some(Some(deadline))
    );
}

#[test]
fn scans_skip_expired_keys() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    for i in 0..100 {
        let key = format!("key{:03}", i).into_bytes();
        if i % 2 == 0 {
            db.put_with_expiry(key, b"v".to_vec(), past()).unwrap();
        } else {
            db.put(key, b"v".to_vec()).unwrap();
        }
    }

    let scanned = db.scan(b"key", b"kez").unwrap();
    assert_eq!(scanned.len(), 50, "expired keys must not appear in a scan");
    for (key, _) in &scanned {
        let i: usize = String::from_utf8_lossy(&key[3..]).parse().unwrap();
        assert_eq!(i % 2, 1, "key {i} should have expired");
    }
}

#[test]
fn a_scan_does_not_fall_through_an_expired_version_to_an_older_one() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    db.put(b"k".to_vec(), b"permanent".to_vec()).unwrap();
    db.put_with_expiry(b"k".to_vec(), b"temporary".to_vec(), past())
        .unwrap();

    assert!(
        db.scan(b"a", b"z").unwrap().is_empty(),
        "the scan uncovered a version the expiry was meant to retire"
    );
}

#[test]
fn compaction_collects_expired_versions() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    for i in 0..300 {
        db.put_with_expiry(format!("key{:04}", i).into_bytes(), vec![b'x'; 64], past())
            .unwrap();
    }
    db.compact_now().unwrap();

    let stats = db.stats();
    assert!(
        stats.expired_total > 0,
        "compaction should have collected expired versions: {stats:?}"
    );
    assert!(stats.to_prometheus().contains("lsm_expired_total"));

    for i in 0..300 {
        assert_eq!(db.get(&format!("key{:04}", i).into_bytes()).unwrap(), None);
    }
}

#[test]
fn a_write_batch_can_carry_deadlines() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    let deadline = future();
    let mut batch = WriteBatch::new();
    batch
        .put(b"plain".to_vec(), b"v".to_vec())
        .put_with_expiry(b"live".to_vec(), b"v".to_vec(), deadline)
        .put_with_expiry(b"dead".to_vec(), b"v".to_vec(), past());
    db.write_batch(batch).unwrap();

    assert_eq!(db.get(&b"plain".to_vec()).unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(&b"live".to_vec()).unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(&b"dead".to_vec()).unwrap(), None);
    assert_eq!(db.expiry(&b"live".to_vec()).unwrap(), Some(Some(deadline)));

    // And the deadline survives the batch's WAL record.
    drop(db);
    let db = Storage::with_config(dir.path(), config()).unwrap();
    assert_eq!(db.expiry(&b"live".to_vec()).unwrap(), Some(Some(deadline)));
    assert_eq!(db.get(&b"dead".to_vec()).unwrap(), None);
}

#[test]
fn transactions_can_set_deadlines_and_read_their_own() {
    let dir = TempDir::new().unwrap();
    let db = SharedStorage::with_config(dir.path(), config()).unwrap();

    db.transaction(4, |tx| {
        tx.put_with_expiry(b"dead".to_vec(), b"v".to_vec(), past());
        tx.put_with_expiry(b"live".to_vec(), b"v".to_vec(), future());
        // Read-your-own-writes honours the deadline too.
        assert_eq!(tx.get(&b"dead".to_vec())?, None);
        assert_eq!(tx.get(&b"live".to_vec())?, Some(b"v".to_vec()));
        Ok(())
    })
    .unwrap();

    assert_eq!(db.get(&b"dead".to_vec()).unwrap(), None);
    assert_eq!(db.get(&b"live".to_vec()).unwrap(), Some(b"v".to_vec()));
}

#[test]
fn a_snapshot_isolates_from_writes_but_not_from_time() {
    // Documented semantics: a snapshot pins which versions exist, not the
    // clock. A key that expires after the snapshot was taken still stops
    // being visible through it.
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    db.put_with_ttl(b"k".to_vec(), b"v".to_vec(), Duration::from_millis(80))
        .unwrap();
    let snap = db.snapshot();
    assert_eq!(
        db.get_at(&snap, &b"k".to_vec()).unwrap(),
        Some(b"v".to_vec())
    );

    sleep(Duration::from_millis(160));

    assert_eq!(
        db.get_at(&snap, &b"k".to_vec()).unwrap(),
        None,
        "a snapshot does not freeze the clock"
    );
}

#[test]
fn a_ttl_expires_on_the_real_clock() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    db.put_with_ttl(b"k".to_vec(), b"v".to_vec(), Duration::from_millis(80))
        .unwrap();
    assert_eq!(db.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));

    sleep(Duration::from_millis(160));
    assert_eq!(db.get(&b"k".to_vec()).unwrap(), None);
}

#[test]
fn shared_storage_exposes_the_same_api() {
    let dir = TempDir::new().unwrap();
    let db = SharedStorage::with_config(dir.path(), config()).unwrap();

    let deadline = future();
    db.put_with_expiry(b"live".to_vec(), b"v".to_vec(), deadline)
        .unwrap();
    db.put_with_expiry(b"dead".to_vec(), b"v".to_vec(), past())
        .unwrap();
    db.put_with_ttl(b"ttl".to_vec(), b"v".to_vec(), Duration::from_secs(3600))
        .unwrap();

    assert_eq!(db.get(&b"live".to_vec()).unwrap(), Some(b"v".to_vec()));
    assert_eq!(db.get(&b"dead".to_vec()).unwrap(), None);
    assert_eq!(db.expiry(&b"live".to_vec()).unwrap(), Some(Some(deadline)));
    assert_eq!(db.expiry(&b"dead".to_vec()).unwrap(), None);
    assert!(db.expiry(&b"ttl".to_vec()).unwrap().unwrap().is_some());
}

#[test]
fn a_deleted_key_stays_deleted_regardless_of_deadlines() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    db.put_with_expiry(b"k".to_vec(), b"v".to_vec(), future())
        .unwrap();
    db.delete(&b"k".to_vec()).unwrap();

    assert_eq!(db.get(&b"k".to_vec()).unwrap(), None);
    assert_eq!(db.expiry(&b"k".to_vec()).unwrap(), None);
}

#[test]
fn a_permanent_write_over_an_expiring_one_clears_the_deadline() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    db.put_with_expiry(b"k".to_vec(), b"v1".to_vec(), now_ms() + 50)
        .unwrap();
    db.put(b"k".to_vec(), b"v2".to_vec()).unwrap();

    sleep(Duration::from_millis(120));

    assert_eq!(
        db.get(&b"k".to_vec()).unwrap(),
        Some(b"v2".to_vec()),
        "the newer permanent write must not inherit the old deadline"
    );
    assert_eq!(db.expiry(&b"k".to_vec()).unwrap(), Some(None));
}
