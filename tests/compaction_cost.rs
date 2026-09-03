//! Cost-aware compaction.
//!
//! Compaction used to rewrite a level whenever it crossed a size threshold,
//! regardless of whether the merge could reclaim anything. When a level's
//! tables are mutually disjoint no key appears twice, so the merge reads and
//! rewrites every byte to produce the same entries in a different file. These
//! tests check that such a level is *promoted* instead, that a level with real
//! overlap is still merged, and — most importantly — that promotion cannot
//! surface stale data.

use lsm_rust::{Storage, StorageConfig};
use std::path::Path;
use tempfile::TempDir;

/// Small thresholds so a few hundred writes drive several compactions.
fn config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 4 * 1024,
        compaction_size_threshold: 16 * 1024,
        level0_file_limit: 2,
        block_cache_size: 0,
        ..StorageConfig::default()
    }
}

fn value(i: usize) -> Vec<u8> {
    format!("value-{:06}", i).repeat(4).into_bytes()
}

fn key(i: usize) -> Vec<u8> {
    format!("key{:06}", i).into_bytes()
}

fn sst_names(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            (p.extension().and_then(|s| s.to_str()) == Some("sst"))
                .then(|| p.file_name().unwrap().to_string_lossy().into_owned())
        })
        .collect();
    names.sort();
    names
}

#[test]
fn an_append_only_workload_is_promoted_rather_than_rewritten() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    // Strictly increasing keys: every flush covers a range above the last, so
    // the tables in a level never share a key.
    for i in 0..2000 {
        db.put(key(i), value(i)).unwrap();
    }
    db.compact_now().unwrap();

    let stats = db.stats();
    assert!(
        stats.compaction_moves_total > 0,
        "disjoint levels should be promoted, not rewritten: {stats:?}"
    );
    assert!(
        stats.compactions_total >= stats.compaction_moves_total,
        "moves are a subset of compaction runs: {stats:?}"
    );

    // Every value is still readable and correct.
    for i in 0..2000 {
        assert_eq!(db.get(&key(i)).unwrap(), Some(value(i)), "key {i}");
    }
}

#[test]
fn an_overwriting_workload_is_still_merged() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    // The same small key set written over and over: every table covers the
    // same range, so merges genuinely collapse versions.
    for round in 0..40 {
        for i in 0..50 {
            db.put(key(i), value(round * 1000 + i)).unwrap();
        }
    }
    db.compact_now().unwrap();

    let stats = db.stats();
    assert!(stats.compactions_total > 0, "{stats:?}");
    assert_eq!(
        stats.compaction_moves_total, 0,
        "overlapping tables must be merged, not promoted: {stats:?}"
    );

    for i in 0..50 {
        assert_eq!(db.get(&key(i)).unwrap(), Some(value(39 * 1000 + i)));
    }
}

#[test]
fn a_promoted_table_still_shadows_older_data_at_its_new_level() {
    // The correctness risk of promotion: a promoted table lands beside tables
    // already at the destination, which may hold an older version of the same
    // key. It has to win. The generations below are written in ascending key
    // order so each level's tables stay mutually disjoint and promotion keeps
    // firing while older versions sit in the levels underneath.
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    for generation in ["first", "second", "third"] {
        for i in 0..400 {
            db.put(key(i), generation.repeat(16).into_bytes()).unwrap();
        }
        db.compact_now().unwrap();
    }

    let stats = db.stats();
    assert!(
        stats.compaction_moves_total > 1,
        "test is vacuous unless promotions happened while older versions \
         existed below: {stats:?}"
    );

    for i in 0..400 {
        assert_eq!(
            db.get(&key(i)).unwrap(),
            Some("third".repeat(16).into_bytes()),
            "key {i} read a stale version after promotion"
        );
    }
}

/// Levels below 0 never reach their size threshold under this config, so
/// level 0 promotes into level 1 repeatedly and level 1 accumulates
/// overlapping generations of the same keys — the exact shape where the
/// order of promoted tables decides which version a read sees.
fn promote_into_populated_level_config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 2 * 1024,
        compaction_size_threshold: 1 << 30,
        level0_file_limit: 2,
        block_cache_size: 0,
        ..StorageConfig::default()
    }
}

#[test]
fn promotion_into_a_populated_level_keeps_the_newest_version() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), promote_into_populated_level_config()).unwrap();

    for generation in ["first", "second", "third"] {
        for i in 0..300 {
            db.put(key(i), generation.repeat(8).into_bytes()).unwrap();
        }
    }

    let stats = db.stats();
    assert!(
        stats.compaction_moves_total > 1,
        "test needs repeated promotions to be meaningful: {stats:?}"
    );
    let destination = stats
        .levels
        .iter()
        .find(|l| l.level == 1)
        .expect("level 1 should hold the promoted tables");
    assert!(
        destination.num_sstables > 1,
        "promotions must land beside tables that were already there: {stats:?}"
    );

    // Promoted tables come from a shallower level, so they are newer than
    // everything already at the destination and must be consulted first.
    for i in 0..300 {
        assert_eq!(
            db.get(&key(i)).unwrap(),
            Some("third".repeat(8).into_bytes()),
            "key {i} read a version shadowed by a later promotion"
        );
    }
}

#[test]
fn deletes_survive_promotion() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();

    for i in 0..600 {
        db.put(key(i), value(i)).unwrap();
    }
    db.compact_now().unwrap();

    for i in (0..600).step_by(3) {
        db.delete(&key(i)).unwrap();
    }
    db.compact_now().unwrap();

    for i in 0..600 {
        let expected = if i % 3 == 0 { None } else { Some(value(i)) };
        assert_eq!(db.get(&key(i)).unwrap(), expected, "key {i}");
    }
}

#[test]
fn promoted_tables_are_renamed_to_match_the_manifest() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();
    for i in 0..2000 {
        db.put(key(i), value(i)).unwrap();
    }
    db.compact_now().unwrap();
    assert!(db.stats().compaction_moves_total > 0);
    drop(db);

    // A promoted file is linked under its new level's name and the old name
    // unlinked, so a filename never disagrees with the level the manifest
    // records. That matters because a store whose manifest is lost falls back
    // to reading levels out of the filenames, and a mislabelled table would
    // then be consulted in the wrong order.
    let manifest = std::fs::read_to_string(dir.path().join("MANIFEST")).unwrap();
    let mut entries = 0;
    for line in manifest.lines().skip(2) {
        let mut parts = line.split_whitespace();
        let (Some(level), Some(_seq), Some(filename)) = (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        assert!(
            filename.starts_with(&format!("L{}_", level)),
            "manifest puts {filename} at level {level}"
        );
        assert!(
            dir.path().join(filename).exists(),
            "manifest names a missing table: {filename}"
        );
        entries += 1;
    }
    assert!(entries > 0, "expected a populated manifest");

    // And nothing was left behind by the link-then-unlink sequence.
    let on_disk = sst_names(dir.path());
    assert_eq!(
        on_disk.len(),
        entries,
        "unreferenced tables left on disk: {on_disk:?}"
    );
}

#[test]
fn a_promoted_store_reopens_with_all_of_its_data() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();
    for i in 0..2000 {
        db.put(key(i), value(i)).unwrap();
    }
    db.compact_now().unwrap();
    let moves = db.stats().compaction_moves_total;
    assert!(moves > 0);
    drop(db);

    // Promotion commits through the manifest exactly as a merge does, so a
    // restart must find every table where the manifest says it is.
    let reopened = Storage::with_config(dir.path(), config()).unwrap();
    for i in 0..2000 {
        assert_eq!(reopened.get(&key(i)).unwrap(), Some(value(i)), "key {i}");
    }
}

#[test]
fn scans_are_unaffected_by_promotion() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();
    for i in 0..1500 {
        db.put(key(i), value(i)).unwrap();
    }
    db.compact_now().unwrap();
    assert!(db.stats().compaction_moves_total > 0);

    let scanned = db.scan(&key(100), &key(200)).unwrap();
    assert_eq!(scanned.len(), 100);
    for (offset, (k, v)) in scanned.iter().enumerate() {
        assert_eq!(k, &key(100 + offset));
        assert_eq!(v, &value(100 + offset));
    }
}

#[test]
fn snapshots_taken_before_promotion_still_read_their_own_version() {
    // Uses the config that keeps promotions flowing into a populated level,
    // so the snapshot really is read across promoted tables.
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), promote_into_populated_level_config()).unwrap();

    for i in 0..300 {
        db.put(key(i), b"before".repeat(8).to_vec()).unwrap();
    }
    let snap = db.snapshot();
    let moves_before = db.stats().compaction_moves_total;

    for i in 0..300 {
        db.put(key(i), b"after".repeat(8).to_vec()).unwrap();
    }

    assert!(
        db.stats().compaction_moves_total > moves_before,
        "test is only meaningful if a promotion happened after the snapshot"
    );

    // Promotion moves whole tables without collapsing versions, so the
    // snapshot's versions are still there to be found.
    for i in 0..300 {
        assert_eq!(
            db.get_at(&snap, &key(i)).unwrap(),
            Some(b"before".repeat(8).to_vec()),
            "snapshot lost key {i} across promotion"
        );
    }
}

#[test]
fn the_move_counter_is_exposed_to_prometheus() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), config()).unwrap();
    for i in 0..2000 {
        db.put(key(i), value(i)).unwrap();
    }
    db.compact_now().unwrap();

    let stats = db.stats();
    let rendered = stats.to_prometheus();
    assert!(rendered.contains(&format!(
        "lsm_compaction_moves_total {}",
        stats.compaction_moves_total
    )));
    assert!(rendered.contains("# TYPE lsm_compaction_moves_total counter"));
}
