//! Keys at the edges of the byte-string ordering.
//!
//! Empty keys and runs of `0xFF` are where range logic goes wrong:
//! `prefix_successor` has no successor to return for an all-`0xFF` prefix, and
//! the scan bounds have to stay half-open regardless. Everything here is
//! checked in the memtable, again after a flush and compaction, and again
//! after a restart.
use lsm_rust::{Storage, StorageConfig};
use tempfile::TempDir;

fn cfg() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 1024,
        compaction_size_threshold: 4096,
        level0_file_limit: 2,
        block_cache_size: 0,
        ..StorageConfig::default()
    }
}

fn check(label: &str, got: Vec<Vec<u8>>, want: Vec<Vec<u8>>) {
    assert_eq!(got, want, "{label}");
}

#[test]
fn empty_and_high_byte_keys_survive_every_path() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::with_config(dir.path(), cfg()).unwrap();

    let keys: Vec<Vec<u8>> = vec![
        b"".to_vec(),
        b"\x00".to_vec(),
        b"a".to_vec(),
        b"\xff".to_vec(),
        b"\xff\x00".to_vec(),
        b"\xff\xff".to_vec(),
        b"\xff\xff\xff".to_vec(),
    ];
    for k in &keys {
        db.put(k.clone(), b"v".to_vec()).unwrap();
    }

    // Every key must be individually retrievable.
    for k in &keys {
        assert_eq!(db.get(k).unwrap(), Some(b"v".to_vec()), "get({k:?})");
    }

    let mut sorted = keys.clone();
    sorted.sort();

    // Full scan, unbounded end.
    let all: Vec<Vec<u8>> = db
        .scan_iter(b"", None)
        .unwrap()
        .map(|e| e.unwrap().0)
        .collect();
    check("scan_iter unbounded", all, sorted.clone());

    // Prefix scans over 0xFF prefixes (prefix_successor has no successor here).
    let ff: Vec<Vec<u8>> = db
        .scan_prefix(b"\xff")
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    check(
        "prefix 0xff",
        ff,
        sorted
            .iter()
            .filter(|k| k.starts_with(b"\xff"))
            .cloned()
            .collect(),
    );

    let ffff: Vec<Vec<u8>> = db
        .scan_prefix(b"\xff\xff")
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    check(
        "prefix 0xffff",
        ffff,
        sorted
            .iter()
            .filter(|k| k.starts_with(b"\xff\xff"))
            .cloned()
            .collect(),
    );

    // Empty prefix = everything.
    let empty: Vec<Vec<u8>> = db
        .scan_prefix(b"")
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    check("prefix empty", empty, sorted.clone());

    // Bounds: start inclusive, end exclusive.
    let r: Vec<Vec<u8>> = db
        .scan(b"a", b"\xff")
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    check("scan [a, 0xff)", r, vec![b"a".to_vec()]);

    // Force everything to disk and repeat, so the same paths run over SSTables.
    for i in 0..300 {
        db.put(format!("filler{i:04}").into_bytes(), vec![b'x'; 32])
            .unwrap();
    }
    db.compact_now().unwrap();

    for k in &keys {
        assert_eq!(
            db.get(k).unwrap(),
            Some(b"v".to_vec()),
            "after flush: get({k:?})"
        );
    }
    let ff2: Vec<Vec<u8>> = db
        .scan_prefix(b"\xff")
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    check(
        "after-flush prefix 0xff",
        ff2,
        sorted
            .iter()
            .filter(|k| k.starts_with(b"\xff"))
            .cloned()
            .collect(),
    );

    let all2: Vec<Vec<u8>> = db
        .scan_iter(b"", None)
        .unwrap()
        .map(|e| e.unwrap().0)
        .filter(|k| !k.starts_with(b"filler"))
        .collect();
    check("after-flush unbounded", all2, sorted.clone());

    // And across a restart.
    drop(db);
    let db = Storage::with_config(dir.path(), cfg()).unwrap();
    for k in &keys {
        assert_eq!(
            db.get(k).unwrap(),
            Some(b"v".to_vec()),
            "after restart: get({k:?})"
        );
    }
}
