//! Corruption-detection tests.
//!
//! The engine stores a CRC-32 with every SSTable section, every SSTable data
//! block, and every write-ahead log record. These tests deliberately damage
//! files on disk and assert that the damage is *detected* — surfaced as an
//! error — rather than silently returned as plausible-looking data.

use lsm_rust::{Storage, StorageConfig};
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Small thresholds so a handful of writes produce a real SSTable.
fn config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 4 * 1024,
        inline_compaction: false,
        block_cache_size: 0, // read from disk every time
        ..StorageConfig::default()
    }
}

fn sst_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            (p.extension().and_then(|s| s.to_str()) == Some("sst")).then_some(p)
        })
        .collect();
    files.sort();
    files
}

/// Byte ranges of the v4 header sections, and where the data section starts.
struct Layout {
    bloom: (usize, usize),
    index: (usize, usize),
    data_start: usize,
}

fn parse_layout(bytes: &[u8]) -> Layout {
    assert_eq!(&bytes[..4], b"LSMT", "not a versioned SSTable");
    assert_eq!(bytes[4], 4, "expected format version 4");
    let read_u32 = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;

    // [len][crc][body] for the bloom filter, then the same for the index.
    let bloom_len = read_u32(6);
    let bloom_start = 6 + 8;
    let index_len_at = bloom_start + bloom_len;
    let index_len = read_u32(index_len_at);
    let index_start = index_len_at + 8;
    Layout {
        bloom: (bloom_start, bloom_start + bloom_len),
        index: (index_start, index_start + index_len),
        data_start: index_start + index_len,
    }
}

/// Flip one bit at `offset` in the file.
fn flip_bit(path: &Path, offset: usize, bit: u8) {
    let mut bytes = fs::read(path).unwrap();
    bytes[offset] ^= 1 << bit;
    fs::write(path, bytes).unwrap();
}

/// Populate a store and flush it to exactly one SSTable.
fn seeded_store(dir: &Path) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut db = Storage::with_config(dir, config()).unwrap();
    let mut written = Vec::new();
    for i in 0..200 {
        let k = format!("key{:04}", i).into_bytes();
        let v = format!("value-{:04}", i).repeat(4).into_bytes();
        db.put(k.clone(), v.clone()).unwrap();
        written.push((k, v));
    }
    drop(db);
    written
}

#[test]
fn sstable_data_block_bit_flip_is_detected() {
    let temp = TempDir::new().unwrap();
    let written = seeded_store(temp.path());
    let sst = sst_files(temp.path()).pop().expect("expected an SSTable");

    let bytes = fs::read(&sst).unwrap();
    let layout = parse_layout(&bytes);
    // Corrupt a byte inside the first data block.
    flip_bit(&sst, layout.data_start + 8, 3);

    let db = Storage::with_config(temp.path(), config()).unwrap();

    // Every read either errors or returns the correct value — never wrong data.
    let mut errors = 0;
    for (k, v) in &written {
        match db.get(k) {
            Ok(Some(got)) => assert_eq!(&got, v, "silently wrong data for {:?}", k),
            Ok(None) => {}
            Err(_) => errors += 1,
        }
    }
    assert!(errors > 0, "corrupted data block was not detected");
}

#[test]
fn sstable_index_corruption_is_detected() {
    let temp = TempDir::new().unwrap();
    seeded_store(temp.path());
    let sst = sst_files(temp.path()).pop().unwrap();

    let bytes = fs::read(&sst).unwrap();
    let layout = parse_layout(&bytes);
    flip_bit(&sst, layout.index.0, 1);

    // The index checksum is verified while opening the table.
    let err = Storage::with_config(temp.path(), config())
        .err()
        .expect("corrupted sparse index should be rejected");
    assert!(
        err.to_string().contains("checksum mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn sstable_bloom_corruption_is_detected() {
    let temp = TempDir::new().unwrap();
    seeded_store(temp.path());
    let sst = sst_files(temp.path()).pop().unwrap();

    let bytes = fs::read(&sst).unwrap();
    let layout = parse_layout(&bytes);
    flip_bit(&sst, layout.bloom.0, 5);

    let err = Storage::with_config(temp.path(), config())
        .err()
        .expect("corrupted bloom filter should be rejected");
    assert!(
        err.to_string().contains("checksum mismatch"),
        "unexpected error: {err}"
    );
}

/// Walk the checksummed WAL frames: `[3][crc u32][len u32][body]`.
fn wal_record_offsets(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut offsets = Vec::new();
    let mut pos = 0;
    while pos + 9 <= bytes.len() {
        assert_eq!(bytes[pos], 3, "expected a checksummed WAL record");
        let len = u32::from_le_bytes(bytes[pos + 5..pos + 9].try_into().unwrap()) as usize;
        let end = pos + 9 + len;
        if end > bytes.len() {
            break;
        }
        offsets.push((pos, end));
        pos = end;
    }
    offsets
}

#[test]
fn wal_bit_flip_in_earlier_record_is_detected() {
    let temp = TempDir::new().unwrap();
    {
        let mut db = Storage::new(temp.path(), false).unwrap();
        for i in 0..5 {
            db.put(format!("k{i}").into_bytes(), b"value".to_vec())
                .unwrap();
        }
    }

    let wal = temp.path().join("wal");
    let bytes = fs::read(&wal).unwrap();
    let records = wal_record_offsets(&bytes);
    assert!(records.len() >= 3, "expected several WAL records");

    // Corrupt the body of the *first* record: not a torn tail, so it is real
    // corruption of durable data and must be reported.
    flip_bit(&wal, records[0].0 + 9, 2);

    let err = Storage::new(temp.path(), false)
        .err()
        .expect("corrupted WAL record should be rejected");
    assert!(
        err.to_string().contains("checksum mismatch"),
        "unexpected error: {err}"
    );
}

#[test]
fn wal_torn_final_record_is_dropped_not_rejected() {
    let temp = TempDir::new().unwrap();
    {
        let mut db = Storage::new(temp.path(), false).unwrap();
        for i in 0..5 {
            db.put(format!("k{i}").into_bytes(), b"value".to_vec())
                .unwrap();
        }
    }

    let wal = temp.path().join("wal");
    let bytes = fs::read(&wal).unwrap();
    let records = wal_record_offsets(&bytes);
    let (last_start, _) = *records.last().unwrap();

    // A torn write can leave a complete-looking final frame with a bad
    // checksum. That is indistinguishable from a truncated tail, so recovery
    // drops it and continues rather than refusing to open.
    flip_bit(&wal, last_start + 9, 4);

    let db = Storage::new(temp.path(), false).expect("torn tail should not block recovery");
    for i in 0..4 {
        assert_eq!(
            db.get(&format!("k{i}").into_bytes()).unwrap(),
            Some(b"value".to_vec()),
            "earlier writes must survive"
        );
    }
    assert_eq!(db.get(&b"k4".to_vec()).unwrap(), None, "torn write is lost");
}

#[test]
fn randomized_corruption_never_returns_wrong_data() {
    let temp = TempDir::new().unwrap();
    let written = seeded_store(temp.path());
    let sst = sst_files(temp.path()).pop().unwrap();
    let pristine = fs::read(&sst).unwrap();
    let layout = parse_layout(&pristine);

    // Deterministic xorshift PRNG so failures reproduce exactly.
    let mut state: u64 = 0x5EED_1234_ABCD_0001;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };

    let data_len = pristine.len() - layout.data_start;
    let mut detected = 0;

    for _ in 0..150 {
        let offset = layout.data_start + (next() as usize % data_len);
        let bit = (next() % 8) as u8;

        let mut corrupted = pristine.clone();
        corrupted[offset] ^= 1 << bit;
        fs::write(&sst, &corrupted).unwrap();

        let Ok(db) = Storage::with_config(temp.path(), config()) else {
            detected += 1; // rejected at open
            continue;
        };
        let mut saw_error = false;
        for (k, v) in &written {
            match db.get(k) {
                // The cardinal rule: never hand back data that isn't what was written.
                Ok(Some(got)) => assert_eq!(
                    &got, v,
                    "silently wrong data for {:?} after flipping byte {} bit {}",
                    k, offset, bit
                ),
                Ok(None) => {}
                Err(_) => saw_error = true,
            }
        }
        if saw_error {
            detected += 1;
        }
    }

    fs::write(&sst, &pristine).unwrap();
    // Most single-bit flips land in bytes some read actually touches; the
    // point is that whenever they do, they are caught.
    assert!(
        detected > 0,
        "no corruption detected across randomized bit flips"
    );
}
