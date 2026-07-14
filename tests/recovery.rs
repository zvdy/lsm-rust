//! Crash-recovery integration tests.
//!
//! These exercise the store across restarts: WAL replay, truncated WAL
//! tails (crash mid-append), sequence-number continuity, and a model-based
//! random workload interrupted by restarts. Implements the "Recovery
//! testing" roadmap item.

use lsm_rust::{Compression, Storage, StorageConfig};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

/// Small thresholds so tests exercise flushes and compaction cheaply.
fn small_config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 8 * 1024,
        compaction_size_threshold: 32 * 1024,
        ..StorageConfig::default()
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("key{:05}", i).into_bytes()
}

fn value(i: usize) -> Vec<u8> {
    format!("value{}", i).repeat(10).into_bytes()
}

#[test]
fn restart_preserves_multi_level_data() {
    let temp_dir = TempDir::new().unwrap();

    // Write enough to spread data across the memtable, level 0, and
    // compacted levels
    let mut storage = Storage::with_config(temp_dir.path(), small_config()).unwrap();
    for i in 0..1000 {
        storage.put(key(i), value(i)).unwrap();
    }
    drop(storage);

    let recovered = Storage::with_config(temp_dir.path(), small_config()).unwrap();
    for i in 0..1000 {
        assert_eq!(recovered.get(&key(i)).unwrap(), Some(value(i)), "key {}", i);
    }
}

#[test]
fn sequence_numbers_continue_after_restart() {
    let temp_dir = TempDir::new().unwrap();

    // First run: force at least one flush
    let mut storage = Storage::with_config(temp_dir.path(), small_config()).unwrap();
    for i in 0..200 {
        storage.put(key(i), value(i)).unwrap();
    }
    drop(storage);

    let count_before = fs::read_dir(temp_dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "sst")
        })
        .count();
    assert!(count_before > 0, "expected at least one SSTable from run 1");

    // Second run: more flushes must not reuse (and clobber) existing
    // sequence numbers
    let mut storage = Storage::with_config(temp_dir.path(), small_config()).unwrap();
    for i in 200..400 {
        storage.put(key(i), value(i)).unwrap();
    }
    drop(storage);

    let recovered = Storage::with_config(temp_dir.path(), small_config()).unwrap();
    for i in 0..400 {
        assert_eq!(recovered.get(&key(i)).unwrap(), Some(value(i)), "key {}", i);
    }
}

#[test]
fn truncated_wal_tail_recovers_complete_prefix() {
    let temp_dir = TempDir::new().unwrap();

    // Writes small enough that nothing flushes: all state lives in the WAL
    let mut storage = Storage::new(temp_dir.path(), false).unwrap();
    for i in 0..10 {
        storage.put(key(i), value(i)).unwrap();
    }
    drop(storage);

    // Simulate a crash mid-append: chop bytes off the WAL tail
    let wal_path = temp_dir.path().join("wal");
    let len = fs::metadata(&wal_path).unwrap().len();
    let file = fs::OpenOptions::new().write(true).open(&wal_path).unwrap();
    file.set_len(len - 5).unwrap();
    drop(file);

    // Recovery keeps every complete entry and drops only the torn tail
    let recovered = Storage::new(temp_dir.path(), false).unwrap();
    for i in 0..9 {
        assert_eq!(recovered.get(&key(i)).unwrap(), Some(value(i)), "key {}", i);
    }
    assert_eq!(recovered.get(&key(9)).unwrap(), None);
}

#[test]
fn deletes_survive_restart_at_every_stage() {
    let temp_dir = TempDir::new().unwrap();

    let mut storage = Storage::with_config(temp_dir.path(), small_config()).unwrap();

    // Stage 1: delete while the value is only in the memtable/WAL
    storage.put(key(1), value(1)).unwrap();
    storage.delete(&key(1)).unwrap();

    // Stage 2: delete after the value was flushed into an SSTable
    storage.put(key(2), value(2)).unwrap();
    for i in 100..300 {
        storage.put(key(i), value(i)).unwrap(); // force flushes
    }
    storage.delete(&key(2)).unwrap();

    drop(storage);
    let recovered = Storage::with_config(temp_dir.path(), small_config()).unwrap();
    assert_eq!(recovered.get(&key(1)).unwrap(), None);
    assert_eq!(recovered.get(&key(2)).unwrap(), None);
    assert_eq!(recovered.get(&key(100)).unwrap(), Some(value(100)));
}

#[test]
fn recovery_with_compressed_sstables() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        compression: Compression::Lz4,
        ..small_config()
    };

    let mut storage = Storage::with_config(temp_dir.path(), config.clone()).unwrap();
    for i in 0..500 {
        storage.put(key(i), value(i)).unwrap();
    }
    storage.delete(&key(250)).unwrap();
    drop(storage);

    let recovered = Storage::with_config(temp_dir.path(), config).unwrap();
    assert_eq!(recovered.get(&key(0)).unwrap(), Some(value(0)));
    assert_eq!(recovered.get(&key(499)).unwrap(), Some(value(499)));
    assert_eq!(recovered.get(&key(250)).unwrap(), None);
}

/// Tiny deterministic PRNG (xorshift) so the model-based test is
/// reproducible without adding a dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

/// Apply a random workload to the store and an in-memory model, restarting
/// the store several times along the way; the store must always agree with
/// the model afterwards.
#[test]
fn model_based_random_ops_with_restarts() {
    let temp_dir = TempDir::new().unwrap();
    let mut model: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    let mut rng = Rng(0xC0FFEE);

    fn reopen(path: &Path) -> Storage {
        Storage::with_config(
            path,
            StorageConfig {
                memtable_size_threshold: 4 * 1024,
                compaction_size_threshold: 16 * 1024,
                ..StorageConfig::default()
            },
        )
        .unwrap()
    }

    let mut storage = reopen(temp_dir.path());
    const KEY_SPACE: u64 = 100;

    for round in 0..5 {
        for _ in 0..300 {
            let k = format!("key{:03}", rng.next() % KEY_SPACE).into_bytes();
            match rng.next() % 10 {
                // 20% deletes, 80% puts with random payloads
                0 | 1 => {
                    storage.delete(&k).unwrap();
                    model.remove(&k);
                }
                _ => {
                    let v = format!("payload{}", rng.next()).repeat(8).into_bytes();
                    storage.put(k.clone(), v.clone()).unwrap();
                    model.insert(k, v);
                }
            }
        }

        // Restart and verify the store agrees with the model on the whole
        // key space (present and absent keys alike)
        drop(storage);
        storage = reopen(temp_dir.path());
        for i in 0..KEY_SPACE {
            let k = format!("key{:03}", i).into_bytes();
            assert_eq!(
                storage.get(&k).unwrap(),
                model.get(&k).cloned(),
                "round {} key {}",
                round,
                i
            );
        }
    }
}
