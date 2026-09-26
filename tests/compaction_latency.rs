//! A compaction must not stop the store.
//!
//! The background compactor used to take the store's write lock and hold it
//! for the whole of `compact_now()`, so every read and every write stalled for
//! as long as the merge took. Compaction is the one operation whose cost grows
//! with the size of the data rather than the size of the request, which makes
//! it the worst possible thing to hold an exclusive lock across.
//!
//! The merge now runs with the store unlocked, which is sound because SSTables
//! are immutable once written: the inputs cannot change underneath it. The lock
//! is taken twice and briefly, to choose the work and to install the result.
//!
//! The assertion is relative rather than a fixed millisecond bound, so it means
//! the same thing on a slow CI runner as on a fast laptop: first measure what a
//! synchronous compaction of this fixture costs, then require that no single
//! write pays more than a small fraction of it while the same compaction runs
//! in the background.

use lsm_rust::{Compression, SharedStorage, Storage, StorageConfig, WalSync};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Builds the fixture: a small memtable so the fill produces several level-0
/// tables for the compactor to merge.
fn fill_config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 256 * 1024,
        compaction_size_threshold: 64 * 1024 * 1024,
        level0_file_limit: 4,
        // Compaction is driven by the background thread alone, so the write
        // path never does it inline and the measurement is unambiguous.
        inline_compaction: false,
        compression: Compression::None,
        ..StorageConfig::default()
    }
}

/// Opens the same fixture for measurement, with everything that is not lock
/// contention taken out of the write path.
///
/// The thresholds are per-handle rather than stored in the data directory, so
/// the tables the fill produced are still there to compact.
///
/// Two things had to go, and leaving either in measures the wrong thing:
///
///   - `WalSync::Always` fsyncs every write. On a contended runner one fsync
///     can take hundreds of milliseconds, which has nothing to do with who
///     holds the lock. An earlier version of this test failed CI at a 255 ms
///     "stall" for exactly that reason.
///   - A small memtable makes the writer thread trigger its own flush, which
///     writes an SSTable under the write lock. That is a stall the writer
///     inflicts on itself, not one compaction inflicts on it.
///
/// Neither is affected by the change under test, so both are noise here. What
/// remains is the question the test is asking: while a merge runs, can a
/// writer take the lock?
fn measure_config() -> StorageConfig {
    StorageConfig {
        // Large enough that the writer never flushes during the measurement.
        memtable_size_threshold: 256 * 1024 * 1024,
        wal_sync: WalSync::Batched {
            every_n_writes: usize::MAX,
        },
        ..fill_config()
    }
}

/// Fill a store with overlapping key ranges across several flushes, so the
/// planner merges rather than promoting. A promotion only relinks files and
/// would not hold the lock long enough to measure.
fn fill(dir: &Path) {
    let mut db = Storage::with_config(dir, fill_config()).unwrap();
    for round in 0..8 {
        for i in 0..2000 {
            db.put(
                format!("key{:06}", i).into_bytes(),
                format!("round{}-{}", round, "v".repeat(180)).into_bytes(),
            )
            .unwrap();
        }
    }
}

#[test]
fn a_background_compaction_does_not_stall_writes() {
    // What the same compaction costs when it is allowed to hold the store.
    let baseline = {
        let temp = TempDir::new().unwrap();
        fill(temp.path());
        let mut db = Storage::with_config(temp.path(), measure_config()).unwrap();
        let started = Instant::now();
        db.compact_now().unwrap();
        started.elapsed()
    };
    assert!(
        baseline > Duration::from_millis(20),
        "the fixture must take long enough to compact to measure anything, took {baseline:?}"
    );

    // The same work, run by the background compactor while a writer hammers
    // the store.
    let temp = TempDir::new().unwrap();
    fill(temp.path());
    let db = SharedStorage::with_config(temp.path(), measure_config()).unwrap();
    let before = db.stats().unwrap().compactions_total;

    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = Arc::clone(&stop);
    let writer_db = db.clone();
    let writer = thread::spawn(move || {
        let mut worst = Duration::ZERO;
        let mut writes = 0u64;
        while !writer_stop.load(Ordering::Relaxed) {
            let started = Instant::now();
            writer_db
                .put(format!("live{:06}", writes).into_bytes(), b"v".to_vec())
                .unwrap();
            worst = worst.max(started.elapsed());
            writes += 1;
        }
        (worst, writes)
    });

    let compactor = db.spawn_compactor(Duration::from_millis(10));

    // Wait for at least one compaction to be recorded, then a moment more so
    // the writer's samples span it.
    let deadline = Instant::now() + Duration::from_secs(30);
    while db.stats().unwrap().compactions_total == before {
        assert!(Instant::now() < deadline, "no compaction ran within 30s");
        thread::sleep(Duration::from_millis(5));
    }
    thread::sleep(Duration::from_millis(50));

    stop.store(true, Ordering::Relaxed);
    let (worst, writes) = writer.join().unwrap();
    assert_eq!(
        compactor.error_count(),
        0,
        "the background compactor reported errors"
    );
    drop(compactor);

    assert!(writes > 0, "the writer should have made progress");

    // Holding the lock across the merge would put `worst` at or above the
    // baseline. Released, the longest a write can wait is the manifest commit.
    // Half the synchronous cost. Holding the lock puts the stall at or above
    // the full cost, so this separates the two decisively while leaving room
    // for a runner that preempts the writer at an unlucky moment.
    let bound = baseline / 2;
    eprintln!(
        "compaction {baseline:?} synchronous; worst write stall {worst:?} \
         over {writes} writes (bound {bound:?})"
    );
    assert!(
        worst < bound,
        "a write stalled {worst:?} during a compaction that takes {baseline:?} \
         synchronously (bound {bound:?}, {writes} writes completed)"
    );
}
