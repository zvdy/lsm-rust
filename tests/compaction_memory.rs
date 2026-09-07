//! Compaction must not hold the level it is compacting.
//!
//! Compacting a level means reading every entry of several SSTables and
//! writing one sorted table out. Doing that through a map keyed by every
//! entry in the level makes peak memory a multiple of the level's size — and
//! the level being compacted is, by definition, the largest thing around. It
//! also happens under the store's write lock, so the cost is paid as a stall
//! by every reader and writer.
//!
//! The inputs are already sorted, so the merge needs only one entry per table
//! in flight. This test measures that the merge holds no more than that.
//!
//! One test per file on purpose: it measures process-wide allocation, and
//! cargo gives each integration test file its own process, so the count is not
//! polluted by the rest of the suite running in parallel.

use lsm_rust::{Compression, Storage, StorageConfig};
use std::alloc::{GlobalAlloc, Layout, System};
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use tempfile::TempDir;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Counting;

fn grew(size: usize) {
    let now = LIVE.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

fn shrank(size: usize) {
    // `fetch_update` rather than `fetch_sub`: an allocation made before the
    // counter was armed would otherwise underflow it.
    let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
        Some(live.saturating_sub(size))
    });
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grew(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            grew(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        shrank(layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            shrank(layout.size());
            grew(new_size);
        }
        new
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Compression is pinned off so the bytes on disk are the bytes being merged,
/// which makes the ratio below mean what it says.
fn config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 256 * 1024,
        // High enough that level 1 is never itself compacted during the run.
        compaction_size_threshold: 64 * 1024 * 1024,
        level0_file_limit: 4,
        // Compaction is driven explicitly, so the measurement covers it alone.
        inline_compaction: false,
        compression: Compression::None,
        ..StorageConfig::default()
    }
}

fn sstable_bytes(dir: &std::path::Path) -> u64 {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            path.extension()
                .is_some_and(|ext| ext == "sst")
                .then(|| fs::metadata(&path).unwrap().len())
        })
        .sum()
}

#[test]
fn compacting_a_level_does_not_hold_the_level() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::with_config(temp.path(), config()).unwrap();

    // Overlapping key ranges across flushes, so the planner merges rather than
    // promoting — a promotion moves no data and would measure nothing.
    for round in 0..8 {
        for i in 0..2000 {
            db.put(
                format!("key{:06}", i).into_bytes(),
                format!("round{}-{}", round, "v".repeat(180)).into_bytes(),
            )
            .unwrap();
        }
    }
    drop(db);

    let on_disk = sstable_bytes(temp.path());
    assert!(
        on_disk > 2 * 1024 * 1024,
        "the fixture should build a level worth measuring, got {on_disk} bytes"
    );

    let mut db = Storage::with_config(temp.path(), config()).unwrap();
    // Armed after the store is open, so the measurement covers the compaction
    // and not the fixture.
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    let before = PEAK.load(Ordering::Relaxed);
    db.compact_now().unwrap();
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(before);

    // Merging through a map of the whole level measured 2.55x the level's
    // size; the k-way merge measures 1.39x, the remainder being the output
    // table, which is buffered until the header can be written. The bound is
    // set between the two: it fails on the old shape and leaves room for
    // allocator differences across platforms.
    let ratio = peak as f64 / on_disk as f64;
    assert!(
        ratio < 2.0,
        "compacting {on_disk} bytes peaked at {peak} bytes ({ratio:.2}x the level)"
    );
}
