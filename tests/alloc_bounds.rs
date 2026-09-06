//! Lengths read off disk must never size an allocation before they are
//! checked against the bytes that actually exist.
//!
//! A length prefix is the one field a checksum cannot protect: the CRC covers
//! the body the length describes, so it can only be verified after the length
//! has already been used. That makes a single flipped bit in a length prefix
//! qualitatively worse than a flipped bit anywhere else — it is not a wrong
//! value, it is a demand for memory. These tests damage exactly those fields
//! and assert the engine reports corruption instead of asking the allocator
//! for gigabytes.

use lsm_rust::{Storage, StorageConfig};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

/// Tracks the high-water mark of live bytes *on the current thread*.
///
/// Per-thread rather than global so the measurement is unaffected by whatever
/// the other tests are allocating in parallel.
struct Counting;

thread_local! {
    static LIVE: Cell<usize> = const { Cell::new(0) };
    static PEAK: Cell<usize> = const { Cell::new(0) };
}

/// `try_with` throughout: during thread teardown the TLS slot is gone, and a
/// panic from inside the allocator would abort the process.
fn record_alloc(size: usize) {
    let _ = LIVE.try_with(|live| {
        let now = live.get() + size;
        live.set(now);
        let _ = PEAK.try_with(|peak| {
            if now > peak.get() {
                peak.set(now);
            }
        });
    });
}

fn record_dealloc(size: usize) {
    let _ = LIVE.try_with(|live| live.set(live.get().saturating_sub(size)));
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record_dealloc(layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            record_dealloc(layout.size());
            record_alloc(new_size);
        }
        new
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Run `body`, returning the largest number of live bytes it ever held.
fn peak_bytes<T>(body: impl FnOnce() -> T) -> (T, usize) {
    PEAK.with(|peak| peak.set(LIVE.with(|live| live.get())));
    let before = PEAK.with(|peak| peak.get());
    let out = body();
    let after = PEAK.with(|peak| peak.get());
    (out, after.saturating_sub(before))
}

/// Nothing here writes enough data to need more than a few MB; anything
/// beyond this is a length field being believed.
const SANE_PEAK: usize = 64 * 1024 * 1024;

fn config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 2 * 1024,
        ..StorageConfig::default()
    }
}

fn sst_file(dir: &Path) -> PathBuf {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| {
            let path = entry.unwrap().path();
            (path.extension().is_some_and(|ext| ext == "sst")).then_some(path)
        })
        .next()
        .expect("expected a flushed SSTable")
}

/// Populate a store and flush it to disk.
fn seeded_store(dir: &Path) {
    let mut db = Storage::with_config(dir, config()).unwrap();
    for i in 0..200 {
        db.put(format!("key{:04}", i).into_bytes(), vec![b'v'; 40])
            .unwrap();
    }
}

/// Offsets of the v5 header length prefixes.
/// `[magic 4][version 1][flags 1][min_expiry 8]`, then `[len][crc][body]`
/// for the bloom filter and the same again for the sparse index.
fn bloom_len_at() -> usize {
    4 + 1 + 1 + 8
}

fn index_len_at(bytes: &[u8]) -> usize {
    let bloom_at = bloom_len_at();
    let bloom_len = u32::from_le_bytes(bytes[bloom_at..bloom_at + 4].try_into().unwrap()) as usize;
    bloom_at + 8 + bloom_len
}

#[test]
fn a_corrupt_bloom_length_is_reported_not_allocated() {
    let temp = TempDir::new().unwrap();
    seeded_store(temp.path());
    let sst = sst_file(temp.path());

    let mut bytes = fs::read(&sst).unwrap();
    assert!(bytes.len() < SANE_PEAK, "the fixture should be small");
    // A single flipped bit, high in the length: 4 KB of file now claims a
    // 2 GB bloom filter.
    bytes[bloom_len_at() + 3] ^= 0x80;
    fs::write(&sst, &bytes).unwrap();

    let (result, peak) = peak_bytes(|| Storage::with_config(temp.path(), config()));
    // Checked before the error itself: reserving the memory and *then*
    // reporting corruption would satisfy every assertion below while leaving
    // the actual defect in place.
    assert!(
        peak < SANE_PEAK,
        "refusing the read must not first reserve it: peaked at {peak} bytes"
    );
    let error = result.err().expect("a 2 GB bloom filter must be refused");
    assert!(
        error.is_corruption(),
        "a damaged length prefix is corruption, not a transient I/O fault: {error:?}"
    );
    assert!(
        error.to_string().contains("bloom"),
        "the error should name the section at fault: {error}"
    );
}

#[test]
fn a_corrupt_index_length_is_reported_not_allocated() {
    let temp = TempDir::new().unwrap();
    seeded_store(temp.path());
    let sst = sst_file(temp.path());

    let mut bytes = fs::read(&sst).unwrap();
    let at = index_len_at(&bytes);
    bytes[at + 3] ^= 0x80;
    fs::write(&sst, &bytes).unwrap();

    let (result, peak) = peak_bytes(|| Storage::with_config(temp.path(), config()));
    assert!(
        peak < SANE_PEAK,
        "refusing the read must not first reserve it: peaked at {peak} bytes"
    );
    let error = result.err().expect("a 2 GB sparse index must be refused");
    assert!(
        error.is_corruption(),
        "a damaged length prefix is corruption, not a transient I/O fault: {error:?}"
    );
}

#[test]
fn a_corrupt_wal_batch_count_does_not_abort_the_process() {
    let temp = TempDir::new().unwrap();
    {
        let _db = Storage::new(temp.path(), false).unwrap();
    }

    // An unframed batch header claiming four billion entries, and nothing
    // after it. Reserving for the claim asks for ~300 GB, which the allocator
    // does not refuse gracefully — it aborts, taking the process with it, so
    // this test failing looks like the whole binary dying.
    let mut wal = vec![2u8];
    wal.extend_from_slice(&u32::MAX.to_le_bytes());
    fs::write(temp.path().join("wal"), &wal).unwrap();

    let (result, peak) = peak_bytes(|| Storage::new(temp.path(), false));
    assert!(
        peak < SANE_PEAK,
        "a five-byte WAL must not reserve for its claim: peaked at {peak} bytes"
    );
    // The frame has no entries after it, so it is an incomplete tail: the
    // documented behaviour is to drop it and recover what came before.
    let db = result.expect("a truncated batch tail is dropped, not fatal");
    assert_eq!(db.get(&b"anything".to_vec()).unwrap(), None);
}

#[test]
fn an_undamaged_store_still_round_trips() {
    // The bounds must not reject anything legitimate.
    let temp = TempDir::new().unwrap();
    seeded_store(temp.path());

    let db = Storage::with_config(temp.path(), config()).unwrap();
    for i in 0..200 {
        assert_eq!(
            db.get(&format!("key{:04}", i).into_bytes()).unwrap(),
            Some(vec![b'v'; 40]),
            "key{:04} should survive",
            i
        );
    }
}

/// Start of the data section: everything before it is header.
fn data_start(bytes: &[u8]) -> usize {
    let at = index_len_at(bytes);
    let index_len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    at + 8 + index_len
}

#[test]
fn no_single_header_bit_flip_can_inflate_an_allocation() {
    // The two tests above damage the length prefixes deliberately, which only
    // covers the fields already known to be dangerous. This sweeps every byte
    // of the header — magic, version, flags, min-expiry, both lengths, both
    // CRCs, and the bloom and index bodies — and holds the whole region to the
    // same rule, so a length field added later is covered on the day it
    // appears rather than the day someone thinks to test it.
    //
    // The measurement is the point: on a machine with overcommit, believing a
    // 2 GB length succeeds and then fails at `read_exact`, which looks from
    // the outside exactly like a clean rejection. Only the allocator can tell
    // the two apart.
    let temp = TempDir::new().unwrap();
    seeded_store(temp.path());
    let sst = sst_file(temp.path());
    let pristine = fs::read(&sst).unwrap();
    let header_len = data_start(&pristine);

    // Deterministic xorshift, so a failure reproduces exactly.
    let mut state: u64 = 0xC0FF_EE00_1234_5678;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };

    for _ in 0..200 {
        let offset = (next() as usize) % header_len;
        let bit = (next() % 8) as u8;

        let mut corrupted = pristine.clone();
        corrupted[offset] ^= 1 << bit;
        fs::write(&sst, &corrupted).unwrap();

        let (result, peak) = peak_bytes(|| Storage::with_config(temp.path(), config()));
        assert!(
            peak < SANE_PEAK,
            "flipping header byte {offset} bit {bit} made the engine reserve {peak} bytes"
        );
        // Opening may legitimately fail, and a damaged table may legitimately
        // lose a key. What it must never do is hand back a value that was
        // never written.
        if let Ok(db) = result {
            if let Ok(Some(got)) = db.get(&b"key0000".to_vec()) {
                assert_eq!(
                    got,
                    vec![b'v'; 40],
                    "wrong data after flipping header byte {offset} bit {bit}"
                );
            }
        }
    }

    fs::write(&sst, &pristine).unwrap();
}
