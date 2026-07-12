//! Criterion benchmarks for the core storage operations.
//!
//! Run with `cargo bench`. Results land in `target/criterion/` with HTML
//! reports comparing runs, which makes regressions visible when touching the
//! write, read, or compaction paths.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use lsm_rust::{Storage, StorageConfig};
use std::hint::black_box;
use tempfile::TempDir;

const VALUE_SIZE: usize = 128;

fn key(i: usize) -> Vec<u8> {
    format!("key{:08}", i).into_bytes()
}

fn value() -> Vec<u8> {
    vec![b'v'; VALUE_SIZE]
}

/// Sequential puts, including WAL fsync cost — this is the durable write path.
fn bench_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("put");
    group.throughput(Throughput::Bytes(VALUE_SIZE as u64));
    group.sample_size(20);

    group.bench_function("put_128b", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = Storage::new(temp_dir.path(), false).unwrap();
        let mut i = 0usize;
        b.iter(|| {
            storage.put(key(i), value()).unwrap();
            i += 1;
        });
    });

    group.finish();
}

/// Reads served straight from the memtable (no disk access).
fn bench_get_memtable(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut storage = Storage::new(temp_dir.path(), false).unwrap();
    // Stay below the default 512KB flush threshold so everything is in memory
    for i in 0..1000 {
        storage.put(key(i), value()).unwrap();
    }

    c.bench_function("get/memtable_hit", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = key(i % 1000);
            black_box(storage.get(&k).unwrap());
            i += 1;
        });
    });
}

fn sstable_backed_storage() -> (TempDir, Storage) {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        // Small threshold so the dataset is flushed into several SSTables
        memtable_size_threshold: 16 * 1024,
        ..StorageConfig::default()
    };
    let mut storage = Storage::with_config(temp_dir.path(), config).unwrap();
    for i in 0..2000 {
        storage.put(key(i), value()).unwrap();
    }
    (temp_dir, storage)
}

/// Reads that must go to disk, found in an SSTable.
fn bench_get_sstable(c: &mut Criterion) {
    let (_temp_dir, storage) = sstable_backed_storage();

    c.bench_function("get/sstable_hit", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = key(i % 2000);
            black_box(storage.get(&k).unwrap());
            i += 1;
        });
    });
}

/// Reads for absent keys — dominated by Bloom filter checks, which should
/// avoid touching disk for almost every table.
fn bench_get_missing(c: &mut Criterion) {
    let (_temp_dir, storage) = sstable_backed_storage();

    c.bench_function("get/missing_key", |b| {
        let mut i = 0usize;
        b.iter(|| {
            let k = format!("absent{:08}", i).into_bytes();
            black_box(storage.get(&k).unwrap());
            i += 1;
        });
    });
}

/// Deletes, including WAL fsync cost.
fn bench_delete(c: &mut Criterion) {
    c.bench_function("delete/tombstone_write", |b| {
        let temp_dir = TempDir::new().unwrap();
        let mut storage = Storage::new(temp_dir.path(), false).unwrap();
        let mut i = 0usize;
        b.iter(|| {
            let k = key(i);
            storage.delete(&k).unwrap();
            i += 1;
        });
    });
}

criterion_group!(
    benches,
    bench_put,
    bench_get_memtable,
    bench_get_sstable,
    bench_get_missing,
    bench_delete
);
criterion_main!(benches);
