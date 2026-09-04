//! Concurrency stress: many writers, a background compactor, live scanners
//! and checkpoints all running against one store at once.
use lsm_rust::{SharedStorage, StorageConfig};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn cfg() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 4 * 1024,
        compaction_size_threshold: 16 * 1024,
        level0_file_limit: 2,
        inline_compaction: false, // driven by the background compactor
        block_cache_size: 1 << 20,
        ..StorageConfig::default()
    }
}

#[test]
fn concurrent_increments_never_lose_an_update() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(SharedStorage::with_config(dir.path(), cfg()).unwrap());
    let _compactor = db.spawn_compactor(Duration::from_millis(5));

    const THREADS: usize = 8;
    const PER_THREAD: usize = 40;
    const COUNTERS: usize = 4;

    for c in 0..COUNTERS {
        db.put(format!("ctr{c}").into_bytes(), b"0".to_vec())
            .unwrap();
    }

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..PER_THREAD {
                let key = format!("ctr{}", (t + i) % COUNTERS).into_bytes();
                db.transaction(500, |tx| {
                    let cur = tx.get(&key)?.unwrap_or_else(|| b"0".to_vec());
                    let n: u64 = String::from_utf8_lossy(&cur).parse().unwrap_or(0);
                    tx.put(key.clone(), (n + 1).to_string().into_bytes());
                    Ok(())
                })
                .unwrap_or_else(|e| panic!("thread {t} op {i} failed: {e}"));
                // Unrelated traffic to keep flushes and compactions running.
                db.put(format!("noise{t}_{i}").into_bytes(), vec![b'x'; 64])
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let total: u64 = (0..COUNTERS)
        .map(|c| {
            let v = db.get(&format!("ctr{c}").into_bytes()).unwrap().unwrap();
            String::from_utf8_lossy(&v).parse::<u64>().unwrap()
        })
        .sum();
    assert_eq!(
        total,
        (THREADS * PER_THREAD) as u64,
        "lost updates under contention"
    );
}

#[test]
fn concurrent_readers_writers_and_checkpoints() {
    let dir = TempDir::new().unwrap();
    let cp = TempDir::new().unwrap();
    let db = Arc::new(SharedStorage::with_config(dir.path(), cfg()).unwrap());
    let _compactor = db.spawn_compactor(Duration::from_millis(5));

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = Vec::new();

    // Writers
    for t in 0..4 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            for i in 0..300 {
                db.put(format!("k{t}_{i:04}").into_bytes(), vec![b'v'; 48])
                    .unwrap();
                if i % 7 == 0 {
                    db.put_with_ttl(
                        format!("t{t}_{i:04}").into_bytes(),
                        vec![b'v'; 48],
                        Duration::from_millis(30),
                    )
                    .unwrap();
                }
            }
        }));
    }
    // Scanners — must never see a torn or impossible view.
    for _ in 0..2 {
        let db = Arc::clone(&db);
        let stop = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let mut n = 0usize;
                db.scan_for_each(b"k", Some(b"l"), |_k, _v| {
                    n += 1;
                    Ok(())
                })
                .unwrap();
                let _ = db.stats().unwrap();
            }
        }));
    }
    // Checkpointer
    {
        let db = Arc::clone(&db);
        let root = cp.path().to_path_buf();
        handles.push(thread::spawn(move || {
            for i in 0..5 {
                db.checkpoint(root.join(format!("cp{i}"))).unwrap();
                thread::sleep(Duration::from_millis(10));
            }
        }));
    }

    // Writers and the checkpointer finish; then stop the scanners.
    for (i, h) in handles.into_iter().enumerate() {
        if i == 4 {
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
        }
        h.join().unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    for t in 0..4 {
        for i in 0..300 {
            assert_eq!(
                db.get(&format!("k{t}_{i:04}").into_bytes()).unwrap(),
                Some(vec![b'v'; 48]),
                "lost k{t}_{i:04}"
            );
        }
    }
}
