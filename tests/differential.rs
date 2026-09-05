//! A randomized differential test: the engine against a plain in-memory model.
//!
//! `recovery.rs` already does this for puts, deletes and restarts. This widens
//! it to the surfaces added since — expiry, write batches, compaction,
//! snapshots, checkpoints and every scan shape — and runs the whole thing
//! under each combination of LZ4 compression and the block cache, which the
//! feature-specific suites mostly exercise one at a time.
//!
//! Deadlines are always far in the past or far in the future, so the clock
//! cannot move underneath a round and make the comparison flaky.

use lsm_rust::{Compression, Storage, StorageConfig, WriteBatch};
use std::collections::BTreeMap;
use std::path::Path;
use tempfile::TempDir;

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

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn past() -> u64 {
    now_ms() - 3_600_000
}
fn future() -> u64 {
    now_ms() + 3_600_000
}

static LZ4: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static CACHE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

fn cfg() -> StorageConfig {
    use std::sync::atomic::Ordering::Relaxed;
    StorageConfig {
        memtable_size_threshold: 4 * 1024,
        compaction_size_threshold: 16 * 1024,
        level0_file_limit: 2,
        block_cache_size: if CACHE.load(Relaxed) { 1 << 20 } else { 0 },
        compression: if LZ4.load(Relaxed) {
            Compression::Lz4
        } else {
            Compression::None
        },
        ..StorageConfig::default()
    }
}
fn reopen(p: &Path) -> Storage {
    Storage::with_config(p, cfg()).unwrap()
}

type Model = BTreeMap<Vec<u8>, (Vec<u8>, Option<u64>)>;

fn visible(model: &Model) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let now = now_ms();
    model
        .iter()
        .filter(|(_, (_, exp))| exp.is_none_or(|d| now <= d))
        .map(|(k, (v, _))| (k.clone(), v.clone()))
        .collect()
}

fn run_seed(seed: u64) -> Result<(), String> {
    let dir = TempDir::new().unwrap();
    let mut rng = Rng(seed);
    let mut model: Model = BTreeMap::new();
    let mut db = reopen(dir.path());
    const KEYS: u64 = 120;
    let key = |i: u64| format!("key{:04}", i).into_bytes();
    let cp_root = TempDir::new().unwrap();
    let mut pending: Option<(std::path::PathBuf, Model)> = None;

    for round in 0..4 {
        // Snapshot the store and the model together, then mutate and compact
        // underneath both for the rest of the round. A snapshot only pins
        // versions in the Storage that issued it, so it is scoped to a round
        // and released before any reopen.
        let snap = db.snapshot();
        let snap_model = model.clone();

        for _ in 0..250 {
            let k = key(rng.next() % KEYS);
            match rng.next() % 12 {
                0 | 1 => {
                    db.delete(&k).unwrap();
                    model.remove(&k);
                }
                2 => {
                    let v = format!("p{}", rng.next()).repeat(4).into_bytes();
                    db.put_with_expiry(k.clone(), v.clone(), past()).unwrap();
                    model.insert(k, (v, Some(past())));
                }
                3 => {
                    let v = format!("p{}", rng.next()).repeat(4).into_bytes();
                    let d = future();
                    db.put_with_expiry(k.clone(), v.clone(), d).unwrap();
                    model.insert(k, (v, Some(d)));
                }
                4 => {
                    // small mixed batch
                    let mut batch = WriteBatch::new();
                    for _ in 0..3 {
                        let bk = key(rng.next() % KEYS);
                        if rng.next().is_multiple_of(3) {
                            batch.delete(bk.clone());
                            model.remove(&bk);
                        } else {
                            let v = format!("b{}", rng.next()).repeat(4).into_bytes();
                            batch.put(bk.clone(), v.clone());
                            model.insert(bk, (v, None));
                        }
                    }
                    db.write_batch(batch).unwrap();
                }
                5 => {
                    db.compact_now().unwrap();
                }
                _ => {
                    let v = format!("p{}", rng.next()).repeat(4).into_bytes();
                    db.put(k.clone(), v.clone()).unwrap();
                    model.insert(k, (v, None));
                }
            }
        }

        let expected = visible(&model);

        // A checkpoint plus the model as of the same instant.
        if round == 2 {
            let target = cp_root.path().join(format!("cp{seed}"));
            db.checkpoint(&target).unwrap();
            pending = Some((target, model.clone()));
        }

        // Snapshot reads must still see their own versions after later
        // writes and compactions.
        {
            let want = visible(&snap_model);
            for i in 0..KEYS {
                let k = key(i);
                let got = db.get_at(&snap, &k).unwrap();
                if got != want.get(&k).cloned() {
                    return Err(format!(
                        "seed {seed} round {round} SNAPSHOT GET {} diverged",
                        String::from_utf8_lossy(&k)
                    ));
                }
            }
            let scanned: BTreeMap<_, _> = db
                .scan_at(&snap, b"", b"zzz")
                .unwrap()
                .into_iter()
                .collect();
            if scanned != want {
                return Err(format!(
                    "seed {seed} round {round} SNAPSHOT SCAN: got {} want {}",
                    scanned.len(),
                    want.len()
                ));
            }
        }

        // Prefix scans
        for p in ["key00", "key01", "key1"] {
            let got: BTreeMap<_, _> = db.scan_prefix(p.as_bytes()).unwrap().into_iter().collect();
            let want: BTreeMap<_, _> = expected
                .iter()
                .filter(|(k, _)| k.starts_with(p.as_bytes()))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if got != want {
                return Err(format!(
                    "seed {seed} round {round} PREFIX {p}: got {} want {}",
                    got.len(),
                    want.len()
                ));
            }
        }

        drop(snap);
        if round % 2 == 1 {
            drop(db);
            db = reopen(dir.path());
        }

        // 1. point lookups over the whole key space
        for i in 0..KEYS {
            let k = key(i);
            let got = db.get(&k).unwrap();
            let want = expected.get(&k).cloned();
            if got != want {
                return Err(format!(
                    "seed {seed} round {round} GET {} : got {:?} want {:?}",
                    String::from_utf8_lossy(&k),
                    got.as_ref()
                        .map(|v| String::from_utf8_lossy(&v[..8.min(v.len())]).to_string()),
                    want.as_ref()
                        .map(|v| String::from_utf8_lossy(&v[..8.min(v.len())]).to_string())
                ));
            }
        }
        // 2. full scan
        let scanned: BTreeMap<_, _> = db.scan(b"", b"zzz").unwrap().into_iter().collect();
        if scanned != expected {
            return Err(format!(
                "seed {seed} round {round} FULL SCAN mismatch: got {} keys want {}",
                scanned.len(),
                expected.len()
            ));
        }
        // 3. windowed scans
        for w in 0..6 {
            let lo = rng.next() % KEYS;
            let hi = (lo + 1 + rng.next() % 40).min(KEYS);
            let got: BTreeMap<_, _> = db.scan(&key(lo), &key(hi)).unwrap().into_iter().collect();
            let want: BTreeMap<_, _> = expected
                .range(key(lo)..key(hi))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if got != want {
                return Err(format!(
                    "seed {seed} round {round} SCAN[{lo},{hi}) w{w}: got {} want {}",
                    got.len(),
                    want.len()
                ));
            }
        }
    }

    // The checkpoint, opened cold, must match the model as of when it was taken.
    if let Some((path, cp_model)) = pending {
        let restored = reopen(&path);
        let want = visible(&cp_model);
        for i in 0..KEYS {
            let k = key(i);
            let got = restored.get(&k).unwrap();
            if got != want.get(&k).cloned() {
                return Err(format!(
                    "seed {seed} CHECKPOINT GET {} diverged",
                    String::from_utf8_lossy(&k)
                ));
            }
        }
        let scanned: BTreeMap<_, _> = restored.scan(b"", b"zzz").unwrap().into_iter().collect();
        if scanned != want {
            return Err(format!(
                "seed {seed} CHECKPOINT SCAN: got {} want {}",
                scanned.len(),
                want.len()
            ));
        }
    }
    Ok(())
}

#[test]
fn engine_matches_the_model_across_configurations() {
    use std::sync::atomic::Ordering::Relaxed;
    let mut failures = Vec::new();
    for (lz4, cache) in [(false, false), (true, false), (false, true), (true, true)] {
        LZ4.store(lz4, Relaxed);
        CACHE.store(cache, Relaxed);
        for seed in [0xC0FFEE, 42, 0xDEADBEEF] {
            if let Err(e) = run_seed(seed) {
                eprintln!("FUZZFAIL lz4={lz4} cache={cache} {e}");
                failures.push(e);
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} configuration(s) diverged",
        failures.len()
    );
}
