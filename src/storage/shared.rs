use super::{Storage, StorageConfig};
use crate::{Key, Value};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// A cloneable, thread-safe handle to a [`Storage`] instance.
///
/// Clones share the same underlying store. Reads (`get`) take a shared lock
/// and run concurrently with each other; writes (`put`, `delete`) take an
/// exclusive lock. This is coarse-grained locking — writes serialize — which
/// keeps the engine simple while making it safe to share across threads.
///
/// # Example
///
/// ```no_run
/// use lsm_rust::SharedStorage;
/// use std::thread;
///
/// fn main() -> std::io::Result<()> {
///     let db = SharedStorage::new("./data", false)?;
///
///     let writer = {
///         let db = db.clone();
///         thread::spawn(move || db.put(b"key".to_vec(), b"value".to_vec()))
///     };
///     writer.join().unwrap()?;
///
///     assert_eq!(db.get(&b"key".to_vec())?, Some(b"value".to_vec()));
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct SharedStorage {
    inner: Arc<RwLock<Storage>>,
}

fn poisoned() -> io::Error {
    io::Error::other("storage lock poisoned by a panicked thread")
}

impl SharedStorage {
    /// Open (or create) a shared store at `data_dir` with default tuning.
    pub fn new<P: AsRef<Path>>(data_dir: P, verbose: bool) -> io::Result<Self> {
        Ok(Storage::new(data_dir, verbose)?.into_shared())
    }

    /// Open (or create) a shared store with explicit tuning options.
    pub fn with_config<P: AsRef<Path>>(data_dir: P, config: StorageConfig) -> io::Result<Self> {
        Ok(Storage::with_config(data_dir, config)?.into_shared())
    }

    /// Look up the current value for `key`. Concurrent with other reads.
    pub fn get(&self, key: &Key) -> io::Result<Option<Value>> {
        self.inner.read().map_err(|_| poisoned())?.get(key)
    }

    /// Insert or update `key`, durable once this returns.
    pub fn put(&self, key: Key, value: Value) -> io::Result<()> {
        self.inner.write().map_err(|_| poisoned())?.put(key, value)
    }

    /// Delete `key`, durable once this returns.
    pub fn delete(&self, key: &Key) -> io::Result<()> {
        self.inner.write().map_err(|_| poisoned())?.delete(key)
    }

    /// Range scan (`start <= key < end`). Concurrent with other reads.
    pub fn scan(&self, start: &[u8], end: &[u8]) -> io::Result<Vec<(Key, Value)>> {
        self.inner.read().map_err(|_| poisoned())?.scan(start, end)
    }

    /// Prefix scan. Concurrent with other reads.
    pub fn scan_prefix(&self, prefix: &[u8]) -> io::Result<Vec<(Key, Value)>> {
        self.inner
            .read()
            .map_err(|_| poisoned())?
            .scan_prefix(prefix)
    }

    /// Run any pending compactions now, under the write lock.
    pub fn compact_now(&self) -> io::Result<()> {
        self.inner.write().map_err(|_| poisoned())?.compact_now()
    }

    /// Spawn a background thread that runs pending compactions every
    /// `interval`, taking compaction off the write path (pair with
    /// `StorageConfig::inline_compaction = false`).
    ///
    /// The thread stops when the returned handle is dropped, or when every
    /// `SharedStorage` clone has been dropped (it holds only a weak
    /// reference and never keeps the store alive).
    pub fn spawn_compactor(&self, interval: Duration) -> CompactorHandle {
        let weak = Arc::downgrade(&self.inner);
        let stop = Arc::new(AtomicBool::new(false));
        let errors = Arc::new(AtomicUsize::new(0));

        let thread_stop = Arc::clone(&stop);
        let thread_errors = Arc::clone(&errors);
        let thread =
            thread::spawn(move || compactor_loop(weak, interval, &thread_stop, &thread_errors));

        CompactorHandle {
            stop,
            errors,
            thread: Some(thread),
        }
    }
}

fn compactor_loop(
    storage: Weak<RwLock<Storage>>,
    interval: Duration,
    stop: &AtomicBool,
    errors: &AtomicUsize,
) {
    while !stop.load(Ordering::Relaxed) {
        // Sleep in short slices so dropping the handle joins promptly
        let mut slept = Duration::ZERO;
        while slept < interval && !stop.load(Ordering::Relaxed) {
            let step = (interval - slept).min(Duration::from_millis(20));
            thread::sleep(step);
            slept += step;
        }
        if stop.load(Ordering::Relaxed) {
            return;
        }

        // The store has been dropped: nothing left to compact
        let Some(inner) = storage.upgrade() else {
            return;
        };
        let Ok(mut storage) = inner.write() else {
            return; // poisoned by a panicked writer
        };
        if storage.compact_now().is_err() {
            errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Guard for a background compaction thread; dropping it stops the thread.
pub struct CompactorHandle {
    stop: Arc<AtomicBool>,
    errors: Arc<AtomicUsize>,
    thread: Option<JoinHandle<()>>,
}

impl CompactorHandle {
    /// Number of background compaction runs that returned an error.
    pub fn error_count(&self) -> usize {
        self.errors.load(Ordering::Relaxed)
    }
}

impl Drop for CompactorHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Storage {
    /// Wrap this store in a cloneable, thread-safe [`SharedStorage`] handle.
    pub fn into_shared(self) -> SharedStorage {
        SharedStorage {
            inner: Arc::new(RwLock::new(self)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use tempfile::TempDir;

    // SharedStorage must be shareable across threads
    const _: fn() = || {
        fn assert_send_sync<T: Send + Sync + Clone>() {}
        assert_send_sync::<SharedStorage>();
    };

    #[test]
    fn test_concurrent_writers_and_readers() {
        let temp_dir = TempDir::new().unwrap();
        let db = SharedStorage::new(temp_dir.path(), false).unwrap();

        const WRITERS: usize = 4;
        const KEYS_PER_WRITER: usize = 200;

        // Writers fill disjoint key ranges while readers hammer lookups
        let mut handles = Vec::new();
        for w in 0..WRITERS {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for i in 0..KEYS_PER_WRITER {
                    let key = format!("w{}_key{:04}", w, i).into_bytes();
                    let value = format!("w{}_value{}", w, i).into_bytes();
                    db.put(key, value).unwrap();
                }
            }));
        }
        for r in 0..4 {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for i in 0..KEYS_PER_WRITER {
                    // Reads must never fail, whether or not the key exists yet
                    let key = format!("w{}_key{:04}", r, i).into_bytes();
                    db.get(&key).unwrap();
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }

        // Every written key is visible afterwards
        for w in 0..WRITERS {
            for i in 0..KEYS_PER_WRITER {
                let key = format!("w{}_key{:04}", w, i).into_bytes();
                let expected = format!("w{}_value{}", w, i).into_bytes();
                assert_eq!(db.get(&key).unwrap(), Some(expected));
            }
        }
    }

    #[test]
    fn test_concurrent_deletes() {
        let temp_dir = TempDir::new().unwrap();
        let db = SharedStorage::new(temp_dir.path(), false).unwrap();

        for i in 0..100 {
            db.put(format!("key{:03}", i).into_bytes(), b"value".to_vec())
                .unwrap();
        }

        // Two threads delete disjoint halves concurrently
        let handles: Vec<_> = [0usize, 1]
            .into_iter()
            .map(|half| {
                let db = db.clone();
                thread::spawn(move || {
                    for i in (half * 50)..((half + 1) * 50) {
                        if i % 2 == 0 {
                            db.delete(&format!("key{:03}", i).into_bytes()).unwrap();
                        }
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }

        for i in 0..100 {
            let expected = if i % 2 == 0 {
                None
            } else {
                Some(b"value".to_vec())
            };
            assert_eq!(
                db.get(&format!("key{:03}", i).into_bytes()).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn test_shared_handle_survives_flushes() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            memtable_size_threshold: 4 * 1024, // flush frequently under contention
            ..StorageConfig::default()
        };
        let db = SharedStorage::with_config(temp_dir.path(), config).unwrap();

        let handles: Vec<_> = (0..4)
            .map(|w| {
                let db = db.clone();
                thread::spawn(move || {
                    for i in 0..100 {
                        let key = format!("w{}_key{:03}", w, i).into_bytes();
                        db.put(key.clone(), vec![b'x'; 256]).unwrap();
                        assert_eq!(db.get(&key).unwrap(), Some(vec![b'x'; 256]));
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_background_compactor_drains_level0() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            memtable_size_threshold: 8 * 1024,
            inline_compaction: false,
            ..StorageConfig::default()
        };
        let db = SharedStorage::with_config(temp_dir.path(), config).unwrap();
        let compactor = db.spawn_compactor(Duration::from_millis(30));

        for i in 0..800 {
            db.put(format!("key{:04}", i).into_bytes(), vec![b'v'; 64])
                .unwrap();
        }

        // Wait for the background thread to bring level 0 under its limit
        let l0_count = |dir: &Path| {
            std::fs::read_dir(dir)
                .unwrap()
                .filter(|e| {
                    e.as_ref()
                        .unwrap()
                        .file_name()
                        .to_string_lossy()
                        .starts_with("L0_")
                })
                .count()
        };
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while l0_count(temp_dir.path()) >= 4 && std::time::Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            l0_count(temp_dir.path()) < 4,
            "background compactor never drained level 0"
        );
        assert_eq!(compactor.error_count(), 0);

        // Data intact while/after compaction ran concurrently
        for i in [0usize, 400, 799] {
            let key = format!("key{:04}", i).into_bytes();
            assert_eq!(db.get(&key).unwrap(), Some(vec![b'v'; 64]));
        }

        drop(compactor); // stops and joins the thread
    }
}
