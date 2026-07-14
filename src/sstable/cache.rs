use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Key of a cached block: the SSTable file and the block's offset within
/// its data section. SSTable files are immutable and their names are never
/// reused (sequence numbers are monotonic), so entries never go stale.
type CacheKey = (PathBuf, u64);

/// A shared LRU cache of (decompressed) SSTable data blocks, bounded by
/// total cached bytes.
///
/// One cache is shared by all SSTables of a store, so hot blocks stay in
/// memory across flushes and compactions. Point lookups that hit the cache
/// skip both the disk read and (for compressed tables) the decompression.
pub struct BlockCache {
    inner: Mutex<CacheInner>,
    capacity: usize,
}

struct CacheInner {
    map: HashMap<CacheKey, (Arc<Vec<u8>>, u64)>,
    lru: BTreeMap<u64, CacheKey>, // access tick -> key, oldest first
    used_bytes: usize,
    next_tick: u64,
    hits: u64,
    misses: u64,
}

impl BlockCache {
    /// Create a cache holding at most `capacity` bytes of block data.
    pub fn new(capacity: usize) -> Self {
        BlockCache {
            inner: Mutex::new(CacheInner {
                map: HashMap::new(),
                lru: BTreeMap::new(),
                used_bytes: 0,
                next_tick: 0,
                hits: 0,
                misses: 0,
            }),
            capacity,
        }
    }

    /// Look up a block, refreshing its LRU position on a hit.
    pub fn get(&self, path: &Path, offset: u64) -> Option<Arc<Vec<u8>>> {
        let mut inner = self.inner.lock().unwrap();
        let tick = inner.next_tick;
        inner.next_tick += 1;

        let key = (path.to_path_buf(), offset);
        match inner.map.get_mut(&key) {
            Some((data, last_tick)) => {
                let data = Arc::clone(data);
                let old_tick = std::mem::replace(last_tick, tick);
                inner.lru.remove(&old_tick);
                inner.lru.insert(tick, key);
                inner.hits += 1;
                Some(data)
            }
            None => {
                inner.misses += 1;
                None
            }
        }
    }

    /// Insert a block, evicting least-recently-used blocks to stay within
    /// capacity. Blocks larger than the whole cache are not cached.
    pub fn insert(&self, path: &Path, offset: u64, data: Arc<Vec<u8>>) {
        if data.len() > self.capacity {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        let key = (path.to_path_buf(), offset);
        if inner.map.contains_key(&key) {
            return;
        }

        while inner.used_bytes + data.len() > self.capacity {
            let Some((&oldest_tick, _)) = inner.lru.iter().next() else {
                break;
            };
            let evicted_key = inner.lru.remove(&oldest_tick).unwrap();
            if let Some((evicted, _)) = inner.map.remove(&evicted_key) {
                inner.used_bytes -= evicted.len();
            }
        }

        let tick = inner.next_tick;
        inner.next_tick += 1;
        inner.used_bytes += data.len();
        inner.lru.insert(tick, key.clone());
        inner.map.insert(key, (data, tick));
    }

    /// Drop all cached blocks belonging to `path` (e.g. after the file was
    /// deleted by compaction), freeing their memory.
    pub fn purge_file(&self, path: &Path) {
        let mut inner = self.inner.lock().unwrap();
        let stale: Vec<CacheKey> = inner
            .map
            .keys()
            .filter(|(p, _)| p == path)
            .cloned()
            .collect();
        for key in stale {
            if let Some((data, tick)) = inner.map.remove(&key) {
                inner.used_bytes -= data.len();
                inner.lru.remove(&tick);
            }
        }
    }

    /// Bytes currently held by the cache.
    pub fn used_bytes(&self) -> usize {
        self.inner.lock().unwrap().used_bytes
    }

    /// (hits, misses) counters since creation.
    pub fn stats(&self) -> (u64, u64) {
        let inner = self.inner.lock().unwrap();
        (inner.hits, inner.misses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block(n: usize) -> Arc<Vec<u8>> {
        Arc::new(vec![0u8; n])
    }

    #[test]
    fn test_hit_and_miss() {
        let cache = BlockCache::new(1024);
        let path = Path::new("a.sst");

        assert!(cache.get(path, 0).is_none());
        cache.insert(path, 0, block(100));
        assert_eq!(cache.get(path, 0).unwrap().len(), 100);
        assert!(cache.get(path, 999).is_none());
        assert_eq!(cache.stats(), (1, 2));
    }

    #[test]
    fn test_lru_eviction_respects_capacity() {
        let cache = BlockCache::new(250);
        let path = Path::new("a.sst");

        cache.insert(path, 0, block(100));
        cache.insert(path, 1, block(100));
        // Touch block 0 so block 1 becomes least recently used
        cache.get(path, 0);

        cache.insert(path, 2, block(100)); // must evict block 1
        assert!(cache.used_bytes() <= 250);
        assert!(cache.get(path, 0).is_some());
        assert!(cache.get(path, 1).is_none());
        assert!(cache.get(path, 2).is_some());
    }

    #[test]
    fn test_oversized_block_not_cached() {
        let cache = BlockCache::new(50);
        let path = Path::new("a.sst");
        cache.insert(path, 0, block(100));
        assert_eq!(cache.used_bytes(), 0);
    }

    #[test]
    fn test_purge_file() {
        let cache = BlockCache::new(1024);
        cache.insert(Path::new("a.sst"), 0, block(100));
        cache.insert(Path::new("b.sst"), 0, block(100));

        cache.purge_file(Path::new("a.sst"));
        assert!(cache.get(Path::new("a.sst"), 0).is_none());
        assert!(cache.get(Path::new("b.sst"), 0).is_some());
        assert_eq!(cache.used_bytes(), 100);
    }
}
