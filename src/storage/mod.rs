mod manifest;
mod shared;
use manifest::{Manifest, ManifestEntry};
pub use shared::{CompactorHandle, SharedStorage};

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::memtable::MemTable;
use crate::sstable::{BlockCache, CompactionManager, Compression, SSTable, SSTableLookup};
use crate::wal::{Operation, WalSync, WAL};
use crate::{Key, Value};

/// Smallest byte string strictly greater than every string with this
/// prefix, or `None` if no such bound exists (prefix is empty or all 0xFF).
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(&last) = end.last() {
        if last == 0xFF {
            end.pop();
        } else {
            *end.last_mut().unwrap() = last + 1;
            return Some(end);
        }
    }
    None
}

/// Parse `L{level}_{seq}.sst` filenames.
fn parse_sst_filename(path: &Path) -> Option<(usize, u64)> {
    if path.extension().and_then(|s| s.to_str()) != Some("sst") {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    let (level, seq) = stem.strip_prefix('L')?.split_once('_')?;
    Some((level.parse().ok()?, seq.parse().ok()?))
}

static PUT_COUNT: AtomicUsize = AtomicUsize::new(0);
static TOTAL_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Tuning knobs for a [`Storage`] instance.
///
/// The defaults match the engine's historical behavior and are deliberately
/// small so that flushes and compactions are easy to observe; production-like
/// workloads will usually want larger thresholds.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Flush the memtable to a level-0 SSTable once it holds this many bytes.
    pub memtable_size_threshold: usize,
    /// Base size threshold for level compaction; level N compacts when it
    /// exceeds `compaction_size_threshold * level_multiplier^N` bytes.
    pub compaction_size_threshold: usize,
    /// Growth factor between consecutive level size thresholds.
    pub level_multiplier: u32,
    /// Compact level 0 once it holds this many SSTable files.
    pub level0_file_limit: usize,
    /// Compression applied to SSTable data blocks.
    pub compression: Compression,
    /// When the write-ahead log fsyncs (per write, or batched group commit).
    pub wal_sync: WalSync,
    /// Capacity in bytes of the shared block cache for SSTable reads
    /// (0 disables caching).
    pub block_cache_size: usize,
    /// Run compaction inline on the write path when a flush fills a level
    /// (default). Disable to drive compaction yourself via
    /// [`Storage::compact_now`] or a background
    /// [`SharedStorage::spawn_compactor`] thread.
    pub inline_compaction: bool,
    /// Print detailed progress information to stdout.
    pub verbose: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            memtable_size_threshold: 512 * 1024,    // 512KB
            compaction_size_threshold: 1024 * 1024, // 1MB
            level_multiplier: 4,
            level0_file_limit: 4,
            compression: Compression::None,
            wal_sync: WalSync::Always,
            block_cache_size: 4 * 1024 * 1024, // 4MB
            inline_compaction: true,
            verbose: false,
        }
    }
}

/// The main LSM-tree key-value store.
///
/// See the [crate-level documentation](crate) for an overview of the write,
/// read, and compaction paths.
pub struct Storage {
    memtable: MemTable,
    wal: WAL,
    sstables: HashMap<usize, Vec<SSTable>>, // level -> SSTables
    data_dir: PathBuf,
    sstable_counter: u64,
    compaction_manager: CompactionManager,
    block_cache: Option<Arc<BlockCache>>,
    manifest: Manifest,
    config: StorageConfig,
}

impl Storage {
    /// Open (or create) a store at `data_dir` with default tuning.
    ///
    /// Existing SSTables are loaded and any pending write-ahead-log entries
    /// are replayed into the memtable.
    pub fn new<P: AsRef<Path>>(data_dir: P, verbose: bool) -> io::Result<Self> {
        Self::with_config(
            data_dir,
            StorageConfig {
                verbose,
                ..StorageConfig::default()
            },
        )
    }

    /// Open (or create) a store at `data_dir` with explicit tuning options.
    pub fn with_config<P: AsRef<Path>>(data_dir: P, config: StorageConfig) -> io::Result<Self> {
        let verbose = config.verbose;
        if verbose {
            println!("Initializing storage at {:?}", data_dir.as_ref());
        }
        fs::create_dir_all(&data_dir)?;

        let block_cache = (config.block_cache_size > 0)
            .then(|| Arc::new(BlockCache::new(config.block_cache_size)));

        let wal_path = data_dir.as_ref().join("wal");
        let mut wal = WAL::with_sync(wal_path, config.wal_sync)?;
        let mut memtable = MemTable::new();

        // Replay WAL if it exists
        let mut replay_count = 0;
        for (op, key, value) in wal.replay()? {
            match op {
                Operation::Put => {
                    if let Some(value) = value {
                        memtable.insert(key, value);
                        replay_count += 1;
                    }
                }
                Operation::Delete => {
                    memtable.delete(key);
                    replay_count += 1;
                }
            }
        }
        if verbose && replay_count > 0 {
            println!("Replayed {} operations from WAL", replay_count);
        }

        // Load existing SSTables. The manifest is the authoritative record
        // of which tables are live; without one (legacy data directory) we
        // fall back to scanning the directory.
        let manifest = Manifest::new(data_dir.as_ref());
        let had_manifest = manifest.exists();

        let mut found: Vec<(usize, u64, PathBuf)> = Vec::new();
        match manifest.load()? {
            Some(entries) => {
                for entry in entries {
                    found.push((
                        entry.level,
                        entry.seq,
                        data_dir.as_ref().join(&entry.filename),
                    ));
                }

                // Delete unreferenced .sst files: leftovers from a flush or
                // compaction that crashed before its manifest commit, or old
                // tables whose deletion was interrupted after it.
                for dir_entry in fs::read_dir(&data_dir)? {
                    let path = dir_entry?.path();
                    if path.is_file()
                        && parse_sst_filename(&path).is_some()
                        && !found.iter().any(|(_, _, p)| *p == path)
                    {
                        if verbose {
                            println!("Removing orphan SSTable {:?}", path.file_name().unwrap());
                        }
                        fs::remove_file(&path)?;
                    }
                }
            }
            None => {
                for dir_entry in fs::read_dir(&data_dir)? {
                    let path = dir_entry?.path();
                    if path.is_file() {
                        if let Some((level, seq)) = parse_sst_filename(&path) {
                            found.push((level, seq, path));
                        }
                    }
                }
            }
        }

        // Within a level, tables must be ordered oldest to newest so that
        // reads (which scan newest first) and compaction (where the newest
        // value wins) see them consistently. Sort by sequence number since
        // neither manifest order nor directory order is guaranteed.
        found.sort_by_key(|(level, seq, _)| (*level, *seq));

        let mut sstables: HashMap<usize, Vec<SSTable>> = HashMap::new();
        let mut counter = 0;
        let mut total_sstables = 0;
        for (level, seq, path) in found {
            counter = counter.max(seq + 1);
            sstables
                .entry(level)
                .or_default()
                .push(SSTable::with_cache(path, block_cache.clone())?);
            total_sstables += 1;
        }

        if verbose {
            println!(
                "Loaded {} SSTables across {} levels",
                total_sstables,
                sstables.len()
            );
            for (level, tables) in &sstables {
                let total_size: usize = tables.iter().map(|t| t.size()).sum();
                println!(
                    "  Level {}: {} files, {} bytes total",
                    level,
                    tables.len(),
                    total_size
                );
            }
        }

        let compaction_manager = CompactionManager::new(
            config.level_multiplier,
            config.compaction_size_threshold,
            config.level0_file_limit,
        );

        let storage = Storage {
            memtable,
            wal,
            sstables,
            data_dir: data_dir.as_ref().to_path_buf(),
            sstable_counter: counter,
            compaction_manager,
            block_cache,
            manifest,
            config,
        };

        // Give legacy directories a manifest so the next startup (and any
        // crash recovery in between) has an authoritative table record
        if !had_manifest {
            storage.persist_manifest()?;
        }

        Ok(storage)
    }

    /// Atomically record the current live table set in the manifest. Called
    /// at every commit point where the table set changes.
    fn persist_manifest(&self) -> io::Result<()> {
        let mut entries = Vec::new();
        for (level, tables) in &self.sstables {
            for table in tables {
                let path = table.get_path();
                if let Some((_, seq)) = parse_sst_filename(path) {
                    entries.push(ManifestEntry {
                        level: *level,
                        seq,
                        filename: path.file_name().unwrap().to_string_lossy().into_owned(),
                    });
                }
            }
        }
        entries.sort_by_key(|e| (e.level, e.seq));
        self.manifest.write(&entries)
    }

    /// Look up the current value for `key`, or `None` if the key does not
    /// exist or has been deleted.
    pub fn get(&self, key: &Key) -> io::Result<Option<Value>> {
        if self.config.verbose {
            println!("GET {:?}", String::from_utf8_lossy(key));
        }

        // First check memtable; a tombstone there means the key was deleted
        // and must shadow any older value in the SSTables
        match self.memtable.get(key) {
            Some(Some(value)) => {
                if self.config.verbose {
                    println!("  Found in memtable");
                }
                return Ok(Some(value.clone()));
            }
            Some(None) => {
                if self.config.verbose {
                    println!("  Deleted (tombstone in memtable)");
                }
                return Ok(None);
            }
            None => {}
        }

        // Then check SSTables from newest to oldest, level by level
        for level in 0..=self.sstables.keys().max().copied().unwrap_or(0) {
            if let Some(tables) = self.sstables.get(&level) {
                if self.config.verbose {
                    println!("  Searching level {} ({} files)", level, tables.len());
                }
                for (idx, sstable) in tables.iter().rev().enumerate() {
                    // Use bloom filter to avoid unnecessary disk reads
                    if !sstable.might_contain_key(key) {
                        if self.config.verbose {
                            println!(
                                "  Skipped SSTable {} at level {} (Bloom filter negative)",
                                idx, level
                            );
                        }
                        continue;
                    }

                    // Key might be in this SSTable, do a full check
                    match sstable.get(key)? {
                        SSTableLookup::Found(value) => {
                            if self.config.verbose {
                                println!("  Found in SSTable {} at level {}", idx, level);
                            }
                            return Ok(Some(value));
                        }
                        SSTableLookup::Deleted => {
                            if self.config.verbose {
                                println!(
                                    "  Deleted (tombstone in SSTable {} at level {})",
                                    idx, level
                                );
                            }
                            return Ok(None);
                        }
                        SSTableLookup::NotFound => {}
                    }
                }
            }
        }

        if self.config.verbose {
            println!("  Key not found");
        }
        Ok(None)
    }

    /// Return all live key-value pairs with `start <= key < end`, in key
    /// order. Deleted keys are excluded; the newest value wins when a key
    /// exists at several levels.
    pub fn scan(&self, start: &[u8], end: &[u8]) -> io::Result<Vec<(Key, Value)>> {
        self.scan_impl(start, Some(end))
    }

    /// Return all live key-value pairs whose key starts with `prefix`,
    /// in key order.
    pub fn scan_prefix(&self, prefix: &[u8]) -> io::Result<Vec<(Key, Value)>> {
        let end = prefix_successor(prefix);
        self.scan_impl(prefix, end.as_deref())
    }

    fn scan_impl(&self, start: &[u8], end: Option<&[u8]>) -> io::Result<Vec<(Key, Value)>> {
        let mut merged: BTreeMap<Key, Option<Value>> = BTreeMap::new();

        // Insert oldest data first so newer entries overwrite it: deeper
        // levels hold older data, within a level tables are stored oldest to
        // newest, and the memtable is newest of all.
        let mut levels: Vec<_> = self.sstables.keys().copied().collect();
        levels.sort_unstable_by(|a, b| b.cmp(a));
        for level in levels {
            for table in &self.sstables[&level] {
                for (key, value) in table.scan_range(start, end)? {
                    merged.insert(key, value);
                }
            }
        }
        for (key, value) in self.memtable.range(start, end) {
            merged.insert(key.clone(), value.clone());
        }

        // Tombstones shadow older values, then drop out of the result
        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect())
    }

    /// Insert or update `key` with `value`. The write is durable (recorded
    /// in the write-ahead log) once this returns.
    pub fn put(&mut self, key: Key, value: Value) -> io::Result<()> {
        if self.config.verbose {
            let count = PUT_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
            let bytes = TOTAL_BYTES.fetch_add(key.len() + value.len(), Ordering::Relaxed)
                + key.len()
                + value.len();

            if count.is_multiple_of(1000) {
                println!(
                    "\nProgress: {} operations ({:.2} MB written)",
                    count,
                    bytes as f64 / 1_048_576.0
                );
                println!(
                    "Average value size: {:.2} KB",
                    (bytes as f64 / count as f64) / 1024.0
                );
            }
        }

        // Write to WAL first
        self.wal.append(Operation::Put, &key, Some(&value))?;

        // Then update memtable
        self.memtable.insert(key, value);

        self.maybe_flush_memtable()
    }

    /// Delete `key`. The deletion is durable once this returns and shadows
    /// any older value still stored in SSTables.
    pub fn delete(&mut self, key: &Key) -> io::Result<()> {
        if self.config.verbose {
            println!("DELETE {:?}", String::from_utf8_lossy(key));
        }

        // Write to WAL first
        self.wal.append(Operation::Delete, key, None)?;

        // Then record a tombstone in the memtable. The tombstone (not a plain
        // removal) is what shadows older values still living in SSTables and
        // is flushed to disk alongside regular entries.
        self.memtable.delete(key.clone());

        self.maybe_flush_memtable()
    }

    fn maybe_flush_memtable(&mut self) -> io::Result<()> {
        let memtable_size = self.memtable.size();
        if memtable_size >= self.config.memtable_size_threshold {
            if self.config.verbose {
                println!("\n=== Memtable Flush ===");
                println!(
                    "Size: {:.2} MB (threshold: {:.2} MB)",
                    memtable_size as f64 / 1_048_576.0,
                    self.config.memtable_size_threshold as f64 / 1_048_576.0
                );
            }
            self.flush_memtable()?;
        }
        Ok(())
    }

    fn flush_memtable(&mut self) -> io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        if self.config.verbose {
            println!("Entries: {}", self.memtable.len());
            println!(
                "Average entry size: {:.2} KB",
                (self.memtable.size() as f64 / self.memtable.len() as f64) / 1024.0
            );
        }

        // Create new SSTable at level 0
        let sstable_path = self
            .data_dir
            .join(format!("L0_{}.sst", self.sstable_counter));
        let mut sstable = SSTable::with_cache(sstable_path, self.block_cache.clone())?;

        // Write memtable data (including tombstones) to SSTable
        let entries: Vec<_> = self
            .memtable
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        sstable.write_with(&entries, self.config.compression)?;

        if self.config.verbose {
            println!(
                "Created SSTable: L0_{}.sst ({:.2} MB)",
                self.sstable_counter,
                sstable.size() as f64 / 1_048_576.0
            );
        }

        // Add new SSTable to level 0
        self.sstables.entry(0).or_default().push(sstable);
        self.sstable_counter += 1;

        // Commit the new table to the manifest BEFORE clearing the WAL: if
        // we crash in between, the WAL still holds the data; if we cleared
        // first, an unreferenced table would be garbage-collected as an
        // orphan and the flushed writes lost
        self.persist_manifest()?;

        // Clear memtable and WAL
        self.memtable = MemTable::new();
        self.wal.clear()?;

        // Check if compaction is needed at level 0, unless compaction has
        // been taken off the write path
        if self.config.inline_compaction {
            self.maybe_compact(0)?;
        }

        Ok(())
    }

    /// Run any pending compactions across all levels, regardless of the
    /// `inline_compaction` setting. Returns once every level is within its
    /// threshold.
    pub fn compact_now(&mut self) -> io::Result<()> {
        let mut level = 0;
        loop {
            let max_level = self.sstables.keys().max().copied().unwrap_or(0);
            if level > max_level {
                break;
            }
            self.maybe_compact(level)?;
            level += 1;
        }
        Ok(())
    }

    fn maybe_compact(&mut self, level: usize) -> io::Result<()> {
        let Some(tables) = self.sstables.get(&level) else {
            return Ok(());
        };

        let total_size: usize = tables.iter().map(|t| t.size()).sum();

        if self.config.verbose {
            println!("\n=== Compaction Check: Level {} ===", level);
            println!("Files: {}", tables.len());
            println!("Total size: {:.2} MB", total_size as f64 / 1_048_576.0);
        }

        if !self.compaction_manager.should_compact(level, tables) {
            return Ok(());
        }

        if self.config.verbose {
            println!("\n=== Starting Compaction ===");
            println!("Level: {} -> {}", level, level + 1);
            println!("Files to compact: {}", tables.len());
            for (idx, table) in tables.iter().enumerate() {
                println!("  {}: {:.2} MB", idx, table.size() as f64 / 1_048_576.0);
            }
        }

        let next_level = level + 1;

        // Tombstones can only be dropped when no table at or below the
        // output level could still hold an older value for the key;
        // otherwise dropping the tombstone would resurrect that value.
        let drop_tombstones = self
            .sstables
            .iter()
            .all(|(l, tables)| *l <= level || tables.is_empty());

        // Perform compaction (merge in memory, newest value wins)
        let entries = self.compaction_manager.compact(tables, drop_tombstones)?;

        // Get paths of tables to delete
        let table_paths: Vec<_> = tables.iter().map(|t| t.get_path().clone()).collect();

        // Write merged entries as a single SSTable at the next level
        let new_path = self
            .data_dir
            .join(format!("L{}_{}.sst", next_level, self.sstable_counter));

        let mut new_table = SSTable::with_cache(new_path, self.block_cache.clone())?;

        if self.config.verbose {
            println!("\n=== Compaction Results ===");
            println!("Unique entries: {}", entries.len());
        }

        new_table.write_with(&entries, self.config.compression)?;

        let new_table_size = new_table.size();
        if self.config.verbose {
            println!(
                "New SSTable size: {:.2} MB",
                new_table_size as f64 / 1_048_576.0
            );
        }

        // Update sstables collection
        self.sstables.get_mut(&level).unwrap().clear();
        self.sstables.entry(next_level).or_default().push(new_table);
        self.sstable_counter += 1;

        // The manifest rename is the commit point: crash before it and the
        // half-finished compaction output is an orphan; crash after it and
        // the stale inputs are orphans. Either way startup cleans up.
        self.persist_manifest()?;

        // Now delete the old files (and their cached blocks)
        for path in table_paths {
            fs::remove_file(&path)?;
            if let Some(cache) = &self.block_cache {
                cache.purge_file(&path);
            }
        }

        if self.config.verbose {
            let space_saved = total_size.saturating_sub(new_table_size);
            println!(
                "Space reclaimed: {:.2} MB",
                space_saved as f64 / 1_048_576.0
            );
            println!(
                "Compression ratio: {:.2}%",
                (1.0 - (new_table_size as f64 / total_size as f64)) * 100.0
            );
        }

        // Check if next level needs compaction
        self.maybe_compact(next_level)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_storage() -> (TempDir, Storage) {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::new(temp_dir.path(), false).unwrap();
        (temp_dir, storage)
    }

    fn count_sst_files(dir: &Path) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .and_then(|s| s.to_str())
                    == Some("sst")
            })
            .count()
    }

    /// Write enough filler data to force at least one memtable flush.
    fn force_flush(storage: &mut Storage, tag: &str) {
        let filler = vec![b'x'; 4096];
        let threshold = StorageConfig::default().memtable_size_threshold;
        for i in 0..(threshold / filler.len() + 2) {
            let key = format!("filler_{}_{}", tag, i).into_bytes();
            storage.put(key, filler.clone()).unwrap();
        }
    }

    #[test]
    fn test_basic_operations() {
        let (_temp_dir, mut storage) = create_test_storage();

        // Test put and get
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        storage.put(key1.clone(), value1.clone()).unwrap();
        assert_eq!(storage.get(&key1).unwrap(), Some(value1));

        // Test update
        storage.put(key1.clone(), value2.clone()).unwrap();
        assert_eq!(storage.get(&key1).unwrap(), Some(value2));

        // Test delete
        storage.delete(&key1).unwrap();
        assert_eq!(storage.get(&key1).unwrap(), None);

        // Test get non-existent key
        let nonexistent = b"nonexistent".to_vec();
        assert_eq!(storage.get(&nonexistent).unwrap(), None);
    }

    #[test]
    fn test_memtable_flush() {
        let (temp_dir, mut storage) = create_test_storage();
        let data_dir = temp_dir.path();

        // Write enough data to trigger a flush
        let value = vec![b'x'; 1024]; // 1KB value
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            storage.put(key, value.clone()).unwrap();
        }

        // Verify SSTable was created
        assert!(count_sst_files(data_dir) > 0);

        // Verify data is still accessible
        let test_key = b"key0".to_vec();
        assert_eq!(storage.get(&test_key).unwrap(), Some(value));
    }

    #[test]
    fn test_interleaved_operations() {
        let (_temp_dir, mut storage) = create_test_storage();

        // Perform rapid operations
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();

            storage.put(key.clone(), value.clone()).unwrap();
            assert_eq!(storage.get(&key).unwrap(), Some(value.clone()));

            if i % 2 == 0 {
                storage.delete(&key).unwrap();
            }
        }

        // Verify final state
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();

            if i % 2 == 0 {
                assert_eq!(storage.get(&key).unwrap(), None);
            } else {
                assert_eq!(storage.get(&key).unwrap(), Some(value));
            }
        }
    }

    #[test]
    fn test_recovery() {
        let (temp_dir, mut storage) = create_test_storage();

        // Write some data
        let test_data = [
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        for (key, value) in test_data.iter() {
            storage.put(key.clone(), value.clone()).unwrap();
        }

        // Create new storage instance with same path
        drop(storage);
        let recovered_storage = Storage::new(temp_dir.path(), false).unwrap();

        // Verify all data is accessible
        for (key, value) in test_data.iter() {
            assert_eq!(recovered_storage.get(key).unwrap(), Some(value.clone()));
        }
    }

    #[test]
    fn test_delete_persists_after_flush() {
        let (_temp_dir, mut storage) = create_test_storage();

        let key = b"victim".to_vec();
        storage.put(key.clone(), b"value".to_vec()).unwrap();

        // Force the key into an SSTable, then delete it
        force_flush(&mut storage, "a");
        storage.delete(&key).unwrap();
        assert_eq!(storage.get(&key).unwrap(), None);

        // Force the tombstone itself into an SSTable; the delete must still
        // shadow the older on-disk value
        force_flush(&mut storage, "b");
        assert_eq!(storage.get(&key).unwrap(), None);
    }

    #[test]
    fn test_delete_persists_after_restart() {
        let (temp_dir, mut storage) = create_test_storage();

        let key = b"victim".to_vec();
        storage.put(key.clone(), b"value".to_vec()).unwrap();
        force_flush(&mut storage, "a");
        storage.delete(&key).unwrap();

        drop(storage);
        let recovered = Storage::new(temp_dir.path(), false).unwrap();
        assert_eq!(recovered.get(&key).unwrap(), None);
    }

    #[test]
    fn test_compaction_keeps_newest_value() {
        let (_temp_dir, mut storage) = create_test_storage();

        let key = b"contested".to_vec();

        // Write the key, flush, overwrite it, and flush enough times to
        // trigger a level-0 compaction (4 files)
        for round in 0..5 {
            let value = format!("value_round_{}", round).into_bytes();
            storage.put(key.clone(), value).unwrap();
            force_flush(&mut storage, &format!("round{}", round));
        }

        // After flushes and compaction, the newest value must win
        assert_eq!(storage.get(&key).unwrap(), Some(b"value_round_4".to_vec()));
    }

    #[test]
    fn test_compaction() {
        let (temp_dir, mut storage) = create_test_storage();
        let data_dir = temp_dir.path();

        // Write enough data to trigger multiple flushes and compaction
        let value = vec![b'x'; 2048]; // 2KB value
        for i in 0..2000 {
            let key = format!("key{}", i).into_bytes();
            storage.put(key, value.clone()).unwrap();
        }

        // Count SSTable files per level
        let mut level_counts = [0; 4]; // Count files in levels 0-3
        for entry in fs::read_dir(data_dir).unwrap() {
            let filename = entry.unwrap().file_name();
            let name = filename.to_str().unwrap();
            if !name.ends_with(".sst") {
                continue;
            }
            if let Some(level) = name.chars().find(|c| c.is_ascii_digit()) {
                let level_num = level.to_digit(10).unwrap() as usize;
                if level_num < level_counts.len() {
                    level_counts[level_num] += 1;
                }
            }
        }

        // Verify data distribution across levels
        assert!(level_counts[0] <= 4); // Level 0 should not have too many files
        assert!(level_counts.iter().sum::<i32>() > 0); // Should have some files

        // Verify all data is still accessible
        for i in [0, 500, 1999] {
            let key = format!("key{}", i).into_bytes();
            assert_eq!(storage.get(&key).unwrap(), Some(value.clone()));
        }
    }

    #[test]
    fn test_custom_config_flush_threshold() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            memtable_size_threshold: 1024, // tiny threshold: flush almost immediately
            ..StorageConfig::default()
        };
        let mut storage = Storage::with_config(temp_dir.path(), config).unwrap();

        // A single 2KB write exceeds the threshold and must trigger a flush
        storage.put(b"key".to_vec(), vec![b'x'; 2048]).unwrap();
        assert_eq!(count_sst_files(temp_dir.path()), 1);
        assert_eq!(
            storage.get(&b"key".to_vec()).unwrap(),
            Some(vec![b'x'; 2048])
        );
    }

    #[test]
    fn test_compressed_storage_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            memtable_size_threshold: 8 * 1024, // force several flushes
            compression: Compression::Lz4,
            ..StorageConfig::default()
        };
        let mut storage = Storage::with_config(temp_dir.path(), config.clone()).unwrap();

        for i in 0..500 {
            let key = format!("key{:04}", i).into_bytes();
            let value = format!("value{}", i).repeat(20).into_bytes();
            storage.put(key, value).unwrap();
        }
        storage.delete(&b"key0100".to_vec()).unwrap();

        // Everything readable through compressed SSTables
        assert_eq!(
            storage.get(&b"key0000".to_vec()).unwrap(),
            Some(b"value0".repeat(20).to_vec())
        );
        assert_eq!(storage.get(&b"key0100".to_vec()).unwrap(), None);

        // And after a restart (compression flag read back from headers)
        drop(storage);
        let recovered = Storage::with_config(temp_dir.path(), config).unwrap();
        assert_eq!(
            recovered.get(&b"key0499".to_vec()).unwrap(),
            Some(format!("value{}", 499).repeat(20).into_bytes())
        );
        assert_eq!(recovered.get(&b"key0100".to_vec()).unwrap(), None);
    }

    #[test]
    fn test_prefix_successor() {
        assert_eq!(prefix_successor(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(prefix_successor(b"ab\xff"), Some(b"ac".to_vec()));
        assert_eq!(prefix_successor(b"\xff\xff"), None);
        assert_eq!(prefix_successor(b""), None);
    }

    #[test]
    fn test_scan_across_memtable_and_sstables() {
        let (_temp_dir, mut storage) = create_test_storage();

        // Older values end up in SSTables...
        storage.put(b"user:1".to_vec(), b"old1".to_vec()).unwrap();
        storage.put(b"user:2".to_vec(), b"v2".to_vec()).unwrap();
        storage.put(b"user:3".to_vec(), b"v3".to_vec()).unwrap();
        storage.put(b"other:1".to_vec(), b"x".to_vec()).unwrap();
        force_flush(&mut storage, "scan");

        // ...then get overwritten/deleted in the memtable
        storage.put(b"user:1".to_vec(), b"new1".to_vec()).unwrap();
        storage.delete(&b"user:2".to_vec()).unwrap();
        storage.put(b"user:4".to_vec(), b"v4".to_vec()).unwrap();

        let result = storage.scan_prefix(b"user:").unwrap();
        assert_eq!(
            result,
            vec![
                (b"user:1".to_vec(), b"new1".to_vec()), // newest value wins
                (b"user:3".to_vec(), b"v3".to_vec()),
                (b"user:4".to_vec(), b"v4".to_vec()),
                // user:2 deleted, other:1 outside the prefix
            ]
        );

        // Bounded range: end is exclusive
        let result = storage.scan(b"user:1", b"user:4").unwrap();
        let keys: Vec<_> = result.into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"user:1".to_vec(), b"user:3".to_vec()]);
    }

    #[test]
    fn test_scan_survives_compaction_and_restart() {
        let (temp_dir, mut storage) = create_test_storage();

        for i in 0..50 {
            storage
                .put(format!("k{:03}", i).into_bytes(), vec![b'v'; 64])
                .unwrap();
        }
        storage.delete(&b"k025".to_vec()).unwrap();
        // Several flushes to spread data over levels and trigger compaction
        for round in 0..5 {
            force_flush(&mut storage, &format!("scanr{}", round));
        }

        drop(storage);
        let storage = Storage::new(temp_dir.path(), false).unwrap();

        let result = storage.scan(b"k000", b"k050").unwrap();
        assert_eq!(result.len(), 49); // 50 keys minus the deleted one
        assert!(result.iter().all(|(k, _)| k.as_slice() != b"k025"));
        // Sorted by key
        assert!(result.windows(2).all(|w| w[0].0 < w[1].0));
    }

    #[test]
    fn test_batched_wal_sync_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            wal_sync: WalSync::Batched { every_n_writes: 16 },
            ..StorageConfig::default()
        };
        let mut storage = Storage::with_config(temp_dir.path(), config.clone()).unwrap();
        for i in 0..40 {
            storage
                .put(format!("key{}", i).into_bytes(), b"value".to_vec())
                .unwrap();
        }
        storage.delete(&b"key5".to_vec()).unwrap();

        // Clean shutdown syncs the batched tail; everything replays
        drop(storage);
        let recovered = Storage::with_config(temp_dir.path(), config).unwrap();
        assert_eq!(
            recovered.get(&b"key39".to_vec()).unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(recovered.get(&b"key5".to_vec()).unwrap(), None);
    }

    #[test]
    fn test_block_cache_correctness_under_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            memtable_size_threshold: 8 * 1024,
            compaction_size_threshold: 32 * 1024,
            block_cache_size: 64 * 1024, // small cache with eviction pressure
            ..StorageConfig::default()
        };
        let mut storage = Storage::with_config(temp_dir.path(), config).unwrap();

        // Interleave writes and reads so cached blocks live across flushes
        // and compactions that delete their underlying files
        for i in 0..600 {
            let key = format!("key{:04}", i % 200).into_bytes();
            storage
                .put(key.clone(), format!("v{}", i).into_bytes())
                .unwrap();
            let probe = format!("key{:04}", (i * 7) % 200).into_bytes();
            storage.get(&probe).unwrap();
        }

        // Latest value per key must win despite caching
        for k in 0..200 {
            let key = format!("key{:04}", k).into_bytes();
            let latest = (0..600).rev().find(|i| i % 200 == k).unwrap();
            assert_eq!(
                storage.get(&key).unwrap(),
                Some(format!("v{}", latest).into_bytes()),
                "key {}",
                k
            );
        }
    }

    #[test]
    fn test_deferred_compaction_via_compact_now() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            memtable_size_threshold: 8 * 1024,
            inline_compaction: false,
            ..StorageConfig::default()
        };
        let mut storage = Storage::with_config(temp_dir.path(), config).unwrap();

        // Enough flushes that level 0 exceeds its file limit
        for i in 0..800 {
            storage
                .put(format!("key{:04}", i).into_bytes(), vec![b'v'; 64])
                .unwrap();
        }
        let l0_before = fs::read_dir(temp_dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("L0_")
            })
            .count();
        assert!(
            l0_before >= 4,
            "inline compaction disabled: L0 should accumulate files, got {}",
            l0_before
        );

        storage.compact_now().unwrap();

        let l0_after = fs::read_dir(temp_dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("L0_")
            })
            .count();
        assert!(l0_after < 4, "compact_now should drain level 0");

        // All data still readable after manual compaction
        for i in [0, 400, 799] {
            let key = format!("key{:04}", i).into_bytes();
            assert_eq!(storage.get(&key).unwrap(), Some(vec![b'v'; 64]));
        }
    }

    #[test]
    fn test_manifest_written_on_flush() {
        let (temp_dir, mut storage) = create_test_storage();
        assert!(temp_dir.path().join("MANIFEST").exists()); // written at open

        force_flush(&mut storage, "m");
        let manifest = fs::read_to_string(temp_dir.path().join("MANIFEST")).unwrap();
        assert!(
            manifest.lines().any(|l| l.starts_with("0 ")),
            "manifest should list a level-0 table:\n{}",
            manifest
        );
    }

    #[test]
    fn test_orphan_sstables_removed_on_startup() {
        let (temp_dir, mut storage) = create_test_storage();
        storage.put(b"real".to_vec(), b"data".to_vec()).unwrap();
        force_flush(&mut storage, "o");
        drop(storage);

        // Fabricate leftovers of a crashed flush/compaction: a garbage
        // half-written table and a valid table that isn't in the manifest
        fs::write(temp_dir.path().join("L0_9998.sst"), b"garbage").unwrap();
        let mut stray = SSTable::new(temp_dir.path().join("L9_9999.sst")).unwrap();
        stray
            .write(&[(b"ghost".to_vec(), Some(b"boo".to_vec()))])
            .unwrap();

        let recovered = Storage::new(temp_dir.path(), false).unwrap();
        assert!(!temp_dir.path().join("L0_9998.sst").exists());
        assert!(!temp_dir.path().join("L9_9999.sst").exists());
        assert_eq!(recovered.get(&b"ghost".to_vec()).unwrap(), None);
        assert_eq!(
            recovered.get(&b"real".to_vec()).unwrap(),
            Some(b"data".to_vec())
        );
    }

    #[test]
    fn test_legacy_directory_without_manifest() {
        let (temp_dir, mut storage) = create_test_storage();
        storage.put(b"key".to_vec(), b"value".to_vec()).unwrap();
        force_flush(&mut storage, "l");
        drop(storage);

        // Simulate a data directory created before manifests existed
        fs::remove_file(temp_dir.path().join("MANIFEST")).unwrap();

        let recovered = Storage::new(temp_dir.path(), false).unwrap();
        assert_eq!(
            recovered.get(&b"key".to_vec()).unwrap(),
            Some(b"value".to_vec())
        );
        // The fallback scan writes a fresh manifest
        assert!(temp_dir.path().join("MANIFEST").exists());
    }
}
