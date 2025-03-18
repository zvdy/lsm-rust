use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::memtable::MemTable;
use crate::sstable::{CompactionManager, SSTable};
use crate::wal::{Operation, WAL};
use crate::{Key, Value};

const MEMTABLE_SIZE_THRESHOLD: usize = 512 * 1024; // 512KB (smaller for more frequent flushes)
const COMPACTION_SIZE_THRESHOLD: usize = 1024 * 1024; // 1MB
const LEVEL_MULTIPLIER: u32 = 4; // More aggressive compaction

static PUT_COUNT: AtomicUsize = AtomicUsize::new(0);
static TOTAL_BYTES: AtomicUsize = AtomicUsize::new(0);

pub struct Storage {
    memtable: MemTable,
    wal: WAL,
    sstables: HashMap<usize, Vec<SSTable>>, // level -> SSTables
    data_dir: PathBuf,
    sstable_counter: u64,
    compaction_manager: CompactionManager,
    verbose: bool,
}

impl Storage {
    pub fn new<P: AsRef<Path>>(data_dir: P, verbose: bool) -> io::Result<Self> {
        if verbose {
            println!("Initializing storage at {:?}", data_dir.as_ref());
        }
        fs::create_dir_all(&data_dir)?;

        let wal_path = data_dir.as_ref().join("wal");
        let mut wal = WAL::new(wal_path)?;
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
                    memtable.remove(&key);
                    replay_count += 1;
                }
            }
        }
        if verbose && replay_count > 0 {
            println!("Replayed {} operations from WAL", replay_count);
        }

        // Load existing SSTables
        let mut sstables: HashMap<usize, Vec<SSTable>> = HashMap::new();
        let mut counter = 0;
        let mut total_sstables = 0;

        for entry in fs::read_dir(&data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("sst") {
                if let Some(filename) = path.file_stem().and_then(|s| s.to_str()) {
                    // Parse level and sequence number from filename (L{level}_{seq}.sst)
                    if let Some(level_str) = filename.strip_prefix('L') {
                        if let Some((level, seq_str)) = level_str.split_once('_') {
                            if let (Ok(level), Ok(seq)) =
                                (level.parse::<usize>(), seq_str.parse::<u64>())
                            {
                                counter = counter.max(seq + 1);
                                sstables.entry(level).or_default().push(SSTable::new(path)?);
                                total_sstables += 1;
                            }
                        }
                    }
                }
            }
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

        let compaction_manager =
            CompactionManager::new(LEVEL_MULTIPLIER, COMPACTION_SIZE_THRESHOLD);

        Ok(Storage {
            memtable,
            wal,
            sstables,
            data_dir: data_dir.as_ref().to_path_buf(),
            sstable_counter: counter,
            compaction_manager,
            verbose,
        })
    }

    pub fn get(&self, key: &Key) -> io::Result<Option<Value>> {
        if self.verbose {
            println!("GET {:?}", String::from_utf8_lossy(key));
        }

        // First check memtable
        if let Some(value) = self.memtable.get(key) {
            if self.verbose {
                println!("  Found in memtable");
            }
            return Ok(Some(value.clone()));
        }

        // Then check SSTables from newest to oldest, level by level
        for level in 0..=self.sstables.keys().max().copied().unwrap_or(0) {
            if let Some(tables) = self.sstables.get(&level) {
                if self.verbose {
                    println!("  Searching level {} ({} files)", level, tables.len());
                }
                for (idx, sstable) in tables.iter().rev().enumerate() {
                    if let Ok(entries) = sstable.read() {
                        for (k, v) in entries {
                            if k == *key {
                                if self.verbose {
                                    println!("  Found in SSTable {} at level {}", idx, level);
                                }
                                return Ok(Some(v));
                            }
                        }
                    }
                }
            }
        }

        if self.verbose {
            println!("  Key not found");
        }
        Ok(None)
    }

    pub fn put(&mut self, key: Key, value: Value) -> io::Result<()> {
        if self.verbose {
            let count = PUT_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
            let bytes = TOTAL_BYTES.fetch_add(key.len() + value.len(), Ordering::Relaxed)
                + key.len()
                + value.len();

            if count % 1000 == 0 {
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

        // Check if we need to flush memtable to SSTable
        let memtable_size = self.memtable.size();
        if memtable_size >= MEMTABLE_SIZE_THRESHOLD {
            if self.verbose {
                println!("\n=== Memtable Flush ===");
                println!(
                    "Size: {:.2} MB (threshold: {:.2} MB)",
                    memtable_size as f64 / 1_048_576.0,
                    MEMTABLE_SIZE_THRESHOLD as f64 / 1_048_576.0
                );
            }
            self.flush_memtable()?;
        }

        Ok(())
    }

    pub fn delete(&mut self, key: &Key) -> io::Result<()> {
        if self.verbose {
            println!("DELETE {:?}", String::from_utf8_lossy(key));
        }

        // Write to WAL first
        self.wal.append(Operation::Delete, key, None)?;

        // Then update memtable
        self.memtable.remove(key);

        Ok(())
    }

    fn flush_memtable(&mut self) -> io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        if self.verbose {
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
        let mut sstable = SSTable::new(sstable_path)?;

        // Write memtable data to SSTable
        let entries: Vec<_> = self
            .memtable
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        sstable.write(&entries)?;

        if self.verbose {
            println!(
                "Created SSTable: L0_{}.sst ({:.2} MB)",
                self.sstable_counter,
                sstable.size() as f64 / 1_048_576.0
            );
        }

        // Add new SSTable to level 0
        self.sstables.entry(0).or_default().push(sstable);
        self.sstable_counter += 1;

        // Clear memtable and WAL
        self.memtable = MemTable::new();
        self.wal.clear()?;

        // Check if compaction is needed at level 0
        self.maybe_compact(0)?;

        Ok(())
    }

    fn maybe_compact(&mut self, level: usize) -> io::Result<()> {
        if let Some(tables) = self.sstables.get(&level) {
            let total_size: usize = tables.iter().map(|t| t.size()).sum();

            if self.verbose {
                println!("\n=== Compaction Check: Level {} ===", level);
                println!("Files: {}", tables.len());
                println!("Total size: {:.2} MB", total_size as f64 / 1_048_576.0);
            }

            if self.compaction_manager.should_compact(level, tables) {
                if self.verbose {
                    println!("\n=== Starting Compaction ===");
                    println!("Level: {} -> {}", level, level + 1);
                    println!("Files to compact: {}", tables.len());
                    for (idx, table) in tables.iter().enumerate() {
                        println!("  {}: {:.2} MB", idx, table.size() as f64 / 1_048_576.0);
                    }
                }

                // Perform compaction
                let compacted = self.compaction_manager.compact(tables)?;

                // Get paths of tables to delete
                let table_paths: Vec<_> = tables.iter().map(|t| t.get_path().clone()).collect();

                // Move compacted SSTable to next level
                let next_level = level + 1;
                let new_path = self
                    .data_dir
                    .join(format!("L{}_{}.sst", next_level, self.sstable_counter));

                let mut new_table = SSTable::new(new_path)?;
                let entries = compacted.read()?;

                if self.verbose {
                    println!("\n=== Compaction Results ===");
                    println!("Unique entries: {}", entries.len());
                }

                new_table.write(&entries)?;

                let new_table_size = new_table.size();
                if self.verbose {
                    println!(
                        "New SSTable size: {:.2} MB",
                        new_table_size as f64 / 1_048_576.0
                    );
                }

                // Update sstables collection
                self.sstables.get_mut(&level).unwrap().clear();
                self.sstables.entry(next_level).or_default().push(new_table);
                self.sstable_counter += 1;

                // Now delete the old files
                for path in table_paths {
                    fs::remove_file(path)?;
                }

                if self.verbose {
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
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::thread;
    use std::time::Duration;
    use tempfile::TempDir;

    fn create_test_storage() -> (TempDir, Storage) {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::new(temp_dir.path(), false).unwrap();
        (temp_dir, storage)
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

        // Give some time for async operations
        thread::sleep(Duration::from_millis(100));

        // Verify SSTable was created
        let sstable_count = fs::read_dir(data_dir)
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_str()
                    .unwrap()
                    .ends_with(".sst")
            })
            .count();
        assert!(sstable_count > 0);

        // Verify data is still accessible
        let test_key = b"key0".to_vec();
        assert_eq!(storage.get(&test_key).unwrap(), Some(value));
    }

    #[test]
    fn test_concurrent_operations() {
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
        let test_data = vec![
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
    fn test_compaction() {
        let (temp_dir, mut storage) = create_test_storage();
        let data_dir = temp_dir.path();

        // Write enough data to trigger multiple flushes and compaction
        let value = vec![b'x'; 2048]; // 2KB value
        for i in 0..2000 {
            let key = format!("key{}", i).into_bytes();
            storage.put(key, value.clone()).unwrap();
        }

        // Give time for compaction to occur
        thread::sleep(Duration::from_millis(200));

        // Count SSTable files
        let sstable_files: Vec<_> = fs::read_dir(data_dir)
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .file_name()
                    .to_str()
                    .unwrap()
                    .ends_with(".sst")
            })
            .collect();

        // Verify compaction occurred by checking file count and levels
        let mut level_counts = vec![0; 4]; // Count files in levels 0-3
        for entry in sstable_files {
            let filename = entry.unwrap().file_name();
            let name = filename.to_str().unwrap();
            if let Some(level) = name.chars().find(|c| c.is_digit(10)) {
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
        let test_keys = vec![
            format!("key0").into_bytes(),
            format!("key500").into_bytes(),
            format!("key1999").into_bytes(),
        ];

        for key in &test_keys {
            assert_eq!(storage.get(key).unwrap(), Some(value.clone()));
        }
    }
}
