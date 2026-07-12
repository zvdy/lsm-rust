use crate::bloom::BloomFilter;
use crate::{Key, Value};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

mod compaction;
pub use compaction::CompactionManager;

const BLOOM_FALSE_POSITIVE_RATE: f64 = 0.01;
const EXPECTED_ENTRIES_PER_SSTABLE: usize = 1000;

/// Sentinel value length that marks a tombstone (deleted key) on disk.
/// Real values are limited to less than u32::MAX bytes.
const TOMBSTONE_MARKER: u32 = u32::MAX;

/// Result of looking up a key in an SSTable.
#[derive(Debug, PartialEq, Eq)]
pub enum SSTableLookup {
    /// The key exists with this value.
    Found(Value),
    /// The key was deleted; a tombstone shadows any older value.
    Deleted,
    /// The key is not present in this SSTable.
    NotFound,
}

pub struct SSTable {
    path: PathBuf,
    size: usize,
    bloom_filter: Option<BloomFilter>,
}

fn read_u32(buffer: &[u8], pos: usize) -> io::Result<u32> {
    buffer
        .get(pos..pos + 4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "SSTable truncated: expected 4-byte length",
            )
        })
}

fn read_slice(buffer: &[u8], pos: usize, len: usize) -> io::Result<&[u8]> {
    buffer.get(pos..pos + len).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "SSTable truncated: entry data out of bounds",
        )
    })
}

impl SSTable {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let size = if path.exists() {
            fs::metadata(&path)?.len() as usize
        } else {
            0
        };

        let bloom_filter = if path.exists() {
            // Try to load bloom filter from file
            Self::read_bloom_filter(&path).ok()
        } else {
            None
        };

        Ok(SSTable {
            path,
            size,
            bloom_filter,
        })
    }

    /// Write sorted entries to this SSTable. An entry value of `None` is a
    /// tombstone recording a deletion.
    pub fn write(&mut self, data: &[(Key, Option<Value>)]) -> io::Result<()> {
        let mut file = File::create(&self.path)?;
        let mut size = 0;

        // Create a new bloom filter for this SSTable
        let mut bloom = BloomFilter::new(
            data.len().max(EXPECTED_ENTRIES_PER_SSTABLE),
            BLOOM_FALSE_POSITIVE_RATE,
        );

        // Add all keys to the bloom filter (including tombstones, so that
        // deletions are found and can shadow older values)
        for (key, _) in data {
            bloom.insert(key.as_slice());
        }

        // Write bloom filter to the start of the file
        let bloom_bytes = bloom.to_bytes();
        file.write_all(&(bloom_bytes.len() as u32).to_le_bytes())?;
        file.write_all(&bloom_bytes)?;
        size += bloom_bytes.len() + 4; // 4 bytes for size

        // Write format: [key_size][key][value_size][value]
        // Tombstones are encoded with value_size == TOMBSTONE_MARKER and no value bytes.
        for (key, value) in data {
            file.write_all(&(key.len() as u32).to_le_bytes())?;
            file.write_all(key)?;

            match value {
                Some(value) => {
                    file.write_all(&(value.len() as u32).to_le_bytes())?;
                    file.write_all(value)?;
                    size += key.len() + value.len() + 8; // 8 bytes for sizes
                }
                None => {
                    file.write_all(&TOMBSTONE_MARKER.to_le_bytes())?;
                    size += key.len() + 8;
                }
            }
        }

        file.sync_all()?;

        self.size = size;
        self.bloom_filter = Some(bloom);
        Ok(())
    }

    fn read_bloom_filter(path: &PathBuf) -> io::Result<BloomFilter> {
        let mut file = File::open(path)?;

        // Read bloom filter size
        let mut size_bytes = [0u8; 4];
        file.read_exact(&mut size_bytes)?;
        let bloom_size = u32::from_le_bytes(size_bytes) as usize;

        // Read bloom filter data
        let mut bloom_bytes = vec![0u8; bloom_size];
        file.read_exact(&mut bloom_bytes)?;

        BloomFilter::from_bytes(&bloom_bytes)
    }

    fn read_data_section(&self) -> io::Result<Vec<u8>> {
        let mut file = File::open(&self.path)?;

        // Skip the bloom filter
        let mut size_bytes = [0u8; 4];
        file.read_exact(&mut size_bytes)?;
        let bloom_size = u32::from_le_bytes(size_bytes) as usize;
        file.seek(SeekFrom::Current(bloom_size as i64))?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    /// Read all entries (including tombstones) from this SSTable.
    pub fn read(&self) -> io::Result<Vec<(Key, Option<Value>)>> {
        let buffer = self.read_data_section()?;
        let mut data = Vec::new();

        let mut pos = 0;
        while pos < buffer.len() {
            // Read key
            let key_size = read_u32(&buffer, pos)? as usize;
            pos += 4;
            let key = read_slice(&buffer, pos, key_size)?.to_vec();
            pos += key_size;

            // Read value (or tombstone)
            let value_size = read_u32(&buffer, pos)?;
            pos += 4;
            let value = if value_size == TOMBSTONE_MARKER {
                None
            } else {
                let value = read_slice(&buffer, pos, value_size as usize)?.to_vec();
                pos += value_size as usize;
                Some(value)
            };

            data.push((key, value));
        }

        Ok(data)
    }

    pub fn might_contain_key(&self, key: &[u8]) -> bool {
        if let Some(filter) = &self.bloom_filter {
            filter.might_contain(key)
        } else {
            // If no bloom filter, conservatively return true
            true
        }
    }

    /// Look up a key in this SSTable, distinguishing "deleted here" from
    /// "not present here" so that tombstones shadow older SSTables.
    pub fn get(&self, key: &[u8]) -> io::Result<SSTableLookup> {
        // First check the bloom filter
        if let Some(filter) = &self.bloom_filter {
            if !filter.might_contain(key) {
                // Definitely not in this SSTable
                return Ok(SSTableLookup::NotFound);
            }
        }

        // Key might be present, search through file
        let buffer = self.read_data_section()?;

        let mut pos = 0;
        while pos < buffer.len() {
            // Read key
            let key_size = read_u32(&buffer, pos)? as usize;
            pos += 4;
            let current_key = read_slice(&buffer, pos, key_size)?;
            pos += key_size;

            // Read value size
            let value_size = read_u32(&buffer, pos)?;
            pos += 4;

            if value_size == TOMBSTONE_MARKER {
                if current_key == key {
                    return Ok(SSTableLookup::Deleted);
                }
                continue;
            }

            if current_key == key {
                let value = read_slice(&buffer, pos, value_size as usize)?.to_vec();
                return Ok(SSTableLookup::Found(value));
            }

            // Skip this value
            pos += value_size as usize;
        }

        Ok(SSTableLookup::NotFound)
    }

    pub fn size(&self) -> usize {
        if self.size == 0 && self.path.exists() {
            // Lazy load size if not set
            if let Ok(metadata) = fs::metadata(&self.path) {
                return metadata.len() as usize;
            }
        }
        self.size
    }

    pub fn get_path(&self) -> &PathBuf {
        &self.path
    }

    #[allow(dead_code)]
    pub fn delete(self) -> io::Result<()> {
        fs::remove_file(self.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_data() -> Vec<(Key, Option<Value>)> {
        vec![
            (b"key1".to_vec(), Some(b"value1".to_vec())),
            (b"key2".to_vec(), Some(b"value2".to_vec())),
            (b"key3".to_vec(), Some(b"value3".to_vec())),
        ]
    }

    #[test]
    fn test_create_new_sstable() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.sst");
        let table = SSTable::new(path).unwrap();

        assert_eq!(table.size(), 0);
    }

    #[test]
    fn test_write_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.sst");
        let mut table = SSTable::new(path).unwrap();

        let test_data = create_test_data();
        table.write(&test_data).unwrap();

        // Verify size
        assert!(table.size() > 0);

        // Read back and verify
        let read_data = table.read().unwrap();
        assert_eq!(read_data, test_data);
    }

    #[test]
    fn test_size_calculation() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.sst");
        let mut table = SSTable::new(path.clone()).unwrap();

        let test_data = create_test_data();
        table.write(&test_data).unwrap();

        let expected_size = fs::metadata(&path).unwrap().len() as usize;
        assert_eq!(table.size(), expected_size);
    }

    #[test]
    fn test_empty_sstable() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("empty.sst");
        let mut table = SSTable::new(path).unwrap();

        table.write(&[]).unwrap();
        let read_data = table.read().unwrap();
        assert!(read_data.is_empty());
    }

    #[test]
    fn test_large_values() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("large.sst");
        let mut table = SSTable::new(path).unwrap();

        let large_value = vec![b'x'; 1024 * 1024]; // 1MB value
        let test_data = vec![(b"large_key".to_vec(), Some(large_value.clone()))];

        table.write(&test_data).unwrap();
        let read_data = table.read().unwrap();

        assert_eq!(read_data[0].1, Some(large_value));
    }

    #[test]
    fn test_get_path() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.sst");
        let path_clone = path.clone();
        let table = SSTable::new(path).unwrap();

        assert_eq!(table.get_path(), &path_clone);
    }

    #[test]
    fn test_delete() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("to_delete.sst");
        let path_clone = path.clone();

        // Create and write some data to ensure the file exists
        let mut table = SSTable::new(path).unwrap();
        table
            .write(&[(b"key".to_vec(), Some(b"value".to_vec()))])
            .unwrap();

        assert!(path_clone.exists());
        table.delete().unwrap();
        assert!(!path_clone.exists());
    }

    #[test]
    fn test_bloom_filter() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("bloom_test.sst");
        let mut table = SSTable::new(path).unwrap();

        let test_data = create_test_data();
        table.write(&test_data).unwrap();

        // Keys in the set should return true from might_contain_key
        assert!(table.might_contain_key(b"key1"));
        assert!(table.might_contain_key(b"key2"));
        assert!(table.might_contain_key(b"key3"));

        // Test actual get operations
        assert_eq!(
            table.get(b"key1").unwrap(),
            SSTableLookup::Found(b"value1".to_vec())
        );
        assert_eq!(
            table.get(b"key2").unwrap(),
            SSTableLookup::Found(b"value2".to_vec())
        );
        assert_eq!(table.get(b"nonexistent").unwrap(), SSTableLookup::NotFound);
    }

    #[test]
    fn test_tombstone_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("tombstone.sst");
        let mut table = SSTable::new(path.clone()).unwrap();

        let test_data = vec![
            (b"alive".to_vec(), Some(b"value".to_vec())),
            (b"deleted".to_vec(), None),
        ];
        table.write(&test_data).unwrap();

        // Tombstones survive a write/read roundtrip
        assert_eq!(table.read().unwrap(), test_data);

        // get() distinguishes deleted from absent
        assert_eq!(
            table.get(b"alive").unwrap(),
            SSTableLookup::Found(b"value".to_vec())
        );
        assert_eq!(table.get(b"deleted").unwrap(), SSTableLookup::Deleted);
        assert_eq!(table.get(b"absent").unwrap(), SSTableLookup::NotFound);

        // Tombstones survive reopening the file from disk
        let reopened = SSTable::new(path).unwrap();
        assert_eq!(reopened.get(b"deleted").unwrap(), SSTableLookup::Deleted);
    }

    #[test]
    fn test_corrupt_file_returns_error() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("corrupt.sst");
        let mut table = SSTable::new(path.clone()).unwrap();
        table
            .write(&[(b"key".to_vec(), Some(b"value".to_vec()))])
            .unwrap();

        // Truncate the file mid-entry
        let len = fs::metadata(&path).unwrap().len();
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 3).unwrap();

        let reopened = SSTable::new(path).unwrap();
        assert!(reopened.read().is_err());
    }
}
