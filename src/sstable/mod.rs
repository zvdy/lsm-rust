use crate::bloom::BloomFilter;
use crate::{Key, Value};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

mod compaction;
pub use compaction::CompactionManager;

const BLOOM_FALSE_POSITIVE_RATE: f64 = 0.01;
const EXPECTED_ENTRIES_PER_SSTABLE: usize = 1000;

/// Sentinel value length that marks a tombstone (deleted key) on disk.
/// Real values are limited to less than u32::MAX bytes.
const TOMBSTONE_MARKER: u32 = u32::MAX;

/// Magic bytes identifying the versioned SSTable format.
const MAGIC: &[u8; 4] = b"LSMT";
/// Current on-disk format version.
const FORMAT_VERSION: u8 = 2;
/// Number of entries per data block; the sparse index holds one entry per
/// block, so lookups read at most one block of this many entries.
const BLOCK_ENTRY_COUNT: usize = 16;

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

/// One sparse-index entry: the first key of a block and where that block
/// lives inside the data section.
struct IndexEntry {
    first_key: Key,
    offset: u64,
    len: u32,
}

/// How the entries of an SSTable file are laid out on disk.
enum Layout {
    /// No data written yet.
    Empty,
    /// Pre-versioned format: `[bloom_len][bloom][flat entries...]`.
    /// Lookups fall back to a linear scan of the whole data section.
    Legacy { data_start: u64 },
    /// Versioned format (v2): header + bloom + sparse index + data blocks.
    /// Lookups binary-search the index and read a single block.
    V2 {
        data_start: u64,
        index: Vec<IndexEntry>,
    },
}

pub struct SSTable {
    path: PathBuf,
    size: usize,
    bloom_filter: Option<BloomFilter>,
    layout: Layout,
}

fn invalid_data(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg)
}

fn read_u32(buffer: &[u8], pos: usize) -> io::Result<u32> {
    buffer
        .get(pos..pos + 4)
        .map(|b| u32::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(|| invalid_data("SSTable truncated: expected 4-byte length"))
}

fn read_u64(buffer: &[u8], pos: usize) -> io::Result<u64> {
    buffer
        .get(pos..pos + 8)
        .map(|b| u64::from_le_bytes(b.try_into().unwrap()))
        .ok_or_else(|| invalid_data("SSTable truncated: expected 8-byte offset"))
}

fn read_slice(buffer: &[u8], pos: usize, len: usize) -> io::Result<&[u8]> {
    buffer
        .get(pos..pos + len)
        .ok_or_else(|| invalid_data("SSTable truncated: entry data out of bounds"))
}

/// Parse a flat sequence of `[key_len][key][value_len_or_tombstone][value?]`
/// entries covering the whole buffer.
fn parse_entries(buffer: &[u8]) -> io::Result<Vec<(Key, Option<Value>)>> {
    let mut data = Vec::new();
    let mut pos = 0;
    while pos < buffer.len() {
        let key_size = read_u32(buffer, pos)? as usize;
        pos += 4;
        let key = read_slice(buffer, pos, key_size)?.to_vec();
        pos += key_size;

        let value_size = read_u32(buffer, pos)?;
        pos += 4;
        let value = if value_size == TOMBSTONE_MARKER {
            None
        } else {
            let value = read_slice(buffer, pos, value_size as usize)?.to_vec();
            pos += value_size as usize;
            Some(value)
        };

        data.push((key, value));
    }
    Ok(data)
}

/// Encode entries in the flat on-disk entry format.
fn encode_entries(data: &[(Key, Option<Value>)], out: &mut Vec<u8>) {
    for (key, value) in data {
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(key);
        match value {
            Some(value) => {
                out.extend_from_slice(&(value.len() as u32).to_le_bytes());
                out.extend_from_slice(value);
            }
            None => out.extend_from_slice(&TOMBSTONE_MARKER.to_le_bytes()),
        }
    }
}

impl SSTable {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        if !path.exists() {
            return Ok(SSTable {
                path,
                size: 0,
                bloom_filter: None,
                layout: Layout::Empty,
            });
        }

        let size = fs::metadata(&path)?.len() as usize;
        if size == 0 {
            return Ok(SSTable {
                path,
                size: 0,
                bloom_filter: None,
                layout: Layout::Empty,
            });
        }

        let (bloom_filter, layout) = Self::open_layout(&path)?;
        Ok(SSTable {
            path,
            size,
            bloom_filter,
            layout,
        })
    }

    /// Sniff the file header and load the bloom filter and index metadata.
    fn open_layout(path: &Path) -> io::Result<(Option<BloomFilter>, Layout)> {
        let mut file = File::open(path)?;
        let mut magic = [0u8; 4];
        file.read_exact(&mut magic)?;

        if &magic == MAGIC {
            // Versioned format
            let mut version_flags = [0u8; 2];
            file.read_exact(&mut version_flags)?;
            let version = version_flags[0];
            if version != FORMAT_VERSION {
                return Err(invalid_data("unsupported SSTable format version"));
            }

            // Bloom filter
            let mut len_bytes = [0u8; 4];
            file.read_exact(&mut len_bytes)?;
            let bloom_len = u32::from_le_bytes(len_bytes) as usize;
            let mut bloom_bytes = vec![0u8; bloom_len];
            file.read_exact(&mut bloom_bytes)?;
            let bloom = BloomFilter::from_bytes(&bloom_bytes).ok();

            // Sparse index
            file.read_exact(&mut len_bytes)?;
            let index_len = u32::from_le_bytes(len_bytes) as usize;
            let mut index_bytes = vec![0u8; index_len];
            file.read_exact(&mut index_bytes)?;
            let index = Self::parse_index(&index_bytes)?;

            let data_start = file.stream_position()?;
            Ok((bloom, Layout::V2 { data_start, index }))
        } else {
            // Legacy format: the first 4 bytes are the bloom filter length
            let bloom_len = u32::from_le_bytes(magic) as usize;
            let mut bloom_bytes = vec![0u8; bloom_len];
            file.read_exact(&mut bloom_bytes)?;
            let bloom = BloomFilter::from_bytes(&bloom_bytes).ok();
            let data_start = 4 + bloom_len as u64;
            Ok((bloom, Layout::Legacy { data_start }))
        }
    }

    fn parse_index(bytes: &[u8]) -> io::Result<Vec<IndexEntry>> {
        let count = read_u32(bytes, 0)? as usize;
        let mut index = Vec::with_capacity(count);
        let mut pos = 4;
        for _ in 0..count {
            let key_len = read_u32(bytes, pos)? as usize;
            pos += 4;
            let first_key = read_slice(bytes, pos, key_len)?.to_vec();
            pos += key_len;
            let offset = read_u64(bytes, pos)?;
            pos += 8;
            let len = read_u32(bytes, pos)?;
            pos += 4;
            index.push(IndexEntry {
                first_key,
                offset,
                len,
            });
        }
        Ok(index)
    }

    fn serialize_index(index: &[IndexEntry]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(index.len() as u32).to_le_bytes());
        for entry in index {
            bytes.extend_from_slice(&(entry.first_key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&entry.first_key);
            bytes.extend_from_slice(&entry.offset.to_le_bytes());
            bytes.extend_from_slice(&entry.len.to_le_bytes());
        }
        bytes
    }

    /// Write sorted entries to this SSTable in the versioned block format.
    /// An entry value of `None` is a tombstone recording a deletion.
    pub fn write(&mut self, data: &[(Key, Option<Value>)]) -> io::Result<()> {
        // Build the bloom filter over all keys (including tombstones, so
        // that deletions are found and can shadow older values)
        let mut bloom = BloomFilter::new(
            data.len().max(EXPECTED_ENTRIES_PER_SSTABLE),
            BLOOM_FALSE_POSITIVE_RATE,
        );
        for (key, _) in data {
            bloom.insert(key.as_slice());
        }

        // Build data blocks and the sparse index over them
        let mut data_section = Vec::new();
        let mut index = Vec::new();
        for chunk in data.chunks(BLOCK_ENTRY_COUNT) {
            let offset = data_section.len() as u64;
            encode_entries(chunk, &mut data_section);
            let len = (data_section.len() as u64 - offset) as u32;
            index.push(IndexEntry {
                first_key: chunk[0].0.clone(),
                offset,
                len,
            });
        }

        let bloom_bytes = bloom.to_bytes();
        let index_bytes = Self::serialize_index(&index);

        let mut file = File::create(&self.path)?;
        file.write_all(MAGIC)?;
        file.write_all(&[FORMAT_VERSION, 0])?; // version, flags (reserved)
        file.write_all(&(bloom_bytes.len() as u32).to_le_bytes())?;
        file.write_all(&bloom_bytes)?;
        file.write_all(&(index_bytes.len() as u32).to_le_bytes())?;
        file.write_all(&index_bytes)?;
        let data_start = file.stream_position()?;
        file.write_all(&data_section)?;
        file.sync_all()?;

        self.size = (data_start + data_section.len() as u64) as usize;
        self.bloom_filter = Some(bloom);
        self.layout = Layout::V2 { data_start, index };
        Ok(())
    }

    /// Read `len` bytes at `offset` from the start of the file.
    fn read_range(&self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buffer = vec![0u8; len];
        file.read_exact(&mut buffer)?;
        Ok(buffer)
    }

    /// Read the whole file from `offset` to the end.
    fn read_to_end_from(&self, offset: u64) -> io::Result<Vec<u8>> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Ok(buffer)
    }

    /// Read all entries (including tombstones) from this SSTable.
    pub fn read(&self) -> io::Result<Vec<(Key, Option<Value>)>> {
        match &self.layout {
            Layout::Empty => Ok(Vec::new()),
            Layout::Legacy { data_start } => {
                let buffer = self.read_to_end_from(*data_start)?;
                parse_entries(&buffer)
            }
            Layout::V2 { data_start, index } => {
                let mut data = Vec::new();
                for entry in index {
                    let block = self.read_range(data_start + entry.offset, entry.len as usize)?;
                    data.extend(parse_entries(&block)?);
                }
                Ok(data)
            }
        }
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
    ///
    /// On the versioned format this binary-searches the sparse index and
    /// reads a single data block instead of scanning the whole file.
    pub fn get(&self, key: &[u8]) -> io::Result<SSTableLookup> {
        // First check the bloom filter
        if let Some(filter) = &self.bloom_filter {
            if !filter.might_contain(key) {
                return Ok(SSTableLookup::NotFound);
            }
        }

        match &self.layout {
            Layout::Empty => Ok(SSTableLookup::NotFound),
            Layout::Legacy { data_start } => {
                let buffer = self.read_to_end_from(*data_start)?;
                Self::scan_entries(&buffer, key, false)
            }
            Layout::V2 { data_start, index } => {
                // The candidate block is the last one whose first key is
                // <= the target; earlier blocks only hold smaller keys.
                let candidate = index.partition_point(|e| e.first_key.as_slice() <= key);
                if candidate == 0 {
                    return Ok(SSTableLookup::NotFound);
                }
                let entry = &index[candidate - 1];
                let block = self.read_range(data_start + entry.offset, entry.len as usize)?;
                Self::scan_entries(&block, key, true)
            }
        }
    }

    /// Linear scan of a flat entry buffer for `key`. When `sorted` is set,
    /// the scan stops early once it passes where the key would be.
    fn scan_entries(buffer: &[u8], key: &[u8], sorted: bool) -> io::Result<SSTableLookup> {
        let mut pos = 0;
        while pos < buffer.len() {
            let key_size = read_u32(buffer, pos)? as usize;
            pos += 4;
            let current_key = read_slice(buffer, pos, key_size)?;
            pos += key_size;

            let value_size = read_u32(buffer, pos)?;
            pos += 4;

            if current_key == key {
                if value_size == TOMBSTONE_MARKER {
                    return Ok(SSTableLookup::Deleted);
                }
                let value = read_slice(buffer, pos, value_size as usize)?.to_vec();
                return Ok(SSTableLookup::Found(value));
            }

            if sorted && current_key > key {
                return Ok(SSTableLookup::NotFound);
            }

            if value_size != TOMBSTONE_MARKER {
                pos += value_size as usize;
            }
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
        assert_eq!(table.get(b"anything").unwrap(), SSTableLookup::NotFound);
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

    #[test]
    fn test_versioned_header_written() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("header.sst");
        let mut table = SSTable::new(path.clone()).unwrap();
        table
            .write(&[(b"key".to_vec(), Some(b"value".to_vec()))])
            .unwrap();

        let bytes = fs::read(&path).unwrap();
        assert_eq!(&bytes[..4], MAGIC);
        assert_eq!(bytes[4], FORMAT_VERSION);
    }

    #[test]
    fn test_multi_block_lookup() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("blocks.sst");
        let mut table = SSTable::new(path.clone()).unwrap();

        // Well over BLOCK_ENTRY_COUNT entries so the index has many blocks.
        // Keys are zero-padded so lexicographic order matches numeric order.
        let data: Vec<_> = (0..100)
            .map(|i| {
                (
                    format!("key{:04}", i * 2).into_bytes(),
                    Some(format!("value{}", i).into_bytes()),
                )
            })
            .collect();
        table.write(&data).unwrap();

        // Every present key is found (spanning several blocks)
        for (i, (key, value)) in data.iter().enumerate() {
            assert_eq!(
                table.get(key).unwrap(),
                SSTableLookup::Found(value.clone().unwrap()),
                "key index {}",
                i
            );
        }

        // Keys between present keys, before the first and after the last
        assert_eq!(table.get(b"key0001").unwrap(), SSTableLookup::NotFound);
        assert_eq!(table.get(b"key0000\0").unwrap(), SSTableLookup::NotFound);
        assert_eq!(table.get(b"a").unwrap(), SSTableLookup::NotFound);
        assert_eq!(table.get(b"z").unwrap(), SSTableLookup::NotFound);

        // The full dataset survives reopen + read via the index
        let reopened = SSTable::new(path).unwrap();
        assert_eq!(reopened.read().unwrap(), data);
        assert_eq!(
            reopened.get(b"key0100").unwrap(),
            SSTableLookup::Found(b"value50".to_vec())
        );
    }

    #[test]
    fn test_legacy_format_fallback() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("legacy.sst");

        // Hand-craft a legacy (pre-versioned) file:
        // [bloom_len][bloom][key_len][key][value_len][value]...
        let mut bloom = BloomFilter::new(EXPECTED_ENTRIES_PER_SSTABLE, BLOOM_FALSE_POSITIVE_RATE);
        bloom.insert(b"old_key".as_slice());
        bloom.insert(b"gone_key".as_slice());
        let bloom_bytes = bloom.to_bytes();

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(bloom_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&bloom_bytes);
        encode_entries(
            &[
                (b"gone_key".to_vec(), None),
                (b"old_key".to_vec(), Some(b"old_value".to_vec())),
            ],
            &mut bytes,
        );
        fs::write(&path, &bytes).unwrap();

        // The legacy file is readable without the versioned header
        let table = SSTable::new(path).unwrap();
        assert_eq!(
            table.get(b"old_key").unwrap(),
            SSTableLookup::Found(b"old_value".to_vec())
        );
        assert_eq!(table.get(b"gone_key").unwrap(), SSTableLookup::Deleted);
        assert_eq!(table.get(b"missing").unwrap(), SSTableLookup::NotFound);
        assert_eq!(
            table.read().unwrap(),
            vec![
                (b"gone_key".to_vec(), None),
                (b"old_key".to_vec(), Some(b"old_value".to_vec())),
            ]
        );
    }
}
