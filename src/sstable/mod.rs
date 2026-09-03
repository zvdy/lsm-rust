use crate::bloom::BloomFilter;
use crate::checksum::crc32;
use crate::{Key, Seq, Value};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod cache;
mod compaction;
pub use cache::BlockCache;
pub use compaction::CompactionManager;

const BLOOM_FALSE_POSITIVE_RATE: f64 = 0.01;
const EXPECTED_ENTRIES_PER_SSTABLE: usize = 1000;

/// Sentinel value length that marks a tombstone (deleted key) on disk.
/// Real values are limited to less than u32::MAX bytes.
const TOMBSTONE_MARKER: u32 = u32::MAX;

/// Magic bytes identifying the versioned SSTable format.
const MAGIC: &[u8; 4] = b"LSMT";
/// Header flag bit: data blocks are LZ4-compressed.
const FLAG_LZ4: u8 = 0b0000_0001;
/// Current on-disk format version. v3 stores a sequence number per entry
/// (MVCC). v2 is the same layout without the per-entry sequence; it is still
/// read (all entries treated as sequence 0).
const FORMAT_VERSION: u8 = 4;
/// Soft target for the number of entries per data block. A block is never
/// split in the middle of a key's versions, so all versions of a key are
/// always in one block and a point lookup reads a single block.
const BLOCK_ENTRY_COUNT: usize = 16;

/// A single stored version: key, its sequence number, and either a value or
/// a tombstone (`None`).
pub type VersionedEntry = (Key, Seq, Option<Value>);

/// Block compression applied to SSTable data blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Compression {
    /// Blocks are stored uncompressed.
    #[default]
    None,
    /// Each data block is compressed with LZ4.
    Lz4,
}

/// Result of looking up a key in an SSTable at some snapshot.
#[derive(Debug, PartialEq, Eq)]
pub enum SSTableLookup {
    /// The key exists with this value.
    Found(Value),
    /// The key was deleted; a tombstone shadows any older value.
    Deleted,
    /// The key is not present in this SSTable (at the requested snapshot).
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
    /// Pre-versioned format: `[bloom_len][bloom][flat entries...]` with no
    /// sequence numbers. Lookups fall back to a linear scan; entries are
    /// treated as sequence 0.
    Legacy { data_start: u64 },
    /// Versioned format: header + bloom + sparse index + data blocks.
    /// `has_seq` distinguishes v3/v4 (per-entry sequence) from v2 (no
    /// sequence, treated as sequence 0); `has_block_crc` marks v4, whose
    /// sections and data blocks each carry a CRC-32. Lookups binary-search
    /// the index and read a single block.
    Versioned {
        data_start: u64,
        index: Vec<IndexEntry>,
        compression: Compression,
        has_seq: bool,
        has_block_crc: bool,
    },
}

pub struct SSTable {
    path: PathBuf,
    size: usize,
    bloom_filter: Option<BloomFilter>,
    layout: Layout,
    cache: Option<Arc<BlockCache>>,
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
        .ok_or_else(|| invalid_data("SSTable truncated: expected 8-byte field"))
}

fn read_slice(buffer: &[u8], pos: usize, len: usize) -> io::Result<&[u8]> {
    buffer
        .get(pos..pos + len)
        .ok_or_else(|| invalid_data("SSTable truncated: entry data out of bounds"))
}

/// Parse a flat sequence of entries covering the whole buffer. When
/// `has_seq` is false the format carries no sequence number and every entry
/// is assigned sequence 0 (legacy / v2 data, which predates MVCC).
fn parse_entries(buffer: &[u8], has_seq: bool) -> io::Result<Vec<VersionedEntry>> {
    let mut data = Vec::new();
    let mut pos = 0;
    while pos < buffer.len() {
        let key_size = read_u32(buffer, pos)? as usize;
        pos += 4;
        let key = read_slice(buffer, pos, key_size)?.to_vec();
        pos += key_size;

        let seq = if has_seq {
            let s = read_u64(buffer, pos)?;
            pos += 8;
            s
        } else {
            0
        };

        let value_size = read_u32(buffer, pos)?;
        pos += 4;
        let value = if value_size == TOMBSTONE_MARKER {
            None
        } else {
            let value = read_slice(buffer, pos, value_size as usize)?.to_vec();
            pos += value_size as usize;
            Some(value)
        };

        data.push((key, seq, value));
    }
    Ok(data)
}

/// Encode versioned entries in the flat on-disk entry format (v3, with
/// sequence numbers).
fn encode_entries(data: &[VersionedEntry], out: &mut Vec<u8>) {
    for (key, seq, value) in data {
        out.extend_from_slice(&(key.len() as u32).to_le_bytes());
        out.extend_from_slice(key);
        out.extend_from_slice(&seq.to_le_bytes());
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
        Self::with_cache(path, None)
    }

    /// Open an SSTable that serves block reads through the given shared
    /// block cache.
    pub fn with_cache(path: PathBuf, cache: Option<Arc<BlockCache>>) -> io::Result<Self> {
        if !path.exists() {
            return Ok(SSTable {
                path,
                size: 0,
                bloom_filter: None,
                layout: Layout::Empty,
                cache,
            });
        }

        let size = fs::metadata(&path)?.len() as usize;
        if size == 0 {
            return Ok(SSTable {
                path,
                size: 0,
                bloom_filter: None,
                layout: Layout::Empty,
                cache,
            });
        }

        let (bloom_filter, layout) = Self::open_layout(&path)?;
        Ok(SSTable {
            path,
            size,
            bloom_filter,
            layout,
            cache,
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
            // v2: no sequence numbers. v3: per-entry sequence. v4: adds a
            // CRC-32 over every section and data block.
            let (has_seq, has_block_crc) = match version {
                2 => (false, false),
                3 => (true, false),
                4 => (true, true),
                _ => return Err(invalid_data("unsupported SSTable format version")),
            };
            let compression = if version_flags[1] & FLAG_LZ4 != 0 {
                Compression::Lz4
            } else {
                Compression::None
            };

            // Bloom filter, then the sparse index. In v4 each section is
            // preceded by its length and a CRC-32 verified before use, so a
            // corrupted header fails loudly instead of yielding a bogus index.
            let bloom_bytes = Self::read_section(&mut file, has_block_crc, "bloom filter")?;
            let bloom = BloomFilter::from_bytes(&bloom_bytes).ok();

            let index_bytes = Self::read_section(&mut file, has_block_crc, "sparse index")?;
            let index = Self::parse_index(&index_bytes)?;

            let data_start = file.stream_position()?;
            Ok((
                bloom,
                Layout::Versioned {
                    data_start,
                    index,
                    compression,
                    has_seq,
                    has_block_crc,
                },
            ))
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

    /// Read a length-prefixed header section. In v4 the length is followed
    /// by a CRC-32 of the section body, which is verified before the bytes
    /// are handed on; older versions store the body alone.
    fn read_section(file: &mut File, has_crc: bool, what: &str) -> io::Result<Vec<u8>> {
        let mut len_bytes = [0u8; 4];
        file.read_exact(&mut len_bytes)?;
        let len = u32::from_le_bytes(len_bytes) as usize;

        let expected = if has_crc {
            let mut crc_bytes = [0u8; 4];
            file.read_exact(&mut crc_bytes)?;
            Some(u32::from_le_bytes(crc_bytes))
        } else {
            None
        };

        let mut body = vec![0u8; len];
        file.read_exact(&mut body)?;

        if let Some(expected) = expected {
            let actual = crc32(&body);
            if actual != expected {
                return Err(invalid_data(&format!(
                    "{} checksum mismatch: expected {:#010x}, got {:#010x}",
                    what, expected, actual
                )));
            }
        }
        Ok(body)
    }

    /// Write a header section as `[len][crc][body]` (v4 framing).
    fn write_section(file: &mut File, body: &[u8]) -> io::Result<()> {
        file.write_all(&(body.len() as u32).to_le_bytes())?;
        file.write_all(&crc32(body).to_le_bytes())?;
        file.write_all(body)
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

    /// Write single-version entries (sequence 0) uncompressed. Convenience
    /// for callers that don't track sequence numbers (and tests).
    pub fn write(&mut self, data: &[(Key, Option<Value>)]) -> io::Result<()> {
        self.write_with(data, Compression::None)
    }

    /// Write single-version entries (sequence 0) with the given compression.
    pub fn write_with(
        &mut self,
        data: &[(Key, Option<Value>)],
        compression: Compression,
    ) -> io::Result<()> {
        let versioned: Vec<VersionedEntry> = data
            .iter()
            .map(|(k, v)| (k.clone(), 0, v.clone()))
            .collect();
        self.write_versioned(&versioned, compression)
    }

    /// Write versioned entries to this SSTable in the v3 block format.
    ///
    /// `data` must be sorted by `(key ascending, seq descending)`; all
    /// versions of a key must be contiguous. Blocks are cut on key
    /// boundaries so a key's versions never span two blocks.
    pub fn write_versioned(
        &mut self,
        data: &[VersionedEntry],
        compression: Compression,
    ) -> io::Result<()> {
        // Build the bloom filter over all keys (including tombstones, so
        // that deletions are found and can shadow older values)
        let mut bloom = BloomFilter::new(
            data.len().max(EXPECTED_ENTRIES_PER_SSTABLE),
            BLOOM_FALSE_POSITIVE_RATE,
        );
        for (key, _, _) in data {
            bloom.insert(key.as_slice());
        }

        // Build key-aligned data blocks and the sparse index over them
        let mut data_section = Vec::new();
        let mut index = Vec::new();
        let mut block_buf = Vec::new();
        let mut i = 0;
        while i < data.len() {
            let start = i;
            i += 1;
            // Extend the block until it reaches the target size, but never
            // stop in the middle of a key's run of versions.
            while i < data.len() && (i - start < BLOCK_ENTRY_COUNT || data[i].0 == data[i - 1].0) {
                i += 1;
            }
            let chunk = &data[start..i];

            block_buf.clear();
            encode_entries(chunk, &mut block_buf);
            let offset = data_section.len() as u64;
            // The checksum covers the bytes as stored (after compression), so
            // corruption is caught before the decompressor ever sees them.
            let stored = match compression {
                Compression::None => block_buf.clone(),
                Compression::Lz4 => lz4_flex::compress_prepend_size(&block_buf),
            };
            data_section.extend_from_slice(&crc32(&stored).to_le_bytes());
            data_section.extend_from_slice(&stored);
            let len = (data_section.len() as u64 - offset) as u32;
            index.push(IndexEntry {
                first_key: chunk[0].0.clone(),
                offset,
                len,
            });
        }

        let bloom_bytes = bloom.to_bytes();
        let index_bytes = Self::serialize_index(&index);

        if let Some(cache) = &self.cache {
            // The file is being (re)written: drop any cached blocks for it
            cache.purge_file(&self.path);
        }

        let flags = match compression {
            Compression::None => 0,
            Compression::Lz4 => FLAG_LZ4,
        };
        let mut file = File::create(&self.path)?;
        file.write_all(MAGIC)?;
        file.write_all(&[FORMAT_VERSION, flags])?;
        Self::write_section(&mut file, &bloom_bytes)?;
        Self::write_section(&mut file, &index_bytes)?;
        let data_start = file.stream_position()?;
        file.write_all(&data_section)?;
        file.sync_all()?;

        self.size = (data_start + data_section.len() as u64) as usize;
        self.bloom_filter = Some(bloom);
        self.layout = Layout::Versioned {
            data_start,
            index,
            compression,
            has_seq: true,
            has_block_crc: true,
        };
        Ok(())
    }

    /// Read one data block, decompressing it if the table is compressed.
    /// Served from the shared block cache when one is configured.
    fn read_block(
        &self,
        data_start: u64,
        entry: &IndexEntry,
        compression: Compression,
        has_block_crc: bool,
    ) -> io::Result<Arc<Vec<u8>>> {
        if let Some(cache) = &self.cache {
            if let Some(block) = cache.get(&self.path, entry.offset) {
                return Ok(block);
            }
        }

        let mut raw = self.read_range(data_start + entry.offset, entry.len as usize)?;

        // v4 blocks are stored as `[crc u32][payload]`. Verify before doing
        // anything else with the bytes: a corrupted block must surface as an
        // error rather than as decompression garbage or bogus entries.
        if has_block_crc {
            if raw.len() < 4 {
                return Err(invalid_data("data block too short to hold a checksum"));
            }
            let expected = u32::from_le_bytes([raw[0], raw[1], raw[2], raw[3]]);
            let payload = &raw[4..];
            let actual = crc32(payload);
            if actual != expected {
                return Err(invalid_data(&format!(
                    "data block checksum mismatch at offset {}: expected {:#010x}, got {:#010x}",
                    entry.offset, expected, actual
                )));
            }
            raw = payload.to_vec();
        }

        let block = Arc::new(match compression {
            Compression::None => raw,
            Compression::Lz4 => lz4_flex::decompress_size_prepended(&raw)
                .map_err(|e| invalid_data(&format!("failed to decompress block: {}", e)))?,
        });

        if let Some(cache) = &self.cache {
            cache.insert(&self.path, entry.offset, Arc::clone(&block));
        }
        Ok(block)
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

    /// Read every stored version (including tombstones) from this SSTable.
    pub fn read_versioned(&self) -> io::Result<Vec<VersionedEntry>> {
        match &self.layout {
            Layout::Empty => Ok(Vec::new()),
            Layout::Legacy { data_start } => {
                let buffer = self.read_to_end_from(*data_start)?;
                parse_entries(&buffer, false)
            }
            Layout::Versioned {
                data_start,
                index,
                compression,
                has_seq,
                has_block_crc,
            } => {
                let mut data = Vec::new();
                for entry in index {
                    let block =
                        self.read_block(*data_start, entry, *compression, *has_block_crc)?;
                    data.extend(parse_entries(&block, *has_seq)?);
                }
                Ok(data)
            }
        }
    }

    /// Read all entries as `(key, value_or_tombstone)`, dropping sequence
    /// numbers. Convenience for single-version callers and tests.
    pub fn read(&self) -> io::Result<Vec<(Key, Option<Value>)>> {
        Ok(self
            .read_versioned()?
            .into_iter()
            .map(|(k, _seq, v)| (k, v))
            .collect())
    }

    /// Return every version whose key is in `[start, end)`, in
    /// `(key asc, seq desc)` order. `end = None` means unbounded above. Only
    /// the blocks that can intersect the range are read.
    pub fn scan_range_versioned(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> io::Result<Vec<VersionedEntry>> {
        let in_range = |k: &[u8]| k >= start && end.is_none_or(|e| k < e);
        match &self.layout {
            Layout::Empty => Ok(Vec::new()),
            Layout::Legacy { data_start } => {
                let buffer = self.read_to_end_from(*data_start)?;
                Ok(parse_entries(&buffer, false)?
                    .into_iter()
                    .filter(|(k, _, _)| in_range(k))
                    .collect())
            }
            Layout::Versioned {
                data_start,
                index,
                compression,
                has_seq,
                has_block_crc,
            } => {
                // Blocks before the candidate hold only keys < start; blocks
                // whose first key is >= end hold only keys past the range.
                let first = index
                    .partition_point(|e| e.first_key.as_slice() <= start)
                    .saturating_sub(1);
                let mut out = Vec::new();
                for entry in &index[first..] {
                    if end.is_some_and(|e| entry.first_key.as_slice() >= e) {
                        break;
                    }
                    let block =
                        self.read_block(*data_start, entry, *compression, *has_block_crc)?;
                    for (k, seq, v) in parse_entries(&block, *has_seq)? {
                        if in_range(&k) {
                            out.push((k, seq, v));
                        }
                    }
                }
                Ok(out)
            }
        }
    }

    /// Return the newest value per key in `[start, end)`, dropping sequence
    /// numbers and older versions. Convenience for single-version callers
    /// and tests.
    pub fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> io::Result<Vec<(Key, Option<Value>)>> {
        let mut out: Vec<(Key, Option<Value>)> = Vec::new();
        for (k, _seq, v) in self.scan_range_versioned(start, end)? {
            // Versions are (key asc, seq desc): the first entry seen for a
            // key is its newest version.
            if out.last().is_some_and(|(lk, _)| *lk == k) {
                continue;
            }
            out.push((k, v));
        }
        Ok(out)
    }

    pub fn might_contain_key(&self, key: &[u8]) -> bool {
        if let Some(filter) = &self.bloom_filter {
            filter.might_contain(key)
        } else {
            // If no bloom filter, conservatively return true
            true
        }
    }

    /// Look up the newest version of `key` visible at `snapshot_seq`,
    /// distinguishing "deleted here" from "not present here" so that
    /// tombstones shadow older SSTables.
    pub fn get_at(&self, key: &[u8], snapshot_seq: Seq) -> io::Result<SSTableLookup> {
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
                Self::scan_entries_at(&buffer, key, snapshot_seq, false, false)
            }
            Layout::Versioned {
                data_start,
                index,
                compression,
                has_seq,
                has_block_crc,
            } => {
                // The candidate block is the last one whose first key is
                // <= the target; earlier blocks only hold smaller keys.
                let candidate = index.partition_point(|e| e.first_key.as_slice() <= key);
                if candidate == 0 {
                    return Ok(SSTableLookup::NotFound);
                }
                let entry = &index[candidate - 1];
                let block = self.read_block(*data_start, entry, *compression, *has_block_crc)?;
                Self::scan_entries_at(&block, key, snapshot_seq, true, *has_seq)
            }
        }
    }

    /// Look up the latest version of `key` (highest sequence).
    pub fn get(&self, key: &[u8]) -> io::Result<SSTableLookup> {
        self.get_at(key, Seq::MAX)
    }

    /// Scan a flat entry buffer for the newest version of `key` with
    /// sequence `<= snapshot_seq`. Entries are ordered `(key asc, seq desc)`,
    /// so the first matching version at or below the snapshot is the answer.
    /// When `sorted` is set, the scan stops once it passes where the key
    /// would be.
    fn scan_entries_at(
        buffer: &[u8],
        key: &[u8],
        snapshot_seq: Seq,
        sorted: bool,
        has_seq: bool,
    ) -> io::Result<SSTableLookup> {
        let mut pos = 0;
        while pos < buffer.len() {
            let key_size = read_u32(buffer, pos)? as usize;
            pos += 4;
            let current_key = read_slice(buffer, pos, key_size)?;
            pos += key_size;

            let seq = if has_seq {
                let s = read_u64(buffer, pos)?;
                pos += 8;
                s
            } else {
                0
            };

            let value_size = read_u32(buffer, pos)?;
            pos += 4;

            if current_key == key {
                if seq <= snapshot_seq {
                    // Newest version visible to this snapshot
                    if value_size == TOMBSTONE_MARKER {
                        return Ok(SSTableLookup::Deleted);
                    }
                    let value = read_slice(buffer, pos, value_size as usize)?.to_vec();
                    return Ok(SSTableLookup::Found(value));
                }
                // Version is too new for this snapshot; keep looking at
                // older versions of the same key (they come next).
            } else if sorted && current_key > key {
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
    fn test_mvcc_versions_and_snapshot_reads() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("mvcc.sst");
        let mut table = SSTable::new(path.clone()).unwrap();

        // Three versions of "k" plus a deleted "gone", sorted key asc, seq desc
        let data: Vec<VersionedEntry> = vec![
            (b"gone".to_vec(), 7, None),
            (b"k".to_vec(), 9, Some(b"v9".to_vec())),
            (b"k".to_vec(), 5, Some(b"v5".to_vec())),
            (b"k".to_vec(), 1, Some(b"v1".to_vec())),
        ];
        table.write_versioned(&data, Compression::None).unwrap();

        // Snapshot reads pick the newest version at or below the snapshot seq
        assert_eq!(table.get_at(b"k", 0).unwrap(), SSTableLookup::NotFound);
        assert_eq!(
            table.get_at(b"k", 1).unwrap(),
            SSTableLookup::Found(b"v1".to_vec())
        );
        assert_eq!(
            table.get_at(b"k", 4).unwrap(),
            SSTableLookup::Found(b"v1".to_vec())
        );
        assert_eq!(
            table.get_at(b"k", 6).unwrap(),
            SSTableLookup::Found(b"v5".to_vec())
        );
        assert_eq!(
            table.get_at(b"k", 100).unwrap(),
            SSTableLookup::Found(b"v9".to_vec())
        );
        assert_eq!(
            table.get(b"k").unwrap(),
            SSTableLookup::Found(b"v9".to_vec())
        );

        // Tombstone visibility follows the snapshot too
        assert_eq!(table.get_at(b"gone", 6).unwrap(), SSTableLookup::NotFound);
        assert_eq!(table.get_at(b"gone", 7).unwrap(), SSTableLookup::Deleted);

        // Round-trips through disk
        let reopened = SSTable::new(path).unwrap();
        assert_eq!(reopened.read_versioned().unwrap(), data);
        assert_eq!(
            reopened.get_at(b"k", 5).unwrap(),
            SSTableLookup::Found(b"v5".to_vec())
        );
    }

    #[test]
    fn test_many_versions_span_block_boundary() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("versions.sst");
        let mut table = SSTable::new(path).unwrap();

        // One key with far more than BLOCK_ENTRY_COUNT versions: they must
        // stay in a single block so get_at still reads one block.
        let mut data: Vec<VersionedEntry> = (1..=50)
            .rev()
            .map(|s| {
                (
                    b"hot".to_vec(),
                    s as Seq,
                    Some(format!("v{}", s).into_bytes()),
                )
            })
            .collect();
        // Plus neighbouring keys to force multiple blocks around it
        for i in 0..40 {
            data.push((format!("z{:03}", i).into_bytes(), 100, Some(b"z".to_vec())));
        }
        data.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));

        table.write_versioned(&data, Compression::Lz4).unwrap();

        assert_eq!(
            table.get_at(b"hot", 25).unwrap(),
            SSTableLookup::Found(b"v25".to_vec())
        );
        assert_eq!(
            table.get_at(b"hot", 50).unwrap(),
            SSTableLookup::Found(b"v50".to_vec())
        );
        assert_eq!(
            table.get_at(b"z039", 100).unwrap(),
            SSTableLookup::Found(b"z".to_vec())
        );
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

        // Hand-craft a legacy (pre-versioned) file with no sequence numbers:
        // [bloom_len][bloom][key_len][key][value_len][value]...
        let mut bloom = BloomFilter::new(EXPECTED_ENTRIES_PER_SSTABLE, BLOOM_FALSE_POSITIVE_RATE);
        bloom.insert(b"old_key".as_slice());
        bloom.insert(b"gone_key".as_slice());
        let bloom_bytes = bloom.to_bytes();

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&(bloom_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&bloom_bytes);
        // Legacy entry encoding (no seq field)
        for (key, value) in [
            (b"gone_key".to_vec(), None::<Vec<u8>>),
            (b"old_key".to_vec(), Some(b"old_value".to_vec())),
        ] {
            bytes.extend_from_slice(&(key.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&key);
            match value {
                Some(v) => {
                    bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    bytes.extend_from_slice(&v);
                }
                None => bytes.extend_from_slice(&TOMBSTONE_MARKER.to_le_bytes()),
            }
        }
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

    #[test]
    fn test_compressed_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("compressed.sst");
        let mut table = SSTable::new(path.clone()).unwrap();

        let data: Vec<_> = (0..100)
            .map(|i| {
                (
                    format!("key{:04}", i).into_bytes(),
                    if i == 50 {
                        None // a tombstone inside a compressed block
                    } else {
                        Some(format!("value{}", i).repeat(50).into_bytes())
                    },
                )
            })
            .collect();
        table.write_with(&data, Compression::Lz4).unwrap();

        assert_eq!(table.read().unwrap(), data);
        assert_eq!(
            table.get(b"key0010").unwrap(),
            SSTableLookup::Found(b"value10".repeat(50).to_vec())
        );
        assert_eq!(table.get(b"key0050").unwrap(), SSTableLookup::Deleted);
        assert_eq!(table.get(b"missing").unwrap(), SSTableLookup::NotFound);

        // Reopen from disk: compression flag comes from the header
        let reopened = SSTable::new(path).unwrap();
        assert_eq!(reopened.read().unwrap(), data);
        assert_eq!(
            reopened.get(b"key0099").unwrap(),
            SSTableLookup::Found(b"value99".repeat(50).to_vec())
        );
    }

    #[test]
    fn test_compression_reduces_size() {
        let temp_dir = TempDir::new().unwrap();
        let data: Vec<_> = (0..200)
            .map(|i| {
                (
                    format!("key{:04}", i).into_bytes(),
                    Some(vec![b'a'; 512]), // highly compressible values
                )
            })
            .collect();

        let mut plain = SSTable::new(temp_dir.path().join("plain.sst")).unwrap();
        plain.write_with(&data, Compression::None).unwrap();
        let mut packed = SSTable::new(temp_dir.path().join("packed.sst")).unwrap();
        packed.write_with(&data, Compression::Lz4).unwrap();

        assert!(
            packed.size() < plain.size() / 2,
            "expected compressed table ({}) to be much smaller than plain ({})",
            packed.size(),
            plain.size()
        );
    }

    #[test]
    fn test_scan_range_multi_block() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("scan.sst");
        let mut table = SSTable::new(path).unwrap();

        // 100 entries spanning several blocks; one tombstone inside the range
        let data: Vec<_> = (0..100)
            .map(|i| {
                (
                    format!("key{:04}", i).into_bytes(),
                    if i == 42 {
                        None
                    } else {
                        Some(format!("value{}", i).into_bytes())
                    },
                )
            })
            .collect();
        table.write(&data).unwrap();

        // Mid-range scan crossing block boundaries
        let hits = table.scan_range(b"key0040", Some(b"key0045")).unwrap();
        let keys: Vec<_> = hits.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(
            keys,
            (40..45)
                .map(|i| format!("key{:04}", i).into_bytes())
                .collect::<Vec<_>>()
        );
        assert_eq!(hits[2].1, None); // the tombstone is reported

        // Unbounded above
        let tail = table.scan_range(b"key0098", None).unwrap();
        assert_eq!(tail.len(), 2);

        // Before the first key and after the last key
        assert_eq!(table.scan_range(b"a", Some(b"key0000")).unwrap().len(), 0);
        assert_eq!(table.scan_range(b"z", None).unwrap().len(), 0);

        // Whole-table scan matches read()
        assert_eq!(table.scan_range(b"", None).unwrap(), data);
    }

    #[test]
    fn test_block_cache_serves_repeated_reads() {
        let temp_dir = TempDir::new().unwrap();
        let cache = Arc::new(BlockCache::new(1024 * 1024));
        let path = temp_dir.path().join("cached.sst");
        let mut table = SSTable::with_cache(path, Some(Arc::clone(&cache))).unwrap();

        let data: Vec<_> = (0..50)
            .map(|i| {
                (
                    format!("key{:03}", i).into_bytes(),
                    Some(format!("value{}", i).into_bytes()),
                )
            })
            .collect();
        table.write_with(&data, Compression::Lz4).unwrap();

        // First read misses and fills the cache; repeats hit
        assert_eq!(
            table.get(b"key010").unwrap(),
            SSTableLookup::Found(b"value10".to_vec())
        );
        let (_, misses_after_first) = cache.stats();
        for _ in 0..5 {
            assert_eq!(
                table.get(b"key010").unwrap(),
                SSTableLookup::Found(b"value10".to_vec())
            );
        }
        let (hits, misses) = cache.stats();
        assert_eq!(misses, misses_after_first, "repeat reads must not miss");
        assert!(hits >= 5);
        assert!(cache.used_bytes() > 0);
    }
}
