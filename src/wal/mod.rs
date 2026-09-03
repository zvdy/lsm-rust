use crate::checksum::crc32;
use crate::{Key, Value};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    Put,
    Delete,
}

/// A single operation inside an atomically-committed batch.
pub struct BatchEntry<'a> {
    pub op: Operation,
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
}

/// A record recovered from the log during replay: either a standalone
/// operation or a batch that must be applied atomically (all or nothing).
pub enum WalRecord {
    Single(Operation, Key, Option<Value>),
    Batch(Vec<(Operation, Key, Option<Value>)>),
}

/// Marker byte introducing a batch record: `[2][count u32][entry...]`.
const BATCH_MARKER: u8 = 2;

/// Marker byte introducing a checksummed record:
/// `[3][crc u32][body_len u32][body]`, where `body` is an unframed record
/// (a single entry or a batch). Every record written now uses this framing;
/// the unchecksummed markers above are still replayed so a log written by an
/// older build stays readable.
const CRC_MARKER: u8 = 3;

/// When the write-ahead log fsyncs to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalSync {
    /// fsync after every append: an acknowledged write is never lost, at
    /// the cost of one disk sync per operation.
    #[default]
    Always,
    /// Group commit: fsync once every `every_n_writes` appends (and on
    /// memtable flush and shutdown). Much higher write throughput; on a
    /// crash, up to `every_n_writes - 1` acknowledged writes may be lost.
    Batched { every_n_writes: usize },
}

#[allow(clippy::upper_case_acronyms)]
pub struct WAL {
    path: PathBuf,
    file: File,
    sync: WalSync,
    unsynced_writes: usize,
}

impl WAL {
    pub fn new(path: PathBuf) -> crate::Result<Self> {
        Self::with_sync(path, WalSync::Always)
    }

    pub fn with_sync(path: PathBuf, sync: WalSync) -> crate::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        Ok(WAL {
            path,
            file,
            sync,
            unsynced_writes: 0,
        })
    }

    pub fn append(&mut self, op: Operation, key: &[u8], value: Option<&[u8]>) -> crate::Result<()> {
        let mut body = Vec::new();
        Self::write_entry(&mut body, op, key, value)?;
        self.write_framed(&body)?;
        self.file.flush()?;
        self.after_commit()
    }

    /// Append several operations as a single atomic record. On replay the
    /// whole batch is applied or, if a crash truncated it, none of it is.
    pub fn append_batch(&mut self, entries: &[BatchEntry]) -> crate::Result<()> {
        let mut body = Vec::new();
        body.push(BATCH_MARKER);
        body.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for entry in entries {
            Self::write_entry(&mut body, entry.op, entry.key, entry.value)?;
        }
        self.write_framed(&body)?;
        self.file.flush()?;
        self.after_commit()
    }

    /// Wrap a record body in the checksummed frame and append it.
    fn write_framed(&mut self, body: &[u8]) -> crate::Result<()> {
        self.file.write_all(&[CRC_MARKER])?;
        self.file.write_all(&crc32(body).to_le_bytes())?;
        self.file.write_all(&(body.len() as u32).to_le_bytes())?;
        self.file.write_all(body)?;
        Ok(())
    }

    /// Write one `[op][key_size][key][value_size?][value?]` entry.
    fn write_entry(
        file: &mut impl Write,
        op: Operation,
        key: &[u8],
        value: Option<&[u8]>,
    ) -> crate::Result<()> {
        let op_byte = match op {
            Operation::Put => 0u8,
            Operation::Delete => 1u8,
        };
        file.write_all(&[op_byte])?;
        file.write_all(&(key.len() as u32).to_le_bytes())?;
        file.write_all(key)?;
        if let Some(value) = value {
            file.write_all(&(value.len() as u32).to_le_bytes())?;
            file.write_all(value)?;
        }
        Ok(())
    }

    /// Account one commit (a single append or a whole batch) and fsync
    /// according to the sync policy.
    fn after_commit(&mut self) -> crate::Result<()> {
        self.unsynced_writes += 1;
        match self.sync {
            WalSync::Always => self.sync_now()?,
            WalSync::Batched { every_n_writes } => {
                if self.unsynced_writes >= every_n_writes.max(1) {
                    self.sync_now()?;
                }
            }
        }
        Ok(())
    }

    /// Force any batched appends to disk.
    pub fn sync_now(&mut self) -> crate::Result<()> {
        if self.unsynced_writes > 0 {
            self.file.sync_data()?;
            self.unsynced_writes = 0;
        }
        Ok(())
    }

    /// Replay all complete records in the log.
    ///
    /// A crash can leave a partially written record at the end of the file;
    /// such a truncated tail — a single entry or an incomplete batch — is
    /// silently discarded rather than failing recovery of the complete
    /// records before it.
    pub fn replay(&mut self) -> crate::Result<Vec<WalRecord>> {
        let mut records = Vec::new();
        let mut buffer = Vec::new();

        // Reset file pointer to start
        self.file.seek(io::SeekFrom::Start(0))?;
        self.file.read_to_end(&mut buffer)?;

        let mut pos = 0;
        while pos < buffer.len() {
            match buffer[pos] {
                0 | 1 => {
                    let mut p = pos;
                    let Some(entry) = Self::parse_entry(&buffer, &mut p) else {
                        break; // truncated tail
                    };
                    pos = p;
                    records.push(WalRecord::Single(entry.0, entry.1, entry.2));
                }
                BATCH_MARKER => {
                    let mut p = pos + 1;
                    let Some(count) = Self::read_u32(&buffer, &mut p) else {
                        break;
                    };
                    let mut batch = Vec::with_capacity(count as usize);
                    let mut complete = true;
                    for _ in 0..count {
                        match Self::parse_entry(&buffer, &mut p) {
                            Some(entry) => batch.push(entry),
                            None => {
                                complete = false;
                                break;
                            }
                        }
                    }
                    if !complete {
                        break; // truncated batch tail: apply none of it
                    }
                    pos = p;
                    records.push(WalRecord::Batch(batch));
                }
                CRC_MARKER => {
                    let mut p = pos + 1;
                    let (Some(expected), Some(len)) = (
                        Self::read_u32(&buffer, &mut p),
                        Self::read_u32(&buffer, &mut p),
                    ) else {
                        break; // truncated tail
                    };
                    let Some(body) = buffer.get(p..p + len as usize) else {
                        break; // truncated tail
                    };
                    let end = p + len as usize;

                    if crc32(body) != expected {
                        // A torn write while appending the last record leaves a
                        // complete-looking frame with a bad checksum; that is
                        // the same situation as a truncated tail, so drop it.
                        // Anywhere else it is real corruption of durable data.
                        if end >= buffer.len() {
                            break;
                        }
                        return Err(crate::Error::corruption(format!(
                            "WAL record checksum mismatch at offset {}: \
                             expected {:#010x}, got {:#010x}",
                            pos,
                            expected,
                            crc32(body)
                        )));
                    }

                    let Some(record) = Self::parse_body(body) else {
                        return Err(crate::Error::corruption(format!(
                            "malformed WAL record body at offset {}",
                            pos
                        )));
                    };
                    records.push(record);
                    pos = end;
                }
                _ => return Err(crate::Error::corruption("unrecognized WAL operation type")),
            }
        }

        Ok(records)
    }

    /// Parse an unframed record body: either a single entry or a batch.
    /// Returns `None` if the body is malformed or incomplete.
    fn parse_body(body: &[u8]) -> Option<WalRecord> {
        let mut p = 0;
        match *body.first()? {
            0 | 1 => {
                let (op, key, value) = Self::parse_entry(body, &mut p)?;
                Some(WalRecord::Single(op, key, value))
            }
            BATCH_MARKER => {
                p = 1;
                let count = Self::read_u32(body, &mut p)?;
                let mut batch = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    batch.push(Self::parse_entry(body, &mut p)?);
                }
                Some(WalRecord::Batch(batch))
            }
            _ => None,
        }
    }

    /// Parse one `[op][key][value?]` entry, returning None if the buffer ends
    /// before the entry is complete.
    fn parse_entry(buffer: &[u8], pos: &mut usize) -> Option<(Operation, Key, Option<Value>)> {
        let op = match buffer.get(*pos)? {
            0 => Operation::Put,
            1 => Operation::Delete,
            _ => return None,
        };
        *pos += 1;
        let key = Self::read_chunk(buffer, pos)?;
        let value = if matches!(op, Operation::Put) {
            Some(Self::read_chunk(buffer, pos)?)
        } else {
            None
        };
        Some((op, key, value))
    }

    /// Read a length-prefixed chunk, returning None if the buffer ends
    /// before the chunk is complete (a truncated tail).
    fn read_chunk(buffer: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
        let len = Self::read_u32(buffer, pos)? as usize;
        let chunk = buffer.get(*pos..*pos + len)?.to_vec();
        *pos += len;
        Some(chunk)
    }

    fn read_u32(buffer: &[u8], pos: &mut usize) -> Option<u32> {
        let bytes = buffer.get(*pos..*pos + 4)?;
        *pos += 4;
        Some(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    pub fn clear(&mut self) -> crate::Result<()> {
        self.file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(&self.path)?;
        self.file.sync_data()?;
        self.unsynced_writes = 0;
        Ok(())
    }
}

impl Drop for WAL {
    fn drop(&mut self) {
        // Best-effort flush of batched appends on clean shutdown
        let _ = self.sync_now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Flatten replayed records into (op, key, value) tuples for assertions.
    fn flatten(records: Vec<WalRecord>) -> Vec<(Operation, Key, Option<Value>)> {
        let mut out = Vec::new();
        for record in records {
            match record {
                WalRecord::Single(op, k, v) => out.push((op, k, v)),
                WalRecord::Batch(entries) => out.extend(entries),
            }
        }
        out
    }

    #[test]
    fn test_new_wal() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let wal = WAL::new(path).unwrap();
        assert!(wal.path.exists());
    }

    #[test]
    fn test_append_and_replay_put() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::new(path).unwrap();

        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        wal.append(Operation::Put, &key, Some(&value)).unwrap();

        let entries = flatten(wal.replay().unwrap());
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], (Operation::Put, key, Some(value)));
    }

    #[test]
    fn test_append_and_replay_delete() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::new(path).unwrap();

        let key = b"test_key".to_vec();
        wal.append(Operation::Delete, &key, None).unwrap();

        let entries = flatten(wal.replay().unwrap());
        assert_eq!(entries, vec![(Operation::Delete, key, None)]);
    }

    #[test]
    fn test_multiple_operations() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::new(path).unwrap();

        let operations = vec![
            (Operation::Put, b"key1".to_vec(), Some(b"value1".to_vec())),
            (Operation::Delete, b"key2".to_vec(), None),
            (Operation::Put, b"key3".to_vec(), Some(b"value3".to_vec())),
        ];

        for (op, key, value) in &operations {
            wal.append(*op, key, value.as_deref()).unwrap();
        }

        assert_eq!(flatten(wal.replay().unwrap()), operations);
    }

    #[test]
    fn test_append_batch_is_atomic_group() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::new(path.clone()).unwrap();

        wal.append(Operation::Put, b"before", Some(b"x")).unwrap();
        wal.append_batch(&[
            BatchEntry {
                op: Operation::Put,
                key: b"a",
                value: Some(b"1"),
            },
            BatchEntry {
                op: Operation::Delete,
                key: b"b",
                value: None,
            },
        ])
        .unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 2); // one single + one batch
        assert!(matches!(
            records[0],
            WalRecord::Single(Operation::Put, _, _)
        ));
        match &records[1] {
            WalRecord::Batch(entries) => {
                assert_eq!(entries.len(), 2);
                assert_eq!(
                    entries[0],
                    (Operation::Put, b"a".to_vec(), Some(b"1".to_vec()))
                );
                assert_eq!(entries[1], (Operation::Delete, b"b".to_vec(), None));
            }
            _ => panic!("expected a batch record"),
        }
    }

    #[test]
    fn test_truncated_batch_tail_is_dropped_whole() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::new(path.clone()).unwrap();

        wal.append(Operation::Put, b"kept", Some(b"v")).unwrap();
        wal.append_batch(&[
            BatchEntry {
                op: Operation::Put,
                key: b"a",
                value: Some(b"1"),
            },
            BatchEntry {
                op: Operation::Put,
                key: b"b",
                value: Some(b"2"),
            },
        ])
        .unwrap();

        // Chop the tail so the batch is incomplete (crash mid-batch)
        let len = fs::metadata(&path).unwrap().len();
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 3).unwrap();
        drop(file);

        let mut wal = WAL::new(path).unwrap();
        let entries = flatten(wal.replay().unwrap());
        // The whole batch is discarded; only the earlier single survives
        assert_eq!(
            entries,
            vec![(Operation::Put, b"kept".to_vec(), Some(b"v".to_vec()))]
        );
    }

    #[test]
    fn test_clear() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::new(path.clone()).unwrap();

        wal.append(Operation::Put, b"key", Some(b"value")).unwrap();
        assert!(fs::metadata(&path).unwrap().len() > 0);

        wal.clear().unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);
        assert!(wal.replay().unwrap().is_empty());
    }

    #[test]
    fn test_large_entries() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::new(path).unwrap();

        let large_value = vec![b'x'; 1024 * 1024]; // 1MB value
        wal.append(Operation::Put, b"large_key", Some(&large_value))
            .unwrap();

        let entries = flatten(wal.replay().unwrap());
        assert_eq!(
            entries,
            vec![(Operation::Put, b"large_key".to_vec(), Some(large_value))]
        );
    }

    #[test]
    fn test_replay_ignores_truncated_tail() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::new(path.clone()).unwrap();

        wal.append(Operation::Put, b"key1", Some(b"value1"))
            .unwrap();
        wal.append(Operation::Put, b"key2", Some(b"value2"))
            .unwrap();

        // Simulate a crash mid-append: truncate the last few bytes
        let len = fs::metadata(&path).unwrap().len();
        let file = fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len - 3).unwrap();
        drop(file);

        let mut wal = WAL::new(path).unwrap();
        let entries = flatten(wal.replay().unwrap());
        assert_eq!(
            entries,
            vec![(Operation::Put, b"key1".to_vec(), Some(b"value1".to_vec()))]
        );
    }

    #[test]
    fn test_batched_sync_replays_all_writes() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::with_sync(path.clone(), WalSync::Batched { every_n_writes: 8 }).unwrap();

        for i in 0..20 {
            let key = format!("key{}", i).into_bytes();
            wal.append(Operation::Put, &key, Some(b"value")).unwrap();
        }
        drop(wal); // syncs the batched tail

        let mut wal = WAL::new(path).unwrap();
        assert_eq!(flatten(wal.replay().unwrap()).len(), 20);
    }

    #[test]
    fn test_sync_now_resets_pending_counter() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::with_sync(
            path,
            WalSync::Batched {
                every_n_writes: 100,
            },
        )
        .unwrap();

        wal.append(Operation::Put, b"k", Some(b"v")).unwrap();
        assert_eq!(wal.unsynced_writes, 1);
        wal.sync_now().unwrap();
        assert_eq!(wal.unsynced_writes, 0);
    }
}
