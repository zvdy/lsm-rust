use crate::{Key, Value};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::PathBuf;

pub enum Operation {
    Put,
    Delete,
}

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
    pub fn new(path: PathBuf) -> io::Result<Self> {
        Self::with_sync(path, WalSync::Always)
    }

    pub fn with_sync(path: PathBuf, sync: WalSync) -> io::Result<Self> {
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

    pub fn append(&mut self, op: Operation, key: &[u8], value: Option<&[u8]>) -> io::Result<()> {
        // Write format: [op_type][key_size][key][value_size?][value?]
        let op_byte = match op {
            Operation::Put => 0u8,
            Operation::Delete => 1u8,
        };

        self.file.write_all(&[op_byte])?;
        self.file.write_all(&(key.len() as u32).to_le_bytes())?;
        self.file.write_all(key)?;

        if let Some(value) = value {
            self.file.write_all(&(value.len() as u32).to_le_bytes())?;
            self.file.write_all(value)?;
        }

        self.file.flush()?;
        self.unsynced_writes += 1;

        // fsync so operations are durable even if the process or machine
        // crashes right after append() returns; in batched mode the sync is
        // amortized over several appends (group commit)
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
    pub fn sync_now(&mut self) -> io::Result<()> {
        if self.unsynced_writes > 0 {
            self.file.sync_data()?;
            self.unsynced_writes = 0;
        }
        Ok(())
    }

    /// Replay all complete entries in the log.
    ///
    /// A crash can leave a partially written entry at the end of the file;
    /// such a truncated tail is silently discarded rather than failing
    /// recovery of all the preceding complete entries.
    pub fn replay(&mut self) -> io::Result<Vec<(Operation, Key, Option<Value>)>> {
        let mut entries = Vec::new();
        let mut buffer = Vec::new();

        // Reset file pointer to start
        self.file.seek(io::SeekFrom::Start(0))?;
        self.file.read_to_end(&mut buffer)?;

        let mut pos = 0;
        while pos < buffer.len() {
            // Read operation type
            let op = match buffer[pos] {
                0 => Operation::Put,
                1 => Operation::Delete,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid operation type",
                    ))
                }
            };
            pos += 1;

            // Read key
            let Some(key) = Self::read_chunk(&buffer, &mut pos) else {
                break;
            };

            // Read value if present
            let value = if matches!(op, Operation::Put) {
                let Some(value) = Self::read_chunk(&buffer, &mut pos) else {
                    break;
                };
                Some(value)
            } else {
                None
            };

            entries.push((op, key, value));
        }

        Ok(entries)
    }

    /// Read a length-prefixed chunk, returning None if the buffer ends
    /// before the chunk is complete (a truncated tail).
    fn read_chunk(buffer: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
        let len_bytes = buffer.get(*pos..*pos + 4)?;
        let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;
        let chunk = buffer.get(*pos + 4..*pos + 4 + len)?.to_vec();
        *pos += 4 + len;
        Some(chunk)
    }

    pub fn clear(&mut self) -> io::Result<()> {
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

        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), 1);

        match &entries[0] {
            (Operation::Put, k, Some(v)) => {
                assert_eq!(k, &key);
                assert_eq!(v, &value);
            }
            _ => panic!("Expected Put operation"),
        }
    }

    #[test]
    fn test_append_and_replay_delete() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::new(path).unwrap();

        let key = b"test_key".to_vec();
        wal.append(Operation::Delete, &key, None).unwrap();

        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), 1);

        match &entries[0] {
            (Operation::Delete, k, None) => {
                assert_eq!(k, &key);
            }
            _ => panic!("Expected Delete operation"),
        }
    }

    #[test]
    fn test_multiple_operations() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::new(path).unwrap();

        // Append multiple operations
        let operations = vec![
            (Operation::Put, b"key1".to_vec(), Some(b"value1".to_vec())),
            (Operation::Delete, b"key2".to_vec(), None),
            (Operation::Put, b"key3".to_vec(), Some(b"value3".to_vec())),
        ];

        for (op, key, value) in &operations {
            match op {
                Operation::Put => wal.append(Operation::Put, key, value.as_deref()).unwrap(),
                Operation::Delete => wal.append(Operation::Delete, key, None).unwrap(),
            }
        }

        // Replay and verify
        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), operations.len());

        for (i, (op, key, value)) in operations.iter().enumerate() {
            let (replay_op, replay_key, replay_value) = &entries[i];
            assert!(matches!(op, Operation::Put) == matches!(replay_op, Operation::Put));
            assert_eq!(replay_key, key);
            assert_eq!(replay_value, value);
        }
    }

    #[test]
    fn test_clear() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::new(path.clone()).unwrap();

        // Write some data
        wal.append(Operation::Put, b"key", Some(b"value")).unwrap();
        assert!(fs::metadata(&path).unwrap().len() > 0);

        // Clear and verify
        wal.clear().unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), 0);

        // Verify replay returns empty
        let entries = wal.replay().unwrap();
        assert!(entries.is_empty());
    }

    #[test]
    fn test_large_entries() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.wal");
        let mut wal = WAL::new(path).unwrap();

        let large_value = vec![b'x'; 1024 * 1024]; // 1MB value
        wal.append(Operation::Put, b"large_key", Some(&large_value))
            .unwrap();

        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), 1);

        match &entries[0] {
            (Operation::Put, k, Some(v)) => {
                assert_eq!(k, b"large_key");
                assert_eq!(v, &large_value);
            }
            _ => panic!("Expected Put operation with large value"),
        }
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
        let entries = wal.replay().unwrap();

        // Only the first, complete entry is recovered
        assert_eq!(entries.len(), 1);
        match &entries[0] {
            (Operation::Put, k, Some(v)) => {
                assert_eq!(k, b"key1");
                assert_eq!(v, b"value1");
            }
            _ => panic!("Expected Put operation"),
        }
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
        assert_eq!(wal.replay().unwrap().len(), 20);
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
