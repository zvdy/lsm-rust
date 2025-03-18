use crate::{Key, Value};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::PathBuf;

pub enum Operation {
    Put,
    Delete,
}

pub struct WAL {
    path: PathBuf,
    file: File,
}

impl WAL {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        Ok(WAL { path, file })
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
        Ok(())
    }

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
            let key_size = u32::from_le_bytes(buffer[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let key = buffer[pos..pos + key_size].to_vec();
            pos += key_size;

            // Read value if present
            let value = if matches!(op, Operation::Put) {
                let value_size =
                    u32::from_le_bytes(buffer[pos..pos + 4].try_into().unwrap()) as usize;
                pos += 4;
                let value = buffer[pos..pos + value_size].to_vec();
                pos += value_size;
                Some(value)
            } else {
                None
            };

            entries.push((op, key, value));
        }

        Ok(entries)
    }

    pub fn clear(&mut self) -> io::Result<()> {
        self.file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .read(true)
            .open(&self.path)?;
        Ok(())
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
                Operation::Put => wal.append(Operation::Put, &key, value.as_deref()).unwrap(),
                Operation::Delete => wal.append(Operation::Delete, &key, None).unwrap(),
            }
        }

        // Replay and verify
        let entries = wal.replay().unwrap();
        assert_eq!(entries.len(), operations.len());

        for (i, (op, key, value)) in operations.iter().enumerate() {
            match (&entries[i].0, &entries[i].1, &entries[i].2) {
                (replay_op, replay_key, replay_value) => {
                    assert!(matches!(op, Operation::Put) == matches!(replay_op, Operation::Put));
                    assert_eq!(replay_key, key);
                    assert_eq!(replay_value, value);
                }
            }
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
}
