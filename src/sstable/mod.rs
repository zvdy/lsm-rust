use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::PathBuf;
use crate::{Key, Value};

mod compaction;
pub use compaction::CompactionManager;

pub struct SSTable {
    path: PathBuf,
    size: usize,
}

impl SSTable {
    pub fn new(path: PathBuf) -> io::Result<Self> {
        let size = if path.exists() {
            fs::metadata(&path)?.len() as usize
        } else {
            0
        };
        
        Ok(SSTable {
            path,
            size,
        })
    }

    pub fn write(&mut self, data: &[(Key, Value)]) -> io::Result<()> {
        let mut file = File::create(&self.path)?;
        let mut size = 0;

        // Write format: [key_size][key][value_size][value]
        for (key, value) in data {
            // Write key size and key
            file.write_all(&(key.len() as u32).to_le_bytes())?;
            file.write_all(key)?;
            
            // Write value size and value
            file.write_all(&(value.len() as u32).to_le_bytes())?;
            file.write_all(value)?;

            size += key.len() + value.len() + 8; // 8 bytes for sizes
        }

        self.size = size;
        Ok(())
    }

    pub fn read(&self) -> io::Result<Vec<(Key, Value)>> {
        let mut file = File::open(&self.path)?;
        let mut data = Vec::new();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        
        let mut pos = 0;
        while pos < buffer.len() {
            // Read key
            let key_size = u32::from_le_bytes(buffer[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let key = buffer[pos..pos + key_size].to_vec();
            pos += key_size;

            // Read value
            let value_size = u32::from_le_bytes(buffer[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let value = buffer[pos..pos + value_size].to_vec();
            pos += value_size;

            data.push((key, value));
        }

        Ok(data)
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

    fn create_test_data() -> Vec<(Key, Value)> {
        vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
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
        let test_data = vec![(b"large_key".to_vec(), large_value.clone())];
        
        table.write(&test_data).unwrap();
        let read_data = table.read().unwrap();
        
        assert_eq!(read_data[0].1, large_value);
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
        table.write(&[(b"key".to_vec(), b"value".to_vec())]).unwrap();
        
        assert!(path_clone.exists());
        table.delete().unwrap();
        assert!(!path_clone.exists());
    }
} 