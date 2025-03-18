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