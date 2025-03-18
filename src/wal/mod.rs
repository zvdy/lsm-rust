use std::fs::{File, OpenOptions};
use std::io::{self, Write, Read, Seek};
use std::path::PathBuf;
use crate::{Key, Value};

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
                _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid operation type")),
            };
            pos += 1;

            // Read key
            let key_size = u32::from_le_bytes(buffer[pos..pos + 4].try_into().unwrap()) as usize;
            pos += 4;
            let key = buffer[pos..pos + key_size].to_vec();
            pos += key_size;

            // Read value if present
            let value = if matches!(op, Operation::Put) {
                let value_size = u32::from_le_bytes(buffer[pos..pos + 4].try_into().unwrap()) as usize;
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