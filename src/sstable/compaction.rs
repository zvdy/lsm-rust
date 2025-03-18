use super::SSTable;
use crate::{Key, Value};
use std::collections::BTreeMap;
use std::io;

pub struct CompactionManager {
    level_multiplier: u32,
    size_threshold: usize,
}

impl CompactionManager {
    pub fn new(level_multiplier: u32, size_threshold: usize) -> Self {
        CompactionManager {
            level_multiplier,
            size_threshold,
        }
    }

    pub fn should_compact(&self, level: usize, tables: &[SSTable]) -> bool {
        // Get total size of all SSTables at this level
        let level_size: usize = tables.iter().map(|t| t.size()).sum();

        // Level 0 is special - compact when we have more than 4 files
        if level == 0 {
            return tables.len() >= 4;
        }

        // For other levels, use size-based threshold with multiplier
        let level_threshold =
            self.size_threshold * (self.level_multiplier as usize).pow(level as u32);
        println!(
            "Level {} size: {} bytes, threshold: {} bytes",
            level, level_size, level_threshold
        );
        level_size >= level_threshold
    }

    pub fn compact(&self, tables: &[SSTable]) -> io::Result<SSTable> {
        println!("Compacting {} tables", tables.len());
        let mut merged_data: BTreeMap<Key, Value> = BTreeMap::new();

        // Merge all SSTables, newer entries override older ones
        for table in tables.iter().rev() {
            for (key, value) in table.read()? {
                if !merged_data.contains_key(&key) {
                    merged_data.insert(key, value);
                }
            }
        }

        println!("Merged {} unique keys", merged_data.len());

        // Create a new SSTable with merged data
        let mut new_table = SSTable::new(tables[0].get_path().with_file_name(
            format!("compact_{}.sst", 
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        ),
        ))?;

        // Write merged data to new SSTable
        let entries: Vec<_> = merged_data.into_iter().collect();
        new_table.write(&entries)?;

        println!("Created new SSTable of size {} bytes", new_table.size());
        Ok(new_table)
    }
}
