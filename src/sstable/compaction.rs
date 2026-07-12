use super::SSTable;
use crate::{Key, Value};
use std::collections::BTreeMap;
use std::io;

/// Decides when a level needs compaction and merges its SSTables.
pub struct CompactionManager {
    level_multiplier: u32,
    size_threshold: usize,
    level0_file_limit: usize,
}

impl CompactionManager {
    pub fn new(level_multiplier: u32, size_threshold: usize, level0_file_limit: usize) -> Self {
        CompactionManager {
            level_multiplier,
            size_threshold,
            level0_file_limit,
        }
    }

    pub fn should_compact(&self, level: usize, tables: &[SSTable]) -> bool {
        // Level 0 is special - compact based on file count
        if level == 0 {
            return tables.len() >= self.level0_file_limit;
        }

        // For other levels, use size-based threshold with multiplier
        let level_size: usize = tables.iter().map(|t| t.size()).sum();
        let level_threshold =
            self.size_threshold * (self.level_multiplier as usize).pow(level as u32);
        level_size >= level_threshold
    }

    /// Merge the given SSTables into a single sorted entry list.
    ///
    /// `tables` must be ordered from oldest to newest (the order in which
    /// they were created): when the same key appears in several tables, the
    /// newest value wins.
    ///
    /// Tombstones are kept so that deletions keep shadowing older values,
    /// unless `drop_tombstones` is set — which is only safe when no SSTable
    /// below the compaction output level could still contain the key.
    pub fn compact(
        &self,
        tables: &[SSTable],
        drop_tombstones: bool,
    ) -> io::Result<Vec<(Key, Option<Value>)>> {
        let mut merged_data: BTreeMap<Key, Option<Value>> = BTreeMap::new();

        // Later (newer) tables overwrite earlier (older) ones
        for table in tables {
            for (key, value) in table.read()? {
                merged_data.insert(key, value);
            }
        }

        Ok(merged_data
            .into_iter()
            .filter(|(_, value)| !(drop_tombstones && value.is_none()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write_table(dir: &TempDir, name: &str, data: &[(Key, Option<Value>)]) -> SSTable {
        let mut table = SSTable::new(dir.path().join(name)).unwrap();
        table.write(data).unwrap();
        table
    }

    #[test]
    fn test_compact_newest_value_wins() {
        let temp_dir = TempDir::new().unwrap();
        let old = write_table(
            &temp_dir,
            "old.sst",
            &[(b"key".to_vec(), Some(b"old_value".to_vec()))],
        );
        let new = write_table(
            &temp_dir,
            "new.sst",
            &[(b"key".to_vec(), Some(b"new_value".to_vec()))],
        );

        let manager = CompactionManager::new(4, 1024, 4);
        let merged = manager.compact(&[old, new], false).unwrap();

        assert_eq!(merged, vec![(b"key".to_vec(), Some(b"new_value".to_vec()))]);
    }

    #[test]
    fn test_compact_keeps_tombstones() {
        let temp_dir = TempDir::new().unwrap();
        let old = write_table(
            &temp_dir,
            "old.sst",
            &[(b"key".to_vec(), Some(b"value".to_vec()))],
        );
        let new = write_table(&temp_dir, "new.sst", &[(b"key".to_vec(), None)]);

        let manager = CompactionManager::new(4, 1024, 4);

        // With older data possibly below, the tombstone must survive
        let merged = manager.compact(&[old, new], false).unwrap();
        assert_eq!(merged, vec![(b"key".to_vec(), None)]);
    }

    #[test]
    fn test_compact_drops_tombstones_at_last_level() {
        let temp_dir = TempDir::new().unwrap();
        let old = write_table(
            &temp_dir,
            "old.sst",
            &[
                (b"deleted".to_vec(), Some(b"value".to_vec())),
                (b"kept".to_vec(), Some(b"value".to_vec())),
            ],
        );
        let new = write_table(&temp_dir, "new.sst", &[(b"deleted".to_vec(), None)]);

        let manager = CompactionManager::new(4, 1024, 4);
        let merged = manager.compact(&[old, new], true).unwrap();

        assert_eq!(merged, vec![(b"kept".to_vec(), Some(b"value".to_vec()))]);
    }
}
