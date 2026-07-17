use super::{SSTable, VersionedEntry};
use crate::{Key, Seq};
use std::cmp::Reverse;
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

    /// Merge the given SSTables into a single sorted, version-aware entry list.
    ///
    /// `tables` must be ordered from oldest to newest (creation order); when
    /// the same `(key, seq)` appears in several tables — which only happens
    /// for legacy sequence-0 entries — the newest table wins.
    ///
    /// Multi-version garbage collection is driven by `gc_floor`, the oldest
    /// sequence number any live snapshot can read (`Seq::MAX` when there are
    /// no live snapshots). For each key:
    /// - every version with `seq > gc_floor` is kept (some snapshot may need it);
    /// - the newest version with `seq <= gc_floor` is kept (it is what the
    ///   oldest snapshot, and the latest read, see);
    /// - older versions below that floor version are dropped.
    ///
    /// Tombstones are only discarded when `drop_tombstones` is set (no data
    /// below the output level) *and* there are no live snapshots — otherwise a
    /// snapshot could still need to observe the deletion.
    pub fn compact(
        &self,
        tables: &[SSTable],
        drop_tombstones: bool,
        gc_floor: Seq,
    ) -> io::Result<Vec<VersionedEntry>> {
        // Sorted by (key asc, seq desc). Inserting oldest table first means a
        // newer table's value wins for a colliding (key, seq) legacy entry.
        let mut merged: BTreeMap<(Key, Reverse<Seq>), Option<Vec<u8>>> = BTreeMap::new();
        for table in tables {
            for (key, seq, value) in table.read_versioned()? {
                merged.insert((key, Reverse(seq)), value);
            }
        }

        let can_drop_tombstones = drop_tombstones && gc_floor == Seq::MAX;

        let mut out: Vec<VersionedEntry> = Vec::new();
        let mut current_key: Option<Key> = None;
        let mut kept_floor_version = false;
        for ((key, Reverse(seq)), value) in merged {
            // Reset per-key bookkeeping when the key changes
            if current_key.as_ref() != Some(&key) {
                current_key = Some(key.clone());
                kept_floor_version = false;
            }

            if seq > gc_floor {
                out.push((key, seq, value));
                continue;
            }

            if kept_floor_version {
                // An older version shadowed by the floor version: drop it
                continue;
            }
            kept_floor_version = true;

            if can_drop_tombstones && value.is_none() {
                // Safe to garbage-collect the deletion entirely
                continue;
            }
            out.push((key, seq, value));
        }

        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Value;
    use tempfile::TempDir;

    fn write_table(dir: &TempDir, name: &str, data: &[(Key, Option<Value>)]) -> SSTable {
        let mut table = SSTable::new(dir.path().join(name)).unwrap();
        table.write(data).unwrap();
        table
    }

    fn write_versioned_table(dir: &TempDir, name: &str, data: &[VersionedEntry]) -> SSTable {
        let mut table = SSTable::new(dir.path().join(name)).unwrap();
        table
            .write_versioned(data, super::super::Compression::None)
            .unwrap();
        table
    }

    #[test]
    fn test_compact_newest_value_wins() {
        let temp_dir = TempDir::new().unwrap();
        // Same key at different sequences across two tables
        let old = write_versioned_table(
            &temp_dir,
            "old.sst",
            &[(b"key".to_vec(), 1, Some(b"old_value".to_vec()))],
        );
        let new = write_versioned_table(
            &temp_dir,
            "new.sst",
            &[(b"key".to_vec(), 2, Some(b"new_value".to_vec()))],
        );

        let manager = CompactionManager::new(4, 1024, 4);
        // No snapshots: collapse to the newest version
        let merged = manager.compact(&[old, new], false, Seq::MAX).unwrap();

        assert_eq!(
            merged,
            vec![(b"key".to_vec(), 2, Some(b"new_value".to_vec()))]
        );
    }

    #[test]
    fn test_compact_keeps_tombstones() {
        let temp_dir = TempDir::new().unwrap();
        let old = write_versioned_table(
            &temp_dir,
            "old.sst",
            &[(b"key".to_vec(), 1, Some(b"value".to_vec()))],
        );
        let new = write_versioned_table(&temp_dir, "new.sst", &[(b"key".to_vec(), 2, None)]);

        let manager = CompactionManager::new(4, 1024, 4);

        // With older data possibly below (drop_tombstones = false), keep it
        let merged = manager.compact(&[old, new], false, Seq::MAX).unwrap();
        assert_eq!(merged, vec![(b"key".to_vec(), 2, None)]);
    }

    #[test]
    fn test_compact_drops_tombstones_at_last_level() {
        let temp_dir = TempDir::new().unwrap();
        let old = write_versioned_table(
            &temp_dir,
            "old.sst",
            &[
                (b"deleted".to_vec(), 1, Some(b"value".to_vec())),
                (b"kept".to_vec(), 1, Some(b"value".to_vec())),
            ],
        );
        let new = write_versioned_table(&temp_dir, "new.sst", &[(b"deleted".to_vec(), 2, None)]);

        let manager = CompactionManager::new(4, 1024, 4);
        // Last level, no snapshots: tombstone is garbage-collected
        let merged = manager.compact(&[old, new], true, Seq::MAX).unwrap();

        assert_eq!(merged, vec![(b"kept".to_vec(), 1, Some(b"value".to_vec()))]);
    }

    #[test]
    fn test_compact_retains_versions_needed_by_snapshots() {
        let temp_dir = TempDir::new().unwrap();
        let table = write_versioned_table(
            &temp_dir,
            "t.sst",
            &[
                (b"k".to_vec(), 9, Some(b"v9".to_vec())),
                (b"k".to_vec(), 5, Some(b"v5".to_vec())),
                (b"k".to_vec(), 1, Some(b"v1".to_vec())),
            ],
        );
        let manager = CompactionManager::new(4, 1024, 4);

        // A snapshot at seq 5 is live (gc_floor = 5): keep everything with
        // seq > 5 plus the newest version <= 5. v1 is shadowed by v5 and dropped.
        let merged = manager.compact(&[table], false, 5).unwrap();
        assert_eq!(
            merged,
            vec![
                (b"k".to_vec(), 9, Some(b"v9".to_vec())),
                (b"k".to_vec(), 5, Some(b"v5".to_vec())),
            ]
        );
    }

    #[test]
    fn test_compact_collapses_to_latest_without_snapshots() {
        let temp_dir = TempDir::new().unwrap();
        let table = write_versioned_table(
            &temp_dir,
            "t.sst",
            &[
                (b"k".to_vec(), 9, Some(b"v9".to_vec())),
                (b"k".to_vec(), 5, Some(b"v5".to_vec())),
                (b"k".to_vec(), 1, Some(b"v1".to_vec())),
            ],
        );
        let manager = CompactionManager::new(4, 1024, 4);

        // No snapshots: only the newest version survives
        let merged = manager.compact(&[table], false, Seq::MAX).unwrap();
        assert_eq!(merged, vec![(b"k".to_vec(), 9, Some(b"v9".to_vec()))]);
    }

    #[test]
    fn test_compact_legacy_seq0_collision_newest_table_wins() {
        let temp_dir = TempDir::new().unwrap();
        // Two legacy tables both hold key@0 with different values
        let old = write_table(
            &temp_dir,
            "old.sst",
            &[(b"key".to_vec(), Some(b"old".to_vec()))],
        );
        let new = write_table(
            &temp_dir,
            "new.sst",
            &[(b"key".to_vec(), Some(b"new".to_vec()))],
        );

        let manager = CompactionManager::new(4, 1024, 4);
        let merged = manager.compact(&[old, new], false, Seq::MAX).unwrap();
        assert_eq!(merged, vec![(b"key".to_vec(), 0, Some(b"new".to_vec()))]);
    }
}
