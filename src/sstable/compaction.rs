use super::{SSTable, VersionedEntry};
use crate::{Key, Seq};
use std::cmp::Reverse;
use std::collections::BTreeMap;

/// The most tables a level may accumulate through promotion before a real
/// merge is forced to consolidate them.
///
/// Promoted tables are disjoint, so a lookup is filtered by their Bloom
/// filters rather than reading them — but it still *checks* every filter in
/// the level. Without a ceiling an append-only workload would promote for
/// ever and grow that per-lookup cost without bound.
pub const MAX_TABLES_PER_LEVEL: usize = 16;

/// What compaction should do with a level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionPlan {
    /// Read the tables, merge them, and write one table at the next level.
    Merge,
    /// Reinterpret the tables one level down without reading them: no key
    /// appears in more than one, so a merge would rewrite every byte to
    /// produce the same entries.
    Promote,
}

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

    /// Choose between rewriting a level and promoting it, given how many
    /// tables the destination level already holds.
    ///
    /// [`CompactionPlan::Promote`] needs all three of:
    ///
    /// - **No overlap.** With every key in exactly one table there is nothing
    ///   for a merge to collapse, so rewriting is pure cost.
    /// - **More than one table.** A lone table is merged with itself, which is
    ///   not busywork: that is where several versions of a key collapse and
    ///   where tombstones are finally dropped. Promotion would skip both, so
    ///   this case keeps its existing behaviour.
    /// - **Room at the destination.** See [`MAX_TABLES_PER_LEVEL`].
    ///
    /// Anything else merges, so the fallback is always today's behaviour.
    pub fn plan(&self, tables: &[SSTable], destination_tables: usize) -> CompactionPlan {
        if tables.len() < 2 {
            return CompactionPlan::Merge;
        }
        if destination_tables + tables.len() > MAX_TABLES_PER_LEVEL {
            return CompactionPlan::Merge;
        }
        if Self::max_overlap_depth(tables) > 1 {
            return CompactionPlan::Merge;
        }
        CompactionPlan::Promote
    }

    /// The largest number of tables whose key ranges cover any single point.
    ///
    /// This is the cost signal compaction lacks otherwise. A level whose
    /// tables are mutually disjoint has depth 1: merging them would read and
    /// rewrite every byte to produce the same entries in a different file,
    /// because no key appears in more than one table and so nothing can be
    /// deduplicated. Depth 2 or more means keys really are shadowed across
    /// tables, and a merge collapses them.
    ///
    /// Ranges are inclusive, so tables that merely touch at a key count as
    /// overlapping. A table whose range cannot be determined makes the result
    /// `tables.len()` — unknown is treated as maximal overlap, so an
    /// unreadable range can only ever cause more merging, never less.
    pub fn max_overlap_depth(tables: &[SSTable]) -> usize {
        if tables.len() < 2 {
            return tables.len();
        }

        // (key, 0 = range opens, 1 = range closes). Sorting puts an opening
        // before a closing at the same key, so touching ranges overlap.
        let mut events: Vec<(&[u8], u8)> = Vec::with_capacity(tables.len() * 2);
        for table in tables {
            match table.key_range() {
                Some((min, max)) => {
                    events.push((min.as_slice(), 0));
                    events.push((max.as_slice(), 1));
                }
                None => return tables.len(),
            }
        }
        events.sort_unstable();

        let mut depth = 0usize;
        let mut max_depth = 0usize;
        for (_, kind) in events {
            if kind == 0 {
                depth += 1;
                max_depth = max_depth.max(depth);
            } else {
                depth -= 1;
            }
        }
        max_depth
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
    ) -> crate::Result<Vec<VersionedEntry>> {
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

    /// A table spanning `keys`, written in ascending order.
    fn table_spanning(dir: &TempDir, name: &str, keys: &[&str]) -> SSTable {
        let data: Vec<VersionedEntry> = keys
            .iter()
            .map(|k| (k.as_bytes().to_vec(), 1, Some(b"v".to_vec())))
            .collect();
        write_versioned_table(dir, name, &data)
    }

    #[test]
    fn key_range_spans_the_first_and_last_key() {
        let dir = TempDir::new().unwrap();
        // Enough keys to span more than one block, so the maximum genuinely
        // comes from reading the final block rather than the index.
        let keys: Vec<String> = (0..500).map(|i| format!("key{:04}", i)).collect();
        let refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
        let table = table_spanning(&dir, "t.sst", &refs);

        let (min, max) = table.key_range().expect("range").clone();
        assert_eq!(min, b"key0000".to_vec());
        assert_eq!(max, b"key0499".to_vec());
    }

    #[test]
    fn key_range_of_an_empty_table_is_unknown() {
        let dir = TempDir::new().unwrap();
        let table = SSTable::new(dir.path().join("empty.sst")).unwrap();
        assert!(table.key_range().is_none());
    }

    #[test]
    fn overlap_depth_of_disjoint_tables_is_one() {
        let dir = TempDir::new().unwrap();
        let tables = vec![
            table_spanning(&dir, "a.sst", &["a1", "a2"]),
            table_spanning(&dir, "b.sst", &["b1", "b2"]),
            table_spanning(&dir, "c.sst", &["c1", "c2"]),
        ];
        assert_eq!(CompactionManager::max_overlap_depth(&tables), 1);
    }

    #[test]
    fn overlap_depth_counts_tables_covering_a_common_point() {
        let dir = TempDir::new().unwrap();
        // All three span the whole alphabet: any key is covered three times.
        let tables = vec![
            table_spanning(&dir, "a.sst", &["a", "z"]),
            table_spanning(&dir, "b.sst", &["a", "z"]),
            table_spanning(&dir, "c.sst", &["a", "z"]),
        ];
        assert_eq!(CompactionManager::max_overlap_depth(&tables), 3);
    }

    #[test]
    fn overlap_depth_reports_the_deepest_point_not_the_average() {
        let dir = TempDir::new().unwrap();
        // a..m and h..z overlap on h..m; q..z overlaps the second only.
        let tables = vec![
            table_spanning(&dir, "a.sst", &["a", "m"]),
            table_spanning(&dir, "b.sst", &["h", "z"]),
            table_spanning(&dir, "c.sst", &["q", "z"]),
        ];
        assert_eq!(CompactionManager::max_overlap_depth(&tables), 2);
    }

    #[test]
    fn tables_that_only_touch_count_as_overlapping() {
        let dir = TempDir::new().unwrap();
        // Ranges are inclusive, so sharing the endpoint "m" is an overlap:
        // that key really is in both tables and a merge would collapse it.
        let tables = vec![
            table_spanning(&dir, "a.sst", &["a", "m"]),
            table_spanning(&dir, "b.sst", &["m", "z"]),
        ];
        assert_eq!(CompactionManager::max_overlap_depth(&tables), 2);
    }

    #[test]
    fn an_unreadable_range_is_treated_as_maximal_overlap() {
        let dir = TempDir::new().unwrap();
        // An empty table has no range; the level must then be merged rather
        // than promoted, so unknown has to read as "fully overlapping".
        let tables = vec![
            table_spanning(&dir, "a.sst", &["a", "b"]),
            SSTable::new(dir.path().join("empty.sst")).unwrap(),
        ];
        assert_eq!(CompactionManager::max_overlap_depth(&tables), 2);
    }

    #[test]
    fn plan_promotes_only_disjoint_multi_table_levels_with_room() {
        let dir = TempDir::new().unwrap();
        let manager = CompactionManager::new(4, 1024, 4);

        let disjoint = vec![
            table_spanning(&dir, "a.sst", &["a1", "a2"]),
            table_spanning(&dir, "b.sst", &["b1", "b2"]),
        ];
        assert_eq!(manager.plan(&disjoint, 0), CompactionPlan::Promote);

        // Overlapping: a merge actually collapses keys, so it is worth doing.
        let overlapping = vec![
            table_spanning(&dir, "c.sst", &["a", "z"]),
            table_spanning(&dir, "d.sst", &["a", "z"]),
        ];
        assert_eq!(manager.plan(&overlapping, 0), CompactionPlan::Merge);

        // A lone table is merged with itself: that is where versions collapse
        // and tombstones are dropped, which promotion would skip.
        let single = vec![table_spanning(&dir, "e.sst", &["a", "b"])];
        assert_eq!(manager.plan(&single, 0), CompactionPlan::Merge);

        // Disjoint, but the destination is already full enough that promoting
        // would grow per-lookup Bloom checks: consolidate instead.
        assert_eq!(
            manager.plan(&disjoint, MAX_TABLES_PER_LEVEL - 1),
            CompactionPlan::Merge
        );
        assert_eq!(
            manager.plan(&disjoint, MAX_TABLES_PER_LEVEL - 2),
            CompactionPlan::Promote
        );
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
