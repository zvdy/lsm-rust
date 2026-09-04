//! Operational metrics for a [`Storage`](super::Storage) instance.
//!
//! [`Metrics`] holds the cumulative counters bumped on the hot path; they use
//! relaxed atomics so both read (`&self`) and write (`&mut self`) code paths
//! can update them without extra locking. [`StorageStats`] is a point-in-time
//! snapshot — the counters plus a set of gauges describing the store's current
//! shape — and knows how to render itself in the Prometheus text exposition
//! format via [`StorageStats::to_prometheus`].

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative, monotonic operation counters for a store.
///
/// Updated in place on the read and write paths; snapshotted into a
/// [`StorageStats`] by [`Storage::stats`](super::Storage::stats).
#[derive(Debug, Default)]
pub(super) struct Metrics {
    pub puts: AtomicU64,
    pub deletes: AtomicU64,
    pub batches: AtomicU64,
    pub gets: AtomicU64,
    pub scans: AtomicU64,
    pub scan_tables_pruned: AtomicU64,
    pub flushes: AtomicU64,
    pub compactions: AtomicU64,
    pub compaction_moves: AtomicU64,
    pub expired: AtomicU64,
    pub checkpoints: AtomicU64,
}

impl Metrics {
    /// Increment a counter on the hot path. Relaxed ordering is sufficient:
    /// counters are independent and only ever read as an approximate snapshot.
    #[inline]
    pub(super) fn incr(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// Per-level SSTable counts and on-disk sizes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LevelStats {
    /// The LSM level (0 is the newest, freshly flushed level).
    pub level: usize,
    /// Number of SSTable files at this level.
    pub num_sstables: u64,
    /// Total size in bytes of the SSTables at this level.
    pub bytes: u64,
}

/// A point-in-time snapshot of a store's operational metrics.
///
/// The `*_total` fields are cumulative counters that only ever increase over
/// the life of the process; the remaining fields are gauges describing the
/// store's current shape. Obtain one with
/// [`Storage::stats`](super::Storage::stats) or
/// [`SharedStorage::stats`](super::SharedStorage::stats), and render it for a
/// Prometheus scrape with [`StorageStats::to_prometheus`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageStats {
    /// Total put operations applied.
    pub puts_total: u64,
    /// Total delete operations applied.
    pub deletes_total: u64,
    /// Total write batches applied.
    pub batches_total: u64,
    /// Total point lookups served.
    pub gets_total: u64,
    /// Total range/prefix scans served.
    pub scans_total: u64,
    /// Total SSTables a scan skipped because their key range could not
    /// intersect the requested one.
    pub scan_tables_pruned_total: u64,
    /// Total memtable flushes to level-0 SSTables.
    pub flushes_total: u64,
    /// Total compaction runs across all levels.
    pub compactions_total: u64,
    /// How many of those runs promoted a level of mutually disjoint tables
    /// to the next level instead of rewriting it. A subset of
    /// `compactions_total`; the difference is the runs that really merged.
    pub compaction_moves_total: u64,
    /// Total checkpoints taken.
    pub checkpoints_total: u64,
    /// Total expired versions collected by compaction — the point at which
    /// data past its deadline stops being merely hidden and is reclaimed.
    pub expired_total: u64,
    /// Highest MVCC sequence number assigned so far.
    pub sequence: u64,
    /// Number of live snapshots currently pinning versions.
    pub live_snapshots: u64,
    /// Approximate size in bytes of the active memtable.
    pub memtable_bytes: u64,
    /// Number of versioned entries in the active memtable.
    pub memtable_entries: u64,
    /// Per-level SSTable statistics, sorted by level ascending.
    pub levels: Vec<LevelStats>,
}

impl StorageStats {
    /// Total number of SSTable files across every level.
    pub fn total_sstables(&self) -> u64 {
        self.levels.iter().map(|l| l.num_sstables).sum()
    }

    /// Total on-disk size in bytes of every SSTable across every level.
    pub fn total_sstable_bytes(&self) -> u64 {
        self.levels.iter().map(|l| l.bytes).sum()
    }

    /// Render these stats in the Prometheus text exposition format (v0.0.4),
    /// suitable for serving from a `/metrics` endpoint.
    ///
    /// Every metric is namespaced with the `lsm_` prefix. Per-level series
    /// carry a `level` label.
    pub fn to_prometheus(&self) -> String {
        let mut out = String::new();

        let counter = |out: &mut String, name: &str, help: &str, value: u64| {
            let _ = writeln!(out, "# HELP lsm_{name} {help}");
            let _ = writeln!(out, "# TYPE lsm_{name} counter");
            let _ = writeln!(out, "lsm_{name} {value}");
        };
        let gauge = |out: &mut String, name: &str, help: &str, value: u64| {
            let _ = writeln!(out, "# HELP lsm_{name} {help}");
            let _ = writeln!(out, "# TYPE lsm_{name} gauge");
            let _ = writeln!(out, "lsm_{name} {value}");
        };

        counter(
            &mut out,
            "puts_total",
            "Total put operations applied.",
            self.puts_total,
        );
        counter(
            &mut out,
            "deletes_total",
            "Total delete operations applied.",
            self.deletes_total,
        );
        counter(
            &mut out,
            "batches_total",
            "Total write batches applied.",
            self.batches_total,
        );
        counter(
            &mut out,
            "gets_total",
            "Total point lookups served.",
            self.gets_total,
        );
        counter(
            &mut out,
            "scans_total",
            "Total range or prefix scans served.",
            self.scans_total,
        );
        counter(
            &mut out,
            "scan_tables_pruned_total",
            "SSTables skipped by scans whose key range could not match.",
            self.scan_tables_pruned_total,
        );
        counter(
            &mut out,
            "flushes_total",
            "Total memtable flushes to level-0 SSTables.",
            self.flushes_total,
        );
        counter(
            &mut out,
            "compactions_total",
            "Total compaction runs across all levels.",
            self.compactions_total,
        );
        counter(
            &mut out,
            "compaction_moves_total",
            "Compaction runs that promoted disjoint tables instead of rewriting them.",
            self.compaction_moves_total,
        );
        counter(
            &mut out,
            "expired_total",
            "Expired versions collected by compaction.",
            self.expired_total,
        );
        counter(
            &mut out,
            "checkpoints_total",
            "Total checkpoints taken.",
            self.checkpoints_total,
        );

        gauge(
            &mut out,
            "sequence",
            "Highest MVCC sequence number assigned so far.",
            self.sequence,
        );
        gauge(
            &mut out,
            "live_snapshots",
            "Number of live snapshots currently pinning versions.",
            self.live_snapshots,
        );
        gauge(
            &mut out,
            "memtable_bytes",
            "Approximate size in bytes of the active memtable.",
            self.memtable_bytes,
        );
        gauge(
            &mut out,
            "memtable_entries",
            "Number of versioned entries in the active memtable.",
            self.memtable_entries,
        );

        // Per-level gauges carry a `level` label.
        let _ = writeln!(
            out,
            "# HELP lsm_sstables Number of SSTable files per level."
        );
        let _ = writeln!(out, "# TYPE lsm_sstables gauge");
        for l in &self.levels {
            let _ = writeln!(
                out,
                "lsm_sstables{{level=\"{}\"}} {}",
                l.level, l.num_sstables
            );
        }
        let _ = writeln!(
            out,
            "# HELP lsm_sstable_bytes On-disk size in bytes of SSTables per level."
        );
        let _ = writeln!(out, "# TYPE lsm_sstable_bytes gauge");
        for l in &self.levels {
            let _ = writeln!(
                out,
                "lsm_sstable_bytes{{level=\"{}\"}} {}",
                l.level, l.bytes
            );
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_helpers_sum_levels() {
        let stats = StorageStats {
            levels: vec![
                LevelStats {
                    level: 0,
                    num_sstables: 3,
                    bytes: 100,
                },
                LevelStats {
                    level: 1,
                    num_sstables: 1,
                    bytes: 400,
                },
            ],
            ..StorageStats::default()
        };
        assert_eq!(stats.total_sstables(), 4);
        assert_eq!(stats.total_sstable_bytes(), 500);
    }

    #[test]
    fn prometheus_output_is_well_formed() {
        let stats = StorageStats {
            puts_total: 10,
            gets_total: 25,
            sequence: 11,
            memtable_bytes: 2048,
            levels: vec![LevelStats {
                level: 0,
                num_sstables: 2,
                bytes: 4096,
            }],
            ..StorageStats::default()
        };
        let text = stats.to_prometheus();

        // Counters and gauges are declared with matching TYPE lines
        assert!(text.contains("# TYPE lsm_puts_total counter"));
        assert!(text.contains("\nlsm_puts_total 10\n"));
        assert!(text.contains("# TYPE lsm_gets_total counter"));
        assert!(text.contains("\nlsm_gets_total 25\n"));
        assert!(text.contains("# TYPE lsm_sequence gauge"));
        assert!(text.contains("\nlsm_sequence 11\n"));
        assert!(text.contains("\nlsm_memtable_bytes 2048\n"));

        // Per-level series carry a level label
        assert!(text.contains("lsm_sstables{level=\"0\"} 2"));
        assert!(text.contains("lsm_sstable_bytes{level=\"0\"} 4096"));

        // Every non-comment, non-blank line must be `name value`
        for line in text.lines() {
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let parts: Vec<&str> = line.rsplitn(2, ' ').collect();
            assert_eq!(parts.len(), 2, "malformed sample line: {line:?}");
            assert!(
                parts[0].parse::<u64>().is_ok(),
                "non-numeric value in: {line:?}"
            );
        }
    }

    #[test]
    fn empty_stats_render_without_level_series() {
        let text = StorageStats::default().to_prometheus();
        assert!(text.contains("lsm_puts_total 0"));
        // No level rows, but the HELP/TYPE headers are still present
        assert!(text.contains("# TYPE lsm_sstables gauge"));
        assert!(!text.contains("lsm_sstables{"));
    }
}
