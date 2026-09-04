//! Streaming range scans.
//!
//! [`ScanIter`] merges the memtable and every SSTable lazily, pulling one data
//! block at a time and yielding matches in key order as the caller consumes
//! them. A scan over a wide range therefore costs memory proportional to the
//! number of sources — not to the size of the range.

use super::Snapshot;
use crate::memtable::MemTable;
use crate::sstable::{RangeCursor, SSTable, VersionedEntry};
use crate::{Expiry, Key, Seq, Value, Version};
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// One stream of `(key ascending, seq descending)` versions feeding the merge.
enum Source<'a> {
    /// The in-memory table; already sorted, and never fails.
    Mem(Box<dyn Iterator<Item = VersionedEntry> + 'a>),
    /// One on-disk table, read block by block.
    Table(RangeCursor<'a>),
}

impl Source<'_> {
    fn next_entry(&mut self) -> Option<crate::Result<VersionedEntry>> {
        match self {
            Source::Mem(iter) => iter.next().map(Ok),
            Source::Table(cursor) => cursor.next(),
        }
    }
}

/// The head entry of one source, ordered for the merge heap.
struct HeapItem {
    key: Key,
    seq: Seq,
    version: Version,
    source: usize,
}

// `BinaryHeap` is a max-heap, so "greater" means "popped first": we want the
// smallest key first and, among equal keys, the newest version first.
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .key
            .cmp(&self.key)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq == other.seq
    }
}
impl Eq for HeapItem {}

/// A lazy, ordered range scan over the whole store.
///
/// Yields live `(key, value)` pairs in ascending key order, showing for each
/// key the newest version visible to the scan's snapshot. Deleted keys are
/// skipped. Each item is a [`Result`](crate::Result) because blocks are read
/// from disk as the scan advances; the first error ends the iteration.
pub struct ScanIter<'a> {
    sources: Vec<Source<'a>>,
    heap: BinaryHeap<HeapItem>,
    snapshot_seq: Seq,
    /// Wall-clock time the scan reads at, sampled once when it is created so
    /// that a long traversal sees one consistent instant rather than letting
    /// keys wink out midway through.
    now: Expiry,
    failed: bool,
}

impl<'a> ScanIter<'a> {
    pub(super) fn new(
        memtable: &'a MemTable,
        sstables: impl Iterator<Item = &'a SSTable>,
        start: &[u8],
        end: Option<&[u8]>,
        snapshot_seq: Seq,
    ) -> crate::Result<Self> {
        let mut sources: Vec<Source<'a>> = Vec::new();
        sources.push(Source::Mem(Box::new(
            memtable
                .range(start, end)
                .map(|(k, seq, v)| (k.clone(), seq, v.clone())),
        )));
        for table in sstables {
            sources.push(Source::Table(table.range_cursor(start, end)?));
        }

        let mut scan = ScanIter {
            sources,
            heap: BinaryHeap::new(),
            snapshot_seq,
            now: crate::version::now_ms(),
            failed: false,
        };
        // Prime the heap with the head of every source.
        for index in 0..scan.sources.len() {
            scan.refill(index)?;
        }
        Ok(scan)
    }

    /// Pull the next entry from `index` into the heap, if it has one.
    fn refill(&mut self, index: usize) -> crate::Result<()> {
        if let Some(entry) = self.sources[index].next_entry() {
            let (key, seq, version) = entry?;
            self.heap.push(HeapItem {
                key,
                seq,
                version,
                source: index,
            });
        }
        Ok(())
    }
}

impl Iterator for ScanIter<'_> {
    type Item = crate::Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            let item = self.heap.pop()?;
            if let Err(e) = self.refill(item.source) {
                self.failed = true;
                return Some(Err(e));
            }

            // All versions of a key arrive contiguously, newest first, so the
            // first one visible to the snapshot is the answer — a tombstone
            // included, which shadows everything older.
            let key = item.key;
            let mut chosen = (item.seq <= self.snapshot_seq).then_some(item.version);

            while self.heap.peek().is_some_and(|next| next.key == key) {
                let next = self.heap.pop().expect("peeked");
                if let Err(e) = self.refill(next.source) {
                    self.failed = true;
                    return Some(Err(e));
                }
                if chosen.is_none() && next.seq <= self.snapshot_seq {
                    chosen = Some(next.version);
                }
            }

            // An expired version reads as absent here just as a tombstone
            // does — and, like a tombstone, it has already shadowed every
            // older version of the key, so the key is skipped rather than
            // falling through to what it replaced.
            match chosen.as_ref().and_then(|v| v.visible_at(self.now)) {
                Some(value) => return Some(Ok((key, value.clone()))),
                None => continue,
            }
        }
    }
}

/// A scan bound to a [`Snapshot`], kept alive for the life of the iteration.
pub struct SnapshotScan<'a> {
    pub(super) inner: ScanIter<'a>,
    /// Held so the versions the scan needs are pinned against compaction.
    pub(super) _snapshot: &'a Snapshot,
}

impl Iterator for SnapshotScan<'_> {
    type Item = crate::Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
