//! Optimistic, concurrent transactions over the MVCC engine.
//!
//! A [`Transaction`] reads from a [`Snapshot`] taken when it began and buffers
//! its writes privately. Nothing it writes is visible to anyone else — or
//! durable — until [`Transaction::commit`] succeeds, at which point the whole
//! write set is applied at a single sequence number as one atomic batch.
//!
//! Concurrency is optimistic: transactions never block each other while they
//! run. Conflicts are detected at commit time by checking the transaction's
//! read and write sets against everything committed since its snapshot. A
//! losing transaction is aborted with [`Error::Conflict`](crate::Error::Conflict),
//! which is retriable — see
//! [`SharedStorage::transaction`](super::SharedStorage::transaction) for a
//! helper that retries for you.

use super::Snapshot;
use crate::{Key, Seq, Value};
use std::collections::{BTreeMap, HashSet, VecDeque};

/// How strictly a transaction checks for conflicts when it commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Isolation {
    /// Detect write-write conflicts only: the transaction aborts if another
    /// transaction wrote any key it also wrote.
    ///
    /// This is classic snapshot isolation. It permits *write skew*, where two
    /// transactions read overlapping data and write disjoint keys based on it,
    /// producing a state neither would have produced alone.
    Snapshot,
    /// Also detect read-write conflicts: the transaction aborts if another
    /// transaction wrote any key it *read*, or wrote a key falling inside a
    /// range it scanned.
    ///
    /// This rules out write skew and phantoms, at the cost of more aborts.
    #[default]
    Serializable,
}

/// A half-open key range a transaction scanned, remembered so that a
/// concurrently inserted key inside it counts as a conflict (a phantom).
type ReadRange = (Key, Option<Key>);

/// A bounded record of recently committed write sets, used to decide whether a
/// transaction's snapshot is still valid at commit time.
///
/// Entries older than the oldest live snapshot can never conflict with any
/// future transaction — no transaction can start before them — so they are
/// pruned, keeping this bounded by the number of in-flight transactions.
#[derive(Default)]
pub(super) struct CommitLog {
    entries: VecDeque<(Seq, HashSet<Key>)>,
}

impl CommitLog {
    /// Record the keys written by the commit at `seq`.
    pub(super) fn record(&mut self, seq: Seq, keys: HashSet<Key>) {
        self.entries.push_back((seq, keys));
    }

    /// Drop entries that no live transaction could still be validated against.
    pub(super) fn prune(&mut self, oldest_live: Seq) {
        while let Some((seq, _)) = self.entries.front() {
            if *seq <= oldest_live {
                self.entries.pop_front();
            } else {
                break;
            }
        }
    }

    /// Number of retained commit records (used by tests and metrics).
    pub(super) fn len(&self) -> usize {
        self.entries.len()
    }

    /// Find a key committed after `snapshot_seq` that conflicts with this
    /// transaction's `writes`, its `reads`, or one of its scanned `ranges`.
    pub(super) fn find_conflict(
        &self,
        snapshot_seq: Seq,
        writes: &BTreeMap<Key, Option<Value>>,
        reads: &HashSet<Key>,
        ranges: &[ReadRange],
        isolation: Isolation,
    ) -> Option<Key> {
        for (seq, committed) in &self.entries {
            if *seq <= snapshot_seq {
                continue; // already visible to this transaction
            }
            for key in committed {
                // Write-write: both transactions wrote the same key.
                if writes.contains_key(key) {
                    return Some(key.clone());
                }
                if isolation == Isolation::Serializable {
                    // Read-write: we read a key someone else then wrote.
                    if reads.contains(key) {
                        return Some(key.clone());
                    }
                    // Phantom: a key appeared inside a range we scanned.
                    if ranges.iter().any(|(start, end)| {
                        key.as_slice() >= start.as_slice()
                            && end.as_ref().is_none_or(|e| key.as_slice() < e.as_slice())
                    }) {
                        return Some(key.clone());
                    }
                }
            }
        }
        None
    }
}

/// An in-progress transaction.
///
/// Reads observe the snapshot the transaction began at, overlaid with its own
/// uncommitted writes (read-your-own-writes). Dropping a transaction without
/// committing rolls it back — buffered writes are simply discarded.
///
/// # Example
///
/// ```no_run
/// use lsm_rust::SharedStorage;
///
/// fn main() -> lsm_rust::Result<()> {
///     let db = SharedStorage::new("./data", false)?;
///
///     let mut tx = db.begin()?;
///     let balance = tx.get(&b"account".to_vec())?;
///     tx.put(b"account".to_vec(), b"updated".to_vec());
///     match tx.commit() {
///         Ok(_seq) => println!("committed"),
///         Err(e) if e.is_retriable() => println!("conflict; retry"),
///         Err(e) => return Err(e.into()),
///     }
///     let _ = balance;
///     Ok(())
/// }
/// ```
pub struct Transaction {
    db: super::SharedStorage,
    snapshot: Snapshot,
    /// Buffered writes; `None` is a delete.
    writes: BTreeMap<Key, Option<Value>>,
    reads: HashSet<Key>,
    ranges: Vec<ReadRange>,
    isolation: Isolation,
}

impl Transaction {
    pub(super) fn new(db: super::SharedStorage, snapshot: Snapshot, isolation: Isolation) -> Self {
        Transaction {
            db,
            snapshot,
            writes: BTreeMap::new(),
            reads: HashSet::new(),
            ranges: Vec::new(),
            isolation,
        }
    }

    /// The sequence number this transaction reads at.
    pub fn sequence(&self) -> Seq {
        self.snapshot.sequence()
    }

    /// The isolation level this transaction commits under.
    pub fn isolation(&self) -> Isolation {
        self.isolation
    }

    /// Read `key`, seeing this transaction's own uncommitted writes first and
    /// otherwise the snapshot it began at.
    pub fn get(&mut self, key: &Key) -> crate::Result<Option<Value>> {
        if let Some(buffered) = self.writes.get(key) {
            // Read-your-own-writes; a buffered delete reads back as absent.
            return Ok(buffered.clone());
        }
        self.reads.insert(key.clone());
        self.db.get_at(&self.snapshot, key)
    }

    /// Buffer a put. Nothing is written until [`Transaction::commit`].
    pub fn put(&mut self, key: Key, value: Value) {
        self.writes.insert(key, Some(value));
    }

    /// Buffer a delete. Nothing is written until [`Transaction::commit`].
    pub fn delete(&mut self, key: Key) {
        self.writes.insert(key, None);
    }

    /// Range scan (`start <= key < end`) over the snapshot, merged with this
    /// transaction's buffered writes.
    pub fn scan(&mut self, start: &[u8], end: &[u8]) -> crate::Result<Vec<(Key, Value)>> {
        self.scan_inner(start, Some(end))
    }

    /// Prefix scan over the snapshot, merged with buffered writes.
    pub fn scan_prefix(&mut self, prefix: &[u8]) -> crate::Result<Vec<(Key, Value)>> {
        let end = super::prefix_successor(prefix);
        self.scan_inner(prefix, end.as_deref())
    }

    fn scan_inner(&mut self, start: &[u8], end: Option<&[u8]>) -> crate::Result<Vec<(Key, Value)>> {
        // Remember the range so a key inserted into it by a concurrent
        // transaction is treated as a conflict under Serializable isolation.
        self.ranges.push((start.to_vec(), end.map(|e| e.to_vec())));

        let committed = match end {
            Some(end) => self.db.scan_at(&self.snapshot, start, end)?,
            None => self.db.scan_prefix_at(&self.snapshot, start)?,
        };

        let in_range = |k: &[u8]| k >= start && end.is_none_or(|e| k < e);

        let mut merged: BTreeMap<Key, Value> = committed.into_iter().collect();
        for (key, value) in &self.writes {
            if !in_range(key) {
                continue;
            }
            match value {
                Some(v) => {
                    merged.insert(key.clone(), v.clone());
                }
                None => {
                    merged.remove(key);
                }
            }
        }
        Ok(merged.into_iter().collect())
    }

    /// Number of buffered writes.
    pub fn len(&self) -> usize {
        self.writes.len()
    }

    /// Whether the transaction has buffered no writes.
    pub fn is_empty(&self) -> bool {
        self.writes.is_empty()
    }

    /// Discard the transaction. Equivalent to dropping it; provided so intent
    /// is explicit at the call site.
    pub fn rollback(self) {}

    /// Validate against everything committed since this transaction began and,
    /// if it still holds, apply its writes atomically at a single sequence.
    ///
    /// Returns the sequence number the writes committed at. On
    /// [`Error::Conflict`](crate::Error::Conflict) the transaction had no
    /// effect and may be retried.
    pub fn commit(self) -> crate::Result<Seq> {
        let Transaction {
            db,
            snapshot,
            writes,
            reads,
            ranges,
            isolation,
        } = self;

        // A read-only transaction has nothing to make durable and cannot lose
        // a write-write race, so it always succeeds.
        if writes.is_empty() {
            return Ok(snapshot.sequence());
        }

        db.commit_transaction(&snapshot, writes, reads, ranges, isolation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(items: &[&str]) -> HashSet<Key> {
        items.iter().map(|k| k.as_bytes().to_vec()).collect()
    }

    fn writes(items: &[&str]) -> BTreeMap<Key, Option<Value>> {
        items
            .iter()
            .map(|k| (k.as_bytes().to_vec(), Some(b"v".to_vec())))
            .collect()
    }

    #[test]
    fn commit_log_ignores_commits_visible_to_the_snapshot() {
        let mut log = CommitLog::default();
        log.record(5, keys(&["a"]));
        // The transaction's snapshot already includes seq 5.
        assert!(log
            .find_conflict(5, &writes(&["a"]), &keys(&[]), &[], Isolation::Serializable)
            .is_none());
    }

    #[test]
    fn commit_log_detects_write_write_conflict() {
        let mut log = CommitLog::default();
        log.record(7, keys(&["a"]));
        let conflict = log.find_conflict(5, &writes(&["a"]), &keys(&[]), &[], Isolation::Snapshot);
        assert_eq!(conflict, Some(b"a".to_vec()));
    }

    #[test]
    fn snapshot_isolation_ignores_read_write_conflict() {
        let mut log = CommitLog::default();
        log.record(7, keys(&["a"]));
        // We only read "a" and wrote "b": snapshot isolation permits this.
        assert!(log
            .find_conflict(5, &writes(&["b"]), &keys(&["a"]), &[], Isolation::Snapshot)
            .is_none());
        // Serializable rejects it.
        assert_eq!(
            log.find_conflict(
                5,
                &writes(&["b"]),
                &keys(&["a"]),
                &[],
                Isolation::Serializable
            ),
            Some(b"a".to_vec())
        );
    }

    #[test]
    fn serializable_detects_phantom_in_scanned_range() {
        let mut log = CommitLog::default();
        log.record(7, keys(&["user:5"]));
        let range = vec![(b"user:".to_vec(), Some(b"user;".to_vec()))];
        assert_eq!(
            log.find_conflict(
                5,
                &writes(&["other"]),
                &keys(&[]),
                &range,
                Isolation::Serializable
            ),
            Some(b"user:5".to_vec())
        );
        // A key outside the scanned range is not a conflict.
        let elsewhere = vec![(b"acct:".to_vec(), Some(b"acct;".to_vec()))];
        assert!(log
            .find_conflict(
                5,
                &writes(&["other"]),
                &keys(&[]),
                &elsewhere,
                Isolation::Serializable
            )
            .is_none());
    }

    #[test]
    fn prune_drops_entries_no_live_transaction_can_see() {
        let mut log = CommitLog::default();
        log.record(1, keys(&["a"]));
        log.record(2, keys(&["b"]));
        log.record(9, keys(&["c"]));
        log.prune(2);
        assert_eq!(log.len(), 1);
        // The surviving entry is still able to conflict.
        assert_eq!(
            log.find_conflict(3, &writes(&["c"]), &keys(&[]), &[], Isolation::Snapshot),
            Some(b"c".to_vec())
        );
    }
}
