use crate::{Key, Seq, Value};
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::ops::Bound::{Excluded, Included, Unbounded};

/// In-memory, multi-version sorted table.
///
/// Every write is tagged with a unique, increasing [`Seq`]. Rather than
/// overwriting a key in place, each write is stored as its own version, so a
/// key may have several versions live at once. Entries are keyed by
/// `(key, Reverse(seq))`, which sorts the versions of one key contiguously
/// from newest to oldest — a range query can then land directly on "the
/// newest version at or below sequence N", which is what snapshot reads need.
///
/// A value of `None` is a tombstone recording a deletion. Tombstones are kept
/// (and later flushed to SSTables) so that deletes shadow older values that
/// may still live on disk or in an earlier version.
pub struct MemTable {
    data: BTreeMap<(Key, Reverse<Seq>), Option<Value>>,
    size: usize,
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            data: BTreeMap::new(),
            size: 0,
        }
    }

    /// Insert a value for `key` at version `seq`.
    pub fn insert(&mut self, key: Key, seq: Seq, value: Value) {
        self.insert_entry(key, seq, Some(value));
    }

    /// Record a deletion for `key` at version `seq` (a tombstone).
    pub fn delete(&mut self, key: Key, seq: Seq) {
        self.insert_entry(key, seq, None);
    }

    fn insert_entry(&mut self, key: Key, seq: Seq, value: Option<Value>) {
        let value_len = value.as_ref().map_or(0, |v| v.len());
        let entry_len = key.len() + value_len;
        if let Some(old) = self.data.insert((key, Reverse(seq)), value) {
            // Replacing the same (key, seq) is unusual but keep size honest
            self.size = self
                .size
                .saturating_sub(old.as_ref().map_or(0, |v| v.len()));
        } else {
            self.size += entry_len;
        }
    }

    /// Look up the newest version of `key` visible at `snapshot_seq`
    /// (the version with the largest seq `<= snapshot_seq`). Returns:
    /// - `Some(Some(value))` if that version has a live value
    /// - `Some(None)` if that version is a tombstone (deleted)
    /// - `None` if the memtable has no version of the key visible at `snapshot_seq`
    pub fn get_at(&self, key: &[u8], snapshot_seq: Seq) -> Option<&Option<Value>> {
        let lower = (key.to_vec(), Reverse(snapshot_seq));
        self.data
            .range((Included(lower), Unbounded))
            .next()
            .filter(|((k, _), _)| k.as_slice() == key)
            .map(|(_, v)| v)
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Iterate every stored version in `(key asc, seq desc)` order. Used to
    /// flush the whole memtable (all versions) to an SSTable.
    pub fn iter(&self) -> impl Iterator<Item = (&Key, Seq, &Option<Value>)> {
        self.data.iter().map(|((k, Reverse(seq)), v)| (k, *seq, v))
    }

    /// Iterate every version whose key is in `[start, end)`, in
    /// `(key asc, seq desc)` order. `end = None` means unbounded above.
    pub fn range<'a>(
        &'a self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> impl Iterator<Item = (&'a Key, Seq, &'a Option<Value>)> {
        // (key, Reverse(u64::MAX)) is the smallest tuple with a given key,
        // so it includes every version of that key and beyond.
        let lower = Included((start.to_vec(), Reverse(Seq::MAX)));
        let upper = match end {
            Some(e) => Excluded((e.to_vec(), Reverse(Seq::MAX))),
            None => Unbounded,
        };
        self.data
            .range((lower, upper))
            .map(|((k, Reverse(seq)), v)| (k, *seq, v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_memtable() {
        let table = MemTable::new();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
        assert_eq!(table.size(), 0);
    }

    #[test]
    fn test_insert_and_get() {
        let mut table = MemTable::new();
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        table.insert(key.clone(), 1, value.clone());
        assert_eq!(table.len(), 1);
        assert_eq!(table.size(), key.len() + value.len());
        assert_eq!(table.get_at(&key, Seq::MAX), Some(&Some(value)));
    }

    #[test]
    fn test_versions_are_isolated_by_seq() {
        let mut table = MemTable::new();
        let key = b"k".to_vec();
        table.insert(key.clone(), 1, b"v1".to_vec());
        table.insert(key.clone(), 5, b"v5".to_vec());
        table.insert(key.clone(), 9, b"v9".to_vec());

        // Snapshot reads see the newest version at or below their seq
        assert_eq!(table.get_at(&key, 0), None); // before any version
        assert_eq!(table.get_at(&key, 1), Some(&Some(b"v1".to_vec())));
        assert_eq!(table.get_at(&key, 4), Some(&Some(b"v1".to_vec())));
        assert_eq!(table.get_at(&key, 5), Some(&Some(b"v5".to_vec())));
        assert_eq!(table.get_at(&key, 8), Some(&Some(b"v5".to_vec())));
        assert_eq!(table.get_at(&key, 9), Some(&Some(b"v9".to_vec())));
        assert_eq!(table.get_at(&key, Seq::MAX), Some(&Some(b"v9".to_vec())));
        // All three versions are retained
        assert_eq!(table.len(), 3);
    }

    #[test]
    fn test_delete_leaves_tombstone_version() {
        let mut table = MemTable::new();
        let key = b"k".to_vec();
        table.insert(key.clone(), 1, b"v".to_vec());
        table.delete(key.clone(), 2);

        // The latest version is a tombstone, older is still visible by seq
        assert_eq!(table.get_at(&key, 2), Some(&None));
        assert_eq!(table.get_at(&key, 1), Some(&Some(b"v".to_vec())));
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn test_iter_yields_all_versions_newest_first() {
        let mut table = MemTable::new();
        table.insert(b"a".to_vec(), 1, b"a1".to_vec());
        table.insert(b"a".to_vec(), 3, b"a3".to_vec());
        table.insert(b"b".to_vec(), 2, b"b2".to_vec());

        let got: Vec<_> = table
            .iter()
            .map(|(k, s, v)| (k.clone(), s, v.clone()))
            .collect();
        assert_eq!(
            got,
            vec![
                (b"a".to_vec(), 3, Some(b"a3".to_vec())), // key asc, seq desc
                (b"a".to_vec(), 1, Some(b"a1".to_vec())),
                (b"b".to_vec(), 2, Some(b"b2".to_vec())),
            ]
        );
    }

    #[test]
    fn test_range() {
        let mut table = MemTable::new();
        for (i, k) in ["a", "b", "c", "d"].iter().enumerate() {
            table.insert(k.as_bytes().to_vec(), i as Seq + 1, b"v".to_vec());
        }

        let keys: Vec<_> = table
            .range(b"b", Some(b"d"))
            .map(|(k, _, _)| k.clone())
            .collect();
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]);

        let keys: Vec<_> = table.range(b"c", None).map(|(k, _, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"c".to_vec(), b"d".to_vec()]);
    }
}
