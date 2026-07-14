use crate::{Key, Value};
use std::collections::BTreeMap;
use std::ops::Bound;

/// In-memory sorted table of key -> entry, where an entry of `None` is a
/// tombstone recording that the key was deleted. Tombstones must be kept (and
/// later flushed to SSTables) so that deletes shadow older values that may
/// still live on disk.
pub struct MemTable {
    data: BTreeMap<Key, Option<Value>>,
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

    pub fn insert(&mut self, key: Key, value: Value) -> Option<Option<Value>> {
        self.insert_entry(key, Some(value))
    }

    /// Record a deletion for `key` as a tombstone entry.
    pub fn delete(&mut self, key: Key) -> Option<Option<Value>> {
        self.insert_entry(key, None)
    }

    fn insert_entry(&mut self, key: Key, value: Option<Value>) -> Option<Option<Value>> {
        let key_len = key.len();
        let value_len = value.as_ref().map_or(0, |v| v.len());

        // If key exists, subtract its size before adding new one
        if let Some(old_value) = self.data.get(&key) {
            let old_len = old_value.as_ref().map_or(0, |v| v.len());
            self.size = self.size.saturating_sub(key_len + old_len);
        }

        self.size += key_len + value_len;
        self.data.insert(key, value)
    }

    /// Look up a key. Returns:
    /// - `Some(Some(value))` if the key has a live value
    /// - `Some(None)` if the key was deleted (tombstone)
    /// - `None` if the memtable has no entry for the key
    pub fn get(&self, key: &[u8]) -> Option<&Option<Value>> {
        self.data.get(key)
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

    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Option<Value>)> {
        self.data.iter()
    }

    /// Iterate entries with `start <= key < end` in key order.
    /// `end = None` means unbounded above.
    pub fn range<'a>(
        &'a self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> impl Iterator<Item = (&'a Key, &'a Option<Value>)> {
        let lower = Bound::Included(start);
        let upper = end.map_or(Bound::Unbounded, Bound::Excluded);
        self.data.range::<[u8], _>((lower, upper))
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
        let key_len = key.len();
        let value_len = value.len();

        // Test insert
        assert!(table.insert(key.clone(), value.clone()).is_none());
        assert_eq!(table.len(), 1);
        assert_eq!(table.size(), key_len + value_len);

        // Test get
        assert_eq!(table.get(&key), Some(&Some(value)));
    }

    #[test]
    fn test_update_existing_key() {
        let mut table = MemTable::new();
        let key = b"test_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        table.insert(key.clone(), value1.clone());
        let old_value = table.insert(key.clone(), value2.clone());

        assert_eq!(old_value, Some(Some(value1)));
        assert_eq!(table.get(&key), Some(&Some(value2.clone())));
        assert_eq!(table.len(), 1);
        assert_eq!(table.size(), key.len() + value2.len());
    }

    #[test]
    fn test_delete_leaves_tombstone() {
        let mut table = MemTable::new();
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();

        table.insert(key.clone(), value.clone());
        let old = table.delete(key.clone());

        assert_eq!(old, Some(Some(value)));
        // The tombstone is still an entry: get returns Some(None)
        assert_eq!(table.get(&key), Some(&None));
        assert_eq!(table.len(), 1);
        // Only the key contributes to size after deletion
        assert_eq!(table.size(), key.len());
    }

    #[test]
    fn test_delete_nonexistent_creates_tombstone() {
        let mut table = MemTable::new();
        assert!(table.delete(b"nonexistent".to_vec()).is_none());
        // Deleting an unknown key still records a tombstone, since the key
        // may exist in an SSTable on disk.
        assert_eq!(table.get(b"nonexistent"), Some(&None));
    }

    #[test]
    fn test_iterator() {
        let mut table = MemTable::new();
        let entries = vec![
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        for (key, value) in entries.iter() {
            table.insert(key.clone(), value.clone());
        }

        let iter_entries: Vec<_> = table
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().unwrap()))
            .collect();

        // BTreeMap iteration is already sorted by key
        assert_eq!(iter_entries, entries);
    }

    #[test]
    fn test_size_tracking() {
        let mut table = MemTable::new();
        let mut expected_size = 0;

        // Insert multiple entries
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            expected_size += key.len() + value.len();
            table.insert(key, value);
        }

        assert_eq!(table.size(), expected_size);

        // Deleting an entry replaces its value with a tombstone
        let key = b"key0".to_vec();
        let removed_value = table.delete(key.clone()).unwrap().unwrap();
        expected_size -= removed_value.len();

        assert_eq!(table.size(), expected_size);
    }

    #[test]
    fn test_range() {
        let mut table = MemTable::new();
        for k in ["a", "b", "c", "d"] {
            table.insert(k.as_bytes().to_vec(), b"v".to_vec());
        }
        table.delete(b"c".to_vec());

        let keys: Vec<_> = table
            .range(b"b", Some(b"d"))
            .map(|(k, _)| k.clone())
            .collect();
        assert_eq!(keys, vec![b"b".to_vec(), b"c".to_vec()]); // tombstone included

        let keys: Vec<_> = table.range(b"c", None).map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"c".to_vec(), b"d".to_vec()]);
    }
}
