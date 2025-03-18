use crate::{Key, Value};
use std::collections::BTreeMap;

pub struct MemTable {
    data: BTreeMap<Key, Value>,
    size: usize,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            data: BTreeMap::new(),
            size: 0,
        }
    }

    pub fn insert(&mut self, key: Key, value: Value) -> Option<Value> {
        let key_len = key.len();
        let value_len = value.len();

        // If key exists, subtract its size before adding new one
        if let Some(old_value) = self.data.get(&key) {
            self.size = self.size.saturating_sub(key_len + old_value.len());
        }

        self.size += key_len + value_len;
        self.data.insert(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Option<&Value> {
        self.data.get(key)
    }

    pub fn remove(&mut self, key: &[u8]) -> Option<Value> {
        if let Some(value) = self.data.remove(key) {
            self.size -= key.len() + value.len();
            Some(value)
        } else {
            None
        }
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

    pub fn iter(&self) -> impl Iterator<Item = (&Key, &Value)> {
        self.data.iter()
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
        assert_eq!(table.get(&key), Some(&value));
    }

    #[test]
    fn test_update_existing_key() {
        let mut table = MemTable::new();
        let key = b"test_key".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        table.insert(key.clone(), value1.clone());
        let old_value = table.insert(key.clone(), value2.clone());

        assert_eq!(old_value, Some(value1));
        assert_eq!(table.get(&key), Some(&value2));
        assert_eq!(table.len(), 1);
        assert_eq!(table.size(), key.len() + value2.len());
    }

    #[test]
    fn test_remove() {
        let mut table = MemTable::new();
        let key = b"test_key".to_vec();
        let value = b"test_value".to_vec();
        let total_size = key.len() + value.len();

        table.insert(key.clone(), value.clone());
        assert_eq!(table.size(), total_size);

        let removed = table.remove(&key);
        assert_eq!(removed, Some(value));
        assert!(table.is_empty());
        assert_eq!(table.size(), 0);
        assert_eq!(table.get(&key), None);
    }

    #[test]
    fn test_remove_nonexistent() {
        let mut table = MemTable::new();
        assert!(table.remove(b"nonexistent").is_none());
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

        let mut iter_entries: Vec<_> = table.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        iter_entries.sort();

        let mut expected = entries.clone();
        expected.sort();

        assert_eq!(iter_entries, expected);
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

        // Remove some entries
        let key = b"key0".to_vec();
        let removed_value = table.remove(&key).unwrap();
        expected_size -= key.len() + removed_value.len();

        assert_eq!(table.size(), expected_size);
    }
}
