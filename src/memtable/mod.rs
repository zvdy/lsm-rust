use std::collections::BTreeMap;
use crate::{Key, Value};

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
        self.size += key.len() + value.len();
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