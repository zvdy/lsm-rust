//! A Log-Structured Merge Tree (LSM tree) key-value store.
//!
//! Writes go to a write-ahead log (for durability) and an in-memory
//! memtable; when the memtable exceeds a size threshold it is flushed to an
//! immutable, sorted on-disk SSTable. Background compaction merges SSTables
//! into deeper levels, deduplicating keys and discarding deleted entries.
//! Reads consult the memtable first, then SSTables from newest to oldest,
//! using per-table Bloom filters to skip tables that cannot contain the key.
//!
//! # Example
//!
//! ```no_run
//! use lsm_rust::Storage;
//!
//! fn main() -> std::io::Result<()> {
//!     let mut db = Storage::new("./data", false)?;
//!
//!     db.put(b"name".to_vec(), b"Jane Doe".to_vec())?;
//!     assert_eq!(db.get(&b"name".to_vec())?, Some(b"Jane Doe".to_vec()));
//!
//!     db.delete(&b"name".to_vec())?;
//!     assert_eq!(db.get(&b"name".to_vec())?, None);
//!     Ok(())
//! }
//! ```

pub mod bloom;
pub mod memtable;
pub mod sstable;
pub mod storage;
pub mod wal;

/// Keys are arbitrary byte strings.
pub type Key = Vec<u8>;
/// Values are arbitrary byte strings.
pub type Value = Vec<u8>;

pub use storage::{Storage, StorageConfig};
