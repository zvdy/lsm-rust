//! One trait every engine is driven through, so the harness cannot
//! accidentally give one of them a shorter code path than another.

use std::path::Path;

/// How durable a write must be before it counts as done.
///
/// This is the single most important axis of a cross-engine comparison and
/// the one most often got wrong. These engines do not agree on a default:
/// lsm-rust fsyncs every write, sled flushes on a timer, redb lets a
/// transaction choose, and RocksDB leaves its WAL unsynced. Comparing any two
/// of them at their defaults measures the defaults, not the engines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Every write is on stable storage before it is acknowledged. Survives
    /// power loss.
    Synced,
    /// Writes may sit in memory or the page cache. Survives a process crash,
    /// not a power cut. This is what most engines do by default and what most
    /// published numbers are measured at.
    Buffered,
}

impl Durability {
    pub fn label(self) -> &'static str {
        match self {
            Durability::Synced => "synced",
            Durability::Buffered => "buffered",
        }
    }
}

/// A key-value store the harness can drive.
pub trait Engine {
    /// Open a store at `path` under the given durability.
    fn open(path: &Path, durability: Durability) -> anyhow::Result<Self>
    where
        Self: Sized;

    /// Human-readable name, including the version being measured.
    fn name() -> &'static str
    where
        Self: Sized;

    /// What this engine actually does under each durability setting, so a
    /// reader can check the mapping rather than trust it.
    fn durability_note(durability: Durability) -> &'static str
    where
        Self: Sized;

    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()>;
    fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;
    fn delete(&mut self, key: &[u8]) -> anyhow::Result<()>;

    /// Ordered pairs in `[lo, hi)`.
    fn scan(&self, lo: &[u8], hi: &[u8]) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>>;

    /// Make everything written so far durable. Called once at the end of a
    /// write phase so that a buffered engine is not credited with work it has
    /// not finished.
    fn sync(&mut self) -> anyhow::Result<()>;
}

/// Total bytes on disk under `path`, after a sync.
pub fn disk_bytes(path: &Path) -> u64 {
    fn walk(p: &Path) -> u64 {
        let Ok(entries) = std::fs::read_dir(p) else {
            return 0;
        };
        entries
            .filter_map(|e| e.ok())
            .map(|e| match e.file_type() {
                Ok(t) if t.is_dir() => walk(&e.path()),
                Ok(_) => e.metadata().map(|m| m.len()).unwrap_or(0),
                Err(_) => 0,
            })
            .sum()
    }
    walk(path)
}
