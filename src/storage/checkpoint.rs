//! Consistent point-in-time copies of a store.
//!
//! # Why this exists
//!
//! Crash recovery and backup solve different problems, and the engine only
//! had the first. A process that dies — power loss, an OOM kill, a panic —
//! is already handled: the WAL is fsynced before a write is acknowledged, the
//! manifest rename is the commit point for a flush or compaction, and startup
//! sweeps whatever the interruption left behind.
//!
//! None of that helps when the *data itself* is gone: someone deletes the
//! data directory, the disk fails, or a bad deploy writes garbage keys. Note
//! that checksums do not close this gap either — a CRC-32 *detects* a rotted
//! block and turns it into [`Error::Corruption`](crate::Error::Corruption)
//! rather than plausible-looking data, but it cannot reconstruct the bytes.
//! Only a second copy can.
//!
//! # Why copying the directory by hand does not work
//!
//! Copying a live data directory races compaction three ways, and every one
//! of them produces a copy that *opens cleanly* while being silently wrong:
//!
//! - The manifest is replaced between copying `MANIFEST` and copying the
//!   tables, so the copy names tables that were never captured.
//! - Compaction unlinks an input table while it is being read, truncating it.
//! - The old manifest is captured alongside newly written tables, which are
//!   then unreferenced — and so deleted as orphans the next time the copy is
//!   opened, because the manifest is authoritative.
//!
//! [`Storage::checkpoint`](super::Storage::checkpoint) takes the exclusive
//! lock for the duration, so the table set, the WAL and the manifest it
//! captures are all from the same instant.
//!
//! # What it costs on disk
//!
//! Almost nothing at first, then it grows — lazily, and only in proportion to
//! how much compaction rewrites afterwards.
//!
//! SSTables are immutable once written, so each one is captured as a **hard
//! link**: a directory entry pointing at the same inode, not a second copy of
//! the data. A checkpoint of a 10 GB store initially costs a few directory
//! entries plus a manifest.
//!
//! The growth comes later. When compaction unlinks an input table, that
//! normally drops the link count to zero and frees the extents; a checkpoint
//! holding a link drops it to one instead, and the space stays. So a
//! checkpoint's real cost is *the bytes compaction has rewritten since it was
//! taken* — not the size of the store. A quiet store costs approximately
//! nothing indefinitely; a heavily compacting one can approach a second full
//! copy.
//!
//! That cost is reclaimed by deleting the checkpoint directory, so checkpoints
//! are meant to be short-lived: take one, copy it to wherever backups actually
//! live, then remove it. Keeping many on the same volume is what makes this
//! expensive.
//!
//! The WAL is the exception to hard-linking. It is *appended to*, so sharing
//! its inode would let writes made after the checkpoint bleed into it. It is
//! copied instead. It only holds writes since the last flush, so it is small.
//!
//! # Restoring
//!
//! A checkpoint directory *is* a data directory: open it with
//! [`Storage::new`](super::Storage::new) and its WAL replays like any other.
//! There is no separate restore call to get wrong.

use crate::Seq;
use std::path::PathBuf;

/// What a [`Storage::checkpoint`](super::Storage::checkpoint) captured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointInfo {
    /// The directory the checkpoint was written to. Opening it with
    /// [`Storage::new`](super::Storage::new) restores this state.
    pub path: PathBuf,
    /// Number of SSTables captured.
    pub tables: usize,
    /// Total size of those SSTables. This is the size of the data the
    /// checkpoint makes available, **not** the disk it consumed: hard-linked
    /// tables share their bytes with the live store until compaction rewrites
    /// them. See the module docs for how that cost accrues.
    pub table_bytes: u64,
    /// Size of the copied write-ahead log, which is genuinely duplicated.
    pub wal_bytes: u64,
    /// The MVCC sequence the checkpoint is consistent as of. Reads on the
    /// restored store see exactly the writes at or below this.
    pub sequence: Seq,
    /// Whether every table was captured as a hard link. False means the
    /// target is on a different filesystem, so the tables were copied in
    /// full — correct, but `table_bytes` of real disk rather than near-zero.
    pub hard_linked: bool,
}

impl CheckpointInfo {
    /// Bytes this checkpoint is guaranteed to have duplicated on disk right
    /// away: the WAL always, plus the tables when they had to be copied
    /// instead of linked.
    ///
    /// This is a floor, not the eventual cost. Linked tables begin sharing
    /// their bytes and start consuming disk of their own only as compaction
    /// rewrites the live copies out from under them.
    pub fn bytes_duplicated(&self) -> u64 {
        if self.hard_linked {
            self.wal_bytes
        } else {
            self.wal_bytes + self.table_bytes
        }
    }
}
