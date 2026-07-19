use crate::Seq;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Tracks the sequence numbers of all live [`Snapshot`]s so compaction knows
/// the oldest version any reader might still need.
#[derive(Default)]
pub(super) struct SnapshotRegistry {
    /// snapshot sequence -> reference count (several snapshots may share a seq)
    live: Mutex<BTreeMap<Seq, usize>>,
}

impl SnapshotRegistry {
    pub(super) fn acquire(self: &Arc<Self>, seq: Seq) -> Snapshot {
        *self.live.lock().unwrap().entry(seq).or_insert(0) += 1;
        Snapshot {
            seq,
            registry: Arc::clone(self),
        }
    }

    fn release(&self, seq: Seq) {
        let mut live = self.live.lock().unwrap();
        if let Some(count) = live.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                live.remove(&seq);
            }
        }
    }

    /// The oldest sequence any live snapshot can read, or `None` if there are
    /// no live snapshots.
    pub(super) fn oldest(&self) -> Option<Seq> {
        self.live.lock().unwrap().keys().next().copied()
    }

    /// The number of live snapshots (summed across shared sequences).
    pub(super) fn live_count(&self) -> u64 {
        self.live.lock().unwrap().values().map(|&c| c as u64).sum()
    }
}

/// A consistent, read-only view of the store as of the moment it was taken.
///
/// Reads made through a snapshot (`Storage::get_at`, `scan_at`, and the
/// `SharedStorage` equivalents) observe exactly the writes that had committed
/// when the snapshot was created — later writes, deletes, flushes, and
/// compactions are invisible to it. Holding a snapshot keeps the versions it
/// needs from being garbage-collected, so drop it when finished.
pub struct Snapshot {
    seq: Seq,
    registry: Arc<SnapshotRegistry>,
}

impl Snapshot {
    /// The sequence number this snapshot reads at (it sees versions with a
    /// sequence number at or below this value).
    pub fn sequence(&self) -> Seq {
        self.seq
    }
}

impl std::fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Snapshot").field("seq", &self.seq).finish()
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.registry.release(self.seq);
    }
}
