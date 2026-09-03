mod manifest;
mod scan;
mod shared;
mod snapshot;
mod stats;
mod transaction;
use manifest::{Manifest, ManifestEntry};
pub use scan::{ScanIter, SnapshotScan};
pub use shared::{CompactorHandle, SharedStorage};
pub use snapshot::Snapshot;
use snapshot::SnapshotRegistry;
use stats::Metrics;
pub use stats::{LevelStats, StorageStats};
use transaction::CommitLog;
pub use transaction::{Isolation, Transaction, TransactionError};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::memtable::MemTable;
use crate::sstable::{BlockCache, CompactionManager, Compression, SSTable, SSTableLookup};
use crate::wal::{BatchEntry, Operation, WalRecord, WalSync, WAL};
use crate::{Key, Seq, Value};

/// Smallest byte string strictly greater than every string with this
/// prefix, or `None` if no such bound exists (prefix is empty or all 0xFF).
fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(&last) = end.last() {
        if last == 0xFF {
            end.pop();
        } else {
            *end.last_mut().unwrap() = last + 1;
            return Some(end);
        }
    }
    None
}

/// Parse `L{level}_{seq}.sst` filenames.
fn parse_sst_filename(path: &Path) -> Option<(usize, u64)> {
    if path.extension().and_then(|s| s.to_str()) != Some("sst") {
        return None;
    }
    let stem = path.file_stem()?.to_str()?;
    let (level, seq) = stem.strip_prefix('L')?.split_once('_')?;
    Some((level.parse().ok()?, seq.parse().ok()?))
}

/// Apply a single replayed/batched operation to the memtable at `seq`.
fn apply_op(memtable: &mut MemTable, seq: Seq, op: Operation, key: Key, value: Option<Value>) {
    match op {
        Operation::Put => {
            if let Some(value) = value {
                memtable.insert(key, seq, value);
            }
        }
        Operation::Delete => memtable.delete(key, seq),
    }
}

/// A set of writes committed atomically at a single sequence number.
///
/// Build one with [`WriteBatch::put`] / [`WriteBatch::delete`], then apply it
/// with [`Storage::write_batch`]. Atomicity has two facets:
/// - **Visibility**: every op shares one sequence number, so a [`Snapshot`]
///   taken before the batch sees none of it and one taken after sees all of
///   it — never a partial batch.
/// - **Durability**: the batch is written to the WAL as one record, so a
///   crash mid-commit recovers either the whole batch or none of it.
///
/// Within a batch, a later op on a key supersedes an earlier one.
#[derive(Default)]
pub struct WriteBatch {
    ops: Vec<(Operation, Key, Option<Value>)>,
}

impl WriteBatch {
    /// Create an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue a put of `key` = `value`.
    pub fn put(&mut self, key: Key, value: Value) -> &mut Self {
        self.ops.push((Operation::Put, key, Some(value)));
        self
    }

    /// Queue a delete of `key`.
    pub fn delete(&mut self, key: Key) -> &mut Self {
        self.ops.push((Operation::Delete, key, None));
        self
    }

    /// Number of queued operations.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Whether the batch has no queued operations.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

static PUT_COUNT: AtomicUsize = AtomicUsize::new(0);
static TOTAL_BYTES: AtomicUsize = AtomicUsize::new(0);

/// Tuning knobs for a [`Storage`] instance.
///
/// The defaults match the engine's historical behavior and are deliberately
/// small so that flushes and compactions are easy to observe; production-like
/// workloads will usually want larger thresholds.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Flush the memtable to a level-0 SSTable once it holds this many bytes.
    pub memtable_size_threshold: usize,
    /// Base size threshold for level compaction; level N compacts when it
    /// exceeds `compaction_size_threshold * level_multiplier^N` bytes.
    pub compaction_size_threshold: usize,
    /// Growth factor between consecutive level size thresholds.
    pub level_multiplier: u32,
    /// Compact level 0 once it holds this many SSTable files.
    pub level0_file_limit: usize,
    /// Compression applied to SSTable data blocks.
    pub compression: Compression,
    /// When the write-ahead log fsyncs (per write, or batched group commit).
    pub wal_sync: WalSync,
    /// Capacity in bytes of the shared block cache for SSTable reads
    /// (0 disables caching).
    pub block_cache_size: usize,
    /// Run compaction inline on the write path when a flush fills a level
    /// (default). Disable to drive compaction yourself via
    /// [`Storage::compact_now`] or a background
    /// [`SharedStorage::spawn_compactor`] thread.
    pub inline_compaction: bool,
    /// Print detailed progress information to stdout.
    pub verbose: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            memtable_size_threshold: 512 * 1024,    // 512KB
            compaction_size_threshold: 1024 * 1024, // 1MB
            level_multiplier: 4,
            level0_file_limit: 4,
            compression: Compression::None,
            wal_sync: WalSync::Always,
            block_cache_size: 4 * 1024 * 1024, // 4MB
            inline_compaction: true,
            verbose: false,
        }
    }
}

/// The main LSM-tree key-value store.
///
/// See the [crate-level documentation](crate) for an overview of the write,
/// read, and compaction paths.
pub struct Storage {
    memtable: MemTable,
    wal: WAL,
    sstables: HashMap<usize, Vec<SSTable>>, // level -> SSTables
    data_dir: PathBuf,
    sstable_counter: u64,
    /// Highest MVCC sequence number assigned so far. The next write uses
    /// `seq_counter + 1`; a read or snapshot at "now" sees seqs `<= seq_counter`.
    seq_counter: Seq,
    snapshots: Arc<SnapshotRegistry>,
    /// Write sets of recent commits, used to validate optimistic transactions.
    commits: CommitLog,
    metrics: Metrics,
    compaction_manager: CompactionManager,
    block_cache: Option<Arc<BlockCache>>,
    manifest: Manifest,
    config: StorageConfig,
}

impl Storage {
    /// Open (or create) a store at `data_dir` with default tuning.
    ///
    /// Existing SSTables are loaded and any pending write-ahead-log entries
    /// are replayed into the memtable.
    pub fn new<P: AsRef<Path>>(data_dir: P, verbose: bool) -> io::Result<Self> {
        Self::with_config(
            data_dir,
            StorageConfig {
                verbose,
                ..StorageConfig::default()
            },
        )
    }

    /// Open (or create) a store at `data_dir` with explicit tuning options.
    pub fn with_config<P: AsRef<Path>>(data_dir: P, config: StorageConfig) -> io::Result<Self> {
        let verbose = config.verbose;
        if verbose {
            println!("Initializing storage at {:?}", data_dir.as_ref());
        }
        fs::create_dir_all(&data_dir)?;

        let block_cache = (config.block_cache_size > 0)
            .then(|| Arc::new(BlockCache::new(config.block_cache_size)));

        // Load existing SSTables. The manifest is the authoritative record
        // of which tables are live and how far the MVCC sequence counter has
        // advanced; without one (legacy data directory) we fall back to
        // scanning the directory and start sequences from zero.
        let manifest = Manifest::new(data_dir.as_ref());
        let had_manifest = manifest.exists();

        let mut found: Vec<(usize, u64, PathBuf)> = Vec::new();
        let mut seq_counter: Seq = 0;
        match manifest.load()? {
            Some(state) => {
                seq_counter = state.last_seq;
                for entry in state.entries {
                    found.push((
                        entry.level,
                        entry.seq,
                        data_dir.as_ref().join(&entry.filename),
                    ));
                }

                // Delete unreferenced .sst files: leftovers from a flush or
                // compaction that crashed before its manifest commit, or old
                // tables whose deletion was interrupted after it.
                for dir_entry in fs::read_dir(&data_dir)? {
                    let path = dir_entry?.path();
                    if path.is_file()
                        && parse_sst_filename(&path).is_some()
                        && !found.iter().any(|(_, _, p)| *p == path)
                    {
                        if verbose {
                            println!("Removing orphan SSTable {:?}", path.file_name().unwrap());
                        }
                        fs::remove_file(&path)?;
                    }
                }
            }
            None => {
                for dir_entry in fs::read_dir(&data_dir)? {
                    let path = dir_entry?.path();
                    if path.is_file() {
                        if let Some((level, seq)) = parse_sst_filename(&path) {
                            found.push((level, seq, path));
                        }
                    }
                }
            }
        }

        let wal_path = data_dir.as_ref().join("wal");
        let mut wal = WAL::with_sync(wal_path, config.wal_sync)?;
        let mut memtable = MemTable::new();

        // Replay the WAL, assigning fresh sequence numbers continuing from the
        // manifest's high-water mark. The WAL preserves write order, so the
        // replayed sequences match what the entries held before the crash and
        // land above every sequence in the loaded SSTables.
        let mut replay_count = 0;
        for record in wal.replay()? {
            match record {
                // A standalone op gets its own sequence number
                WalRecord::Single(op, key, value) => {
                    seq_counter += 1;
                    apply_op(&mut memtable, seq_counter, op, key, value);
                    replay_count += 1;
                }
                // A batch's ops all share one sequence number (atomic visibility)
                WalRecord::Batch(entries) => {
                    seq_counter += 1;
                    for (op, key, value) in entries {
                        apply_op(&mut memtable, seq_counter, op, key, value);
                        replay_count += 1;
                    }
                }
            }
        }
        if verbose && replay_count > 0 {
            println!("Replayed {} operations from WAL", replay_count);
        }

        // Within a level, tables must be ordered oldest to newest so that
        // reads (which scan newest first) and compaction (where the newest
        // value wins) see them consistently. Sort by sequence number since
        // neither manifest order nor directory order is guaranteed.
        found.sort_by_key(|(level, seq, _)| (*level, *seq));

        let mut sstables: HashMap<usize, Vec<SSTable>> = HashMap::new();
        let mut counter = 0;
        let mut total_sstables = 0;
        for (level, seq, path) in found {
            counter = counter.max(seq + 1);
            sstables
                .entry(level)
                .or_default()
                .push(SSTable::with_cache(path, block_cache.clone())?);
            total_sstables += 1;
        }

        if verbose {
            println!(
                "Loaded {} SSTables across {} levels",
                total_sstables,
                sstables.len()
            );
            for (level, tables) in &sstables {
                let total_size: usize = tables.iter().map(|t| t.size()).sum();
                println!(
                    "  Level {}: {} files, {} bytes total",
                    level,
                    tables.len(),
                    total_size
                );
            }
        }

        let compaction_manager = CompactionManager::new(
            config.level_multiplier,
            config.compaction_size_threshold,
            config.level0_file_limit,
        );

        let storage = Storage {
            memtable,
            wal,
            sstables,
            data_dir: data_dir.as_ref().to_path_buf(),
            sstable_counter: counter,
            seq_counter,
            snapshots: Arc::new(SnapshotRegistry::default()),
            commits: CommitLog::default(),
            metrics: Metrics::default(),
            compaction_manager,
            block_cache,
            manifest,
            config,
        };

        // Give legacy directories a manifest so the next startup (and any
        // crash recovery in between) has an authoritative table record
        if !had_manifest {
            storage.persist_manifest()?;
        }

        Ok(storage)
    }

    /// Atomically record the current live table set in the manifest. Called
    /// at every commit point where the table set changes.
    fn persist_manifest(&self) -> io::Result<()> {
        let mut entries = Vec::new();
        for (level, tables) in &self.sstables {
            for table in tables {
                let path = table.get_path();
                if let Some((_, seq)) = parse_sst_filename(path) {
                    entries.push(ManifestEntry {
                        level: *level,
                        seq,
                        filename: path.file_name().unwrap().to_string_lossy().into_owned(),
                    });
                }
            }
        }
        entries.sort_by_key(|e| (e.level, e.seq));
        self.manifest.write(&entries, self.seq_counter)
    }

    /// Take a snapshot: a consistent, read-only view of the store as of now.
    ///
    /// Reads through the returned [`Snapshot`] (via [`Storage::get_at`] /
    /// [`Storage::scan_at`]) see only writes that had committed when it was
    /// taken. Holding it pins the versions it needs against compaction, so
    /// drop it when done.
    pub fn snapshot(&self) -> Snapshot {
        self.snapshots.acquire(self.seq_counter)
    }

    /// The highest MVCC sequence number assigned so far.
    ///
    /// Every put, delete, and write-batch advances it by one. Record the value
    /// at a moment of interest (a logical checkpoint) and later revisit the
    /// store's state as of that moment with [`Storage::snapshot_at`] — the
    /// sequence survives restarts, since it is persisted in the manifest and
    /// restored on open.
    pub fn current_sequence(&self) -> Seq {
        self.seq_counter
    }

    /// Open a snapshot positioned at a specific historical sequence number
    /// ("time-travel"): reads through it observe the newest version of each
    /// key committed at or before `seq`, exactly as an ordinary [`Snapshot`]
    /// taken at that moment would have.
    ///
    /// `seq` must not exceed [`Storage::current_sequence`] — there is no
    /// travelling into the future. Like any snapshot, holding the returned
    /// value pins the versions it needs against garbage collection.
    ///
    /// # Retention
    ///
    /// Time-travel is exact for any sequence at or above the store's retained
    /// history floor. Older versions that compaction has already collapsed
    /// (because no live snapshot pinned them) are *not* resurrected: a read at
    /// such a sequence sees the oldest version still on disk. To guarantee a
    /// past sequence stays readable, open the time-travel snapshot before the
    /// compaction that would collapse it, or keep a snapshot pinned meanwhile.
    pub fn snapshot_at(&self, seq: Seq) -> io::Result<Snapshot> {
        if seq > self.seq_counter {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot time-travel to sequence {} beyond the current sequence {}",
                    seq, self.seq_counter
                ),
            ));
        }
        Ok(self.snapshots.acquire(seq))
    }

    /// Take a point-in-time snapshot of the store's operational metrics.
    ///
    /// Returns cumulative operation counters plus gauges describing the
    /// store's current shape (memtable occupancy, per-level SSTable counts and
    /// sizes, live snapshots, and the MVCC sequence). Render the result for a
    /// Prometheus scrape with [`StorageStats::to_prometheus`].
    pub fn stats(&self) -> StorageStats {
        use std::sync::atomic::Ordering::Relaxed;

        let mut levels: Vec<LevelStats> = self
            .sstables
            .iter()
            .filter(|(_, tables)| !tables.is_empty())
            .map(|(&level, tables)| LevelStats {
                level,
                num_sstables: tables.len() as u64,
                bytes: tables.iter().map(|t| t.size() as u64).sum(),
            })
            .collect();
        levels.sort_by_key(|l| l.level);

        StorageStats {
            puts_total: self.metrics.puts.load(Relaxed),
            deletes_total: self.metrics.deletes.load(Relaxed),
            batches_total: self.metrics.batches.load(Relaxed),
            gets_total: self.metrics.gets.load(Relaxed),
            scans_total: self.metrics.scans.load(Relaxed),
            flushes_total: self.metrics.flushes.load(Relaxed),
            compactions_total: self.metrics.compactions.load(Relaxed),
            sequence: self.seq_counter,
            live_snapshots: self.snapshots.live_count(),
            memtable_bytes: self.memtable.size() as u64,
            memtable_entries: self.memtable.len() as u64,
            levels,
        }
    }

    /// Look up the current value for `key`, or `None` if the key does not
    /// exist or has been deleted.
    pub fn get(&self, key: &Key) -> io::Result<Option<Value>> {
        self.get_at_seq(key, self.seq_counter)
    }

    /// Look up `key` as seen by `snapshot`, ignoring any writes that
    /// committed after the snapshot was taken.
    pub fn get_at(&self, snapshot: &Snapshot, key: &Key) -> io::Result<Option<Value>> {
        self.get_at_seq(key, snapshot.sequence())
    }

    /// Read the newest version of `key` with sequence `<= snapshot_seq`.
    ///
    /// Correctness relies on an invariant of the LSM layout: for a given key,
    /// a higher sequence number always lives in a newer (shallower) table, so
    /// scanning newest-to-oldest and returning at the first visible version
    /// yields the newest version the snapshot can see.
    fn get_at_seq(&self, key: &Key, snapshot_seq: Seq) -> io::Result<Option<Value>> {
        Metrics::incr(&self.metrics.gets);
        if self.config.verbose {
            println!("GET {:?} @ {}", String::from_utf8_lossy(key), snapshot_seq);
        }

        // First check the memtable; a tombstone visible to the snapshot means
        // the key was deleted and must shadow any older value in the SSTables
        match self.memtable.get_at(key, snapshot_seq) {
            Some(Some(value)) => return Ok(Some(value.clone())),
            Some(None) => return Ok(None),
            None => {}
        }

        // Then check SSTables from newest to oldest, level by level
        for level in 0..=self.sstables.keys().max().copied().unwrap_or(0) {
            if let Some(tables) = self.sstables.get(&level) {
                for sstable in tables.iter().rev() {
                    // Use bloom filter to avoid unnecessary disk reads
                    if !sstable.might_contain_key(key) {
                        continue;
                    }

                    match sstable.get_at(key, snapshot_seq)? {
                        SSTableLookup::Found(value) => return Ok(Some(value)),
                        SSTableLookup::Deleted => return Ok(None),
                        SSTableLookup::NotFound => {}
                    }
                }
            }
        }

        Ok(None)
    }

    /// Return all live key-value pairs with `start <= key < end`, in key
    /// order. Deleted keys are excluded; the newest value wins when a key
    /// exists at several levels.
    pub fn scan(&self, start: &[u8], end: &[u8]) -> io::Result<Vec<(Key, Value)>> {
        self.scan_at_seq(start, Some(end), self.seq_counter)
    }

    /// Return all live key-value pairs whose key starts with `prefix`,
    /// in key order.
    pub fn scan_prefix(&self, prefix: &[u8]) -> io::Result<Vec<(Key, Value)>> {
        let end = prefix_successor(prefix);
        self.scan_at_seq(prefix, end.as_deref(), self.seq_counter)
    }

    /// Range scan as seen by `snapshot`.
    pub fn scan_at(
        &self,
        snapshot: &Snapshot,
        start: &[u8],
        end: &[u8],
    ) -> io::Result<Vec<(Key, Value)>> {
        self.scan_at_seq(start, Some(end), snapshot.sequence())
    }

    /// Prefix scan as seen by `snapshot`.
    pub fn scan_prefix_at(
        &self,
        snapshot: &Snapshot,
        prefix: &[u8],
    ) -> io::Result<Vec<(Key, Value)>> {
        let end = prefix_successor(prefix);
        self.scan_at_seq(prefix, end.as_deref(), snapshot.sequence())
    }

    fn scan_at_seq(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        snapshot_seq: Seq,
    ) -> io::Result<Vec<(Key, Value)>> {
        self.scan_iter_at_seq(start, end, snapshot_seq)?.collect()
    }

    /// Stream live entries in `[start, end)` in key order, newest visible
    /// version per key, without materializing the whole range.
    ///
    /// Prefer this over [`Storage::scan`] for wide ranges: the returned
    /// iterator reads one SSTable block at a time as it advances, so memory
    /// stays proportional to the number of tables rather than to the number of
    /// matching keys. Each item is a [`Result`] because blocks are read from
    /// disk lazily; the first error ends the iteration.
    ///
    /// ```no_run
    /// # fn main() -> std::io::Result<()> {
    /// let db = lsm_rust::Storage::new("./data", false)?;
    /// for entry in db.scan_iter(b"user:", Some(b"user;"))? {
    ///     let (key, value) = entry?;
    ///     println!("{} bytes at {:?}", value.len(), String::from_utf8_lossy(&key));
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn scan_iter(&self, start: &[u8], end: Option<&[u8]>) -> io::Result<ScanIter<'_>> {
        self.scan_iter_at_seq(start, end, self.seq_counter)
    }

    /// Stream live entries whose key starts with `prefix`, in key order.
    pub fn scan_prefix_iter(&self, prefix: &[u8]) -> io::Result<ScanIter<'_>> {
        let end = prefix_successor(prefix);
        self.scan_iter_at_seq(prefix, end.as_deref(), self.seq_counter)
    }

    /// Stream live entries in `[start, end)` as seen by `snapshot`.
    ///
    /// The snapshot is borrowed for the life of the iterator, so the versions
    /// the scan needs stay pinned against compaction while it runs.
    pub fn scan_iter_at<'a>(
        &'a self,
        snapshot: &'a Snapshot,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> io::Result<SnapshotScan<'a>> {
        Ok(SnapshotScan {
            inner: self.scan_iter_at_seq(start, end, snapshot.sequence())?,
            _snapshot: snapshot,
        })
    }

    fn scan_iter_at_seq(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        snapshot_seq: Seq,
    ) -> io::Result<ScanIter<'_>> {
        Metrics::incr(&self.metrics.scans);
        ScanIter::new(
            &self.memtable,
            self.sstables.values().flatten(),
            start,
            end,
            snapshot_seq,
        )
    }

    /// Insert or update `key` with `value`. The write is durable (recorded
    /// in the write-ahead log) once this returns.
    pub fn put(&mut self, key: Key, value: Value) -> io::Result<()> {
        if self.config.verbose {
            let count = PUT_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
            let bytes = TOTAL_BYTES.fetch_add(key.len() + value.len(), Ordering::Relaxed)
                + key.len()
                + value.len();

            if count.is_multiple_of(1000) {
                println!(
                    "\nProgress: {} operations ({:.2} MB written)",
                    count,
                    bytes as f64 / 1_048_576.0
                );
                println!(
                    "Average value size: {:.2} KB",
                    (bytes as f64 / count as f64) / 1024.0
                );
            }
        }

        // Write to WAL first
        self.wal.append(Operation::Put, &key, Some(&value))?;

        // Assign the next sequence number and record this version
        self.seq_counter += 1;
        let seq = self.seq_counter;
        self.memtable.insert(key.clone(), seq, value);
        Metrics::incr(&self.metrics.puts);
        self.note_commit(seq, HashSet::from([key]));

        self.maybe_flush_memtable()
    }

    /// Delete `key`. The deletion is durable once this returns and shadows
    /// any older value still stored in SSTables.
    pub fn delete(&mut self, key: &Key) -> io::Result<()> {
        if self.config.verbose {
            println!("DELETE {:?}", String::from_utf8_lossy(key));
        }

        // Write to WAL first
        self.wal.append(Operation::Delete, key, None)?;

        // Record a tombstone version in the memtable. The tombstone (not a
        // plain removal) is what shadows older values still living in SSTables
        // and is flushed to disk alongside regular entries.
        self.seq_counter += 1;
        let seq = self.seq_counter;
        self.memtable.delete(key.clone(), seq);
        Metrics::incr(&self.metrics.deletes);
        self.note_commit(seq, HashSet::from([key.clone()]));

        self.maybe_flush_memtable()
    }

    /// Apply a [`WriteBatch`] atomically: all of its operations commit at a
    /// single sequence number and are recovered together on a crash.
    pub fn write_batch(&mut self, batch: WriteBatch) -> io::Result<()> {
        if batch.ops.is_empty() {
            return Ok(());
        }

        // Durably record the whole batch as one atomic WAL record first
        {
            let entries: Vec<BatchEntry> = batch
                .ops
                .iter()
                .map(|(op, key, value)| BatchEntry {
                    op: *op,
                    key,
                    value: value.as_deref(),
                })
                .collect();
            self.wal.append_batch(&entries)?;
        }

        // One sequence number for the whole batch gives atomic visibility;
        // later ops on a key supersede earlier ones (same seq, insert wins).
        self.seq_counter += 1;
        let seq = self.seq_counter;
        let mut written = HashSet::with_capacity(batch.ops.len());
        for (op, key, value) in batch.ops {
            written.insert(key.clone());
            apply_op(&mut self.memtable, seq, op, key, value);
        }
        Metrics::incr(&self.metrics.batches);
        self.note_commit(seq, written);

        self.maybe_flush_memtable()
    }

    /// Record the keys a commit at `seq` wrote, so in-flight transactions can
    /// detect that their snapshot went stale, then drop records no live
    /// transaction could still be validated against.
    ///
    /// Every write path funnels through here — including plain `put`, `delete`
    /// and `write_batch` — because a transaction must conflict with concurrent
    /// non-transactional writes just as it does with transactional ones.
    fn note_commit(&mut self, seq: Seq, keys: HashSet<Key>) {
        self.commits.record(seq, keys);
        let oldest_live = self.snapshots.oldest().unwrap_or(self.seq_counter);
        self.commits.prune(oldest_live);
    }

    /// Validate an optimistic transaction against everything committed since
    /// its snapshot and, if it still holds, apply its writes atomically.
    ///
    /// Runs entirely under the engine's exclusive lock, so validation and
    /// application are a single atomic step: no other commit can slip between
    /// deciding that a transaction is valid and making its writes visible.
    pub(crate) fn commit_transaction(
        &mut self,
        snapshot_seq: Seq,
        writes: BTreeMap<Key, Option<Value>>,
        reads: HashSet<Key>,
        ranges: Vec<(Key, Option<Key>)>,
        isolation: Isolation,
    ) -> Result<Seq, TransactionError> {
        if let Some(key) =
            self.commits
                .find_conflict(snapshot_seq, &writes, &reads, &ranges, isolation)
        {
            return Err(TransactionError::Conflict { key });
        }

        // Apply as one atomic batch: a single sequence number and one WAL
        // record, so the transaction is visible and durable all-or-nothing.
        let mut batch = WriteBatch::new();
        for (key, value) in writes {
            match value {
                Some(value) => {
                    batch.put(key, value);
                }
                None => {
                    batch.delete(key);
                }
            }
        }
        // `write_batch` records the write set and prunes for us.
        self.write_batch(batch)?;

        Ok(self.seq_counter)
    }

    /// Number of commit records currently retained for conflict detection.
    pub fn tracked_commits(&self) -> usize {
        self.commits.len()
    }

    fn maybe_flush_memtable(&mut self) -> io::Result<()> {
        let memtable_size = self.memtable.size();
        if memtable_size >= self.config.memtable_size_threshold {
            if self.config.verbose {
                println!("\n=== Memtable Flush ===");
                println!(
                    "Size: {:.2} MB (threshold: {:.2} MB)",
                    memtable_size as f64 / 1_048_576.0,
                    self.config.memtable_size_threshold as f64 / 1_048_576.0
                );
            }
            self.flush_memtable()?;
        }
        Ok(())
    }

    fn flush_memtable(&mut self) -> io::Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }
        Metrics::incr(&self.metrics.flushes);

        if self.config.verbose {
            println!("Entries: {}", self.memtable.len());
            println!(
                "Average entry size: {:.2} KB",
                (self.memtable.size() as f64 / self.memtable.len() as f64) / 1024.0
            );
        }

        // Create new SSTable at level 0
        let sstable_path = self
            .data_dir
            .join(format!("L0_{}.sst", self.sstable_counter));
        let mut sstable = SSTable::with_cache(sstable_path, self.block_cache.clone())?;

        // Write memtable data (all versions, including tombstones) to SSTable
        let entries: Vec<_> = self
            .memtable
            .iter()
            .map(|(k, seq, v)| (k.clone(), seq, v.clone()))
            .collect();

        sstable.write_versioned(&entries, self.config.compression)?;

        if self.config.verbose {
            println!(
                "Created SSTable: L0_{}.sst ({:.2} MB)",
                self.sstable_counter,
                sstable.size() as f64 / 1_048_576.0
            );
        }

        // Add new SSTable to level 0
        self.sstables.entry(0).or_default().push(sstable);
        self.sstable_counter += 1;

        // Commit the new table to the manifest BEFORE clearing the WAL: if
        // we crash in between, the WAL still holds the data; if we cleared
        // first, an unreferenced table would be garbage-collected as an
        // orphan and the flushed writes lost
        self.persist_manifest()?;

        // Clear memtable and WAL
        self.memtable = MemTable::new();
        self.wal.clear()?;

        // Check if compaction is needed at level 0, unless compaction has
        // been taken off the write path
        if self.config.inline_compaction {
            self.maybe_compact(0)?;
        }

        Ok(())
    }

    /// Run any pending compactions across all levels, regardless of the
    /// `inline_compaction` setting. Returns once every level is within its
    /// threshold.
    pub fn compact_now(&mut self) -> io::Result<()> {
        let mut level = 0;
        loop {
            let max_level = self.sstables.keys().max().copied().unwrap_or(0);
            if level > max_level {
                break;
            }
            self.maybe_compact(level)?;
            level += 1;
        }
        Ok(())
    }

    fn maybe_compact(&mut self, level: usize) -> io::Result<()> {
        let Some(tables) = self.sstables.get(&level) else {
            return Ok(());
        };

        let total_size: usize = tables.iter().map(|t| t.size()).sum();

        if self.config.verbose {
            println!("\n=== Compaction Check: Level {} ===", level);
            println!("Files: {}", tables.len());
            println!("Total size: {:.2} MB", total_size as f64 / 1_048_576.0);
        }

        if !self.compaction_manager.should_compact(level, tables) {
            return Ok(());
        }
        Metrics::incr(&self.metrics.compactions);

        if self.config.verbose {
            println!("\n=== Starting Compaction ===");
            println!("Level: {} -> {}", level, level + 1);
            println!("Files to compact: {}", tables.len());
            for (idx, table) in tables.iter().enumerate() {
                println!("  {}: {:.2} MB", idx, table.size() as f64 / 1_048_576.0);
            }
        }

        let next_level = level + 1;

        // Tombstones can only be dropped when no table at or below the
        // output level could still hold an older value for the key;
        // otherwise dropping the tombstone would resurrect that value.
        let drop_tombstones = self
            .sstables
            .iter()
            .all(|(l, tables)| *l <= level || tables.is_empty());

        // Versions older than the oldest live snapshot's floor can be
        // garbage-collected; with no live snapshots everything collapses to
        // the newest version per key.
        let gc_floor = self.snapshots.oldest().unwrap_or(Seq::MAX);

        // Perform compaction (merge in memory, retaining versions as needed)
        let entries = self
            .compaction_manager
            .compact(tables, drop_tombstones, gc_floor)?;

        // Get paths of tables to delete
        let table_paths: Vec<_> = tables.iter().map(|t| t.get_path().clone()).collect();

        // Write merged entries as a single SSTable at the next level
        let new_path = self
            .data_dir
            .join(format!("L{}_{}.sst", next_level, self.sstable_counter));

        let mut new_table = SSTable::with_cache(new_path, self.block_cache.clone())?;

        if self.config.verbose {
            println!("\n=== Compaction Results ===");
            println!("Unique entries: {}", entries.len());
        }

        new_table.write_versioned(&entries, self.config.compression)?;

        let new_table_size = new_table.size();
        if self.config.verbose {
            println!(
                "New SSTable size: {:.2} MB",
                new_table_size as f64 / 1_048_576.0
            );
        }

        // Update sstables collection
        self.sstables.get_mut(&level).unwrap().clear();
        self.sstables.entry(next_level).or_default().push(new_table);
        self.sstable_counter += 1;

        // The manifest rename is the commit point: crash before it and the
        // half-finished compaction output is an orphan; crash after it and
        // the stale inputs are orphans. Either way startup cleans up.
        self.persist_manifest()?;

        // Now delete the old files (and their cached blocks)
        for path in table_paths {
            fs::remove_file(&path)?;
            if let Some(cache) = &self.block_cache {
                cache.purge_file(&path);
            }
        }

        if self.config.verbose {
            let space_saved = total_size.saturating_sub(new_table_size);
            println!(
                "Space reclaimed: {:.2} MB",
                space_saved as f64 / 1_048_576.0
            );
            println!(
                "Compression ratio: {:.2}%",
                (1.0 - (new_table_size as f64 / total_size as f64)) * 100.0
            );
        }

        // Check if next level needs compaction
        self.maybe_compact(next_level)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn create_test_storage() -> (TempDir, Storage) {
        let temp_dir = TempDir::new().unwrap();
        let storage = Storage::new(temp_dir.path(), false).unwrap();
        (temp_dir, storage)
    }

    fn count_sst_files(dir: &Path) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .filter(|entry| {
                entry
                    .as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .and_then(|s| s.to_str())
                    == Some("sst")
            })
            .count()
    }

    /// Write enough filler data to force at least one memtable flush.
    fn force_flush(storage: &mut Storage, tag: &str) {
        let filler = vec![b'x'; 4096];
        let threshold = StorageConfig::default().memtable_size_threshold;
        for i in 0..(threshold / filler.len() + 2) {
            let key = format!("filler_{}_{}", tag, i).into_bytes();
            storage.put(key, filler.clone()).unwrap();
        }
    }

    #[test]
    fn test_basic_operations() {
        let (_temp_dir, mut storage) = create_test_storage();

        // Test put and get
        let key1 = b"key1".to_vec();
        let value1 = b"value1".to_vec();
        let value2 = b"value2".to_vec();

        storage.put(key1.clone(), value1.clone()).unwrap();
        assert_eq!(storage.get(&key1).unwrap(), Some(value1));

        // Test update
        storage.put(key1.clone(), value2.clone()).unwrap();
        assert_eq!(storage.get(&key1).unwrap(), Some(value2));

        // Test delete
        storage.delete(&key1).unwrap();
        assert_eq!(storage.get(&key1).unwrap(), None);

        // Test get non-existent key
        let nonexistent = b"nonexistent".to_vec();
        assert_eq!(storage.get(&nonexistent).unwrap(), None);
    }

    #[test]
    fn test_memtable_flush() {
        let (temp_dir, mut storage) = create_test_storage();
        let data_dir = temp_dir.path();

        // Write enough data to trigger a flush
        let value = vec![b'x'; 1024]; // 1KB value
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            storage.put(key, value.clone()).unwrap();
        }

        // Verify SSTable was created
        assert!(count_sst_files(data_dir) > 0);

        // Verify data is still accessible
        let test_key = b"key0".to_vec();
        assert_eq!(storage.get(&test_key).unwrap(), Some(value));
    }

    #[test]
    fn test_interleaved_operations() {
        let (_temp_dir, mut storage) = create_test_storage();

        // Perform rapid operations
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();

            storage.put(key.clone(), value.clone()).unwrap();
            assert_eq!(storage.get(&key).unwrap(), Some(value.clone()));

            if i % 2 == 0 {
                storage.delete(&key).unwrap();
            }
        }

        // Verify final state
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();

            if i % 2 == 0 {
                assert_eq!(storage.get(&key).unwrap(), None);
            } else {
                assert_eq!(storage.get(&key).unwrap(), Some(value));
            }
        }
    }

    #[test]
    fn test_recovery() {
        let (temp_dir, mut storage) = create_test_storage();

        // Write some data
        let test_data = [
            (b"key1".to_vec(), b"value1".to_vec()),
            (b"key2".to_vec(), b"value2".to_vec()),
            (b"key3".to_vec(), b"value3".to_vec()),
        ];

        for (key, value) in test_data.iter() {
            storage.put(key.clone(), value.clone()).unwrap();
        }

        // Create new storage instance with same path
        drop(storage);
        let recovered_storage = Storage::new(temp_dir.path(), false).unwrap();

        // Verify all data is accessible
        for (key, value) in test_data.iter() {
            assert_eq!(recovered_storage.get(key).unwrap(), Some(value.clone()));
        }
    }

    #[test]
    fn test_delete_persists_after_flush() {
        let (_temp_dir, mut storage) = create_test_storage();

        let key = b"victim".to_vec();
        storage.put(key.clone(), b"value".to_vec()).unwrap();

        // Force the key into an SSTable, then delete it
        force_flush(&mut storage, "a");
        storage.delete(&key).unwrap();
        assert_eq!(storage.get(&key).unwrap(), None);

        // Force the tombstone itself into an SSTable; the delete must still
        // shadow the older on-disk value
        force_flush(&mut storage, "b");
        assert_eq!(storage.get(&key).unwrap(), None);
    }

    #[test]
    fn test_delete_persists_after_restart() {
        let (temp_dir, mut storage) = create_test_storage();

        let key = b"victim".to_vec();
        storage.put(key.clone(), b"value".to_vec()).unwrap();
        force_flush(&mut storage, "a");
        storage.delete(&key).unwrap();

        drop(storage);
        let recovered = Storage::new(temp_dir.path(), false).unwrap();
        assert_eq!(recovered.get(&key).unwrap(), None);
    }

    #[test]
    fn test_compaction_keeps_newest_value() {
        let (_temp_dir, mut storage) = create_test_storage();

        let key = b"contested".to_vec();

        // Write the key, flush, overwrite it, and flush enough times to
        // trigger a level-0 compaction (4 files)
        for round in 0..5 {
            let value = format!("value_round_{}", round).into_bytes();
            storage.put(key.clone(), value).unwrap();
            force_flush(&mut storage, &format!("round{}", round));
        }

        // After flushes and compaction, the newest value must win
        assert_eq!(storage.get(&key).unwrap(), Some(b"value_round_4".to_vec()));
    }

    #[test]
    fn test_compaction() {
        let (temp_dir, mut storage) = create_test_storage();
        let data_dir = temp_dir.path();

        // Write enough data to trigger multiple flushes and compaction
        let value = vec![b'x'; 2048]; // 2KB value
        for i in 0..2000 {
            let key = format!("key{}", i).into_bytes();
            storage.put(key, value.clone()).unwrap();
        }

        // Count SSTable files per level
        let mut level_counts = [0; 4]; // Count files in levels 0-3
        for entry in fs::read_dir(data_dir).unwrap() {
            let filename = entry.unwrap().file_name();
            let name = filename.to_str().unwrap();
            if !name.ends_with(".sst") {
                continue;
            }
            if let Some(level) = name.chars().find(|c| c.is_ascii_digit()) {
                let level_num = level.to_digit(10).unwrap() as usize;
                if level_num < level_counts.len() {
                    level_counts[level_num] += 1;
                }
            }
        }

        // Verify data distribution across levels
        assert!(level_counts[0] <= 4); // Level 0 should not have too many files
        assert!(level_counts.iter().sum::<i32>() > 0); // Should have some files

        // Verify all data is still accessible
        for i in [0, 500, 1999] {
            let key = format!("key{}", i).into_bytes();
            assert_eq!(storage.get(&key).unwrap(), Some(value.clone()));
        }
    }

    #[test]
    fn test_custom_config_flush_threshold() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            memtable_size_threshold: 1024, // tiny threshold: flush almost immediately
            ..StorageConfig::default()
        };
        let mut storage = Storage::with_config(temp_dir.path(), config).unwrap();

        // A single 2KB write exceeds the threshold and must trigger a flush
        storage.put(b"key".to_vec(), vec![b'x'; 2048]).unwrap();
        assert_eq!(count_sst_files(temp_dir.path()), 1);
        assert_eq!(
            storage.get(&b"key".to_vec()).unwrap(),
            Some(vec![b'x'; 2048])
        );
    }

    #[test]
    fn test_compressed_storage_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            memtable_size_threshold: 8 * 1024, // force several flushes
            compression: Compression::Lz4,
            ..StorageConfig::default()
        };
        let mut storage = Storage::with_config(temp_dir.path(), config.clone()).unwrap();

        for i in 0..500 {
            let key = format!("key{:04}", i).into_bytes();
            let value = format!("value{}", i).repeat(20).into_bytes();
            storage.put(key, value).unwrap();
        }
        storage.delete(&b"key0100".to_vec()).unwrap();

        // Everything readable through compressed SSTables
        assert_eq!(
            storage.get(&b"key0000".to_vec()).unwrap(),
            Some(b"value0".repeat(20).to_vec())
        );
        assert_eq!(storage.get(&b"key0100".to_vec()).unwrap(), None);

        // And after a restart (compression flag read back from headers)
        drop(storage);
        let recovered = Storage::with_config(temp_dir.path(), config).unwrap();
        assert_eq!(
            recovered.get(&b"key0499".to_vec()).unwrap(),
            Some(format!("value{}", 499).repeat(20).into_bytes())
        );
        assert_eq!(recovered.get(&b"key0100".to_vec()).unwrap(), None);
    }

    #[test]
    fn test_prefix_successor() {
        assert_eq!(prefix_successor(b"abc"), Some(b"abd".to_vec()));
        assert_eq!(prefix_successor(b"ab\xff"), Some(b"ac".to_vec()));
        assert_eq!(prefix_successor(b"\xff\xff"), None);
        assert_eq!(prefix_successor(b""), None);
    }

    #[test]
    fn test_scan_across_memtable_and_sstables() {
        let (_temp_dir, mut storage) = create_test_storage();

        // Older values end up in SSTables...
        storage.put(b"user:1".to_vec(), b"old1".to_vec()).unwrap();
        storage.put(b"user:2".to_vec(), b"v2".to_vec()).unwrap();
        storage.put(b"user:3".to_vec(), b"v3".to_vec()).unwrap();
        storage.put(b"other:1".to_vec(), b"x".to_vec()).unwrap();
        force_flush(&mut storage, "scan");

        // ...then get overwritten/deleted in the memtable
        storage.put(b"user:1".to_vec(), b"new1".to_vec()).unwrap();
        storage.delete(&b"user:2".to_vec()).unwrap();
        storage.put(b"user:4".to_vec(), b"v4".to_vec()).unwrap();

        let result = storage.scan_prefix(b"user:").unwrap();
        assert_eq!(
            result,
            vec![
                (b"user:1".to_vec(), b"new1".to_vec()), // newest value wins
                (b"user:3".to_vec(), b"v3".to_vec()),
                (b"user:4".to_vec(), b"v4".to_vec()),
                // user:2 deleted, other:1 outside the prefix
            ]
        );

        // Bounded range: end is exclusive
        let result = storage.scan(b"user:1", b"user:4").unwrap();
        let keys: Vec<_> = result.into_iter().map(|(k, _)| k).collect();
        assert_eq!(keys, vec![b"user:1".to_vec(), b"user:3".to_vec()]);
    }

    #[test]
    fn test_scan_survives_compaction_and_restart() {
        let (temp_dir, mut storage) = create_test_storage();

        for i in 0..50 {
            storage
                .put(format!("k{:03}", i).into_bytes(), vec![b'v'; 64])
                .unwrap();
        }
        storage.delete(&b"k025".to_vec()).unwrap();
        // Several flushes to spread data over levels and trigger compaction
        for round in 0..5 {
            force_flush(&mut storage, &format!("scanr{}", round));
        }

        drop(storage);
        let storage = Storage::new(temp_dir.path(), false).unwrap();

        let result = storage.scan(b"k000", b"k050").unwrap();
        assert_eq!(result.len(), 49); // 50 keys minus the deleted one
        assert!(result.iter().all(|(k, _)| k.as_slice() != b"k025"));
        // Sorted by key
        assert!(result.windows(2).all(|w| w[0].0 < w[1].0));
    }

    #[test]
    fn test_batched_wal_sync_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            wal_sync: WalSync::Batched { every_n_writes: 16 },
            ..StorageConfig::default()
        };
        let mut storage = Storage::with_config(temp_dir.path(), config.clone()).unwrap();
        for i in 0..40 {
            storage
                .put(format!("key{}", i).into_bytes(), b"value".to_vec())
                .unwrap();
        }
        storage.delete(&b"key5".to_vec()).unwrap();

        // Clean shutdown syncs the batched tail; everything replays
        drop(storage);
        let recovered = Storage::with_config(temp_dir.path(), config).unwrap();
        assert_eq!(
            recovered.get(&b"key39".to_vec()).unwrap(),
            Some(b"value".to_vec())
        );
        assert_eq!(recovered.get(&b"key5".to_vec()).unwrap(), None);
    }

    #[test]
    fn test_block_cache_correctness_under_compaction() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            memtable_size_threshold: 8 * 1024,
            compaction_size_threshold: 32 * 1024,
            block_cache_size: 64 * 1024, // small cache with eviction pressure
            ..StorageConfig::default()
        };
        let mut storage = Storage::with_config(temp_dir.path(), config).unwrap();

        // Interleave writes and reads so cached blocks live across flushes
        // and compactions that delete their underlying files
        for i in 0..600 {
            let key = format!("key{:04}", i % 200).into_bytes();
            storage
                .put(key.clone(), format!("v{}", i).into_bytes())
                .unwrap();
            let probe = format!("key{:04}", (i * 7) % 200).into_bytes();
            storage.get(&probe).unwrap();
        }

        // Latest value per key must win despite caching
        for k in 0..200 {
            let key = format!("key{:04}", k).into_bytes();
            let latest = (0..600).rev().find(|i| i % 200 == k).unwrap();
            assert_eq!(
                storage.get(&key).unwrap(),
                Some(format!("v{}", latest).into_bytes()),
                "key {}",
                k
            );
        }
    }

    #[test]
    fn test_deferred_compaction_via_compact_now() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            memtable_size_threshold: 8 * 1024,
            inline_compaction: false,
            ..StorageConfig::default()
        };
        let mut storage = Storage::with_config(temp_dir.path(), config).unwrap();

        // Enough flushes that level 0 exceeds its file limit
        for i in 0..800 {
            storage
                .put(format!("key{:04}", i).into_bytes(), vec![b'v'; 64])
                .unwrap();
        }
        let l0_before = fs::read_dir(temp_dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("L0_")
            })
            .count();
        assert!(
            l0_before >= 4,
            "inline compaction disabled: L0 should accumulate files, got {}",
            l0_before
        );

        storage.compact_now().unwrap();

        let l0_after = fs::read_dir(temp_dir.path())
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("L0_")
            })
            .count();
        assert!(l0_after < 4, "compact_now should drain level 0");

        // All data still readable after manual compaction
        for i in [0, 400, 799] {
            let key = format!("key{:04}", i).into_bytes();
            assert_eq!(storage.get(&key).unwrap(), Some(vec![b'v'; 64]));
        }
    }

    #[test]
    fn test_manifest_written_on_flush() {
        let (temp_dir, mut storage) = create_test_storage();
        assert!(temp_dir.path().join("MANIFEST").exists()); // written at open

        force_flush(&mut storage, "m");
        let manifest = fs::read_to_string(temp_dir.path().join("MANIFEST")).unwrap();
        assert!(
            manifest.lines().any(|l| l.starts_with("0 ")),
            "manifest should list a level-0 table:\n{}",
            manifest
        );
    }

    #[test]
    fn test_orphan_sstables_removed_on_startup() {
        let (temp_dir, mut storage) = create_test_storage();
        storage.put(b"real".to_vec(), b"data".to_vec()).unwrap();
        force_flush(&mut storage, "o");
        drop(storage);

        // Fabricate leftovers of a crashed flush/compaction: a garbage
        // half-written table and a valid table that isn't in the manifest
        fs::write(temp_dir.path().join("L0_9998.sst"), b"garbage").unwrap();
        let mut stray = SSTable::new(temp_dir.path().join("L9_9999.sst")).unwrap();
        stray
            .write(&[(b"ghost".to_vec(), Some(b"boo".to_vec()))])
            .unwrap();

        let recovered = Storage::new(temp_dir.path(), false).unwrap();
        assert!(!temp_dir.path().join("L0_9998.sst").exists());
        assert!(!temp_dir.path().join("L9_9999.sst").exists());
        assert_eq!(recovered.get(&b"ghost".to_vec()).unwrap(), None);
        assert_eq!(
            recovered.get(&b"real".to_vec()).unwrap(),
            Some(b"data".to_vec())
        );
    }

    #[test]
    fn test_legacy_directory_without_manifest() {
        let (temp_dir, mut storage) = create_test_storage();
        storage.put(b"key".to_vec(), b"value".to_vec()).unwrap();
        force_flush(&mut storage, "l");
        drop(storage);

        // Simulate a data directory created before manifests existed
        fs::remove_file(temp_dir.path().join("MANIFEST")).unwrap();

        let recovered = Storage::new(temp_dir.path(), false).unwrap();
        assert_eq!(
            recovered.get(&b"key".to_vec()).unwrap(),
            Some(b"value".to_vec())
        );
        // The fallback scan writes a fresh manifest
        assert!(temp_dir.path().join("MANIFEST").exists());
    }
}
