# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Everything below is on `main` and unreleased; `0.1.0` has not been published to
crates.io yet.

### Added

- **Scans skip tables they cannot match.** A scan opened a cursor over every
  SSTable in the store; it now compares each table's first and last key against
  the requested range and skips those that cannot intersect it, saving a cursor,
  a heap slot and — for a table lying entirely below the scan — a block read
  that returned only keys the merge discarded. The check is exact rather than
  estimated, and a table whose range is unknown is never skipped. New
  `lsm_scan_tables_pruned_total` metric.
- **Per-key expiry (TTL).** `put_with_ttl` / `put_with_expiry` on `Storage`,
  `SharedStorage`, `WriteBatch` and `Transaction` attach a deadline to a write;
  `expiry()` reports it, distinguishing "no such key", "no deadline" and "expires
  at". Deadlines are absolute Unix milliseconds resolved at write time, so they
  survive restarts without being refreshed. An expired version keeps *shadowing*
  older versions of the same key rather than vanishing — compaction rewrites it
  as a tombstone, so expiry can never uncover the value it replaced — and the
  usual tombstone rules then reclaim it. RESP gains `SET key value EX|PX n` and
  `TTL key` with Redis's `-2`/`-1`/seconds convention. New `lsm_expired_total`
  metric. A snapshot isolates a reader from writes but not from the clock: a key
  stops being visible through an existing snapshot once its deadline passes.
- **Cost-aware compaction.** A merge earns its cost by collapsing keys that
  appear in more than one table. When a level's tables are mutually disjoint
  there are none, so the level is now *promoted* to the next level without
  reading or rewriting a byte, instead of being merged into an identical copy.
  Append-only and time-ordered workloads benefit most: in the test suite's
  ascending-key workload, 12 of 14 compaction runs become promotions. Levels
  with real overlap still merge exactly as before, and a lone table still
  merges with itself so version collapsing and tombstone dropping are
  unaffected. New `lsm_compaction_moves_total` metric, and
  `SSTable::key_range()` / `CompactionManager::max_overlap_depth()` expose the
  signal behind the decision.
- **Consistent checkpoints.** `Storage::checkpoint(dir)` /
  `SharedStorage::checkpoint(dir)` write a point-in-time copy of the store
  under the exclusive lock, so the captured tables, write-ahead log and
  manifest are all from the same instant. SSTables are hard-linked (immutable,
  so sharing the inode is safe and nearly free); the WAL is copied, since it is
  still being appended to. A checkpoint directory is itself a data directory —
  restore by opening it. `CheckpointInfo` reports what was captured and how
  much disk was genuinely duplicated. New `lsm_checkpoints_total` metric.
- **A unified error type.** Every fallible call now returns
  `lsm_rust::Result<T>`, whose `Error` separates `Corruption`, `Conflict`,
  `InvalidArgument` and `Io` — so a caller can tell a losing transaction from a
  damaged file without matching on error strings. `Error::is_retriable()` and
  `Error::is_corruption()` classify a failure without a `match`.
- **Concurrent transactions.** Optimistic `begin`/`commit`/`rollback` with
  read-your-own-writes, buffered writes that are invisible until commit, and
  conflict detection at commit time with retriable aborts.
  `Isolation::Serializable` (the default) catches write-write, read-write and
  phantom conflicts; `Isolation::Snapshot` catches write-write only.
  `SharedStorage::transaction` retries aborts for you.
- **End-to-end checksums.** A CRC-32 on every SSTable section, every SSTable
  data block, and every write-ahead log record, verified on read, so on-disk
  corruption surfaces as an error instead of silently wrong data. SSTable
  format v4; a corruption test suite damages files on disk to prove detection.
- **Time-travel reads.** `current_sequence()` records a logical checkpoint and
  `snapshot_at(seq)` reopens the store's state as of that point. The sequence
  is persisted in the manifest, so checkpoints survive restarts.
- **Prometheus metrics.** `Storage::stats()` / `SharedStorage::stats()` return
  operation counters and live gauges; `lsm-rust serve --metrics-addr` exposes
  them at `/metrics` in the text exposition format.
- **Atomic write batches.** `WriteBatch` commits multiple puts and deletes at a
  single sequence number and as one framed WAL record — visible and durable
  all-or-nothing.
- **MVCC snapshot isolation.** Every write is sequence-numbered; a `Snapshot`
  reads a consistent view unaffected by later writes, flushes, or compactions,
  with a snapshot-driven garbage-collection floor in compaction.
- **Redis-protocol server.** `lsm-rust serve` speaks RESP2, so `redis-cli` and
  Redis client libraries work against the store.
- **Crash-atomic manifest.** A `MANIFEST` file is the authoritative record of
  live SSTables and the sequence high-water mark; interrupted flushes and
  compactions are cleaned up as orphans on startup.
- **Block cache.** A shared LRU cache of decompressed blocks.
- **Background compaction.** `spawn_compactor()` takes compaction off the write
  path; `compact_now()` drives it manually.
- **WAL group commit.** `WalSync::Batched { every_n_writes }` amortizes fsyncs.
- **Range and prefix scans**, with newest-wins merging across the memtable and
  every level.
- **Concurrent access** via a cloneable `SharedStorage` handle.
- **LZ4 block compression**, selectable through `StorageConfig`.
- **Sparse index blocks** in SSTables, so a point lookup reads one block.
- **Configuration** through `StorageConfig` (flush and compaction thresholds,
  level growth, compression, WAL sync policy, cache size).
- **Benchmarks** (criterion) and a crash-recovery test suite including a
  model-based randomized workload verified across restarts.
- **Project tooling**: a `Makefile` mirroring CI, an architecture deep-dive in
  `docs/ARCHITECTURE.md`, governance and community docs, and a hardened CI
  workflow with a single `ci` gate job.
- **Supply-chain and release tooling**: crates.io package metadata, a declared
  and CI-verified MSRV, `cargo-deny` license/advisory policy, an OpenSSF
  Scorecard workflow, and a tag-driven release workflow.

### Changed

- **Breaking:** the public API returns `lsm_rust::Result<T>` instead of
  `std::io::Result<T>`, and `TransactionError` is gone — its `Conflict` variant
  is now `Error::Conflict` and its `Io` variant is now `Error::Io`.
  `Error` converts to and from `std::io::Error` (`Corruption` maps to
  `InvalidData`, `Conflict` to `WouldBlock`, `InvalidArgument` to
  `InvalidInput`, and `Io` passes through unchanged), so callers whose own
  functions still return `std::io::Result` compile without edits. Callers that
  named `TransactionError` should use `lsm_rust::Error`; callers that matched
  on `err.kind()` should match on the variant instead.
- SSTable on-disk format evolved to v5 (per-entry expiry, plus an 8-byte
  header field holding the earliest deadline in the table so compaction can see
  there is something to reclaim without reading the data). v4, v3, v2, and
  pre-header legacy files remain readable; entries without a deadline are the
  same size they were. The write-ahead log gained a matching record type for a
  put that carries a deadline, and older logs still replay.
- Compaction now merges rather than promotes a level holding expired versions.
  Promotion never reads the data, so it can reclaim nothing — without this an
  append-only workload with TTLs, which is exactly the shape that promotes most
  eagerly, would keep expired data indefinitely.
- WAL records are now written in a checksummed frame; older unframed records
  still replay.

### Fixed

- **The memtable's tracked size no longer drifts towards zero.** Replacing the
  same `(key, seq)` subtracted the old value's bytes without adding the new
  value's. Every op in a write batch commits at one sequence number, so a batch
  that writes one key twice — a documented, supported usage — hit this path: in
  a reproduction, 200 KiB of live data reported **50 bytes** and triggered
  **zero flushes** against an 8 KiB threshold. Since that threshold is the
  memtable's memory bound, it could grow without limit, and
  `lsm_memtable_bytes` reported a gauge far below reality. Values were always
  read back correctly.
- The wall-clock TTL tests no longer race the write they are timing. They
  asserted a key was still present within 80 ms of a `put_with_ttl`, but every
  put fsyncs, and on a slow CI runner that can outlast the deadline — the key
  was correctly hidden and the test failed. Presence is now checked against a
  deadline nothing can outrun, and expiry against one already past, so neither
  direction depends on how fast the machine is.
- `SSTable::key_range` no longer assumes a legacy (pre-versioned) file is
  sorted, taking the true minimum and maximum instead of the first and last
  entry. Point lookups already read legacy files with `sorted: false`; the
  range had been trusting an ordering nothing guarantees, and an understated
  range could have let compaction or a scan skip a table holding matching keys.

### Security

- Dependencies are audited on every CI run (`cargo audit`) and checked against
  a license and advisory policy (`cargo deny`).

[Unreleased]: https://github.com/zvdy/lsm-rust/commits/main
