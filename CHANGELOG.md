# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Everything below is on `main` and unreleased; `0.1.0` has not been published to
crates.io yet.

### Added

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
- SSTable on-disk format evolved to v4 (checksums). v3, v2, and pre-header
  legacy files remain readable.
- WAL records are now written in a checksummed frame; older unframed records
  still replay.

### Security

- Dependencies are audited on every CI run (`cargo audit`) and checked against
  a license and advisory policy (`cargo deny`).

[Unreleased]: https://github.com/zvdy/lsm-rust/commits/main
