# LSM Tree + SSTable Database in Rust

[![Rust CI](https://github.com/zvdy/lsm-rust/actions/workflows/rust.yml/badge.svg)](https://github.com/zvdy/lsm-rust/actions/workflows/rust.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

An educational, batteries-included implementation of a Log-Structured Merge
Tree (LSM tree) storage engine in Rust — usable as a library or through the
demo binary. It features a durable write-ahead log, tombstone-based deletes,
multi-level compaction, Bloom filters, sparse index blocks, and optional LZ4
block compression.

## Features

- **Durable writes** — every operation is appended to a write-ahead log and
  fsynced before it is acknowledged; torn tail entries are discarded on
  recovery
- **Tombstone deletes** — deletions persist across flushes, compaction, and
  restarts, and shadow older values on disk
- **Multi-level compaction** — newest-value-wins merging with deduplication;
  tombstones are garbage-collected once no older data can be shadowed
- **Bloom filters** — per-table probabilistic filters skip SSTables that
  cannot contain a key
- **Sparse index blocks** — point lookups binary-search a block index and
  read a single block instead of scanning the file
- **Optional LZ4 compression** — per-block compression keeps point reads
  cheap while shrinking tables
- **Snapshot isolation (MVCC)** — every write is sequence-numbered, and a
  `Snapshot` reads a consistent view as of the moment it was taken, unaffected
  by later writes, deletes, flushes, or compactions
- **Time-travel reads** — record a sequence number as a logical checkpoint
  and later reopen the store's state as of that point with `snapshot_at`; the
  sequence is persisted in the manifest, so checkpoints survive restarts
- **Atomic write batches** — group multiple puts and deletes into one commit
  that is durable and visible all-or-nothing (a crash recovers the whole
  batch or none of it)
- **Concurrent access** — a cloneable `SharedStorage` handle with concurrent
  reads and serialized writes
- **Range and prefix scans** — ordered scans with newest-wins merging across
  the memtable and all levels, reading only the blocks that intersect the
  range
- **Block cache** — a shared LRU cache of hot (decompressed) blocks makes
  repeated point reads sub-microsecond
- **WAL group commit** — optionally batch fsyncs across writes for much
  higher write throughput
- **Background compaction** — compaction can be taken off the write path and
  run on a dedicated thread
- **Crash-atomic manifest** — a MANIFEST file is the authoritative record of
  live SSTables; interrupted flushes and compactions are cleaned up as
  orphans on the next startup
- **Redis-protocol server** — `lsm-rust serve` exposes the store over RESP,
  so `redis-cli` and Redis client libraries work out of the box
- **Prometheus metrics** — `Storage::stats()` exposes operation counters and
  live gauges, and `lsm-rust serve --metrics-addr` serves them at `/metrics`
  in the Prometheus text exposition format for scraping
- **Configurable** — flush/compaction thresholds, level growth, and
  compression are tunable via `StorageConfig`
- **Benchmarked and tested** — a criterion suite covers the hot paths, and a
  dedicated crash-recovery test suite (including model-based random
  workloads across restarts) guards correctness

## Architecture

### Write path

```mermaid
flowchart LR
    W([PUT / DELETE]) --> WAL["WAL<br/>append + fsync"]
    WAL --> MT["MemTable<br/>(sorted, in memory)"]
    MT -- "size ≥ threshold" --> FL["Flush"]
    FL --> L0["Level 0 SSTable"]
    L0 -- "≥ 4 files at L0" --> CP["Compaction"]
    CP --> LN["Level N+1 SSTable"]
    FL -. "clear WAL + MemTable" .-> WAL
```

1. Each write is appended to the write-ahead log and fsynced, so it survives
   a crash the moment the call returns.
2. The entry (or a tombstone, for deletes) is inserted into the in-memory
   MemTable.
3. When the MemTable exceeds its size threshold, it is flushed to disk as an
   immutable Level 0 SSTable, and the WAL is cleared.
4. When a level fills up, compaction merges its tables into the next level.

### Read path

```mermaid
flowchart TD
    G([GET key]) --> MT{"MemTable<br/>entry?"}
    MT -- "value" --> RV([Return value])
    MT -- "tombstone" --> RN([Return None])
    MT -- "absent" --> IT["Next SSTable<br/>newest → oldest, L0 → LN"]
    IT --> BF{"Bloom filter:<br/>might contain?"}
    BF -- "no" --> IT
    BF -- "yes" --> IX["Binary-search sparse index,<br/>read + scan one block"]
    IX -- "value" --> RV
    IX -- "tombstone" --> RN
    IX -- "absent" --> IT
    IT -- "no tables left" --> RN
```

The MemTable always has the freshest state, so it is consulted first; a
tombstone found anywhere along the way ends the search immediately, which is
what keeps deleted keys deleted even when older SSTables still hold values
for them.

### Compaction

```mermaid
flowchart LR
    subgraph LN["Level N"]
        T1["SSTable (oldest)"]
        T2["SSTable"]
        T3["SSTable (newest)"]
    end
    T1 --> M
    T2 --> M
    T3 --> M
    M["Merge:<br/>• newest value wins<br/>• deduplicate keys<br/>• drop tombstones at last level"]
    M --> OUT["Level N+1 SSTable"]
```

Level 0 compacts once it holds 4 files; deeper levels compact on a size
threshold that grows by a configurable multiplier per level. Tombstones are
only dropped when no level at or below the output could still contain an
older value for the key.

## On-disk formats

**SSTable (versioned, v3)** — v3 stores a sequence number per entry (MVCC).
v2 (no sequence) and pre-header legacy files remain readable through fallback
paths, with their entries treated as sequence 0:

```text
┌───────┬─────────┬───────┬─────────────┬─────────────┬───────────────────┐
│ magic │ version │ flags │ bloom       │ sparse      │ data blocks       │
│ LSMT  │ u8 = 3  │ u8    │ len + bytes │ index       │ key-aligned, one  │
│       │         │       │             │ len + bytes │ block per lookup  │
└───────┴─────────┴───────┴─────────────┴─────────────┴───────────────────┘

index entry:  [first_key_len u32][first_key][block_offset u64][block_len u32]
data entry:   [key_len u32][key][seq u64][value_len u32][value]
tombstone:    [key_len u32][key][seq u64][0xFFFFFFFF]
```

Within a block, entries are sorted by `(key ascending, seq descending)` and a
key's versions are never split across blocks, so a snapshot lookup reads a
single block and returns the newest version at or below its sequence.

**WAL** — `[op u8][key_len u32][key][value_len u32][value]` per entry, where
`op` is 0 for put (with value) and 1 for delete (without). An atomic write
batch is one framed record — `[2][count u32]` followed by `count` entries —
recovered whole or not at all. Replay tolerates a truncated final record.

## Usage

### As a library

```rust
use lsm_rust::{Compression, SharedStorage, Storage, StorageConfig};

fn main() -> std::io::Result<()> {
    // Simple: defaults + verbosity flag
    let mut db = Storage::new("./data", false)?;
    db.put(b"name".to_vec(), b"Jane Doe".to_vec())?;
    assert_eq!(db.get(&b"name".to_vec())?, Some(b"Jane Doe".to_vec()));
    db.delete(&b"name".to_vec())?;

    // Tuned: explicit configuration
    let db = Storage::with_config(
        "./data2",
        StorageConfig {
            memtable_size_threshold: 4 * 1024 * 1024, // 4 MB
            compression: Compression::Lz4,
            ..StorageConfig::default()
        },
    )?;

    // Concurrent: cloneable handle, safe to share across threads
    let shared = db.into_shared();
    let handle = shared.clone();
    std::thread::spawn(move || handle.get(&b"key".to_vec())).join().unwrap()?;
    let _ = SharedStorage::new("./data3", false)?; // or construct directly

    // Ordered scans (end-exclusive ranges, or by prefix)
    let _users = shared.scan_prefix(b"user:")?;
    let _range = shared.scan(b"user:100", b"user:200")?;

    // Snapshot isolation: a consistent view unaffected by later writes
    let mut db = Storage::new("./data4", false)?;
    db.put(b"k".to_vec(), b"v1".to_vec())?;
    let snap = db.snapshot();
    db.put(b"k".to_vec(), b"v2".to_vec())?; // committed after the snapshot
    assert_eq!(db.get_at(&snap, &b"k".to_vec())?, Some(b"v1".to_vec())); // snapshot view
    assert_eq!(db.get(&b"k".to_vec())?, Some(b"v2".to_vec())); // latest view

    // Time-travel: revisit the store as of a recorded sequence checkpoint
    let checkpoint = db.current_sequence(); // save this (it survives restarts)
    db.put(b"k".to_vec(), b"v3".to_vec())?;
    let past = db.snapshot_at(checkpoint)?;
    assert_eq!(db.get_at(&past, &b"k".to_vec())?, Some(b"v2".to_vec()));

    // Atomic write batch: all-or-nothing, one commit
    let mut batch = lsm_rust::WriteBatch::new();
    batch.put(b"a".to_vec(), b"1".to_vec()).delete(b"k".to_vec());
    db.write_batch(batch)?;

    // Operational metrics, renderable in Prometheus text format
    let stats = db.stats();
    println!("puts={} sstables={}", stats.puts_total, stats.total_sstables());
    print!("{}", stats.to_prometheus());

    // Background compaction off the write path
    let compactor = shared.spawn_compactor(std::time::Duration::from_secs(1));
    drop(compactor); // stops the thread

    Ok(())
}
```

### Demo binary

```bash
cargo run --release -- demo      # scripted demo: basic ops + compaction run
cargo run --release -- demo -v   # with verbose engine logging
```

### Server mode (Redis protocol)

```bash
cargo run --release -- serve --addr 127.0.0.1:6379 --data ./data
```

Then use any Redis client:

```text
$ redis-cli
127.0.0.1:6379> SET user:1 "Jane"
OK
127.0.0.1:6379> GET user:1
"Jane"
127.0.0.1:6379> KEYS user:*
1) "user:1"
127.0.0.1:6379> DEL user:1
(integer) 1
```

Supported commands: `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`,
`KEYS <prefix>*` (trailing-star globs), `QUIT`.

### Prometheus metrics

Pass `--metrics-addr` to also expose a `/metrics` endpoint alongside the RESP
server:

```bash
cargo run --release -- serve --addr 127.0.0.1:6379 --metrics-addr 127.0.0.1:9898
```

```text
$ curl -s http://127.0.0.1:9898/metrics
# HELP lsm_puts_total Total put operations applied.
# TYPE lsm_puts_total counter
lsm_puts_total 42
# HELP lsm_sequence Highest MVCC sequence number assigned so far.
# TYPE lsm_sequence gauge
lsm_sequence 51
# HELP lsm_sstables Number of SSTable files per level.
# TYPE lsm_sstables gauge
lsm_sstables{level="0"} 2
...
```

Exposed metrics include operation counters (`lsm_puts_total`,
`lsm_deletes_total`, `lsm_batches_total`, `lsm_gets_total`, `lsm_scans_total`,
`lsm_flushes_total`, `lsm_compactions_total`) and gauges for the MVCC sequence,
live snapshots, memtable occupancy, and per-level SSTable counts and sizes.
Point a Prometheus scrape at the endpoint, or read the same snapshot in-process
via `Storage::stats()` / `SharedStorage::stats()`.

Live endpoint after ~9,000 operations (writes, reads, deletes, prefix scans)
that drove 11 flushes and 3 compactions:

![lsm-rust Prometheus metrics endpoint](docs/images/prometheus-metrics-endpoint.png)

### Docker

```bash
docker build -t lsm-rust .
docker run -it lsm-rust
```

## Configuration

| `StorageConfig` field | Default | Meaning |
| --- | --- | --- |
| `memtable_size_threshold` | 512 KB | Flush the MemTable to a Level 0 SSTable at this size |
| `compaction_size_threshold` | 1 MB | Base size threshold for level compaction |
| `level_multiplier` | 4 | Growth factor of the threshold per level (`base * multiplier^N`) |
| `level0_file_limit` | 4 | Compact Level 0 at this many files |
| `compression` | `None` | `Compression::Lz4` enables per-block LZ4 |
| `wal_sync` | `Always` | `Batched { every_n_writes }` amortizes fsyncs (group commit) |
| `block_cache_size` | 4 MB | Shared LRU block cache capacity (0 disables) |
| `inline_compaction` | `true` | Disable to drive compaction via `compact_now()` or `spawn_compactor()` |
| `verbose` | `false` | Engine progress logging to stdout |

## Performance

Indicative numbers from the criterion suite on a Linux container (release
build, 128-byte values; run `cargo bench` for your own hardware):

| Operation | Time | Notes |
| --- | --- | --- |
| `put` / `delete` | ~0.9 ms | Dominated by the per-write WAL fsync |
| `get` (MemTable hit) | ~210 ns | Pure in-memory BTreeMap lookup |
| `get` (SSTable hit) | ~4 µs (~1 µs cached) | Index binary search + one block read |
| `get` (missing key) | ~400 ns | Bloom filters avoid disk almost always |

Criterion writes HTML reports to `target/criterion/` and compares against
previous runs, so regressions in the hot paths show up in review.

## Testing

```bash
cargo test              # unit + integration + doc tests
cargo test --test recovery  # crash-recovery suite only
cargo bench             # criterion benchmarks
```

The recovery suite exercises restarts with multi-level data, torn WAL tails,
delete persistence at every lifecycle stage, compressed stores, and a
deterministic model-based random workload verified across restarts.

## Project structure

```text
lsm-rust/
├── src/
│   ├── lib.rs            # Crate root: public API and docs
│   ├── main.rs           # Demo binary
│   ├── storage/
│   │   ├── mod.rs        # Engine: WAL + MemTable + levels + compaction
│   │   └── shared.rs     # SharedStorage: thread-safe handle
│   ├── memtable/mod.rs   # Sorted in-memory table with tombstones
│   ├── sstable/
│   │   ├── mod.rs        # Versioned on-disk tables: bloom, index, blocks
│   │   └── compaction.rs # Level merge policy and merging
│   ├── bloom/mod.rs      # Bloom filter
│   └── wal/mod.rs        # Write-ahead log
├── benches/storage.rs    # Criterion benchmarks
└── tests/recovery.rs     # Crash-recovery integration tests
```

## Roadmap

- [x] SSTable compaction
- [x] Bloom filters for faster lookups
- [x] Index blocks in SSTables
- [x] Concurrent access support
- [x] Configuration options
- [x] Benchmarking suite
- [x] Compression support (LZ4)
- [x] Recovery testing
- [x] Versioned on-disk format
- [x] Manifest file for atomic table-set updates
- [x] Network server mode (RESP / Redis protocol)
- [x] Range scans / iterators
- [x] Background (off-thread) compaction
- [x] WAL group commit / batched fsync
- [x] Block cache for hot reads
- [x] Snapshot isolation / MVCC
- [x] Write batches (atomic multi-key commits)
- [x] Metrics endpoint (Prometheus text format)
- [x] Time-travel reads (open a snapshot at a persisted sequence)

## Community and Contributing

Contributions of all kinds are welcome — bug reports, documentation, and
code. Please read:

- [CONTRIBUTING.md](CONTRIBUTING.md) — development setup and PR process
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) — community standards
- [SECURITY.md](SECURITY.md) — how to report vulnerabilities privately
- [GOVERNANCE.md](GOVERNANCE.md) and [MAINTAINERS.md](MAINTAINERS.md) — how the project is run

## License

MIT License
