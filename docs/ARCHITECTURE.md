# Architecture

`lsm-rust` is a Log-Structured Merge (LSM) tree storage engine. Writes are
made durable in a write-ahead log and buffered in an in-memory table; that
table is periodically flushed to immutable, sorted on-disk files (SSTables),
which background compaction merges into deeper levels. Reads consult the
in-memory table first, then SSTables from newest to oldest, using per-table
Bloom filters and sparse indexes to avoid unnecessary I/O.

## Components

```mermaid
flowchart TB
    subgraph frontends["Front ends"]
        RESP["RespServer<br/>(Redis protocol)"]
        MET["MetricsServer<br/>(/metrics)"]
    end

    SH["SharedStorage<br/>Arc&lt;RwLock&lt;Storage&gt;&gt;"]
    ST["Storage<br/>(engine orchestrator)"]

    subgraph engine["Engine internals"]
        WAL["WAL<br/>write-ahead log"]
        MT["MemTable<br/>multi-version, sorted"]
        SST["SSTables<br/>per level"]
        BC["BlockCache<br/>shared LRU"]
        MAN["Manifest<br/>live table set + last seq"]
        SR["SnapshotRegistry<br/>live read sequences"]
    end

    RESP --> SH
    MET --> SH
    SH --> ST
    ST --> WAL
    ST --> MT
    ST --> SST
    ST --> MAN
    ST --> SR
    SST --> BC
```

- **Storage** is the engine: it owns the WAL, the active MemTable, the leveled
  SSTables, the manifest, the snapshot registry, and the block cache, and it
  coordinates the write, read, flush, and compaction paths.
- **SharedStorage** wraps `Storage` in an `Arc<RwLock<…>>` for concurrent use:
  reads take a shared lock and run in parallel; writes take the exclusive lock
  and serialize.
- **RespServer** and **MetricsServer** are optional network front ends over a
  `SharedStorage` — the Redis-protocol endpoint and the Prometheus scrape
  endpoint respectively.

## Write path

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

1. Each write is appended to the write-ahead log and fsynced, so it survives a
   crash the moment the call returns.
2. The entry (or a tombstone, for deletes) is inserted into the in-memory
   MemTable at the next monotonic sequence number.
3. When the MemTable exceeds its size threshold, it is flushed to disk as an
   immutable Level 0 SSTable, and the WAL is cleared.
4. When a level fills up, compaction merges its tables into the next level.

An **atomic write batch** takes one sequence number for all of its operations
and is written to the WAL as a single framed record, so it is both visible and
durable all-or-nothing.

## Read path

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
what keeps deleted keys deleted even when older SSTables still hold values for
them.

### Snapshots and time-travel

Every version is tagged with its sequence number, and a read carries a
*snapshot sequence*: it returns, for each key, the newest version whose
sequence is at or below the snapshot. A `Snapshot` captures the current
sequence; `snapshot_at(seq)` captures an arbitrary historical one. Correctness
rests on an LSM invariant — for a given key, a higher sequence always lives in
a newer (shallower) table — so a newest-to-oldest scan can stop at the first
version the snapshot can see.

Holding a snapshot registers its sequence in the **SnapshotRegistry**, which
publishes the oldest live sequence as a garbage-collection floor: compaction
keeps every version newer than the floor and the newest version at or below it,
and drops the rest.

## Compaction

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
    M["Merge:<br/>• newest value wins<br/>• deduplicate keys<br/>• honor the GC floor<br/>• drop tombstones at last level"]
    M --> OUT["Level N+1 SSTable"]
```

Level 0 compacts once it holds `level0_file_limit` files (4 by default);
deeper levels compact on a size threshold that grows by `level_multiplier` per
level. Tombstones are only dropped when no level at or below the output could
still contain an older value for the key. Compaction can run inline on the
write path or on a dedicated background thread.

## On-disk formats

### SSTable (versioned, v4)

v4 adds a CRC-32 to every section and every data block. v3 (sequence numbers,
no checksums), v2 (no sequence numbers) and pre-header legacy files remain
readable through fallback paths; v2 and legacy entries are treated as
sequence 0.

```text
┌───────┬─────────┬───────┬───────────────────┬───────────────────┬─────────────────┐
│ magic │ version │ flags │ bloom             │ sparse index      │ data blocks     │
│ LSMT  │ u8 = 4  │ u8    │ len + crc + bytes │ len + crc + bytes │ crc + payload   │
└───────┴─────────┴───────┴───────────────────┴───────────────────┴─────────────────┘

section:      [len u32][crc32 u32][body]
data block:   [crc32 u32][payload]        payload = entries, LZ4-compressed if flagged
index entry:  [first_key_len u32][first_key][block_offset u64][block_len u32]
data entry:   [key_len u32][key][seq u64][value_len u32][value]
tombstone:    [key_len u32][key][seq u64][0xFFFFFFFF]
```

Within a block, entries are sorted by `(key ascending, seq descending)` and a
key's versions are never split across blocks, so a snapshot lookup reads a
single block and returns the newest version at or below its sequence. The
`flags` byte records per-block compression (e.g. LZ4).

A block's checksum covers the bytes **as stored** — after compression — so
corruption is caught before the decompressor is handed the data. Section
checksums are verified while opening the table, so a damaged index or bloom
filter fails loudly at open instead of silently mis-routing later lookups.

### Write-ahead log (WAL)

```text
record: [3][crc32 u32][body_len u32][body]

body — one of:
  entry:  [op u8][key_len u32][key][value_len u32][value]
  batch:  [2][count u32] followed by `count` entries
```

`op` is `0` for a put (with value) and `1` for a delete (without). An atomic
write batch is a body introduced by marker `2`, recovered whole or not at all.

Every record written is wrapped in the checksummed frame (marker `3`); the
older unframed markers are still replayed so a log written by an earlier build
stays readable. Replay tolerates a truncated final record, and a *checksum*
failure on the final record is treated the same way — a torn write is
indistinguishable from truncation, so it is dropped and recovery continues. A
checksum failure anywhere earlier in the log is real corruption of durable data
and is reported as an error.

### Manifest

The `MANIFEST` file is the authoritative record of which SSTables are live and
how far the sequence counter has advanced (`last_seq`). It is rewritten
atomically at every flush and compaction commit point; on startup, `.sst` files
not referenced by the manifest are treated as orphans from an interrupted
operation and removed. The persisted `last_seq` is what lets a recorded
sequence checkpoint remain meaningful across restarts for time-travel reads.

## Concurrency model

`SharedStorage` uses a single `RwLock` around the whole engine: coarse-grained,
but simple and correct. Reads (`get`, `scan`, snapshot reads, `stats`) take the
shared lock and proceed concurrently; writes (`put`, `delete`, `write_batch`,
`compact_now`) take the exclusive lock and serialize. The background compactor
holds only a `Weak` reference, so it never keeps the store alive on its own and
stops cleanly when the last handle is dropped.
