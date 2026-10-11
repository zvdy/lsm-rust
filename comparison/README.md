# Cross-engine comparison

A benchmark and correctness harness that runs lsm-rust against other embedded
key-value stores through one interface, so the comparison measures the engines
rather than the harness.

```bash
cd comparison
cargo run --release -- verify --keys 20000          # do they agree?
cargo run --release -- bench  --keys 50000 --repeats 3
cargo run --release --features rocksdb -- bench     # include RocksDB
```

Requires Rust 1.98 or newer, which is lsm-rust's MSRV. RocksDB is optional
because it compiles a bundled C++ library and takes roughly ten minutes from
cold.

This crate is deliberately detached from the parent: lsm-rust ships with two
dependencies and a cargo-deny policy, and the engines compared here drag in far
more. None of it belongs in the published crate's dependency graph.

## What it does

**`verify`** replays one deterministic put/overwrite/delete script against
every engine and compares the final key space. Agreement with three
independent implementations, one of which is a B-tree sharing no design
lineage, is real evidence. An engine agreeing with itself is not.

**`bench`** runs seven workloads under two durability settings, three times
each, reporting the median with the spread.

## Fairness

Benchmarks that ship with an engine tend to flatter it. These are the
deliberate choices:

- **One trait.** Every engine is driven through `Engine`, so none gets a
  shorter code path.
- **Durability is an explicit axis, not a default.** These engines disagree
  about what a write means: lsm-rust fsyncs every write, sled flushes on a
  timer, redb lets a transaction choose, RocksDB leaves its WAL unsynced.
  Comparing defaults measures the defaults. Both columns are reported, and
  each adapter states what it mapped the setting onto.
- **Identical operations.** The sequence comes from a fixed seed.
- **Medians, with spread.** A single run of a storage benchmark is noise.
- **Correctness is checked separately**, because a fast wrong answer is not a
  result.

## What it does not measure

Concurrency, recovery time, memory use, compaction debt under sustained write
pressure, or anything beyond one process on one machine's local disk. Cache
sizes are each engine's default and are not equalised. Nothing here is a
general ranking.

## Results

Recorded from a run on 4 logical cores, Linux, 50,000 keys of 16 bytes with
100-byte values, 3 repeats. Reproduce with the command above; absolute numbers
will differ per machine, the shape should not.

### lsm-rust is not the fastest engine here

That was the hope going in, and it is not what the harness found. Honest
summary of where it stands:

**Where it wins**

- **Smallest on-disk footprint, in both durability modes**: 14.4 MB, against
  redb 14.9/32.1 MB, sled 28.1/27.0 MB and fjall 64.0 MB. Compaction and LZ4
  are doing their job.
- **Faster than redb on every write workload**, by roughly 1.6x on inserts and
  1.8x on deletes under fsync-per-write.
- Competitive p99 on writes: 585 us synced, 11.7 us buffered.

**Where it loses, and by how much**

- **Point reads, by about 5x.** 111,701 ops/s against redb 544,486, sled
  515,175, fjall 508,468. p99 of 36.8 us against 3.6 to 6.3 us. This is the
  largest gap and the one worth fixing.
- **Scans, by about 3.4x against redb**: 13,309 against 44,369 ops/s.
- **Inserts**, behind fjall by 3.3x and sled by 2.1x buffered.
- **Mixed 70/30**, behind sled by 5.4x.

### Diagnosing the read gap

Comparing the same workload at a size that fits the 4 MB block cache against
one that exceeds it separates two causes:

| rand_get_hit (ops/s) | lsm-rust | redb | sled | fjall |
| --- | --- | --- | --- | --- |
| 3,000 keys, fits in cache | 389,897 | 729,566 | 957,556 | 891,707 |
| 50,000 keys, exceeds cache | 111,701 | 544,486 | 515,175 | 508,468 |
| degradation | **3.5x** | 1.3x | 1.9x | 1.8x |

Two separate problems, not one:

1. **Constant per-read overhead.** Even with the whole working set cached,
   lsm-rust is 2 to 2.5x slower than every peer. Nothing is touching the disk
   in that row, so the cost is CPU on the lookup path: a bloom check and a
   newest-to-oldest walk across every table in every level, and a
   multi-version map lookup where a peer does one B-tree descent.
2. **A more expensive miss.** It degrades 3.5x when the working set outgrows
   the cache, where peers degrade 1.3 to 1.9x. `SSTable::read_range` calls
   `File::open` on **every block read**, so a miss pays an open and a seek
   that peers, which hold handles open or memory-map, do not.

The second has an obvious fix: hold the file open per table and use positioned
reads. The first needs profiling before guessing further.

An API wart the harness also surfaced: `get` and `delete` take `&Key`, that is
`&Vec<u8>`, so a caller holding a `&[u8]` must allocate to call them. Every
engine compared here accepts `&[u8]` or `impl AsRef<[u8]>`. That allocation is
small next to a 5x gap, but it is paid on every single read.

### Correctness

All four engines agree exactly on the final key space after a 20,000-operation
script of interleaved puts, overwrites and deletes: 18,062 live keys, identical
in every engine.
