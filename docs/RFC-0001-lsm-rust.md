# RFC 0001: The lsm-rust Storage Engine

```
Project: lsm-rust                                              Z. Vidal
Request for Comments: 0001                                  Category: Informational
                                                            September 2026
```

## Abstract

This document specifies the on-disk formats, algorithms, and durability
guarantees of lsm-rust, a Log-Structured Merge-tree (LSM-tree) storage engine.
It describes the write-ahead log, the sorted string table (SSTable) format,
the manifest, the compaction policy, the multi-version concurrency control
(MVCC) model, and the wire protocols exposed by the optional front ends.

The design follows the general thesis of [ONEIL96], deferring and batching
index changes and cascading them from a memory-resident component through
geometrically larger disk-resident components. It departs from that paper's
mechanism in ways enumerated in Section 12.

This document is written to be sufficient for an independent implementation
to read and write the formats described, and for an operator to reason about
what survives a crash.

## Status of This Memo

This memo is an Informational document describing a specific implementation.
It is not a product of the IETF, is not an Internet Standard, and does not
create or modify any registry. It follows the structural conventions of
[RFC7322] so that it can be read and reviewed like a specification rather
than like prose documentation.

The formats described here are **not frozen**. Version 0.1.0 of the software
has not been published, and the SSTable format has reached version 5 with
earlier versions still readable. Section 11 records the compatibility rules
that do hold.

## Requirements Language

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD",
"SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this document are to be
interpreted as described in BCP 14 [RFC2119] [RFC8174] when, and only when,
they appear in all capitals, as shown here.

## Table of Contents

1. Introduction
2. Terminology
3. Architecture
4. Data Model
5. On-Disk Formats
6. Operations
7. Compaction
8. Concurrency and Isolation
9. Durability and Recovery
10. Front-End Protocols
11. Limits and Compatibility
12. Relationship to the Original LSM-Tree
13. Security Considerations
14. IANA Considerations
15. References

---

## 1. Introduction

An LSM-tree trades read amplification for write amplification. Writes are
buffered in memory and flushed sequentially; the resulting immutable files are
merged in the background. Reads may have to consult several files, which is
the cost paid for never updating a record in place.

lsm-rust implements this as an embeddable Rust library with two optional
network front ends. It is a single-node engine: there is no replication,
failover, or clustering.

This document specifies what the engine writes, what it promises, and what it
does not.

## 2. Terminology

**Key**, **Value**: opaque, ordered byte strings. Keys are compared
lexicographically by unsigned byte value.

**Sequence number** (`seq`): a 64-bit, monotonically increasing integer
assigned to each write. It establishes a total order over all writes and is
the basis of MVCC.

**Version**: a value or a tombstone, optionally carrying an expiry deadline,
recorded at a given sequence number.

**Tombstone**: a version recording a deletion. It MUST shadow older versions
of the same key rather than removing them.

**MemTable**: the in-memory, multi-version sorted table receiving writes.

**SSTable**: an immutable, sorted, on-disk table. Once written, an SSTable
MUST NOT be modified.

**Level**: a set of SSTables. Level 0 receives MemTable flushes; deeper levels
receive compaction output.

**Manifest**: the authoritative record of which SSTables are live and how far
the sequence counter has advanced.

**Snapshot**: a sequence number at which reads observe a consistent view.

## 3. Architecture

```
          writes                                reads
             |                                    |
             v                                    v
       +-----------+                        +-----------+
       |    WAL    |  (fsync before ack)    |  MemTable |
       +-----------+                        +-----------+
             |                                    |
             v                                    v
       +-----------+   flush    +---------------------------+
       |  MemTable | ---------> |  Level 0 SSTables         |
       +-----------+            +---------------------------+
                                            | compaction
                                            v
                                +---------------------------+
                                |  Level 1 .. Level N       |
                                +---------------------------+

       +-----------+
       | MANIFEST  |  names the live tables and the sequence high-water mark
       +-----------+
```

A write is appended to the WAL and fsynced (subject to the sync policy of
Section 9.1) before it is inserted into the MemTable. A read consults the
MemTable first, then each level's tables from newest to oldest.

## 4. Data Model

### 4.1 Versions

Every write creates a new version rather than overwriting. A version is:

- a **value**, or a **tombstone**; and
- an OPTIONAL **expiry deadline**, an absolute time in Unix milliseconds.

Versions of one key are ordered by sequence number, newest first.

### 4.2 Visibility

A read at snapshot sequence `S` MUST return the newest version of the key
whose `seq` is less than or equal to `S`, or report absence if that version is
a tombstone or has expired.

### 4.3 Expiry

A deadline is absolute and resolved at write time, so it MUST survive a
restart without being refreshed.

An expired version MUST continue to shadow older versions of the same key. It
MUST NOT simply be skipped: doing so would uncover an older value and resurrect
data the deadline was meant to retire. Compaction rewrites an expired version
as a tombstone, after which the ordinary tombstone rules govern its removal.

A snapshot isolates a reader from writes, not from time. A key that expires
after a snapshot is taken MUST stop being visible to that snapshot, because
sequence numbers order writes against each other and say nothing about the
clock.

## 5. On-Disk Formats

All integers are little-endian unless stated otherwise. All checksums are
CRC-32 using the reflected polynomial 0xEDB88320, as used by zlib, gzip, and
PNG.

A data directory contains:

| Name              | Contents                                  |
| ----------------- | ----------------------------------------- |
| `MANIFEST`        | the live table set and sequence high-water mark |
| `MANIFEST.tmp`    | transient; present only mid-write         |
| `wal`             | the write-ahead log                       |
| `L<level>_<seq>.sst` | an SSTable                             |

An SSTable filename encodes its level and the sequence number reserved for it.
A reader MUST treat the manifest, not the directory listing, as authoritative:
a `.sst` file the manifest does not name is an orphan from an interrupted
flush or compaction and MUST be deleted at startup.

### 5.1 Write-Ahead Log

The log is a sequence of frames:

```
+--------+-----------+-----------+------------------+
| 0x03   | crc32 u32 | len   u32 | body (len bytes) |
+--------+-----------+-----------+------------------+
```

The CRC covers the body only. A frame whose CRC does not match MUST be treated
as follows:

- if it is the **last** frame in the file, it is a torn write from a crash
  during append and MUST be discarded, with every preceding frame recovered;
- otherwise it is corruption of durable data and the implementation MUST
  report an error rather than skip it.

A body is either a single entry or a batch:

```
single:  <entry>
batch:   0x02 | count u32 | <entry> x count
```

An entry is:

```
+------+-----------+-------------+---------------------+-------------+---------+
| op   | key_len   | key         | expires_at (op=4)   | value_len   | value   |
| u8   | u32       | key_len     | u64                 | u32         | value_len|
+------+-----------+-------------+---------------------+-------------+---------+
```

| `op` | Meaning                          | `expires_at` | `value` |
| ---- | -------------------------------- | ------------ | ------- |
| 0x00 | Put                              | absent       | present |
| 0x01 | Delete                           | absent       | absent  |
| 0x04 | Put with expiry                  | present      | present |

Sequence numbers are NOT stored in the log. They are reassigned on replay,
continuing from the manifest's high-water mark, which preserves write order.
A batch MUST be applied at a single sequence number so that it becomes visible
atomically.

For backward compatibility a reader MUST also accept unframed records, in
which the body appears directly without the 0x03 frame header. Such records
carry no checksum.

### 5.2 SSTable

Format version 5:

```
+----------------+-------------+-----------+------------------+
| magic "LSMT"   | version u8  | flags u8  | min_expiry u64   |
|    4 bytes     |    = 5      |           |                  |
+----------------+-------------+-----------+------------------+
| bloom_len u32  | bloom_crc u32 | bloom filter (bloom_len)   |
+----------------+---------------+----------------------------+
| index_len u32  | index_crc u32 | sparse index  (index_len)  |
+----------------+---------------+----------------------------+
| data blocks ...                                             |
+-------------------------------------------------------------+
```

`flags` bit 0 set means data blocks are LZ4-compressed. All other bits are
reserved and MUST be zero.

`min_expiry` is the earliest deadline anywhere in the table, or 0 if nothing in
it expires. It lets compaction decide whether a table has anything to reclaim
without reading it.

A length prefix is the one field a checksum cannot protect, because the CRC
covers the body the length describes and can only be verified after the length
has been used. An implementation MUST therefore reject a length larger than the
bytes remaining in the file before allocating for it.

#### 5.2.1 Bloom Filter

```
| size u32 | num_hash_functions u32 | bitset ceil(size/8) bytes |
```

Bits are packed least-significant-bit first within each byte. Parameters
follow the standard optimum for a target false-positive rate `p` and expected
element count `n`: `size = ceil(-n * ln(p) / (ln 2)^2)` and
`num_hash_functions = ceil((size / n) * ln 2)`. The implementation uses
`p = 0.01`.

A filter MAY report a false positive; it MUST NOT report a false negative. A
table written without a filter MUST be treated as possibly containing every
key.

#### 5.2.2 Sparse Index

```
| count u32 | entry x count |

entry: | key_len u32 | first_key | offset u64 | len u32 |
```

`offset` is relative to the start of the data section. `len` includes the
block's CRC prefix. Entries are ordered by `first_key`.

#### 5.2.3 Data Blocks

```
| crc32 u32 | payload |
```

The CRC covers the payload **as stored**, that is after compression, so
corruption is detected before the decompressor sees the bytes. When LZ4 is in
use the payload begins with the uncompressed length.

A block holds a target of 16 entries but MUST NOT be split in the middle of a
key's run of versions, so that all versions of a key are reachable from one
index entry.

#### 5.2.4 Entry Encoding

```
| key_len u32 | key | seq u64 | value_field u32 | ... |
```

`value_field` is overloaded:

| `value_field`        | Meaning        | Bytes that follow                    |
| -------------------- | -------------- | ------------------------------------ |
| 0xFFFFFFFF           | tombstone      | none                                 |
| 0xFFFFFFFE           | value + expiry | `deadline u64`, `value_len u32`, value |
| otherwise            | value          | value of `value_field` bytes         |

Entries are ordered by key ascending, then by sequence descending.

Because two length values are reserved as markers, a value of exactly
0xFFFFFFFF or 0xFFFFFFFE bytes is not representable and MUST be rejected at
write time. See Section 11.1.

### 5.3 Manifest

The manifest is a text file:

```
lsm-manifest v1
seq <last_seq>
<level> <seq> <filename>
<level> <seq> <filename>
...
```

`last_seq` is the highest sequence number assigned at the time of writing. On
startup the sequence counter MUST resume from it, so sequence numbers remain
monotonic across restarts.

The manifest MUST be replaced atomically, by writing `MANIFEST.tmp`, fsyncing
it, renaming it over `MANIFEST`, and then fsyncing the containing directory.
The directory fsync is REQUIRED: without it the rename itself may not survive
a power failure.

## 6. Operations

### 6.1 Write

1. Reject keys and values exceeding the limits of Section 11.1.
2. Append to the WAL and sync according to policy.
3. Assign the next sequence number.
4. Insert the version into the MemTable.
5. Flush if the MemTable is over its threshold.

Step 2 MUST precede step 4. A write that is visible but not durable would be
lost by a crash after it was acknowledged.

### 6.2 Read

Consult the MemTable, then each level from shallowest to deepest, and within a
level the tables from newest to oldest. Return the first version found whose
sequence is at or below the snapshot. A Bloom filter miss MUST skip the table
without reading a block.

### 6.3 Scan

A scan merges the MemTable with one cursor per SSTable, yielding keys in order
with the newest visible version winning. A table whose key range does not
intersect the requested range MUST be skipped without opening a cursor.

Scans are streaming: memory is proportional to the number of participating
tables, not to the size of the range.

### 6.4 Flush

1. Write the MemTable's versions, including tombstones, to a new level-0
   SSTable and fsync it.
2. Commit the new table to the manifest.
3. Clear the MemTable and truncate the WAL.

The ordering is REQUIRED. Truncating the WAL before the manifest commit would
lose the flushed writes if a crash occurred in between, because the new table
would be an unreferenced orphan and deleted at startup.

## 7. Compaction

### 7.1 Trigger

Level 0 is judged by file count, because its tables come straight from
MemTable flushes and freely overlap, so every one of them must be consulted on
a read. Deeper levels are judged by total size, with thresholds in geometric
progression:

```
threshold(N) = compaction_size_threshold * level_multiplier^N
```

This geometric progression is the result derived in Theorem 3.1 of [ONEIL96],
which shows total I/O is minimised when component sizes grow by a constant
ratio.

### 7.2 Plan

A level is either **merged** or **promoted**. Promotion applies when all of
the following hold:

- the level has at least two tables;
- moving them would not push the destination over its table ceiling;
- no table in the level holds an expired version; and
- the tables are mutually disjoint, that is the maximum overlap depth is 1.

A promotion reinterprets the tables one level down without reading them: no
key appears in more than one, so a merge would rewrite every byte to produce
the same entries. This is the common shape for append-only and time-ordered
keys.

Otherwise the level is merged: its tables are read, merged, and written as one
table at the next level.

### 7.3 Merge

Inputs are already sorted by key ascending and sequence descending, so the
merge is a k-way merge over one cursor per table, holding one block per table
rather than the level. For each key:

- every version with `seq` greater than the GC floor MUST be kept, because a
  live snapshot may need it;
- the newest version at or below the GC floor MUST be kept;
- older versions below the floor MAY be dropped;
- an expired version MUST be rewritten as a tombstone rather than dropped; and
- a tombstone MAY be dropped only when no table at or below the output level
  could hold an older value for that key, and there are no live snapshots.

The GC floor is the oldest sequence any live snapshot can read.

### 7.4 Commit

A compaction MAY run with the store unlocked, because its inputs are
immutable. If it does:

- no other compaction may start while one is in flight, or it would delete the
  files being read;
- the commit MUST remove exactly the tables the merge consumed, rather than
  clearing the level, because a flush may have added a table meanwhile; and
- the commit MUST verify those tables are still present, and discard its
  output if they are not.

A GC floor read before the merge and used after it is safe in one direction
only: the floor rises as snapshots are released, so a stale value retains
versions a fresher one would have collected. Retaining too much costs a later
pass; retaining too little would be data loss.

As in a flush, the manifest replacement is the commit point.

## 8. Concurrency and Isolation

### 8.1 Model

The engine serialises writes and allows concurrent reads. Reads are never
blocked by a compaction merge, only by the brief manifest commit.

### 8.2 Snapshots

A snapshot pins a sequence number and a GC floor, so compaction MUST NOT
collect versions the snapshot can still read.

### 8.3 Transactions

Transactions are optimistic: they do not block, and conflicts are detected at
commit. Two isolation levels are offered:

| Level          | Detects                                          | Permits              |
| -------------- | ------------------------------------------------ | -------------------- |
| Snapshot       | write-write                                      | write skew, phantoms |
| Serializable   | write-write, read-write, phantoms in scanned ranges | nothing           |

A transaction reads its own writes. A commit applies the whole write set at
one sequence number as a single WAL record, so it is atomic in both visibility
and durability. A conflicting commit MUST return a retriable error and MUST
NOT partially apply.

## 9. Durability and Recovery

### 9.1 Sync Policy

| Policy    | Behaviour                                        |
| --------- | ------------------------------------------------ |
| Always    | fsync every append; an acknowledged write is never lost |
| Batched   | fsync every N appends; a crash MAY lose up to N-1 acknowledged writes |

`Always` is the default. `Batched` trades the durability guarantee for
throughput and MUST NOT be used where acknowledged writes are expected to
survive.

### 9.2 The Durability Chain

```
WAL append + fsync
      -> SSTable written + fsync
            -> MANIFEST.tmp + fsync
                  -> rename over MANIFEST
                        -> fsync directory
                              -> WAL truncated
```

Nothing is committed before the data it references is durable.

### 9.3 Recovery

On startup an implementation MUST:

1. Read the manifest. It names the live tables and the sequence high-water
   mark.
2. Delete every `.sst` file the manifest does not name. These are orphans from
   a flush or compaction interrupted before its commit, or tables whose
   deletion was interrupted after it.
3. Replay the WAL into the MemTable, assigning fresh sequence numbers
   continuing from the high-water mark, dropping a torn final frame.

The manifest rename being the commit point gives two crash windows, both
recoverable: a crash before it leaves the new output as an orphan, and a crash
after it leaves the superseded inputs as orphans. Step 2 resolves either.

### 9.4 Checkpoints

A checkpoint is a consistent, point-in-time copy of the store, written while
holding it exclusively so the tables, WAL, and manifest agree.

SSTables are immutable and SHOULD be captured as hard links, so a checkpoint
costs almost nothing up front. The WAL is mutable and MUST be copied. Where
linking is impossible, tables MUST be copied.

The manifest written into a checkpoint MUST record the **persisted** sequence
number, not the live counter, because the copied WAL will replay on top and
advance it again. Recording the live value would double-count.

A checkpoint directory is itself a data directory; restoring means opening it.
There is no separate restore path.

The cost accrues later: when compaction unlinks a table, a checkpoint holding
a link keeps the extents alive. The real cost is the bytes compaction has
rewritten since the checkpoint was taken, not the size of the store.
Checkpoints are therefore intended to be short-lived.

## 10. Front-End Protocols

Both front ends are OPTIONAL and disabled unless started.

### 10.1 RESP

A subset of RESP2 sufficient for standard Redis clients:

`PING`, `ECHO`, `SET key value [EX seconds | PX milliseconds]`, `GET`, `DEL`,
`EXISTS`, `TTL`, `KEYS <prefix>*`, `COMMAND`, `QUIT`.

`TTL` follows the Redis convention: -2 for no such key, -1 for no deadline,
otherwise the remaining whole seconds.

`KEYS` supports trailing-star prefixes only. Any other glob MUST be rejected
rather than silently mishandled.

A declared bulk length is a claim, not evidence. An implementation MUST NOT
size a buffer from it before the bytes arrive, MUST bound the summed payload
of one command, and MUST verify the CRLF terminator that follows a bulk
payload rather than consuming two bytes unexamined.

### 10.2 Metrics

An HTTP/1.1 endpoint serving `GET /metrics` in the Prometheus text exposition
format, plus `/`, `/health`, and `/healthz` as liveness probes.

The request line and headers MUST be bounded both per line and in total, and
a request exceeding the budget SHOULD be answered with 431 before the
connection is closed.

## 11. Limits and Compatibility

### 11.1 Size Limits

| Quantity | Limit           | Reason                                    |
| -------- | --------------- | ----------------------------------------- |
| Key      | 2^32 - 1 bytes  | four-byte length prefix                   |
| Value    | 2^32 - 3 bytes  | two lengths are reserved as markers (5.2.4) |

An oversized key or value MUST be rejected with an error. It MUST NOT be
truncated, because the length would wrap and the write would be silently
corrupted rather than refused.

### 11.2 Format Versions

| Version | Adds                                              |
| ------- | ------------------------------------------------- |
| legacy  | bloom length prefix, unversioned                  |
| 2       | magic and version header; no sequence numbers     |
| 3       | per-entry sequence numbers                        |
| 4       | CRC-32 on every section and data block            |
| 5       | per-entry expiry and the `min_expiry` header field |

An implementation MUST read every version listed and SHOULD write only the
newest. Compaction migrates older tables forward as a side effect.

The formats are not frozen. Until a 1.0 release, a version bump MAY change any
structure described here, subject only to the rule that previously written
tables remain readable.

## 12. Relationship to the Original LSM-Tree

lsm-rust follows the thesis of [ONEIL96] but not its mechanism. The
differences are material and are recorded here so the lineage is not
overstated.

**Shared.** Deferring and batching index changes; cascading from a memory
component through geometrically larger disk components (Theorem 3.1);
deletion by markers that migrate outward and annihilate on merge (Section
2.3); reclaiming data by asserting a predicate during merge, which the expiry
of Section 4.3 is a special case of; recovery from a log plus a checkpoint
recording component locations, with merge output written to new locations
rather than over old ones (Section 4.2).

**Divergent.**

- The paper specifies a **rolling merge**: a cursor circulating continuously
  through the key space between each component pair, with an emptying block
  and a filling block, giving steady amortised I/O. lsm-rust performs discrete
  whole-level merges, which are burstier.
- In the paper each component is a single B-tree-like structure that *absorbs*
  entries from the one above. Here a level is a set of independent sorted
  runs, and a merge appends its output to the next level rather than merging
  into it.
- The paper's disk components are B-tree-like with 100%-full nodes packed into
  multi-page blocks. lsm-rust uses SSTables with a sparse block index.
- The paper has no Bloom filters; a find consults every component. Bloom
  filters come from the later Bigtable and LevelDB lineage, as do the block
  cache and block compression.
- MVCC snapshots and optimistic transactions are outside the paper's scope.
- The paper's **long-latency find**, a find note that migrates outward
  accumulating results, has no counterpart here.

Because a level holds several possibly overlapping runs, the compaction
strategy is closer to *tiered* than to the *leveled* strategy of LevelDB, in
which each level below zero is a single sorted run of disjoint tables.

## 13. Security Considerations

**Trust model.** The engine trusts its data directory and does not trust
network input. A data directory is assumed to be written only by this engine;
an attacker with write access to it can substitute arbitrary content, and no
checksum defends against that, since an attacker can recompute one. Checksums
detect accidental corruption, not tampering. There is no authentication,
authorisation, or encryption at any layer.

**Network exposure.** Neither front end authenticates. Both MUST be bound to a
trusted interface or placed behind something that authenticates. The RESP
front end exposes full read and write access to the store; the metrics
endpoint exposes operational counts and key-space sizes, which may itself be
sensitive.

**Resource exhaustion.** Length-prefixed formats invite amplification, where a
small input causes a large allocation. Both the network parsers and the
on-disk readers MUST validate a declared length against what is actually
available before allocating, as required in Sections 5.2 and 10.1. Protocol
lines MUST be bounded, since a line must be buffered before it can be
inspected.

**Corruption handling.** A corrupt file MUST surface as an error distinguishable
from a transient I/O fault, so a caller can tell a damaged file from a
hiccuping disk. An implementation MUST NOT return data that fails its
checksum, and MUST NOT abort the process on a value read from a file; a
damaged length field is a demand for memory, not a valid size.

**Denial of service.** A single writer serialises all writes, so a slow or
large write delays others. The engine does not rate-limit, quota, or bound the
number of connections.

**Durability as a security property.** `WalSync::Batched` can lose
acknowledged writes. Where an acknowledgement is relied upon as a record that
something happened, the default `Always` MUST be retained.

## 14. IANA Considerations

This document has no IANA actions. It defines no URI schemes, media types,
port numbers, or protocol parameters requiring registration. The RESP subset
of Section 10.1 is a client-compatible reimplementation of an existing
protocol and registers nothing.

## 15. References

### 15.1 Normative References

- **[RFC2119]** Bradner, S., "Key words for use in RFCs to Indicate
  Requirement Levels", BCP 14, RFC 2119, March 1997.
- **[RFC8174]** Leiba, B., "Ambiguity of Uppercase vs Lowercase in RFC 2119
  Key Words", BCP 14, RFC 8174, May 2017.
- **[RFC1952]** Deutsch, P., "GZIP file format specification version 4.3",
  RFC 1952, May 1996. (CRC-32 definition.)

### 15.2 Informative References

- **[ONEIL96]** O'Neil, P., Cheng, E., Gawlick, D., and E. O'Neil, "The
  Log-Structured Merge-Tree (LSM-Tree)", Acta Informatica 33, 1996.
  https://www.cs.umb.edu/~poneil/lsmtree.pdf
- **[BIGTABLE]** Chang, F., et al., "Bigtable: A Distributed Storage System
  for Structured Data", OSDI, 2006.
- **[RFC7322]** Flanagan, H. and S. Ginoza, "RFC Style Guide", RFC 7322,
  September 2014.
- **[BLOOM70]** Bloom, B., "Space/Time Trade-offs in Hash Coding with
  Allowable Errors", CACM 13(7), 1970.
- **[ARCHITECTURE]** `docs/ARCHITECTURE.md` in this repository, which covers
  the same ground discursively and with diagrams.

## Author's Address

```
Z. Vidal
https://github.com/zvdy/lsm-rust
```
