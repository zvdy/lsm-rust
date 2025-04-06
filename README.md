# LSM Tree + SSTable Database in Rust

A minimal implementation of a Log-Structured Merge Tree (LSM Tree) with Sorted String Tables (SSTable) in Rust. This implementation features automatic compaction, multi-level storage, and detailed logging capabilities.

## Features

- **Log-Structured Storage**: All writes are sequential, optimizing write performance
- **Multi-Level Compaction**: Automatic compaction when level size thresholds are reached
- **Write-Ahead Logging**: Ensures durability of operations
- **Verbose Logging**: Detailed insights into operations with `-v` flag
- **Memory-Efficient**: Automatic flushing of MemTable when size threshold is reached
- **Data Integrity**: Verified through comprehensive testing
- **Bloom Filters**: Faster lookups with probabilistic filtering

## Architecture and Data Flow

### Write Path
```
┌─────────┐     ┌─────────────┐     ┌──────────┐     ┌───────────┐
│  Write  │────▶│ Write-Ahead │────▶│ MemTable │────▶│  SSTable  │
│ Request │     │     Log     │     │(in-memory)│     │ (Level 0) │
└─────────┘     └─────────────┘     └──────────┘     └───────────┘
                                         │                 │
                                         │                 ▼
                                         │           ┌───────────┐
                                         │           │ Compaction│
                                         │           │  Process  │
                                         │           └───────────┘
                                         │                 │
                                         │                 ▼
                                         │           ┌───────────┐
                                         │           │  SSTable  │
                                         │           │ (Level N) │
                                         │           └───────────┘
                                         ▼
                                    ┌──────────┐
                                    │   Flush  │
                                    │(if size >│
                                    │threshold)│
                                    └──────────┘
```

1. Each write is first recorded in the Write-Ahead Log (WAL)
2. Then the data is inserted into the in-memory MemTable
3. When MemTable reaches the size threshold (512KB), it's flushed to disk as a Level 0 SSTable
4. Periodically, compaction merges SSTables from one level to the next

### Read Path
```
┌─────────┐     ┌──────────┐
│  Read   │────▶│ MemTable │────┐
│ Request │     │  Check   │    │
└─────────┘     └──────────┘    │
                     │          │
                 Not Found      │ Found
                     │          │
                     ▼          │
             ┌──────────────┐   │
             │ Level 0      │   │
             │ SSTables     │   │
             │ (with Bloom  │   │
             │  filters)    │   │
             └──────────────┘   │
                     │          │
                 Not Found      │ Found
                     │          │
                     ▼          │
             ┌──────────────┐   │
             │ Level 1...N  │   │
             │ SSTables     │   │
             │ (with Bloom  │   │
             │  filters)    │   │
             └──────────────┘   │
                     │          │
                     ▼          ▼
               ┌─────────┐    ┌─────────┐
               │ Return  │    │ Return  │
               │  Null   │    │  Value  │
               └─────────┘    └─────────┘
```

1. First check the MemTable for the most recent data
2. If not found, check Level 0 SSTables from newest to oldest
3. Continue checking higher levels if needed
4. Bloom filters quickly skip SSTables that definitely don't contain the key
5. Return the value if found, or null if not present in any location

### Compaction Process
```
┌────────────┐     ┌────────────┐     ┌────────────┐      
│  SSTable   │     │  SSTable   │     │  SSTable   │      
│  (Level N) │     │  (Level N) │     │  (Level N) │      
└────────────┘     └────────────┘     └────────────┘      
       │                 │                  │             
       └─────────────────┼──────────────────┘             
                         ▼                               
                  ┌────────────┐                          
                  │   Merge    │  ┌─ Deduplication        
                  │   Process  │  ├─ Sort by key          
                  └────────────┘  └─ Remove tombstones    
                         │                                
                         ▼                                
                  ┌────────────┐                          
                  │  SSTable   │                          
                  │ (Level N+1)│                          
                  └────────────┘                          
```

1. When a level reaches its threshold, compaction is triggered
2. Multiple SSTables from the same level are merged
3. During the merge, keys are deduplicated (keeping the newest values)
4. The result is written to the next level
5. This process continues as needed through multiple levels

## Performance Characteristics

- **Write Performance**:
  - Sequential writes to MemTable: O(log n)
  - MemTable flush threshold: 512KB
  - Average write size: ~0.86KB per entry
  
- **Read Performance**:
  - MemTable lookup: O(log n)
  - SSTable lookup: O(1) Bloom filter check + O(n) if potentially present
  - Bloom filters eliminate unnecessary disk I/O for non-existent keys
  - Reads check MemTable first, then traverse levels

- **Compaction**:
  - Level 0 compaction trigger: 4 files or 2MB total size
  - Size multiplier between levels: 4x
  - Level N threshold: base_threshold * (multiplier^N)
  - Compaction reduces space through deduplication
  
- **Space Efficiency**:
  - Automatic garbage collection during compaction
  - Deduplication of entries during compaction
  - Multi-level storage for better space utilization

## Test Results

The implementation has been tested with:
- Basic operations (PUT/GET/DELETE)
- Large dataset operations (5000 entries)
- Compaction triggers and level management
- Data integrity verification
- Bloom filter false positive tests

Sample test output with verbose logging:
```
=== Test Statistics ===
- Operations: 5000
- Total Data Written: 4.22 MB
- Average Value Size: 0.86 KB
- Compaction Events: 3
- Final SSTable Count: 2
- Maximum Level Reached: 2
```

## Components

1. **MemTable**
   - In-memory sorted key-value store using BTreeMap
   - Size-based flushing (512KB threshold)
   - Fast read/write operations

2. **SSTable (Sorted String Table)**
   - Immutable on-disk storage
   - Level-based organization
   - Format: `[bloom_size][bloom_filter][key_size][key][value_size][value]...`
   - Includes Bloom filter for efficient lookups

3. **Bloom Filter**
   - Probabilistic data structure for testing set membership
   - Eliminates unnecessary disk reads for non-existent keys
   - Configurable false positive rate (default: 1%)

4. **WAL (Write-Ahead Log)**
   - Ensures durability
   - Records all write operations
   - Format: `[op_type][key_size][key][value_size?][value?]`

5. **Storage**
   - Main database interface
   - Manages MemTable, SSTables, and WAL
   - Handles compaction and level management

## Project Structure

```ascii
lsm-rust/
├── src/
│   ├── main.rs           # Example usage and tests
│   ├── memtable/        
│   │   └── mod.rs       # In-memory storage
│   ├── sstable/
│   │   ├── mod.rs       # On-disk storage
│   │   └── compaction.rs # Compaction logic
│   ├── storage/
│   │   └── mod.rs       # Main interface
│   ├── bloom/
│   │   └── mod.rs       # Bloom filter implementation
│   └── wal/
│       └── mod.rs       # Write-ahead log
├── Cargo.toml
├── Dockerfile
└── README.md
```

## Setup

### Local Setup

1. Clone the repository:
```bash
git clone https://github.com/zvdy/lsm-rust.git
cd lsm-rust
```

2. Build the project:
```bash
cargo build --release
```

3. Run with verbose logging:
```bash
cargo run --release -- -v
```

### Docker Setup

1. Build the Docker image:
```bash
docker build -t lsm-rust .
```

2. Run the container:
```bash
docker run -it lsm-rust
```

## Usage Example

```rust
use storage::Storage;

fn main() -> io::Result<()> {
    // Create a new database instance with verbose logging
    let mut db = Storage::new("./data", true)?;

    // Insert data
    db.put(b"name".to_vec(), b"John Doe".to_vec())?;

    // Retrieve data
    if let Ok(Some(name)) = db.get(b"name") {
        println!("name: {}", String::from_utf8_lossy(&name));
    }

    // Delete data
    db.delete(b"name")?;

    Ok(())
}
```

## Future Improvements

- [X] SSTable compaction
- [X] Bloom filters for faster lookups
- [ ] Index blocks in SSTables
- [ ] Concurrent access support
- [ ] Configuration options
- [ ] Benchmarking suite
- [ ] Compression support
- [ ] Recovery testing
- [ ] Custom serialization formats

## License

MIT License