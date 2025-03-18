# Build stage
FROM rust:1.75-slim-bullseye AS builder

WORKDIR /usr/src/lsm-rust

# First, create the source layout
RUN mkdir -p src/memtable src/sstable src/storage src/wal

# Copy manifest
COPY ./Cargo.toml .

# Copy all source files
COPY ./src/main.rs ./src/
COPY ./src/memtable/mod.rs ./src/memtable/
COPY ./src/sstable/mod.rs ./src/sstable/
COPY ./src/storage/mod.rs ./src/storage/
COPY ./src/wal/mod.rs ./src/wal/

# Build the project
RUN cargo build --release

# Runtime stage
FROM debian:bullseye-slim

# Install necessary runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user and group
RUN groupadd -r lsmuser && useradd -r -g lsmuser lsmuser

# Create data and config directories with proper permissions
RUN mkdir -p /data /etc/lsm-rust && \
    chown -R lsmuser:lsmuser /data /etc/lsm-rust && \
    chmod 755 /data /etc/lsm-rust

# Copy the built binary
COPY --from=builder /usr/src/lsm-rust/target/release/lsm-rust /usr/local/bin/
RUN chmod +x /usr/local/bin/lsm-rust

# Switch to non-root user
USER lsmuser

# Set data directory as volume
VOLUME ["/data"]

# Set working directory
WORKDIR /data

# Health check
HEALTHCHECK --interval=30s --timeout=3s \
    CMD pgrep lsm-rust || exit 1

# Environment variables
ENV LSM_DATA_DIR=/data \
    RUST_LOG=info

# Run the binary
ENTRYPOINT ["lsm-rust"]
CMD ["--data-dir", "/data"] 