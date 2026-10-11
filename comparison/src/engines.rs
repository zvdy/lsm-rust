//! Adapters. Each one maps the harness's two durability modes onto whatever
//! the engine actually offers, and says so in `durability_note` so the mapping
//! is auditable rather than asserted.

use crate::engine::{Durability, Engine};
use anyhow::Context;
use redb::ReadableDatabase as _;
use std::path::Path;

// ---------------------------------------------------------------- lsm-rust

pub struct LsmRust {
    db: lsm_rust::Storage,
}

impl Engine for LsmRust {
    fn open(path: &Path, durability: Durability) -> anyhow::Result<Self> {
        let config = lsm_rust::StorageConfig {
            wal_sync: match durability {
                Durability::Synced => lsm_rust::WalSync::Always,
                // Matched to the others' buffered behaviour: a bounded group
                // commit rather than "never sync".
                Durability::Buffered => lsm_rust::WalSync::Batched {
                    every_n_writes: 1024,
                },
            },
            ..lsm_rust::StorageConfig::default()
        };
        Ok(Self {
            db: lsm_rust::Storage::with_config(path, config).context("open lsm-rust")?,
        })
    }

    fn name() -> &'static str {
        concat!("lsm-rust ", env!("CARGO_PKG_VERSION"))
    }

    fn durability_note(durability: Durability) -> &'static str {
        match durability {
            Durability::Synced => "WalSync::Always: fsync per write",
            Durability::Buffered => "WalSync::Batched: fsync every 1024 writes",
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.db.put(key.to_vec(), value.to_vec())?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.db.get(&key.to_vec())?)
    }

    fn delete(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.db.delete(&key.to_vec())?;
        Ok(())
    }

    fn scan(&self, lo: &[u8], hi: &[u8]) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(self.db.scan(lo, hi)?)
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        // A put returns only once the WAL write has been issued, and Batched
        // syncs on flush and shutdown, so there is nothing further to force.
        Ok(())
    }
}

// -------------------------------------------------------------------- redb

pub struct Redb {
    db: redb::Database,
    durability: Durability,
}

const REDB_TABLE: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("kv");

impl Redb {
    fn write_durability(&self) -> redb::Durability {
        match self.durability {
            Durability::Synced => redb::Durability::Immediate,
            // redb 4.x offers only Immediate or None; None is its buffered
            // mode, so that is what the Buffered column measures.
            Durability::Buffered => redb::Durability::None,
        }
    }
}

impl Engine for Redb {
    fn open(path: &Path, durability: Durability) -> anyhow::Result<Self> {
        std::fs::create_dir_all(path)?;
        let db = redb::Database::create(path.join("redb.db")).context("open redb")?;
        // Create the table so later read transactions do not fail on a missing one.
        let tx = db.begin_write()?;
        {
            let _ = tx.open_table(REDB_TABLE)?;
        }
        tx.commit()?;
        Ok(Self { db, durability })
    }

    fn name() -> &'static str {
        "redb 4.3"
    }

    fn durability_note(durability: Durability) -> &'static str {
        match durability {
            Durability::Synced => "Durability::Immediate: commit fsyncs",
            Durability::Buffered => "Durability::None: commit does not fsync",
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        let mut tx = self.db.begin_write()?;
        tx.set_durability(self.write_durability())?;
        {
            let mut table = tx.open_table(REDB_TABLE)?;
            table.insert(key, value)?;
        }
        tx.commit()?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let tx = self.db.begin_read()?;
        let table = tx.open_table(REDB_TABLE)?;
        Ok(table.get(key)?.map(|v| v.value().to_vec()))
    }

    fn delete(&mut self, key: &[u8]) -> anyhow::Result<()> {
        let mut tx = self.db.begin_write()?;
        tx.set_durability(self.write_durability())?;
        {
            let mut table = tx.open_table(REDB_TABLE)?;
            table.remove(key)?;
        }
        tx.commit()?;
        Ok(())
    }

    fn scan(&self, lo: &[u8], hi: &[u8]) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let tx = self.db.begin_read()?;
        let table = tx.open_table(REDB_TABLE)?;
        let mut out = Vec::new();
        for row in table.range(lo..hi)? {
            let (k, v) = row?;
            out.push((k.value().to_vec(), v.value().to_vec()));
        }
        Ok(out)
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        // An Immediate commit has already synced; force one for Eventual.
        let mut tx = self.db.begin_write()?;
        tx.set_durability(redb::Durability::Immediate)?;
        tx.commit()?;
        Ok(())
    }
}

// -------------------------------------------------------------------- sled

pub struct Sled {
    db: sled::Db,
    durability: Durability,
}

impl Engine for Sled {
    fn open(path: &Path, durability: Durability) -> anyhow::Result<Self> {
        let config = sled::Config::new().path(path);
        let config = match durability {
            // sled has no per-write fsync mode; flushing after each write is
            // the only way to get the same guarantee, and that is what the
            // Synced column costs it. Noted rather than hidden.
            Durability::Synced => config.flush_every_ms(None),
            Durability::Buffered => config.flush_every_ms(Some(1000)),
        };
        Ok(Self {
            db: config.open().context("open sled")?,
            durability,
        })
    }

    fn name() -> &'static str {
        "sled 0.34"
    }

    fn durability_note(durability: Durability) -> &'static str {
        match durability {
            Durability::Synced => "explicit flush() after every write",
            Durability::Buffered => "background flush every 1000 ms",
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.db.insert(key, value)?;
        if self.durability == Durability::Synced {
            self.db.flush()?;
        }
        Ok(())
    }

    fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?.map(|v| v.to_vec()))
    }

    fn delete(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.db.remove(key)?;
        if self.durability == Durability::Synced {
            self.db.flush()?;
        }
        Ok(())
    }

    fn scan(&self, lo: &[u8], hi: &[u8]) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        for row in self.db.range(lo..hi) {
            let (k, v) = row?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        self.db.flush()?;
        Ok(())
    }
}

// ------------------------------------------------------------------- fjall

pub struct Fjall {
    db: fjall::Database,
    keyspace: fjall::Keyspace,
    durability: Durability,
}

impl Engine for Fjall {
    fn open(path: &Path, durability: Durability) -> anyhow::Result<Self> {
        let db = fjall::Database::builder(path)
            .open()
            .context("open fjall")?;
        let keyspace = db.keyspace("kv", fjall::KeyspaceCreateOptions::default)?;
        Ok(Self {
            db,
            keyspace,
            durability,
        })
    }

    fn name() -> &'static str {
        "fjall 3.1"
    }

    fn durability_note(durability: Durability) -> &'static str {
        match durability {
            Durability::Synced => "PersistMode::SyncAll after every write",
            Durability::Buffered => "journal buffered, persisted at the end",
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        self.keyspace.insert(key, value)?;
        if self.durability == Durability::Synced {
            self.db.persist(fjall::PersistMode::SyncAll)?;
        }
        Ok(())
    }

    fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.keyspace.get(key)?.map(|v| v.to_vec()))
    }

    fn delete(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.keyspace.remove(key)?;
        if self.durability == Durability::Synced {
            self.db.persist(fjall::PersistMode::SyncAll)?;
        }
        Ok(())
    }

    fn scan(&self, lo: &[u8], hi: &[u8]) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        // fjall yields a Guard, which defers the read of the pair itself.
        for guard in self.keyspace.range(lo..hi) {
            let (k, v) = guard.into_inner()?;
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        self.db.persist(fjall::PersistMode::SyncAll)?;
        Ok(())
    }
}

// ---------------------------------------------------------------- rocksdb

#[cfg(feature = "rocksdb")]
pub struct RocksDb {
    db: rocksdb::DB,
    durability: Durability,
}

#[cfg(feature = "rocksdb")]
impl Engine for RocksDb {
    fn open(path: &Path, durability: Durability) -> anyhow::Result<Self> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        Ok(Self {
            db: rocksdb::DB::open(&opts, path).context("open rocksdb")?,
            durability,
        })
    }

    fn name() -> &'static str {
        "rocksdb 0.25"
    }

    fn durability_note(durability: Durability) -> &'static str {
        match durability {
            Durability::Synced => "WriteOptions::set_sync(true): fsync per write",
            Durability::Buffered => "default WAL, unsynced",
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> anyhow::Result<()> {
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(self.durability == Durability::Synced);
        self.db.put_opt(key, value, &opts)?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?)
    }

    fn delete(&mut self, key: &[u8]) -> anyhow::Result<()> {
        let mut opts = rocksdb::WriteOptions::default();
        opts.set_sync(self.durability == Durability::Synced);
        self.db.delete_opt(key, &opts)?;
        Ok(())
    }

    fn scan(&self, lo: &[u8], hi: &[u8]) -> anyhow::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out = Vec::new();
        let iter = self
            .db
            .iterator(rocksdb::IteratorMode::From(lo, rocksdb::Direction::Forward));
        for row in iter {
            let (k, v) = row?;
            if k.as_ref() >= hi {
                break;
            }
            out.push((k.to_vec(), v.to_vec()));
        }
        Ok(out)
    }

    fn sync(&mut self) -> anyhow::Result<()> {
        self.db.flush()?;
        Ok(())
    }
}
