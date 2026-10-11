//! Workloads and measurement.
//!
//! Every engine runs the identical operation sequence, generated from a fixed
//! seed, so the comparison is not also a comparison of random draws.

use crate::engine::{disk_bytes, Durability, Engine};
use std::path::Path;
use std::time::{Duration, Instant};

/// A key and the value stored under it.
pub type Pair = (Vec<u8>, Vec<u8>);

pub const KEY_SIZE: usize = 16;
pub const VALUE_SIZE: usize = 100;

/// Deterministic xorshift64*, so a run is reproducible and every engine sees
/// the same keys in the same order.
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed | 1)
    }
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// Keys are fixed width and zero padded so that lexicographic order matches
/// numeric order. Every engine here is ordered, and a scan that returned a
/// different set per engine would not be comparable.
pub fn key_for(n: u64) -> Vec<u8> {
    format!("key{:013}", n).into_bytes()
}

pub fn value_for(n: u64) -> Vec<u8> {
    let mut v = format!("val{:013}", n).into_bytes();
    v.resize(VALUE_SIZE, b'.');
    v
}

#[derive(Debug, Clone)]
pub struct Measurement {
    pub workload: &'static str,
    pub ops: u64,
    pub elapsed: Duration,
    /// Sorted per-operation latencies, in nanoseconds.
    latencies: Vec<u64>,
}

impl Measurement {
    fn new(workload: &'static str, elapsed: Duration, mut latencies: Vec<u64>) -> Self {
        latencies.sort_unstable();
        Measurement {
            workload,
            ops: latencies.len() as u64,
            elapsed,
            latencies,
        }
    }

    pub fn ops_per_sec(&self) -> f64 {
        if self.elapsed.as_secs_f64() == 0.0 {
            return 0.0;
        }
        self.ops as f64 / self.elapsed.as_secs_f64()
    }

    /// Latency at the given quantile, in microseconds.
    pub fn percentile_us(&self, q: f64) -> f64 {
        if self.latencies.is_empty() {
            return 0.0;
        }
        let idx = ((self.latencies.len() - 1) as f64 * q).round() as usize;
        self.latencies[idx] as f64 / 1000.0
    }
}

/// Run `op` `count` times, timing each call.
fn timed<F>(workload: &'static str, count: u64, mut op: F) -> anyhow::Result<Measurement>
where
    F: FnMut(u64) -> anyhow::Result<()>,
{
    let mut latencies = Vec::with_capacity(count as usize);
    let started = Instant::now();
    for i in 0..count {
        let t = Instant::now();
        op(i)?;
        latencies.push(t.elapsed().as_nanos() as u64);
    }
    let elapsed = started.elapsed();
    Ok(Measurement::new(workload, elapsed, latencies))
}

pub struct EngineResult {
    pub engine: &'static str,
    #[allow(dead_code, reason = "reported via Durability in the section header")]
    pub durability: Durability,
    pub durability_note: &'static str,
    pub measurements: Vec<Measurement>,
    pub disk_bytes: u64,
    /// Checksum over every value read back, to confirm the engines agree on
    /// what they returned and not merely on how fast they returned it.
    #[allow(dead_code, reason = "cross-engine agreement is asserted by `verify`")]
    pub read_checksum: u64,
}

/// The full workload sequence against one engine.
///
/// `n` is the number of keys inserted; read and scan phases are sized from it.
pub fn run<E: Engine>(dir: &Path, durability: Durability, n: u64) -> anyhow::Result<EngineResult> {
    std::fs::create_dir_all(dir)?;
    let mut engine = E::open(dir, durability)?;
    let mut measurements = Vec::new();
    let mut checksum: u64 = 0;

    // Sequential insert: keys in ascending order, the friendliest case for an
    // LSM tree and the least friendly for a B-tree's page splits.
    measurements.push(timed("seq_insert", n, |i| {
        engine.put(&key_for(i), &value_for(i))
    })?);
    engine.sync()?;

    // Random insert over a disjoint key range, so it is insertion rather than
    // overwrite.
    let mut rng = Rng::new(0x5EED_1234);
    let order: Vec<u64> = (0..n).map(|_| n + (rng.next() % n)).collect();
    measurements.push(timed("rand_insert", n, |i| {
        let k = order[i as usize];
        engine.put(&key_for(k), &value_for(k))
    })?);
    engine.sync()?;

    // Random reads that hit.
    let mut rng = Rng::new(0xBEEF_4321);
    let hits: Vec<u64> = (0..n).map(|_| rng.next() % n).collect();
    measurements.push(timed("rand_get_hit", n, |i| {
        let k = hits[i as usize];
        if let Some(v) = engine.get(&key_for(k))? {
            checksum = checksum.wrapping_mul(31).wrapping_add(v.len() as u64);
            checksum = checksum.wrapping_add(v[3] as u64);
        }
        Ok(())
    })?);

    // Random reads that miss. A bloom filter should show up here.
    measurements.push(timed("rand_get_miss", n, |i| {
        let absent = key_for(10_000_000 + i);
        if engine.get(&absent)?.is_some() {
            anyhow::bail!("a key that was never written came back");
        }
        Ok(())
    })?);

    // Short scans, the shape an index range query takes.
    let scans = (n / 10).max(1);
    let mut rng = Rng::new(0xC0FF_EE11);
    measurements.push(timed("scan_100", scans, |_| {
        let start = rng.next() % n.saturating_sub(100).max(1);
        let rows = engine.scan(&key_for(start), &key_for(start + 100))?;
        checksum = checksum.wrapping_mul(31).wrapping_add(rows.len() as u64);
        Ok(())
    })?);

    // Mixed read/write, 70/30, the closest of these to a serving workload.
    let mut rng = Rng::new(0xD00D_5678);
    measurements.push(timed("mixed_70r_30w", n, |_| {
        let r = rng.next();
        let k = r % n;
        if r % 10 < 7 {
            if let Some(v) = engine.get(&key_for(k))? {
                checksum = checksum.wrapping_add(v.len() as u64);
            }
            Ok(())
        } else {
            engine.put(&key_for(k), &value_for(k))
        }
    })?);
    engine.sync()?;

    // Deletes over half the key space.
    let half = (n / 2).max(1);
    measurements.push(timed("delete", half, |i| engine.delete(&key_for(i * 2)))?);
    engine.sync()?;

    drop(engine);
    Ok(EngineResult {
        engine: E::name(),
        durability,
        durability_note: E::durability_note(durability),
        measurements,
        disk_bytes: disk_bytes(dir),
        read_checksum: checksum,
    })
}

/// A deterministic operation script, replayed against every engine, whose
/// final visible state must be identical everywhere.
///
/// This is the part of the harness that is not about speed. Three independent
/// implementations agreeing on the result of the same mixed put/delete/scan
/// sequence is real evidence; one engine agreeing with itself is not.
pub fn verify<E: Engine>(dir: &Path, n: u64) -> anyhow::Result<Vec<Pair>> {
    std::fs::create_dir_all(dir)?;
    let mut engine = E::open(dir, Durability::Buffered)?;
    let mut rng = Rng::new(0xA11C_E123);

    for i in 0..n {
        engine.put(&key_for(i), &value_for(i))?;
        // Overwrite an earlier key, so the newest version has to win.
        if i > 0 && i % 3 == 0 {
            let victim = rng.next() % i;
            engine.put(&key_for(victim), &value_for(victim + 1_000_000))?;
        }
        // Delete another, so tombstones have to shadow correctly.
        if i > 0 && i % 7 == 0 {
            let victim = rng.next() % i;
            engine.delete(&key_for(victim))?;
        }
    }
    engine.sync()?;

    let all = engine.scan(&key_for(0), &key_for(n))?;
    drop(engine);
    Ok(all)
}
