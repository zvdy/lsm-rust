//! Cross-engine benchmark and correctness harness for lsm-rust.
//!
//! Usage:
//!   cargo run --release -- bench  [--keys N] [--repeats R]
//!   cargo run --release -- verify [--keys N]
//!
//! Build with `--features rocksdb` to include RocksDB. It compiles a bundled
//! C++ library and takes roughly ten minutes from cold, which is why it is not
//! on by default.
//!
//! ## Reading the output honestly
//!
//! A benchmark that always flatters the engine that ships it is worthless, so
//! a few things are deliberate:
//!
//!   - Every engine is driven through one trait, so none of them gets a
//!     shorter code path than the others.
//!   - Durability is an explicit axis, not a default. These engines do not
//!     agree on what a write means, and comparing them at their defaults
//!     measures the defaults. Both columns are reported.
//!   - The operation sequence comes from a fixed seed and is identical per
//!     engine.
//!   - Results are the median of R repeats, with the spread shown, because a
//!     single run of a storage benchmark is noise.
//!   - `verify` checks the engines agree on the *answer*, not just the speed.
//!
//! What it still does not measure: concurrency, recovery time, long-running
//! compaction debt, memory use, or anything beyond a single process on local
//! disk. Numbers from one machine are not a general ranking.

mod engine;
mod engines;
mod harness;

use engine::Durability;
use harness::{EngineResult, Measurement};
use std::collections::BTreeMap;
use std::path::Path;

const WORKLOADS: &[&str] = &[
    "seq_insert",
    "rand_insert",
    "rand_get_hit",
    "rand_get_miss",
    "scan_100",
    "mixed_70r_30w",
    "delete",
];

fn main() -> anyhow::Result<()> {
    if cfg!(debug_assertions) {
        anyhow::bail!(
            "refusing to run in a debug build: the numbers would be meaningless.\n\
             Use `cargo run --release`."
        );
    }

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mode = args.first().map(String::as_str).unwrap_or("bench");
    let keys = flag(&args, "--keys").unwrap_or(50_000);
    let repeats = flag(&args, "--repeats").unwrap_or(3) as usize;

    let root = std::env::temp_dir().join(format!("lsm-comparison-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;

    let result = match mode {
        "bench" => bench(&root, keys, repeats),
        "verify" => verify(&root, keys),
        other => anyhow::bail!("unknown mode {other:?}; expected `bench` or `verify`"),
    };

    let _ = std::fs::remove_dir_all(&root);
    result
}

fn flag(args: &[String], name: &str) -> Option<u64> {
    let i = args.iter().position(|a| a == name)?;
    args.get(i + 1)?.parse().ok()
}

/// Run every engine under both durability settings, `repeats` times each.
fn bench(root: &Path, keys: u64, repeats: usize) -> anyhow::Result<()> {
    println!("# lsm-rust cross-engine benchmark\n");
    println!("- keys: {keys}");
    println!(
        "- key size: {} bytes, value size: {} bytes",
        harness::KEY_SIZE,
        harness::VALUE_SIZE
    );
    println!("- repeats: {repeats} (median reported, min and max in brackets)");
    println!("- host: {}", host_description());
    println!();

    for durability in [Durability::Synced, Durability::Buffered] {
        let mut all: Vec<Vec<EngineResult>> = Vec::new();

        macro_rules! run_engine {
            ($ty:ty, $slug:literal) => {{
                let mut runs = Vec::new();
                for r in 0..repeats {
                    let dir = root.join(format!("{}-{}-{}", $slug, durability.label(), r));
                    runs.push(harness::run::<$ty>(&dir, durability, keys)?);
                    let _ = std::fs::remove_dir_all(&dir);
                }
                all.push(runs);
            }};
        }

        run_engine!(engines::LsmRust, "lsm-rust");
        run_engine!(engines::Redb, "redb");
        run_engine!(engines::Sled, "sled");
        run_engine!(engines::Fjall, "fjall");
        #[cfg(feature = "rocksdb")]
        run_engine!(engines::RocksDb, "rocksdb");

        report(durability, &all);
    }

    println!("## Caveats\n");
    println!("Single process, single thread, local disk, one host. These");
    println!("numbers say how these engines behaved on this machine under this");
    println!("workload; they are not a general ranking, and no row here is");
    println!("evidence about concurrency, recovery, or sustained compaction");
    println!("under write pressure.");
    Ok(())
}

fn report(durability: Durability, all: &[Vec<EngineResult>]) {
    println!("## Durability: {}\n", durability.label());
    for runs in all {
        println!("- **{}**: {}", runs[0].engine, runs[0].durability_note);
    }
    println!();

    println!("### Throughput (operations per second, higher is better)\n");
    print!("| workload |");
    for runs in all {
        print!(" {} |", runs[0].engine);
    }
    println!();
    print!("| --- |");
    for _ in all {
        print!(" --- |");
    }
    println!();

    for workload in WORKLOADS {
        print!("| {workload} |");
        for runs in all {
            let mut vals: Vec<f64> = runs
                .iter()
                .filter_map(|r| find(&r.measurements, workload))
                .map(|m| m.ops_per_sec())
                .collect();
            vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
            if vals.is_empty() {
                print!(" n/a |");
            } else {
                print!(
                    " {} [{}..{}] |",
                    thousands(vals[vals.len() / 2]),
                    thousands(vals[0]),
                    thousands(vals[vals.len() - 1])
                );
            }
        }
        println!();
    }
    println!();

    println!("### p99 latency (microseconds, lower is better)\n");
    print!("| workload |");
    for runs in all {
        print!(" {} |", runs[0].engine);
    }
    println!();
    print!("| --- |");
    for _ in all {
        print!(" --- |");
    }
    println!();
    for workload in WORKLOADS {
        print!("| {workload} |");
        for runs in all {
            let mut vals: Vec<f64> = runs
                .iter()
                .filter_map(|r| find(&r.measurements, workload))
                .map(|m| m.percentile_us(0.99))
                .collect();
            vals.sort_by(|a, b| a.partial_cmp(b).unwrap());
            if vals.is_empty() {
                print!(" n/a |");
            } else {
                print!(" {:.1} |", vals[vals.len() / 2]);
            }
        }
        println!();
    }
    println!();

    println!("### On-disk size after the run (lower is better)\n");
    print!("|");
    for runs in all {
        print!(" {} |", runs[0].engine);
    }
    println!();
    print!("|");
    for _ in all {
        print!(" --- |");
    }
    println!();
    print!("|");
    for runs in all {
        let mut sizes: Vec<u64> = runs.iter().map(|r| r.disk_bytes).collect();
        sizes.sort_unstable();
        print!(" {} |", bytes(sizes[sizes.len() / 2]));
    }
    println!("\n");
}

fn find<'a>(ms: &'a [Measurement], workload: &str) -> Option<&'a Measurement> {
    ms.iter().find(|m| m.workload == workload)
}

fn thousands(v: f64) -> String {
    let n = v.round() as u64;
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

fn bytes(n: u64) -> String {
    const MB: f64 = 1024.0 * 1024.0;
    format!("{:.1} MB", n as f64 / MB)
}

fn host_description() -> String {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(0);
    format!("{} logical cores, {}", cores, std::env::consts::OS)
}

/// Replay one script against every engine and require identical final state.
fn verify(root: &Path, keys: u64) -> anyhow::Result<()> {
    println!("# Cross-engine agreement check\n");
    println!("Replaying the same {keys}-operation put/overwrite/delete script");
    println!("against every engine and comparing the final key space.\n");

    let mut states: BTreeMap<&str, Vec<harness::Pair>> = BTreeMap::new();

    macro_rules! verify_engine {
        ($ty:ty, $slug:literal) => {{
            let dir = root.join(concat!("verify-", $slug));
            let state = harness::verify::<$ty>(&dir, keys)?;
            let _ = std::fs::remove_dir_all(&dir);
            states.insert(<$ty as engine::Engine>::name(), state);
        }};
    }

    verify_engine!(engines::LsmRust, "lsm-rust");
    verify_engine!(engines::Redb, "redb");
    verify_engine!(engines::Sled, "sled");
    verify_engine!(engines::Fjall, "fjall");
    #[cfg(feature = "rocksdb")]
    verify_engine!(engines::RocksDb, "rocksdb");

    // redb is the reference only because it is a B-tree, so it shares no
    // code or design lineage with the engine under test. Agreeing with it is
    // therefore worth more than agreeing with another LSM.
    let reference_name = "redb 4.3";
    let reference = states
        .get(reference_name)
        .ok_or_else(|| anyhow::anyhow!("reference engine missing"))?
        .clone();

    println!(
        "Reference: {reference_name} ({} live keys)\n",
        reference.len()
    );
    let mut disagreed = false;
    for (name, state) in &states {
        if *name == reference_name {
            continue;
        }
        if *state == reference {
            println!("- {name}: AGREES ({} keys)", state.len());
        } else {
            disagreed = true;
            println!(
                "- {name}: DISAGREES ({} keys vs {})",
                state.len(),
                reference.len()
            );
            for (i, (a, b)) in state.iter().zip(reference.iter()).enumerate() {
                if a != b {
                    println!(
                        "    first difference at index {i}: {:?} vs {:?}",
                        String::from_utf8_lossy(&a.0),
                        String::from_utf8_lossy(&b.0)
                    );
                    break;
                }
            }
        }
    }

    if disagreed {
        anyhow::bail!("engines did not agree on the final state");
    }
    println!("\nAll engines agree.");
    Ok(())
}
