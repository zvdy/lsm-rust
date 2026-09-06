//! Crash consistency across a compaction.
//!
//! `maybe_compact` states its own safety property in a comment:
//!
//! > The manifest rename is the commit point: crash before it and the
//! > half-finished compaction output is an orphan; crash after it and the
//! > stale inputs are orphans. Either way startup cleans up.
//!
//! Nothing verified it. The existing recovery tests restart the store
//! cleanly, which never leaves the directory in either of those states.
//!
//! No fault injection is needed to reach them. Both are just particular
//! arrangements of files that a real compaction produces, so the tests here
//! capture the directory before and after a genuine compaction and reassemble
//! it as a crash would have left it.

use lsm_rust::{Storage, StorageConfig};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 1024,
        compaction_size_threshold: 8 * 1024,
        level0_file_limit: 3,
        // Compaction is driven explicitly so the before and after states are
        // captured around a known point.
        inline_compaction: false,
        ..StorageConfig::default()
    }
}

/// A key and the value most recently written to it.
type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

/// A compaction captured from both sides, with the data it should preserve.
struct Fixture {
    /// The directory as it stood before the compaction ran.
    before: Snapshot,
    /// And after it finished.
    after: Snapshot,
    /// Every key that must still read back correctly afterwards.
    written: Pairs,
}

/// Everything in a data directory, copied so it survives later changes.
struct Snapshot {
    files: Vec<(String, Vec<u8>)>,
}

fn capture(dir: &Path) -> Snapshot {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            let name = path.file_name().unwrap().to_string_lossy().into_owned();
            files.push((name, fs::read(&path).unwrap()));
        }
    }
    files.sort();
    Snapshot { files }
}

impl Snapshot {
    fn names(&self) -> BTreeSet<&str> {
        self.files.iter().map(|(n, _)| n.as_str()).collect()
    }

    fn sst_names(&self) -> BTreeSet<&str> {
        self.names()
            .into_iter()
            .filter(|n| n.ends_with(".sst"))
            .collect()
    }

    fn get(&self, name: &str) -> &[u8] {
        &self
            .files
            .iter()
            .find(|(n, _)| n == name)
            .unwrap_or_else(|| panic!("{name} not in snapshot"))
            .1
    }
}

/// Lay out a directory holding `sst_names` from whichever snapshot has each,
/// plus the manifest and WAL from `metadata`.
///
/// This is the crash state: the SSTables that existed at the moment of the
/// crash, alongside the manifest and log as they stood at that same moment.
fn lay_out(dir: &Path, sst_names: &BTreeSet<&str>, from: &[&Snapshot], metadata: &Snapshot) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            fs::remove_file(path).unwrap();
        }
    }
    for name in sst_names {
        let bytes = from
            .iter()
            .find_map(|snap| snap.files.iter().find(|(n, _)| n == name).map(|(_, b)| b))
            .unwrap_or_else(|| panic!("{name} in no snapshot"));
        fs::write(dir.join(name), bytes).unwrap();
    }
    for (name, bytes) in &metadata.files {
        if !name.ends_with(".sst") {
            fs::write(dir.join(name), bytes).unwrap();
        }
    }
}

fn remaining_sst_files(dir: &Path) -> BTreeSet<String> {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(|entry| {
            let path: PathBuf = entry.unwrap().path();
            let name = path.file_name()?.to_string_lossy().into_owned();
            name.ends_with(".sst").then_some(name)
        })
        .collect()
}

/// Fill a store until level 0 is over its file limit, then capture the
/// directory before and after a compaction that actually moves data.
fn compaction_before_and_after(dir: &Path) -> Fixture {
    let mut written: Pairs = Vec::new();
    let mut db = Storage::with_config(dir, config()).unwrap();
    // Overlapping key ranges across flushes, so the planner merges rather
    // than promoting: a promotion moves no data and would not exercise the
    // window this test is about.
    for round in 0..6 {
        for i in 0..40 {
            let key = format!("key{:03}", i).into_bytes();
            let value = format!("round{}-value{:03}", round, i).into_bytes();
            db.put(key.clone(), value.clone()).unwrap();
            written.retain(|(k, _)| k != &key);
            written.push((key, value));
        }
    }
    drop(db);

    let mut db = Storage::with_config(dir, config()).unwrap();
    let before = capture(dir);
    db.compact_now().unwrap();
    drop(db);
    let after = capture(dir);

    assert_ne!(
        before.sst_names(),
        after.sst_names(),
        "the fixture must actually compact, or these tests check nothing"
    );
    Fixture {
        before,
        after,
        written,
    }
}

fn assert_all_readable(dir: &Path, expected: &Pairs, what: &str) {
    let db = Storage::with_config(dir, config()).unwrap();
    for (key, value) in expected {
        assert_eq!(
            db.get(key).unwrap().as_deref(),
            Some(value.as_slice()),
            "{what}: {} was lost",
            String::from_utf8_lossy(key)
        );
    }
}

#[test]
fn a_crash_before_the_manifest_commit_keeps_the_inputs() {
    let temp = TempDir::new().unwrap();
    let Fixture {
        before,
        after,
        written,
    } = compaction_before_and_after(temp.path());

    // The compaction wrote its output and then stopped. Both the inputs and
    // the new table are on disk; the manifest still names only the inputs.
    let mut present: BTreeSet<&str> = before.sst_names();
    present.extend(after.sst_names());
    lay_out(temp.path(), &present, &[&before, &after], &before);

    assert_all_readable(temp.path(), &written, "crash before the commit");

    // The output was never committed, so it is an orphan and startup removes
    // it. The inputs stay: they are what the manifest names.
    let remaining = remaining_sst_files(temp.path());
    let expected: BTreeSet<String> = before.sst_names().iter().map(|s| s.to_string()).collect();
    assert_eq!(
        remaining, expected,
        "the uncommitted output should have been cleaned up"
    );
}

#[test]
fn a_crash_after_the_manifest_commit_discards_the_inputs() {
    let temp = TempDir::new().unwrap();
    let Fixture {
        before,
        after,
        written,
    } = compaction_before_and_after(temp.path());

    // The manifest was replaced and the process died before unlinking the
    // inputs, so every file from both sides is present.
    let mut present: BTreeSet<&str> = before.sst_names();
    present.extend(after.sst_names());
    lay_out(temp.path(), &present, &[&before, &after], &after);

    assert_all_readable(temp.path(), &written, "crash after the commit");

    // The manifest names the output, so the stale inputs are the orphans.
    let remaining = remaining_sst_files(temp.path());
    let expected: BTreeSet<String> = after.sst_names().iter().map(|s| s.to_string()).collect();
    assert_eq!(
        remaining, expected,
        "the superseded inputs should have been cleaned up"
    );
}

#[test]
fn a_torn_compaction_output_is_discarded_not_read() {
    let temp = TempDir::new().unwrap();
    let Fixture {
        before,
        after,
        written,
    } = compaction_before_and_after(temp.path());

    let mut present: BTreeSet<&str> = before.sst_names();
    present.extend(after.sst_names());
    lay_out(temp.path(), &present, &[&before, &after], &before);

    // The crash landed mid-write, so the output is half a file. Nothing
    // committed it, so its contents never matter — but a half-written table
    // must not be able to derail startup on its way to being deleted.
    let new: Vec<&str> = after
        .sst_names()
        .difference(&before.sst_names())
        .copied()
        .collect();
    assert!(
        !new.is_empty(),
        "the compaction should have produced a table"
    );
    for name in &new {
        let full = after.get(name);
        fs::write(temp.path().join(name), &full[..full.len() / 2]).unwrap();
    }

    assert_all_readable(temp.path(), &written, "torn compaction output");
    let remaining = remaining_sst_files(temp.path());
    let expected: BTreeSet<String> = before.sst_names().iter().map(|s| s.to_string()).collect();
    assert_eq!(
        remaining, expected,
        "the torn output should have been cleaned up"
    );
}
