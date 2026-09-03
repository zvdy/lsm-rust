//! The public error type distinguishes *why* an operation failed.
//!
//! Before this, everything the engine returned was an `io::Error` and callers
//! had to match on error strings to tell a losing transaction apart from a
//! corrupt file. These tests drive each failure mode from outside the crate
//! and assert it arrives as its own variant, with the interop conversions
//! intact for callers still working in `std::io::Result`.

use lsm_rust::{Error, Isolation, SharedStorage, Storage, StorageConfig};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use tempfile::TempDir;

fn config() -> StorageConfig {
    StorageConfig {
        memtable_size_threshold: 4 * 1024,
        inline_compaction: false,
        block_cache_size: 0,
        ..StorageConfig::default()
    }
}

fn sst_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| {
            let p = e.unwrap().path();
            (p.extension().and_then(|s| s.to_str()) == Some("sst")).then_some(p)
        })
        .collect();
    files.sort();
    files
}

#[test]
fn on_disk_corruption_is_reported_as_corruption() {
    let temp = TempDir::new().unwrap();
    {
        let mut db = Storage::with_config(temp.path(), config()).unwrap();
        for i in 0..200 {
            let k = format!("key{:04}", i).into_bytes();
            db.put(k, format!("value-{:04}", i).repeat(4).into_bytes())
                .unwrap();
        }
    }

    // Damage the sparse index, whose checksum is verified when opening.
    let sst = sst_files(temp.path()).pop().expect("expected an SSTable");
    let mut bytes = fs::read(&sst).unwrap();
    let bloom_len = u32::from_le_bytes(bytes[6..10].try_into().unwrap()) as usize;
    let index_start = 6 + 8 + bloom_len + 8;
    bytes[index_start] ^= 0b10;
    fs::write(&sst, bytes).unwrap();

    let err = Storage::with_config(temp.path(), config())
        .err()
        .expect("a corrupt index must not open silently");

    assert!(matches!(err, Error::Corruption(_)), "{err:?}");
    assert!(err.is_corruption());
    assert!(
        !err.is_retriable(),
        "retrying a read of corrupt bytes cannot help"
    );
}

#[test]
fn a_losing_transaction_is_reported_as_a_conflict() {
    let temp = TempDir::new().unwrap();
    let db = SharedStorage::new(temp.path(), false).unwrap();

    let mut first = db.begin().unwrap();
    let mut second = db.begin().unwrap();
    first.put(b"contended".to_vec(), b"first".to_vec());
    second.put(b"contended".to_vec(), b"second".to_vec());
    first.commit().expect("first commit wins");

    let err = second.commit().expect_err("second must abort");

    match &err {
        Error::Conflict { key } => assert_eq!(key, b"contended"),
        other => panic!("expected a conflict, got {other:?}"),
    }
    assert!(err.is_retriable());
    assert!(!err.is_corruption());
}

#[test]
fn caller_misuse_is_reported_as_an_invalid_argument() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::with_config(temp.path(), config()).unwrap();
    db.put(b"k".to_vec(), b"v".to_vec()).unwrap();

    let err = db
        .snapshot_at(db.current_sequence() + 1)
        .expect_err("a future sequence has no snapshot");

    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
    assert!(!err.is_retriable());
}

#[test]
fn filesystem_failures_stay_io_errors_with_their_original_kind() {
    let temp = TempDir::new().unwrap();
    // A regular file where the data directory should be: creating the
    // directory fails in the OS, not in our parsing.
    let blocker = temp.path().join("not-a-dir");
    fs::write(&blocker, b"occupied").unwrap();

    let err = match Storage::new(blocker.join("data"), false) {
        Err(e) => e,
        Ok(_) => panic!("cannot create a directory under a file"),
    };

    let Error::Io(inner) = &err else {
        panic!("expected Error::Io, got {err:?}");
    };
    // The OS error is carried through rather than flattened into a string.
    assert_ne!(inner.kind(), io::ErrorKind::Other, "{inner:?}");
}

#[test]
fn errors_convert_into_io_error_for_callers_that_have_not_migrated() {
    // A caller whose own signature is still `io::Result` compiles unchanged,
    // because `?` converts through `From<Error> for io::Error`.
    fn legacy_caller(dir: &Path) -> io::Result<Option<Vec<u8>>> {
        let mut db = Storage::new(dir, false)?;
        db.put(b"k".to_vec(), b"v".to_vec())?;
        Ok(db.get(&b"k".to_vec())?)
    }

    let temp = TempDir::new().unwrap();
    assert_eq!(legacy_caller(temp.path()).unwrap(), Some(b"v".to_vec()));

    // And the classification survives the conversion as an `ErrorKind`.
    let conflict: io::Error = Error::Conflict { key: b"k".to_vec() }.into();
    assert_eq!(conflict.kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn the_retry_helper_reports_the_conflict_it_gave_up_on() {
    let temp = TempDir::new().unwrap();
    let db = SharedStorage::new(temp.path(), false).unwrap();
    db.put(b"k".to_vec(), b"0".to_vec()).unwrap();

    // Every attempt is invalidated by a write landing after the snapshot,
    // so the helper exhausts its retries and surfaces the conflict.
    let err = db
        .transaction(2, |tx| {
            let _ = tx.get(&b"k".to_vec())?;
            db.put(b"k".to_vec(), b"interfering".to_vec())?;
            tx.put(b"k".to_vec(), b"never".to_vec());
            Ok(())
        })
        .expect_err("retries must eventually give up");

    assert!(matches!(err, Error::Conflict { .. }), "{err:?}");
    assert!(err.is_retriable());
}

#[test]
fn a_transaction_body_can_fail_with_any_error_variant() {
    let temp = TempDir::new().unwrap();
    let db = SharedStorage::new(temp.path(), false).unwrap();

    // A non-conflict error from the body is returned as-is, not retried.
    let mut attempts = 0;
    let err = db
        .transaction(5, |_tx| -> lsm_rust::Result<()> {
            attempts += 1;
            Err(Error::InvalidArgument("business rule violated".into()))
        })
        .expect_err("the body's error propagates");

    assert_eq!(attempts, 1, "a non-retriable error must not be retried");
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
}

#[test]
fn isolation_levels_still_classify_their_aborts_as_conflicts() {
    let temp = TempDir::new().unwrap();
    let db = SharedStorage::new(temp.path(), false).unwrap();
    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();

    let mut tx = db.begin_with(Isolation::Serializable).unwrap();
    tx.get(&b"a".to_vec()).unwrap();
    tx.put(b"b".to_vec(), b"2".to_vec());

    // Someone else writes the key we read: a read-write conflict.
    db.put(b"a".to_vec(), b"2".to_vec()).unwrap();

    let err = tx.commit().expect_err("serializable must abort");
    assert!(
        matches!(&err, Error::Conflict { key } if key == b"a"),
        "{err:?}"
    );
}
