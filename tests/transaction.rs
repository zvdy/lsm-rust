//! Integration tests for concurrent optimistic transactions.

use lsm_rust::{Error, Isolation, SharedStorage};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

fn db(dir: &TempDir) -> SharedStorage {
    SharedStorage::new(dir.path(), false).unwrap()
}

#[test]
fn commit_makes_writes_visible_and_rollback_does_not() {
    let temp = TempDir::new().unwrap();
    let db = db(&temp);

    let mut tx = db.begin().unwrap();
    tx.put(b"a".to_vec(), b"1".to_vec());
    // Nothing is visible outside the transaction before it commits.
    assert_eq!(db.get(&b"a".to_vec()).unwrap(), None);
    tx.commit().unwrap();
    assert_eq!(db.get(&b"a".to_vec()).unwrap(), Some(b"1".to_vec()));

    let mut tx = db.begin().unwrap();
    tx.put(b"b".to_vec(), b"2".to_vec());
    tx.rollback();
    assert_eq!(db.get(&b"b".to_vec()).unwrap(), None);

    // Dropping without committing rolls back too.
    {
        let mut tx = db.begin().unwrap();
        tx.put(b"c".to_vec(), b"3".to_vec());
    }
    assert_eq!(db.get(&b"c".to_vec()).unwrap(), None);
}

#[test]
fn transaction_reads_its_own_writes() {
    let temp = TempDir::new().unwrap();
    let db = db(&temp);
    db.put(b"k".to_vec(), b"old".to_vec()).unwrap();

    let mut tx = db.begin().unwrap();
    tx.put(b"k".to_vec(), b"new".to_vec());
    assert_eq!(tx.get(&b"k".to_vec()).unwrap(), Some(b"new".to_vec()));

    tx.delete(b"k".to_vec());
    assert_eq!(tx.get(&b"k".to_vec()).unwrap(), None);

    // ...while the outside world still sees the committed value.
    assert_eq!(db.get(&b"k".to_vec()).unwrap(), Some(b"old".to_vec()));
    tx.commit().unwrap();
    assert_eq!(db.get(&b"k".to_vec()).unwrap(), None);
}

#[test]
fn reads_are_isolated_from_concurrent_commits() {
    let temp = TempDir::new().unwrap();
    let db = db(&temp);
    db.put(b"k".to_vec(), b"v1".to_vec()).unwrap();

    let mut tx = db.begin().unwrap();
    assert_eq!(tx.get(&b"k".to_vec()).unwrap(), Some(b"v1".to_vec()));

    // Someone else commits a new value mid-transaction.
    db.put(b"k".to_vec(), b"v2".to_vec()).unwrap();

    // The transaction keeps reading its snapshot.
    assert_eq!(tx.get(&b"k".to_vec()).unwrap(), Some(b"v1".to_vec()));
}

#[test]
fn write_write_conflict_aborts_the_loser() {
    let temp = TempDir::new().unwrap();
    let db = db(&temp);
    db.put(b"k".to_vec(), b"base".to_vec()).unwrap();

    let mut first = db.begin().unwrap();
    let mut second = db.begin().unwrap();

    first.put(b"k".to_vec(), b"from-first".to_vec());
    second.put(b"k".to_vec(), b"from-second".to_vec());

    first.commit().expect("first commit wins");

    let err = second.commit().expect_err("second must abort");
    assert!(err.is_retriable(), "conflict should be retriable: {err}");
    assert!(matches!(err, Error::Conflict { .. }));

    // The loser had no effect at all.
    assert_eq!(
        db.get(&b"k".to_vec()).unwrap(),
        Some(b"from-first".to_vec())
    );
}

#[test]
fn disjoint_writes_commit_concurrently() {
    let temp = TempDir::new().unwrap();
    let db = db(&temp);

    let mut a = db.begin().unwrap();
    let mut b = db.begin().unwrap();
    a.put(b"a".to_vec(), b"1".to_vec());
    b.put(b"b".to_vec(), b"2".to_vec());

    // No overlap in the write sets, so both succeed.
    a.commit().unwrap();
    b.commit().unwrap();

    assert_eq!(db.get(&b"a".to_vec()).unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(&b"b".to_vec()).unwrap(), Some(b"2".to_vec()));
}

#[test]
fn serializable_detects_write_skew_but_snapshot_isolation_allows_it() {
    // Classic write skew: both transactions read x and y, then each writes the
    // one the other read. Under snapshot isolation both commit; serializable
    // isolation must stop one of them.
    for (isolation, expect_conflict) in [
        (Isolation::Snapshot, false),
        (Isolation::Serializable, true),
    ] {
        let temp = TempDir::new().unwrap();
        let db = db(&temp);
        db.put(b"x".to_vec(), b"1".to_vec()).unwrap();
        db.put(b"y".to_vec(), b"1".to_vec()).unwrap();

        let mut t1 = db.begin_with(isolation).unwrap();
        let mut t2 = db.begin_with(isolation).unwrap();

        t1.get(&b"y".to_vec()).unwrap();
        t2.get(&b"x".to_vec()).unwrap();
        t1.put(b"x".to_vec(), b"2".to_vec());
        t2.put(b"y".to_vec(), b"2".to_vec());

        t1.commit().unwrap();
        let second = t2.commit();
        assert_eq!(
            second.is_err(),
            expect_conflict,
            "unexpected outcome for {isolation:?}"
        );
    }
}

#[test]
fn serializable_detects_a_phantom_in_a_scanned_range() {
    let temp = TempDir::new().unwrap();
    let db = db(&temp);
    db.put(b"user:1".to_vec(), b"a".to_vec()).unwrap();

    let mut tx = db.begin().unwrap();
    let seen = tx.scan(b"user:", b"user;").unwrap();
    assert_eq!(seen.len(), 1);
    tx.put(b"summary".to_vec(), b"1 user".to_vec());

    // A concurrent insert lands inside the range this transaction scanned,
    // which would invalidate the summary it computed.
    db.put(b"user:2".to_vec(), b"b".to_vec()).unwrap();

    let err = tx.commit().expect_err("phantom must be detected");
    assert!(err.is_retriable());
}

#[test]
fn scan_merges_buffered_writes_with_the_snapshot() {
    let temp = TempDir::new().unwrap();
    let db = db(&temp);
    db.put(b"k1".to_vec(), b"a".to_vec()).unwrap();
    db.put(b"k2".to_vec(), b"b".to_vec()).unwrap();

    let mut tx = db.begin().unwrap();
    tx.put(b"k3".to_vec(), b"c".to_vec()); // new
    tx.put(b"k1".to_vec(), b"A".to_vec()); // overwrite
    tx.delete(b"k2".to_vec()); // removed

    let got = tx.scan(b"k", b"l").unwrap();
    assert_eq!(
        got,
        vec![
            (b"k1".to_vec(), b"A".to_vec()),
            (b"k3".to_vec(), b"c".to_vec()),
        ]
    );
}

#[test]
fn read_only_transactions_never_conflict() {
    let temp = TempDir::new().unwrap();
    let db = db(&temp);
    db.put(b"k".to_vec(), b"v".to_vec()).unwrap();

    let mut tx = db.begin().unwrap();
    tx.get(&b"k".to_vec()).unwrap();
    db.put(b"k".to_vec(), b"changed".to_vec()).unwrap();

    // Nothing was written, so there is nothing to conflict over.
    tx.commit().expect("read-only commit always succeeds");
}

#[test]
fn retry_helper_serializes_concurrent_increments() {
    // The acid test: many threads incrementing one counter through the retry
    // helper must produce exactly the right total — no lost updates.
    let temp = TempDir::new().unwrap();
    let db = db(&temp);
    db.put(b"counter".to_vec(), b"0".to_vec()).unwrap();

    const THREADS: usize = 8;
    const PER_THREAD: usize = 25;

    let barrier = Arc::new(Barrier::new(THREADS));
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let db = db.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..PER_THREAD {
                    db.transaction(1000, |tx| {
                        let current = tx.get(&b"counter".to_vec())?.unwrap_or_default();
                        let n: u64 = String::from_utf8_lossy(&current).parse().unwrap_or(0);
                        tx.put(b"counter".to_vec(), (n + 1).to_string().into_bytes());
                        Ok(())
                    })
                    .expect("increment should eventually commit");
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    let final_value = db.get(&b"counter".to_vec()).unwrap().unwrap();
    let total: u64 = String::from_utf8_lossy(&final_value).parse().unwrap();
    assert_eq!(
        total,
        (THREADS * PER_THREAD) as u64,
        "lost updates under concurrency"
    );
}

#[test]
fn concurrent_disjoint_transactions_all_commit() {
    let temp = TempDir::new().unwrap();
    let db = db(&temp);

    const THREADS: usize = 8;
    const PER_THREAD: usize = 50;

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..PER_THREAD {
                    let mut tx = db.begin().unwrap();
                    tx.put(
                        format!("t{t}:k{i:03}").into_bytes(),
                        format!("v{i}").into_bytes(),
                    );
                    tx.commit().expect("disjoint keys never conflict");
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }

    for t in 0..THREADS {
        for i in 0..PER_THREAD {
            assert_eq!(
                db.get(&format!("t{t}:k{i:03}").into_bytes()).unwrap(),
                Some(format!("v{i}").into_bytes())
            );
        }
    }
}

#[test]
fn committed_transactions_survive_restart() {
    let temp = TempDir::new().unwrap();
    {
        let db = db(&temp);
        let mut tx = db.begin().unwrap();
        tx.put(b"durable".to_vec(), b"yes".to_vec());
        tx.put(b"also".to_vec(), b"yes".to_vec());
        tx.commit().unwrap();

        let mut rolled_back = db.begin().unwrap();
        rolled_back.put(b"ghost".to_vec(), b"no".to_vec());
        drop(rolled_back);
    }

    let db = SharedStorage::new(temp.path(), false).unwrap();
    assert_eq!(db.get(&b"durable".to_vec()).unwrap(), Some(b"yes".to_vec()));
    assert_eq!(db.get(&b"also".to_vec()).unwrap(), Some(b"yes".to_vec()));
    assert_eq!(db.get(&b"ghost".to_vec()).unwrap(), None);
}

#[test]
fn commit_log_is_pruned_and_does_not_grow_without_bound() {
    let temp = TempDir::new().unwrap();
    let db = db(&temp);

    for i in 0..200 {
        let mut tx = db.begin().unwrap();
        tx.put(format!("k{i}").into_bytes(), b"v".to_vec());
        tx.commit().unwrap();
    }

    // With no long-running transaction pinning older sequences, the retained
    // conflict-detection state stays small rather than growing per commit.
    let tracked = db.tracked_commits().unwrap();
    assert!(
        tracked <= 2,
        "commit log should be pruned, retained {tracked} entries"
    );
}
