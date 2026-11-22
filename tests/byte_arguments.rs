use lsm_rust::{SharedStorage, Storage, WriteBatch};
use tempfile::TempDir;

#[test]
fn storage_accepts_str_slices_arrays_and_vecs() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::new(dir.path(), false).unwrap();

    db.put("a", "1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.put(&b"c"[..], vec![b'3']).unwrap();
    db.put(b"d".to_vec(), "4").unwrap();

    assert_eq!(db.get("a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c".as_slice()).unwrap(), Some(b"3".to_vec()));
    assert_eq!(db.get(b"d").unwrap(), Some(b"4".to_vec()));

    db.delete("a").unwrap();
    db.delete(b"b").unwrap();
    assert_eq!(db.get("a").unwrap(), None);
    assert_eq!(db.get("b").unwrap(), None);
    assert_eq!(db.expiry("c").unwrap(), Some(None));
}

#[test]
fn snapshot_reads_batches_and_ttl_accept_borrowed_keys() {
    let dir = TempDir::new().unwrap();
    let mut db = Storage::new(dir.path(), false).unwrap();

    db.put("k", "v1").unwrap();
    let snap = db.snapshot();
    db.put("k", "v2").unwrap();
    assert_eq!(db.get_at(&snap, "k").unwrap(), Some(b"v1".to_vec()));

    let mut batch = WriteBatch::new();
    batch
        .put("x", "1")
        .delete("k")
        .put_with_ttl("t", "soon", std::time::Duration::from_secs(60));
    db.write_batch(batch).unwrap();
    assert_eq!(db.get("x").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get("k").unwrap(), None);
    assert!(matches!(db.expiry("t").unwrap(), Some(Some(_))));
}

#[test]
fn shared_storage_and_transactions_accept_borrowed_keys() {
    let dir = TempDir::new().unwrap();
    let db = SharedStorage::new(dir.path(), false).unwrap();

    db.put("n", "0").unwrap();
    db.transaction(4, |tx| {
        let current = tx.get("n")?.unwrap();
        assert_eq!(current, b"0");
        tx.put("n", "1");
        tx.put_with_ttl("ttl", "x", std::time::Duration::from_secs(60));
        tx.delete("gone");
        Ok(())
    })
    .unwrap();

    assert_eq!(db.get("n").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"ttl").unwrap(), Some(b"x".to_vec()));
    db.delete("n").unwrap();
    assert_eq!(db.get("n").unwrap(), None);
}
