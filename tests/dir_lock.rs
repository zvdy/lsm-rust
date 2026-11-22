use lsm_rust::{Error, SharedStorage, Storage};
use std::io::ErrorKind;
use tempfile::TempDir;

fn is_busy(err: &Error) -> bool {
    matches!(err, Error::Io(e) if e.kind() == ErrorKind::ResourceBusy)
}

#[test]
fn second_open_of_a_live_directory_is_refused() {
    let dir = TempDir::new().unwrap();
    let _first = Storage::new(dir.path(), false).unwrap();

    let err = Storage::new(dir.path(), false).err().expect("must fail");
    assert!(is_busy(&err), "{err}");
}

#[test]
fn directory_can_be_reopened_after_the_store_is_dropped() {
    let dir = TempDir::new().unwrap();
    let mut first = Storage::new(dir.path(), false).unwrap();
    first.put(b"k".to_vec(), b"v".to_vec()).unwrap();
    drop(first);

    let second = Storage::new(dir.path(), false).unwrap();
    assert_eq!(second.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
}

#[test]
fn shared_handles_keep_the_lock_until_the_last_clone_drops() {
    let dir = TempDir::new().unwrap();
    let db = SharedStorage::new(dir.path(), false).unwrap();
    let clone = db.clone();
    drop(db);

    assert!(is_busy(&Storage::new(dir.path(), false).err().unwrap()));
    drop(clone);
    Storage::new(dir.path(), false).unwrap();
}

#[test]
fn a_checkpoint_does_not_carry_the_lock_with_it() {
    let dir = TempDir::new().unwrap();
    let backup = TempDir::new().unwrap();
    let target = backup.path().join("snap");

    let mut db = Storage::new(dir.path(), false).unwrap();
    db.put(b"k".to_vec(), b"v".to_vec()).unwrap();
    db.checkpoint(&target).unwrap();

    let restored = Storage::new(&target, false).unwrap();
    assert_eq!(restored.get(&b"k".to_vec()).unwrap(), Some(b"v".to_vec()));
}
