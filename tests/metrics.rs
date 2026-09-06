//! Integration tests for the operational metrics / Prometheus endpoint.

use lsm_rust::{SharedStorage, Storage, StorageConfig};
use tempfile::TempDir;

#[test]
fn stats_reflect_operations() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::new(temp.path(), false).unwrap();

    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();
    db.put(b"b".to_vec(), b"2".to_vec()).unwrap();
    db.delete(&b"a".to_vec()).unwrap();
    let _ = db.get(&b"b".to_vec()).unwrap();
    let _ = db.get(&b"missing".to_vec()).unwrap();
    let _ = db.scan(b"a", b"z").unwrap();

    let stats = db.stats();
    assert_eq!(stats.puts_total, 2);
    assert_eq!(stats.deletes_total, 1);
    assert_eq!(stats.gets_total, 2);
    assert_eq!(stats.scans_total, 1);
    // Three writes each advance the sequence counter
    assert_eq!(stats.sequence, 3);
    assert!(stats.memtable_entries >= 3);
    assert!(stats.memtable_bytes > 0);
    // Nothing has flushed yet, so there are no SSTables
    assert_eq!(stats.total_sstables(), 0);
}

#[test]
fn stats_report_sstables_after_flush() {
    let temp = TempDir::new().unwrap();
    let config = StorageConfig {
        memtable_size_threshold: 4 * 1024,
        ..StorageConfig::default()
    };
    let mut db = Storage::with_config(temp.path(), config).unwrap();

    for i in 0..500 {
        db.put(format!("k{:04}", i).into_bytes(), vec![b'v'; 64])
            .unwrap();
    }

    let stats = db.stats();
    assert!(stats.flushes_total >= 1, "at least one flush should occur");
    assert!(
        stats.total_sstables() >= 1,
        "flushed data should appear as SSTables"
    );
    assert!(stats.total_sstable_bytes() > 0);
    // Prometheus rendering carries a per-level series for whichever level(s)
    // the flushed data ended up on (level 0, or deeper after compaction).
    let text = stats.to_prometheus();
    assert!(text.contains("lsm_sstables{level="));
}

#[test]
fn live_snapshots_are_counted() {
    let temp = TempDir::new().unwrap();
    let mut db = Storage::new(temp.path(), false).unwrap();
    db.put(b"k".to_vec(), b"v".to_vec()).unwrap();

    assert_eq!(db.stats().live_snapshots, 0);
    let snap = db.snapshot();
    assert_eq!(db.stats().live_snapshots, 1);
    {
        let _snap2 = db.snapshot();
        assert_eq!(db.stats().live_snapshots, 2);
    }
    assert_eq!(db.stats().live_snapshots, 1);
    drop(snap);
    assert_eq!(db.stats().live_snapshots, 0);
}

#[test]
fn shared_storage_stats() {
    let temp = TempDir::new().unwrap();
    let db = SharedStorage::new(temp.path(), false).unwrap();
    db.put(b"a".to_vec(), b"1".to_vec()).unwrap();

    let stats = db.stats().unwrap();
    assert_eq!(stats.puts_total, 1);
    assert!(stats.to_prometheus().contains("lsm_puts_total 1"));
}

/// Connect, send `request` verbatim, and return the status line.
///
/// A read timeout is deliberate: an unbounded server does not *fail* these
/// tests, it waits for a newline that never comes, and a hung CI job is worse
/// to diagnose than a failed one.
fn status_line_for(request: &[u8]) -> String {
    use std::io::{BufRead, BufReader, Write};
    use std::net::{TcpListener, TcpStream};

    let temp = TempDir::new().unwrap();
    let db = SharedStorage::new(temp.path(), false).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let server = lsm_rust::MetricsServer::spawn(db, listener).unwrap();

    let mut writer = TcpStream::connect(server.local_addr()).unwrap();
    writer
        .set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    let mut reader = BufReader::new(writer.try_clone().unwrap());

    writer.write_all(request).unwrap();
    writer.flush().unwrap();

    let mut status = String::new();
    reader.read_line(&mut status).unwrap();
    status.trim_end().to_string()
}

#[test]
fn an_over_long_request_line_is_rejected() {
    // No newline anywhere: the request line has to be buffered before anything
    // can inspect it, so without a per-line bound this grows without limit and
    // the byte-total limit is never consulted.
    let mut request = vec![b'A'; 70 * 1024];
    request.extend_from_slice(b"\r\n\r\n");
    assert_eq!(
        status_line_for(&request),
        "HTTP/1.1 431 Request Header Fields Too Large"
    );
}

#[test]
fn oversized_headers_are_rejected() {
    // A single header far past the 8 KiB request budget. Checking the total
    // only *after* reading each line lets one line blow the budget by any
    // margin it likes before the check runs.
    let mut request = b"GET /metrics HTTP/1.1\r\nX-Pad: ".to_vec();
    request.extend_from_slice(&vec![b'A'; 16 * 1024]);
    request.extend_from_slice(b"\r\n\r\n");
    assert_eq!(
        status_line_for(&request),
        "HTTP/1.1 431 Request Header Fields Too Large"
    );
}

#[test]
fn a_normal_scrape_is_unaffected_by_the_limits() {
    let request = b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nUser-Agent: Prometheus/2.0\r\nAccept: */*\r\n\r\n";
    assert_eq!(status_line_for(request), "HTTP/1.1 200 OK");
}

#[test]
fn many_small_headers_are_bounded_in_total() {
    // Each line is individually fine; their sum is not. Bounding only the
    // per-line size would let a client stream headers for ever.
    let mut request = b"GET /metrics HTTP/1.1\r\n".to_vec();
    for i in 0..400 {
        request.extend_from_slice(format!("X-Pad-{i:04}: {}\r\n", "p".repeat(64)).as_bytes());
    }
    request.extend_from_slice(b"\r\n");
    assert_eq!(
        status_line_for(&request),
        "HTTP/1.1 431 Request Header Fields Too Large"
    );
}
