//! A declared length must not become an allocation before the bytes arrive.
//!
//! RESP asks the client to announce how long a bulk string will be. That
//! announcement is a claim, and sizing a buffer from it hands anyone who can
//! open a socket a large multiplier: a handful of bytes reserves as much
//! memory as the protocol's per-value ceiling allows, held for as long as the
//! sender cares to stall.
//!
//! This file holds one test on purpose. It measures process-wide allocation —
//! the work happens on the server's own connection thread, so a per-thread
//! counter would miss it — and cargo gives each integration test file its own
//! process, which keeps the measurement free of whatever the rest of the suite
//! is doing in parallel.

use lsm_rust::{RespServer, SharedStorage};
use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tempfile::TempDir;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Counting;

fn grew(size: usize) {
    let now = LIVE.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(now, Ordering::Relaxed);
}

fn shrank(size: usize) {
    // `fetch_update` rather than `fetch_sub`: an allocation made before the
    // counter existed would otherwise underflow it.
    let _ = LIVE.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |live| {
        Some(live.saturating_sub(size))
    });
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            grew(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            grew(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        shrank(layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        if !new.is_null() {
            shrank(layout.size());
            grew(new_size);
        }
        new
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

#[test]
fn a_declared_size_is_not_allocated_before_the_bytes_arrive() {
    let temp = TempDir::new().unwrap();
    let db = SharedStorage::new(temp.path(), false).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let server = RespServer::spawn(db, listener).unwrap();

    let mut client = TcpStream::connect(server.local_addr()).unwrap();
    // Baseline after everything above is built, so the measurement covers only
    // what the server does in response to the bytes below.
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    let before = PEAK.load(Ordering::Relaxed);

    // An announcement of the largest command the protocol permits — a
    // million elements, the first of them 64 MB — and then silence. Neither
    // ever arrives. Both claims used to be believed on the spot: one element
    // slot per declared element, and the full 64 MB for a payload of nothing.
    let announcement = b"*1048576\r\n$67108864\r\n";
    client.write_all(announcement).unwrap();
    client.flush().unwrap();

    // Give the connection thread time to act on the announcement. If it is
    // going to reserve, it does so immediately on parsing the header.
    std::thread::sleep(Duration::from_millis(500));

    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(before);
    let sent = announcement.len();
    assert!(
        peak < 4 * 1024 * 1024,
        "{sent} bytes of input made the server hold {peak} bytes \
         ({}x amplification)",
        peak / sent.max(1)
    );
}
