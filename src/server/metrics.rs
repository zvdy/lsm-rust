//! A minimal HTTP endpoint exposing store metrics for Prometheus.
//!
//! Serves `GET /metrics` in the Prometheus text exposition format, backed by
//! [`SharedStorage::stats`]. The server is deliberately tiny and dependency
//! free — it speaks just enough HTTP/1.1 to answer a scrape:
//!
//! ```text
//! $ curl -s http://127.0.0.1:9898/metrics
//! # HELP lsm_puts_total Total put operations applied.
//! # TYPE lsm_puts_total counter
//! lsm_puts_total 42
//! ...
//! ```
//!
//! One thread accepts connections; each request is served on its own thread
//! and the connection is closed afterwards (`Connection: close`). Dropping the
//! [`MetricsServer`] stops the accept loop and joins it.

use crate::storage::SharedStorage;
use std::io::{self, BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

/// Cap on the request line + headers we will read, to bound per-connection
/// memory against a client that never sends a blank line.
const MAX_REQUEST_BYTES: usize = 8 * 1024;

/// A running Prometheus metrics server. Dropping the handle stops the accept
/// loop and waits for it to exit.
pub struct MetricsServer {
    stop: Arc<AtomicBool>,
    local_addr: std::net::SocketAddr,
    accept_thread: Option<JoinHandle<()>>,
}

impl MetricsServer {
    /// Start serving metrics for `storage` on `listener` in a background
    /// thread.
    pub fn spawn(storage: SharedStorage, listener: TcpListener) -> crate::Result<Self> {
        let local_addr = listener.local_addr()?;
        let stop = Arc::new(AtomicBool::new(false));

        let accept_stop = Arc::clone(&stop);
        let accept_thread = thread::spawn(move || {
            for stream in listener.incoming() {
                if accept_stop.load(Ordering::Relaxed) {
                    break;
                }
                match stream {
                    Ok(stream) => {
                        let storage = storage.clone();
                        thread::spawn(move || {
                            let _ = handle_connection(stream, &storage);
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        Ok(MetricsServer {
            stop,
            local_addr,
            accept_thread: Some(accept_thread),
        })
    }

    /// The address the server is listening on (useful with port 0).
    pub fn local_addr(&self) -> std::net::SocketAddr {
        self.local_addr
    }
}

impl Drop for MetricsServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        // Wake the blocking accept() with a throwaway connection
        let _ = TcpStream::connect(self.local_addr);
        if let Some(thread) = self.accept_thread.take() {
            let _ = thread.join();
        }
    }
}

fn handle_connection(stream: TcpStream, storage: &SharedStorage) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);

    // Parse just the request line ("METHOD PATH VERSION"); the rest of the
    // headers are read and discarded up to the terminating blank line.
    let Some((method, path)) = read_request(&mut reader)? else {
        return Ok(()); // malformed or empty request
    };

    let mut writer = stream;
    match (method.as_str(), path.as_str()) {
        ("GET", "/metrics") => match storage.stats() {
            Ok(stats) => write_response(
                &mut writer,
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                stats.to_prometheus().as_bytes(),
            )?,
            Err(e) => write_response(
                &mut writer,
                "500 Internal Server Error",
                "text/plain; charset=utf-8",
                e.to_string().as_bytes(),
            )?,
        },
        ("GET", "/") | ("GET", "/health") | ("GET", "/healthz") => {
            write_response(&mut writer, "200 OK", "text/plain; charset=utf-8", b"OK\n")?
        }
        ("GET", _) => write_response(
            &mut writer,
            "404 Not Found",
            "text/plain; charset=utf-8",
            b"not found; try /metrics\n",
        )?,
        _ => write_response(
            &mut writer,
            "405 Method Not Allowed",
            "text/plain; charset=utf-8",
            b"method not allowed\n",
        )?,
    }
    writer.flush()
}

/// Read and parse an HTTP request line, consuming headers up to the blank
/// line. Returns `(method, path)`, or `None` on EOF / a malformed start line.
fn read_request(reader: &mut impl BufRead) -> io::Result<Option<(String, String)>> {
    let mut line = String::new();
    let mut total = 0;

    let n = reader.read_line(&mut line)?;
    if n == 0 {
        return Ok(None);
    }
    total += n;

    let mut parts = line.split_whitespace();
    let (Some(method), Some(path)) = (parts.next(), parts.next()) else {
        return Ok(None);
    };
    let (method, path) = (method.to_string(), path.to_string());

    // Drain remaining headers until a blank line, bounding total bytes read.
    loop {
        let mut header = String::new();
        let n = reader.read_line(&mut header)?;
        total += n;
        if n == 0 || header == "\r\n" || header == "\n" {
            break;
        }
        if total > MAX_REQUEST_BYTES {
            break;
        }
    }

    Ok(Some((method, path)))
}

fn write_response(
    w: &mut impl Write,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    write!(
        w,
        "HTTP/1.1 {status}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {len}\r\n\
         Connection: close\r\n\
         \r\n",
        len = body.len(),
    )?;
    w.write_all(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn parses_request_line_and_drains_headers() {
        let raw = "GET /metrics HTTP/1.1\r\nHost: x\r\nAccept: */*\r\n\r\n";
        let mut reader = BufReader::new(raw.as_bytes());
        let (method, path) = read_request(&mut reader).unwrap().unwrap();
        assert_eq!(method, "GET");
        assert_eq!(path, "/metrics");
    }

    #[test]
    fn empty_request_is_none() {
        let mut reader = BufReader::new(&b""[..]);
        assert!(read_request(&mut reader).unwrap().is_none());
    }

    #[test]
    fn write_response_has_headers_and_body() {
        let mut buf = Vec::new();
        write_response(&mut buf, "200 OK", "text/plain", b"hi").unwrap();
        let text = String::from_utf8(buf).unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(text.contains("Content-Length: 2\r\n"));
        assert!(text.contains("Connection: close\r\n"));
        assert!(text.ends_with("\r\n\r\nhi"));
    }

    // A tiny end-to-end scrape over a real socket.
    #[test]
    fn serves_metrics_over_http() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let db = SharedStorage::new(temp.path(), false).unwrap();
        db.put(b"k".to_vec(), b"v".to_vec()).unwrap();

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server = MetricsServer::spawn(db, listener).unwrap();
        let addr = server.local_addr();

        let mut conn = TcpStream::connect(addr).unwrap();
        conn.write_all(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        conn.read_to_string(&mut response).unwrap();

        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("text/plain; version=0.0.4"));
        assert!(response.contains("lsm_puts_total 1"));
        assert!(response.contains("# TYPE lsm_sequence gauge"));
    }

    #[test]
    fn unknown_path_is_404() {
        use tempfile::TempDir;

        let temp = TempDir::new().unwrap();
        let db = SharedStorage::new(temp.path(), false).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let server = MetricsServer::spawn(db, listener).unwrap();

        let mut conn = TcpStream::connect(server.local_addr()).unwrap();
        conn.write_all(b"GET /nope HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .unwrap();
        let mut response = String::new();
        conn.read_to_string(&mut response).unwrap();
        assert!(response.starts_with("HTTP/1.1 404 Not Found"));
    }
}
