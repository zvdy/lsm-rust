//! The crate's error type.
//!
//! Every fallible operation in this crate returns [`Error`], so a caller can
//! tell *why* something failed without inspecting error strings: on-disk
//! corruption, a losing transaction, caller misuse, and underlying I/O are
//! separate variants rather than one opaque [`std::io::Error`].
//!
//! Conversions run both ways. Any `io::Error` becomes [`Error::Io`], so `?`
//! works over `File` and socket calls inside this crate; and any `Error`
//! converts back into an `io::Error`, so callers whose own functions still
//! return [`std::io::Result`] are not stranded:
//!
//! ```no_run
//! use lsm_rust::Storage;
//!
//! // A caller that has not migrated its own signatures still compiles.
//! fn load() -> std::io::Result<Option<Vec<u8>>> {
//!     let db = Storage::new("./data", false)?;
//!     Ok(db.get(&b"key".to_vec())?)
//! }
//! ```

use crate::Key;
use std::fmt;
use std::io;

/// The result of any fallible operation in this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Why an operation failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Durable data did not match its checksum, or an on-disk structure could
    /// not be parsed.
    ///
    /// The bytes read back are not the bytes that were written. The store does
    /// not guess at the intent: it reports this rather than returning data
    /// that merely looks plausible.
    Corruption(String),
    /// A transaction lost an optimistic race and was aborted; it had no
    /// effect and can be retried. See [`Error::is_retriable`].
    Conflict {
        /// A key another transaction committed first.
        key: Key,
    },
    /// The caller asked for something impossible — a sequence number beyond
    /// the store's current one, an unparseable argument.
    InvalidArgument(String),
    /// The underlying filesystem or socket failed.
    Io(io::Error),
}

impl Error {
    /// Whether retrying the operation could succeed.
    ///
    /// True only for [`Error::Conflict`]: the transaction was rolled back
    /// before anything was written, so replaying it against a fresh snapshot
    /// is safe. Corruption, invalid arguments and I/O failures will not fix
    /// themselves on a retry.
    pub fn is_retriable(&self) -> bool {
        matches!(self, Error::Conflict { .. })
    }

    /// Whether this is on-disk corruption, as opposed to any other failure.
    pub fn is_corruption(&self) -> bool {
        matches!(self, Error::Corruption(_))
    }

    /// Build a [`Error::Corruption`] from anything displayable.
    pub(crate) fn corruption(detail: impl fmt::Display) -> Self {
        Error::Corruption(detail.to_string())
    }

    /// Build an [`Error::InvalidArgument`] from anything displayable.
    pub(crate) fn invalid_argument(detail: impl fmt::Display) -> Self {
        Error::InvalidArgument(detail.to_string())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Corruption(detail) => write!(f, "corrupt data: {}", detail),
            Error::Conflict { key } => write!(
                f,
                "transaction conflict on key {:?}",
                String::from_utf8_lossy(key)
            ),
            Error::InvalidArgument(detail) => write!(f, "invalid argument: {}", detail),
            Error::Io(e) => write!(f, "I/O error: {}", e),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<Error> for io::Error {
    /// Map back onto the closest `io::ErrorKind` so callers that still return
    /// [`std::io::Result`] keep working. `Io` passes through untouched, so a
    /// round trip through this crate preserves the original OS error.
    fn from(e: Error) -> Self {
        match e {
            Error::Io(e) => e,
            Error::Corruption(_) => io::Error::new(io::ErrorKind::InvalidData, e.to_string()),
            Error::Conflict { .. } => io::Error::new(io::ErrorKind::WouldBlock, e.to_string()),
            Error::InvalidArgument(_) => io::Error::new(io::ErrorKind::InvalidInput, e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_conflicts_are_retriable() {
        assert!(Error::Conflict { key: b"k".to_vec() }.is_retriable());
        assert!(!Error::corruption("bad crc").is_retriable());
        assert!(!Error::invalid_argument("nope").is_retriable());
        assert!(!Error::from(io::Error::other("disk")).is_retriable());
    }

    #[test]
    fn io_errors_round_trip_without_being_rewrapped() {
        let original = io::Error::new(io::ErrorKind::NotFound, "missing.sst");
        let back: io::Error = Error::from(original).into();
        assert_eq!(back.kind(), io::ErrorKind::NotFound);
        assert_eq!(back.to_string(), "missing.sst");
    }

    #[test]
    fn variants_map_onto_meaningful_io_kinds() {
        let corruption: io::Error = Error::corruption("crc mismatch").into();
        assert_eq!(corruption.kind(), io::ErrorKind::InvalidData);

        let conflict: io::Error = Error::Conflict { key: b"a".to_vec() }.into();
        assert_eq!(conflict.kind(), io::ErrorKind::WouldBlock);

        let invalid: io::Error = Error::invalid_argument("seq 9 > 3").into();
        assert_eq!(invalid.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn display_names_the_conflicting_key() {
        let e = Error::Conflict {
            key: b"user:1".to_vec(),
        };
        assert!(e.to_string().contains("user:1"), "{}", e);
    }

    #[test]
    fn io_variant_exposes_its_source() {
        use std::error::Error as _;
        let e = Error::from(io::Error::other("underlying"));
        assert!(e.source().is_some());
        assert!(Error::corruption("x").source().is_none());
    }
}
