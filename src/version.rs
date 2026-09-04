//! One stored version of a key, and the wall-clock expiry attached to it.

use crate::{Expiry, Value};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The current wall-clock time in milliseconds since the Unix epoch.
///
/// A clock that has been set before 1970 yields 0, which expires everything
/// with a deadline — erring towards hiding data rather than serving something
/// that should already be gone.
pub fn now_ms() -> Expiry {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as Expiry)
        .unwrap_or(0)
}

/// Convert a TTL measured from now into an absolute expiry.
pub fn ttl_to_expiry(ttl: Duration) -> Expiry {
    now_ms().saturating_add(ttl.as_millis() as Expiry)
}

/// One stored version of a key: its value (or a tombstone) plus when that
/// version stops being visible.
///
/// # Expiry is wall-clock, and evaluated on read
///
/// `expires_at` is an absolute instant in Unix milliseconds, not a countdown,
/// so it survives restarts without needing to be refreshed. A version is
/// hidden as soon as that instant passes, whoever is reading and whenever the
/// read happens.
///
/// This means **a snapshot isolates you from writes, not from time**. A
/// snapshot taken before a key expired will still stop returning it once the
/// deadline passes: the version is not deleted by any writer, it simply stops
/// being visible. Sequence numbers order writes against each other and have no
/// relationship to the clock.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Version {
    /// The stored value, or `None` for a tombstone recording a deletion.
    pub value: Option<Value>,
    /// Unix milliseconds after which this version is invisible. `None` means
    /// it never expires; a tombstone never carries one.
    pub expires_at: Option<Expiry>,
}

impl Version {
    /// A value that never expires.
    pub fn live(value: Value) -> Self {
        Version {
            value: Some(value),
            expires_at: None,
        }
    }

    /// A value that becomes invisible at `expires_at` (Unix milliseconds).
    pub fn expiring(value: Value, expires_at: Expiry) -> Self {
        Version {
            value: Some(value),
            expires_at: Some(expires_at),
        }
    }

    /// A tombstone recording a deletion.
    pub fn tombstone() -> Self {
        Version {
            value: None,
            expires_at: None,
        }
    }

    /// Whether this version is a tombstone.
    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// Whether this version's deadline has passed at `now`.
    ///
    /// The deadline is exclusive: a version expiring at exactly `now` is
    /// still visible, and becomes invisible one millisecond later.
    pub fn is_expired_at(&self, now: Expiry) -> bool {
        self.expires_at.is_some_and(|deadline| now > deadline)
    }

    /// The value visible at `now`: `None` for a tombstone *or* for a version
    /// whose deadline has passed.
    ///
    /// Reads go through this rather than touching `value` directly, so an
    /// expired version cannot be served by mistake.
    pub fn visible_at(&self, now: Expiry) -> Option<&Value> {
        if self.is_expired_at(now) {
            return None;
        }
        self.value.as_ref()
    }

    /// This version rewritten as a tombstone once its deadline has passed.
    ///
    /// Compaction uses this: an expired version cannot simply be dropped,
    /// because that would uncover an older version of the same key and
    /// resurrect a value the expiry was supposed to retire. Turning it into a
    /// tombstone keeps it shadowing what lies beneath, and the existing
    /// tombstone rules then decide when it is safe to drop entirely.
    pub fn collect_if_expired(self, now: Expiry) -> Self {
        if self.is_expired_at(now) {
            return Version::tombstone();
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_version_without_a_deadline_never_expires() {
        let v = Version::live(b"v".to_vec());
        assert!(!v.is_expired_at(0));
        assert!(!v.is_expired_at(Expiry::MAX));
        assert_eq!(v.visible_at(Expiry::MAX), Some(&b"v".to_vec()));
    }

    #[test]
    fn the_deadline_is_exclusive() {
        let v = Version::expiring(b"v".to_vec(), 1_000);
        assert!(!v.is_expired_at(999));
        assert!(
            !v.is_expired_at(1_000),
            "expiring at exactly now is visible"
        );
        assert!(v.is_expired_at(1_001));
    }

    #[test]
    fn an_expired_version_is_invisible_even_though_it_holds_a_value() {
        let v = Version::expiring(b"v".to_vec(), 100);
        assert!(v.value.is_some(), "the value is still stored");
        assert_eq!(v.visible_at(101), None, "but it must not be served");
    }

    #[test]
    fn collecting_turns_an_expired_version_into_a_tombstone() {
        let v = Version::expiring(b"v".to_vec(), 100);
        assert_eq!(v.clone().collect_if_expired(50), v, "not yet due");

        let collected = v.collect_if_expired(101);
        assert!(collected.is_tombstone());
        assert_eq!(collected.expires_at, None);
    }

    #[test]
    fn a_tombstone_carries_no_deadline_and_never_expires() {
        let t = Version::tombstone();
        assert!(t.is_tombstone());
        assert!(!t.is_expired_at(Expiry::MAX));
        assert_eq!(t.visible_at(0), None);
    }

    #[test]
    fn a_ttl_becomes_an_absolute_deadline_in_the_future() {
        let before = now_ms();
        let deadline = ttl_to_expiry(Duration::from_secs(60));
        assert!(deadline >= before + 60_000);
        assert!(deadline <= now_ms() + 60_000);
    }
}
