use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Name of the manifest file inside the data directory.
pub const MANIFEST_FILE: &str = "MANIFEST";
const MANIFEST_TMP: &str = "MANIFEST.tmp";
const MANIFEST_VERSION: &str = "lsm-manifest v1";

/// One live SSTable recorded in the manifest.
#[derive(Debug, PartialEq, Eq)]
pub struct ManifestEntry {
    pub level: usize,
    pub seq: u64,
    pub filename: String,
}

/// The manifest is the authoritative record of which SSTables are live.
///
/// It is rewritten atomically (write to a temp file, fsync, rename over the
/// old manifest) at every point where the table set changes — after a flush
/// and after a compaction — making that rename the commit point:
///
/// - A crash *before* the rename leaves the old manifest in place; any
///   half-written new table is simply not referenced and is deleted as an
///   orphan on the next startup.
/// - A crash *after* the rename (but before old files are deleted) leaves
///   the new manifest in place; the stale old tables are unreferenced and
///   likewise removed on the next startup.
pub struct Manifest {
    path: PathBuf,
    dir: PathBuf,
}

impl Manifest {
    pub fn new(data_dir: &Path) -> Self {
        Manifest {
            path: data_dir.join(MANIFEST_FILE),
            dir: data_dir.to_path_buf(),
        }
    }

    pub fn exists(&self) -> bool {
        self.path.exists()
    }

    /// Load the manifest. Returns `None` if no manifest exists (a legacy
    /// data directory); the caller should fall back to a directory scan and
    /// write an initial manifest.
    pub fn load(&self) -> io::Result<Option<Vec<ManifestEntry>>> {
        if !self.exists() {
            return Ok(None);
        }
        let text = fs::read_to_string(&self.path)?;
        let mut lines = text.lines();

        match lines.next() {
            Some(MANIFEST_VERSION) => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unrecognized manifest header",
                ))
            }
        }

        let mut entries = Vec::new();
        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            let mut parts = line.split_whitespace();
            let (Some(level), Some(seq), Some(filename)) =
                (parts.next(), parts.next(), parts.next())
            else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("malformed manifest line: {:?}", line),
                ));
            };
            let level = level
                .parse::<usize>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad manifest level"))?;
            let seq = seq
                .parse::<u64>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad manifest seq"))?;
            entries.push(ManifestEntry {
                level,
                seq,
                filename: filename.to_string(),
            });
        }
        Ok(Some(entries))
    }

    /// Atomically replace the manifest with the given table set.
    pub fn write(&self, entries: &[ManifestEntry]) -> io::Result<()> {
        let tmp_path = self.dir.join(MANIFEST_TMP);
        {
            let mut tmp = File::create(&tmp_path)?;
            writeln!(tmp, "{}", MANIFEST_VERSION)?;
            for entry in entries {
                writeln!(tmp, "{} {} {}", entry.level, entry.seq, entry.filename)?;
            }
            tmp.sync_all()?;
        }
        fs::rename(&tmp_path, &self.path)?;

        // fsync the directory so the rename itself survives a crash
        // (best-effort: not all platforms support opening directories)
        if let Ok(dir) = File::open(&self.dir) {
            let _ = dir.sync_all();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn entry(level: usize, seq: u64) -> ManifestEntry {
        ManifestEntry {
            level,
            seq,
            filename: format!("L{}_{}.sst", level, seq),
        }
    }

    #[test]
    fn test_missing_manifest_is_none() {
        let temp_dir = TempDir::new().unwrap();
        let manifest = Manifest::new(temp_dir.path());
        assert!(!manifest.exists());
        assert_eq!(manifest.load().unwrap(), None);
    }

    #[test]
    fn test_write_and_load_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let manifest = Manifest::new(temp_dir.path());

        let entries = vec![entry(0, 3), entry(0, 4), entry(1, 2)];
        manifest.write(&entries).unwrap();

        assert_eq!(manifest.load().unwrap(), Some(entries));
        // The temp file must not linger after the rename
        assert!(!temp_dir.path().join(MANIFEST_TMP).exists());
    }

    #[test]
    fn test_rewrite_replaces_previous_contents() {
        let temp_dir = TempDir::new().unwrap();
        let manifest = Manifest::new(temp_dir.path());

        manifest.write(&[entry(0, 1), entry(0, 2)]).unwrap();
        manifest.write(&[entry(1, 3)]).unwrap();

        assert_eq!(manifest.load().unwrap(), Some(vec![entry(1, 3)]));
    }

    #[test]
    fn test_corrupt_manifest_is_an_error() {
        let temp_dir = TempDir::new().unwrap();
        fs::write(temp_dir.path().join(MANIFEST_FILE), "not a manifest\n").unwrap();
        let manifest = Manifest::new(temp_dir.path());
        assert!(manifest.load().is_err());
    }
}
