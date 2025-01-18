#![allow(dead_code)]
use rand::prelude::*;
use sha1::{Digest, Sha1};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
};

// Section 1.1: first naive implementation
// problems with this implementation:
// - updates the content as a whole, not practical for large data
// - atomicity is not guaranteed. If the program crashes while writing or syncing the file, it will
//   be corrupted
// - no consistent way to grant concurrent access to the file, while one client is writing to the
//   file, another one could be trying to read it, obtaining potentially invalid data
fn save_data1(path: impl AsRef<Path>, data: impl AsRef<[u8]>) -> io::Result<()> {
    let path = path.as_ref();
    let mut file = File::open(path)?;
    file.write_all(data.as_ref())?;

    file.sync_all() // NOTE: uses the fsync syscall
}

// Section 1.2: adding atomicity through renaming
// this version implements atomic saves through renaming, since at the os level file renames are
// simply a matter of updating a table with mappings from names to file descriptors. Since
// renaming only happens after the contents of the temp file have been completely written,
// concurrent readers either access the old or new data in their entirety. However, we're still
// facing issues in the case of a power loss. In the case of a power loss, this version is not even
// durable (why?), we would need to call fsync on the parent directory as well.
fn save_data2(path: impl AsRef<Path>, data: impl AsRef<[u8]>) -> io::Result<()> {
    let path = path.as_ref();
    let data = data.as_ref();
    let temp_file_path = format!("{}.tmp.{}", path.to_string_lossy(), random::<u8>());
    let mut temp_file = File::create(&temp_file_path)?;
    temp_file.write_all(data)?;
    temp_file.sync_all()?;

    fs::rename(temp_file_path, path)
}

// Section 1.3: append only logs [EXERCISE]
// we can append changes to the file in the form of log entries rather than editing the file's
// contents. E.g.
// (set a = 1); (set b = 2); (set a = 4); (del b); => { "a": 4 }
// problem: the last log could still get corrupted in the case of a power loss. we need to
// implement a checksum mechanism to ensure each log entry is valid
// (set a = 1, sha1(set a = 1)); ... => { "a": 1 }
#[derive(Debug, PartialEq, Eq)]
enum LogEntry {
    Set {
        key: String,
        value: String,
        checksum: String,
    },
    Del {
        key: String,
        checksum: String,
    },
}

const SET_ENTRY: &str = "SET";
const DEL_ENTRY: &str = "DEL";

#[derive(Debug)]
enum LogEntryCreationError {
    InvalidDiscriminant,
    InvalidEntryFormat,
    IncorrectChecksum,
}

impl TryFrom<&str> for LogEntry {
    type Error = LogEntryCreationError;

    fn try_from(value: &str) -> Result<Self, LogEntryCreationError> {
        let mut hasher = Sha1::default();
        let mut segments = value.split(' ');
        let discriminant = segments
            .next()
            .ok_or(LogEntryCreationError::InvalidEntryFormat)?;

        hasher.update(discriminant);

        let key = segments
            .next()
            .ok_or(LogEntryCreationError::InvalidEntryFormat)?;

        hasher.update(key);

        match discriminant {
            SET_ENTRY => {
                let value = segments
                    .next()
                    .ok_or(LogEntryCreationError::InvalidEntryFormat)?;

                hasher.update(value);

                let received_hash = segments
                    .next()
                    .ok_or(LogEntryCreationError::InvalidEntryFormat)?;

                let expected_hash = hasher.finalize();

                if received_hash.as_bytes() != expected_hash.as_slice() {
                    return Err(LogEntryCreationError::IncorrectChecksum);
                }

                Ok(LogEntry::Set {
                    key: key.to_owned(),
                    value: key.to_owned(),
                    checksum: received_hash.to_owned(),
                })
            }
            DEL_ENTRY => {
                let received_hash = segments
                    .next()
                    .ok_or(LogEntryCreationError::InvalidEntryFormat)?;

                let expected_hash = hasher.finalize();

                if received_hash.as_bytes() != expected_hash.as_slice() {
                    return Err(LogEntryCreationError::IncorrectChecksum);
                }

                Ok(LogEntry::Del {
                    key: key.to_owned(),
                    checksum: received_hash.to_owned(),
                })
            }
            _ => Err(LogEntryCreationError::InvalidDiscriminant),
        }
    }
}

impl LogEntry {
    pub fn create_set(key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        let key = key.as_ref();
        let value = value.as_ref();
        let mut hasher = Sha1::default();
        hasher.update(SET_ENTRY);
        hasher.update(key);
        hasher.update(value);
        let checksum = hasher.finalize();
        let checksum = String::from_utf8_lossy(checksum.as_slice()).to_string();

        LogEntry::Set {
            key: key.to_owned(),
            value: value.to_owned(),
            checksum,
        }
    }

    pub fn create_delete(key: impl AsRef<str>) -> Self {
        let key = key.as_ref();
        let mut hasher = Sha1::default();
        hasher.update(DEL_ENTRY);
        hasher.update(key);
        let checksum = hasher.finalize();
        let checksum = String::from_utf8_lossy(checksum.as_slice()).to_string();

        LogEntry::Del {
            key: key.to_owned(),
            checksum,
        }
    }
}

struct AppendOnlyLogDB {
    path: PathBuf,
    entries: Vec<LogEntry>,
}

#[derive(Debug)]
enum AppendOnlyLogDBCreationError {
    IO(io::Error),
    LogEntry(LogEntryCreationError),
}

impl From<io::Error> for AppendOnlyLogDBCreationError {
    fn from(value: io::Error) -> Self {
        Self::IO(value)
    }
}

impl From<LogEntryCreationError> for AppendOnlyLogDBCreationError {
    fn from(value: LogEntryCreationError) -> Self {
        Self::LogEntry(value)
    }
}

impl AppendOnlyLogDB {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, AppendOnlyLogDBCreationError> {
        let path = path.as_ref();
        let file = File::create(path)?;
        file.sync_all()?;

        Ok(Self {
            path: path.to_path_buf(),
            entries: vec![],
        })
    }

    pub fn from_path(path: impl AsRef<Path>) -> Result<Self, AppendOnlyLogDBCreationError> {
        let path = path.as_ref();
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut line = String::new();
        let mut entries = vec![];
        while reader.read_line(&mut line).is_ok() {
            let entry = LogEntry::try_from(line.as_str())?;
            entries.push(entry);
        }

        Ok(Self {
            path: path.to_path_buf(),
            entries,
        })
    }

    pub fn set(&mut self, key: impl AsRef<str>, value: impl AsRef<str>) {
        let entry = LogEntry::create_set(key, value);
        let _ = self.sync_entry(&entry);
        self.entries.push(entry);
    }

    pub fn delete(&mut self, key: impl AsRef<str>) {
        let entry = LogEntry::create_delete(key);
        let sync_res = self.sync_entry(&entry);
        if let Err(err) = sync_res {
            eprintln!("error while syncing state to file: {}", err);
        }

        self.entries.push(entry);
    }

    pub fn get(&self, key: impl AsRef<str>) -> Option<&str> {
        let key = key.as_ref();

        let relevant_entry = self.entries.iter().rev().find(|entry| match entry {
            LogEntry::Set {
                key: entry_key,
                value: _,
                checksum: _,
            } => entry_key == key,
            LogEntry::Del {
                key: entry_key,
                checksum: _,
            } => entry_key == key,
        });

        relevant_entry.and_then(|entry| match entry {
            LogEntry::Set {
                key: _,
                value,
                checksum: _,
            } => Some(value.as_str()),
            LogEntry::Del {
                key: _,
                checksum: _,
            } => None,
        })
    }

    fn sync_entry(&self, entry: &LogEntry) -> io::Result<()> {
        let file = OpenOptions::new().append(true).open(self.path.as_path())?;
        let mut writer = BufWriter::new(file);

        match entry {
            LogEntry::Set {
                key,
                value,
                checksum,
            } => writer.write_fmt(format_args!("{} {} {} {}\n", SET_ENTRY, key, value, checksum)),
            LogEntry::Del { key, checksum } => {
                writer.write_fmt(format_args!("{} {} {}\n", DEL_ENTRY, key, checksum))
            }
        }?;

        let file = writer.into_inner()?;
        file.sync_all()
    }
}

#[cfg(test)]
mod tests_append_only {
    use super::*;

    #[test]
    fn test_set() {
        let mut log = AppendOnlyLogDB::new("/tmp/append-only-log").unwrap();
        log.set("a", "ciao");
        let val = log.get("a");

        assert_eq!(val, Some("ciao"));
    }

    #[test]
    fn test_delete() {
        let mut log = AppendOnlyLogDB::new("/tmp/append-only-log").unwrap();
        log.set("a", "ciao");
        let val = log.get("a");

        assert_eq!(val, Some("ciao"));

        log.delete("a");

        let val = log.get("a");
        assert_eq!(val, None);
    }
}

// Section 1.4: fsync gotchas
// - directories are just a mapping from file names to file descriptors, and just like files, are
// not durable unless fsync is called on them
// - due to os caching, even if fsync fails the updated data might be available anyway
//
