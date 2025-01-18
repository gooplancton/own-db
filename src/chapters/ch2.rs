// Section 2.1: Types of queries
// Three main types of queries:
//  - point query: look up a specific record using a unique key
//  - scan query: scan the whole index for records matching a condition
//  - range query: find a starting point in a sorted index and iterate
//

use std::mem;

use byteorder::{BigEndian, ReadBytesExt};
use sha1::{Digest, Sha1};

// Section 2.2: Hashtables
// Hashtables are useful only for point queries, we'll just implement one for the sake
// of completeness
#[derive(Debug, PartialEq, Eq, Clone)]
struct HashtableEntry {
    pub key: String,
    pub value: String,
}

struct Hashtable {
    inner: Vec<Option<HashtableEntry>>,
    pub size: usize,
}

impl Default for Hashtable {
    fn default() -> Self {
        let inner = vec![None; 100];
        Self { inner, size: 0 }
    }
}

fn hash_key(key: &str) -> usize {
    let mut hasher = Sha1::default();

    hasher.update(key.as_bytes());
    let n = hasher
        .finalize()
        .as_slice()
        .get(0..8)
        .unwrap()
        .read_u64::<BigEndian>()
        .unwrap();

    n as usize
}

impl Hashtable {
    pub fn with_capacity(capacity: usize) -> Self {
        let inner = vec![None; capacity];
        Self { inner, size: 0 }
    }

    pub fn insert(&mut self, key: impl AsRef<str>, value: impl AsRef<str>) {
        let len = self.inner.len();
        let key = key.as_ref();
        let value = value.as_ref();
        let entry = HashtableEntry {
            key: key.to_owned(),
            value: value.to_owned(),
        };

        let n = hash_key(key);
        let start_idx = n % len;
        for offset in 0..len {
            let idx = (start_idx + offset) % len;
            let slot = &self.inner[idx];
            if slot.is_none() {
                self.inner[idx] = Some(entry);
                self.size += 1;

                let occupancy_rate = (self.size as f64) / (self.inner.len() as f64);
                if occupancy_rate > 0.66 {
                    self.rehash(self.size * 2);
                }

                return;
            }
        }

        panic!("out of memory");
    }

    pub fn get(&self, key: impl AsRef<str>) -> Option<&str> {
        let len = self.inner.len();
        let key = key.as_ref();
        let n = hash_key(key);
        let start_idx = n % len;

        for offset in 0..len {
            let idx = (start_idx + offset) % len;
            match self.inner[idx].as_ref() {
                Some(HashtableEntry {
                    key: entry_key,
                    value,
                }) if entry_key == key => return Some(value),
                None => return None,
                _ => continue,
            }
        }

        None
    }

    pub fn delete(&mut self, key: impl AsRef<str>) -> Option<String> {
        let len = self.inner.len();
        let key = key.as_ref();
        let n = hash_key(key);
        let start_idx = n % len;

        for offset in 0..len {
            let idx = (start_idx + offset) % len;
            let entry = self.inner[idx].as_ref();
            if let Some(entry) = entry {
                if entry.key.as_str() == key {
                    let entry = self.inner[idx].take().unwrap();
                    self.size -= 1;

                    return Some(entry.value);
                }
            }
        }

        None
    }

    fn rehash(&mut self, new_capacity: usize) {
        let entries = self.inner.clone();
        self.inner = vec![None; new_capacity];
        self.size = 0;

        entries.into_iter().flatten().for_each(|entry| {
            self.insert(entry.key.as_str(), entry.value.as_str());
        });
    }
}

#[cfg(test)]
mod hashtable_tests {
    use super::Hashtable;

    #[test]
    fn test_get() {
        let mut hashtable = Hashtable::default();
        hashtable.insert("a", "ciao");

        let val = hashtable.get("a");
        assert_eq!(val, Some("ciao"));
    }

    #[test]
    fn test_rehash() {
        let mut hashtable = Hashtable::with_capacity(1);
        hashtable.insert("a", "a");
        hashtable.insert("b", "b");
        hashtable.insert("c", "c");

        let val = hashtable.get("c");
        assert_eq!(val, Some("c"));
    }
}
