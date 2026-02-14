use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use serde_json;

/// Supported Redis data types.
#[derive(Clone, Debug)]
pub enum DataType {
    String(Bytes),
    List(Vec<Bytes>),
    Set(HashSet<Bytes>),
    Hash(HashMap<String, Bytes>),
    ZSet(HashMap<String, f64>),
    Json(serde_json::Value),
}

/// A thread-safe Redis-like database.
#[derive(Clone)]
pub struct Db {
    shared: Arc<RwLock<HashMap<String, DataType>>>,
}

impl Db {
    /// Create a new, empty `Db` instance.
    pub fn new() -> Db {
        Db {
            shared: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the value associated with a key.
    ///
    /// Returns `None` if the key does not exist or if the value is not a String.
    pub fn get(&self, key: &str) -> Option<Bytes> {
        let state = self.shared.read().unwrap();
        match state.get(key) {
            Some(DataType::String(b)) => Some(b.clone()),
            _ => None,
        }
    }

    /// Set the value associated with a key.
    pub fn set(&self, key: String, value: Bytes) {
        let mut state = self.shared.write().unwrap();
        state.insert(key, DataType::String(value));
    }

    /// Delete the value associated with `key`.
    ///
    /// Returns `true` if the value was removed.
    pub fn delete(&self, key: &str) -> bool {
        let mut state = self.shared.write().unwrap();
        state.remove(key).is_some()
    }

    /// Check if a key exists.
    pub fn exists(&self, key: &str) -> bool {
        let state = self.shared.read().unwrap();
        state.contains_key(key)
    }

    /// Return all keys in the database.
    pub fn keys(&self) -> Vec<String> {
        let state = self.shared.read().unwrap();
        state.keys().cloned().collect()
    }

    /// Return the number of keys in the database.
    pub fn len(&self) -> usize {
        let state = self.shared.read().unwrap();
        state.len()
    }

    /// Clear the database.
    pub fn clear(&self) {
        let mut state = self.shared.write().unwrap();
        state.clear();
    }
    
    // Type-specific accessors
    
    pub fn get_value(&self, key: &str) -> Option<DataType> {
        let state = self.shared.read().unwrap();
        state.get(key).cloned()
    }

    pub fn set_value(&self, key: String, value: DataType) {
        let mut state = self.shared.write().unwrap();
        state.insert(key, value);
    }
}
