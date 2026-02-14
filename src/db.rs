use bytes::Bytes;
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use serde_json;
use std::hash::{Hash, Hasher, BuildHasher};
use ahash::{AHashMap, RandomState};

/// Supported Redis data types.
/// Keys and Fields are now Bytes (Zero-Copy).
#[derive(Clone, Debug)]
pub enum DataType {
    String(Bytes),
    List(VecDeque<Bytes>),
    Set(HashSet<Bytes>),
    Hash(AHashMap<Bytes, Bytes>),
    ZSet(AHashMap<Bytes, f64>), // Simplified ZSet
    Json(serde_json::Value),
}

/// A thread-safe, sharded Redis-like database.
#[derive(Clone)]
pub struct Db {
    // Shards for data storage using fast AHashMap and Bytes keys
    shards: Vec<Arc<RwLock<AHashMap<Bytes, DataType>>>>,
    // Hasher builder for consistent sharding
    hasher: RandomState,
}

const SHARD_COUNT: usize = 64;

impl Db {
    /// Create a new, empty `Db` instance with sharding.
    pub fn new() -> Db {
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(Arc::new(RwLock::new(AHashMap::new())));
        }
        Db { 
            shards,
            hasher: RandomState::new()
        }
    }

    fn get_shard(&self, key: &[u8]) -> usize {
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % SHARD_COUNT
    }

    /// Get the value associated with a key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let shard_idx = self.get_shard(key);
        let shard = self.shards[shard_idx].read().unwrap();
        match shard.get(key) {
            Some(DataType::String(b)) => Some(b.clone()),
            _ => None,
        }
    }

    /// Set the value associated with a key.
    pub fn set(&self, key: Bytes, value: Bytes) {
        let shard_idx = self.get_shard(&key);
        let mut shard = self.shards[shard_idx].write().unwrap();
        shard.insert(key, DataType::String(value));
    }

    /// Delete the value associated with `key`.
    pub fn delete(&self, key: &[u8]) -> bool {
        let shard_idx = self.get_shard(key);
        let mut shard = self.shards[shard_idx].write().unwrap();
        shard.remove(key).is_some()
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        let shard_idx = self.get_shard(key);
        let shard = self.shards[shard_idx].read().unwrap();
        shard.contains_key(key)
    }

    /// Return all keys in the database.
    pub fn keys(&self) -> Vec<Bytes> {
        let mut keys = Vec::new();
        for shard in &self.shards {
             let state = shard.read().unwrap();
             keys.extend(state.keys().cloned());
        }
        keys
    }

    /// Return the number of keys in the database.
    pub fn len(&self) -> usize {
        let mut count = 0;
        for shard in &self.shards {
             let state = shard.read().unwrap();
             count += state.len();
        }
        count
    }

    /// Clear the database.
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut state = shard.write().unwrap();
            state.clear();
        }
    }

    // --- Type Specific Operations (Atomic) ---

    // Hash Operations
    pub fn hset(&self, key: Bytes, field: Bytes, value: Bytes) -> usize {
        let shard_idx = self.get_shard(&key);
        let mut shard = self.shards[shard_idx].write().unwrap();
        
        let entry = shard.entry(key).or_insert_with(|| DataType::Hash(AHashMap::new()));
        
        if let DataType::Hash(map) = entry {
            map.insert(field, value);
            1 
        } else {
            0 
        }
    }

    pub fn hget(&self, key: &[u8], field: &[u8]) -> Option<Bytes> {
        let shard_idx = self.get_shard(key);
        let shard = self.shards[shard_idx].read().unwrap();
        
        match shard.get(key) {
            Some(DataType::Hash(map)) => map.get(field).cloned(),
            _ => None,
        }
    }

    pub fn hdel(&self, key: &[u8], field: &[u8]) -> usize {
        let shard_idx = self.get_shard(key);
        let mut shard = self.shards[shard_idx].write().unwrap();
        
        match shard.get_mut(key) {
            Some(DataType::Hash(map)) => if map.remove(field).is_some() { 1 } else { 0 },
            _ => 0,
        }
    }

    pub fn hexists(&self, key: &[u8], field: &[u8]) -> usize {
        let shard_idx = self.get_shard(key);
        let shard = self.shards[shard_idx].read().unwrap();
         match shard.get(key) {
            Some(DataType::Hash(map)) => if map.contains_key(field) { 1 } else { 0 },
            _ => 0,
        }
    }

    pub fn hgetall(&self, key: &[u8]) -> Option<AHashMap<Bytes, Bytes>> {
        let shard_idx = self.get_shard(key);
        let shard = self.shards[shard_idx].read().unwrap();
        match shard.get(key) {
             Some(DataType::Hash(map)) => Some(map.clone()),
             _ => None
        }
    }
    
    pub fn hkeys(&self, key: &[u8]) -> Vec<Bytes> {
         let shard_idx = self.get_shard(key);
         let shard = self.shards[shard_idx].read().unwrap();
         match shard.get(key) {
             Some(DataType::Hash(map)) => map.keys().cloned().collect(),
             _ => Vec::new(),
         }
    }

    pub fn hvals(&self, key: &[u8]) -> Vec<Bytes> {
         let shard_idx = self.get_shard(key);
         let shard = self.shards[shard_idx].read().unwrap();
         match shard.get(key) {
             Some(DataType::Hash(map)) => map.values().cloned().collect(),
             _ => Vec::new(),
         }
    }
    
    pub fn hlen(&self, key: &[u8]) -> usize {
         let shard_idx = self.get_shard(key);
         let shard = self.shards[shard_idx].read().unwrap();
         match shard.get(key) {
             Some(DataType::Hash(map)) => map.len(),
             _ => 0,
         }
    }

    // List Operations
    pub fn lpush(&self, key: Bytes, value: Bytes) -> usize {
        let shard_idx = self.get_shard(&key);
        let mut shard = self.shards[shard_idx].write().unwrap();
        
        let entry = shard.entry(key).or_insert_with(|| DataType::List(VecDeque::new()));
        
        if let DataType::List(list) = entry {
            list.push_front(value);
            list.len()
        } else {
            0
        }
    }
    
    pub fn rpush(&self, key: Bytes, value: Bytes) -> usize {
        let shard_idx = self.get_shard(&key);
        let mut shard = self.shards[shard_idx].write().unwrap();
        
        let entry = shard.entry(key).or_insert_with(|| DataType::List(VecDeque::new()));
        
        if let DataType::List(list) = entry {
            list.push_back(value);
            list.len()
        } else {
            0
        }
    }

    pub fn lpop(&self, key: &[u8]) -> Option<Bytes> {
        let shard_idx = self.get_shard(key);
        let mut shard = self.shards[shard_idx].write().unwrap();
        
        match shard.get_mut(key) {
            Some(DataType::List(list)) => {
                let ret = list.pop_front();
                if list.is_empty() { shard.remove(key); }
                ret
            },
            _ => None,
        }
    }

    pub fn rpop(&self, key: &[u8]) -> Option<Bytes> {
        let shard_idx = self.get_shard(key);
        let mut shard = self.shards[shard_idx].write().unwrap();
        
        match shard.get_mut(key) {
             Some(DataType::List(list)) => {
                let ret = list.pop_back();
                if list.is_empty() { shard.remove(key); }
                ret
             },
             _ => None,
        }
    }

    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Vec<Bytes> {
        let shard_idx = self.get_shard(key);
        let shard = self.shards[shard_idx].read().unwrap();
        
        match shard.get(key) {
            Some(DataType::List(list)) => {
                let len = list.len() as i64;
                if len == 0 { return Vec::new(); }
                
                let start = if start < 0 { len + start } else { start };
                let stop = if stop < 0 { len + stop } else { stop };
                
                let start = start.max(0) as usize;
                let stop = stop.min(len - 1) as usize;
                
                if start > stop || start >= list.len() {
                    return Vec::new();
                }
                
                list.iter().skip(start).take(stop - start + 1).cloned().collect()
            },
            _ => Vec::new(),
        }
    }

    // Set Operations
    pub fn sadd(&self, key: Bytes, member: Bytes) -> usize {
        let shard_idx = self.get_shard(&key);
        let mut shard = self.shards[shard_idx].write().unwrap();
        
        let entry = shard.entry(key).or_insert_with(|| DataType::Set(HashSet::new()));
        
        if let DataType::Set(set) = entry {
            if set.insert(member) { 1 } else { 0 }
        } else {
            0
        }
    }

    pub fn smembers(&self, key: &[u8]) -> Vec<Bytes> {
        let shard_idx = self.get_shard(key);
        let shard = self.shards[shard_idx].read().unwrap();
        
        match shard.get(key) {
            Some(DataType::Set(set)) => set.iter().cloned().collect(),
            _ => Vec::new(),
        }
    }

    pub fn srem(&self, key: &[u8], member: &Bytes) -> usize {
        let shard_idx = self.get_shard(key);
        let mut shard = self.shards[shard_idx].write().unwrap();
        
        match shard.get_mut(key) {
            Some(DataType::Set(set)) => {
                let ret = if set.remove(member) { 1 } else { 0 };
                if set.is_empty() { shard.remove(key); }
                ret
            },
             _ => 0,
        }
    }

    // ZSet Operations
    pub fn zadd(&self, key: Bytes, score: f64, member: Bytes) -> usize {
        let shard_idx = self.get_shard(&key);
        let mut shard = self.shards[shard_idx].write().unwrap();
        
        let entry = shard.entry(key).or_insert_with(|| DataType::ZSet(AHashMap::new()));
        
        if let DataType::ZSet(scores) = entry {
            if scores.insert(member, score).is_none() { 1 } else { 0 }
        } else {
            0
        }
    }

    pub fn zrange(&self, key: &[u8], start: i64, stop: i64, with_scores: bool) -> Vec<(Bytes, f64)> {
        let shard_idx = self.get_shard(key);
        let shard = self.shards[shard_idx].read().unwrap();
        
        match shard.get(key) {
            Some(DataType::ZSet(scores)) => {
                 let mut sorted: Vec<(&Bytes, &f64)> = scores.iter().collect();
                 sorted.sort_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal));
                 
                 let len = sorted.len() as i64;
                 if len == 0 { return Vec::new(); }

                 let start = if start < 0 { len + start } else { start };
                 let stop = if stop < 0 { len + stop } else { stop };
                 
                 let start = start.max(0) as usize;
                 let stop = stop.min(len - 1) as usize;
                 
                 if start > stop || start >= sorted.len() {
                     return Vec::new();
                 }
                 
                 sorted[start..=stop].iter().map(|(k, v)| ((*k).clone(), **v)).collect()
            },
            _ => Vec::new(),
        }
    }

    pub fn get_value_clone(&self, key: &[u8]) -> Option<DataType> {
        let shard_idx = self.get_shard(key);
        let shard = self.shards[shard_idx].read().unwrap();
        shard.get(key).cloned()
    }
    
    pub fn set_value(&self, key: Bytes, value: DataType) {
        let shard_idx = self.get_shard(&key);
        let mut shard = self.shards[shard_idx].write().unwrap();
        shard.insert(key, value);
    }
}
