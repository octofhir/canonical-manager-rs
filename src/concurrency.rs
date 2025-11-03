//! Concurrency abstraction for concurrent maps
//!
//! A thin wrapper to allow swapping backends without touching call sites.

use dashmap::DashMap;
use std::hash::Hash;

pub struct ConcurrentMap<K, V> {
    inner: DashMap<K, V>,
}

impl<K, V> ConcurrentMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            inner: DashMap::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: DashMap::with_capacity(capacity),
        }
    }

    pub fn get_cloned(&self, key: &K) -> Option<V> {
        self.inner.get(key).map(|value| value.clone())
    }

    pub fn insert(&self, key: K, value: V) {
        self.inner.insert(key, value);
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.inner.remove(key).map(|(_, value)| value)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn clear(&self) {
        self.inner.clear();
    }

    pub fn keys(&self) -> Vec<K> {
        self.inner.iter().map(|entry| entry.key().clone()).collect()
    }

    pub fn iter_cloned(&self) -> Vec<(K, V)> {
        self.inner
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .collect()
    }

    pub fn modify<F: FnOnce(V) -> V>(&self, key: &K, f: F) {
        if let Some(mut entry) = self.inner.get_mut(key) {
            let current = entry.clone();
            *entry = f(current);
        }
    }
}

impl<K, V> Default for ConcurrentMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> std::fmt::Debug for ConcurrentMap<K, V>
where
    K: Clone + Eq + std::hash::Hash + std::fmt::Debug,
    V: Clone + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.inner.len();
        f.debug_struct("ConcurrentMap").field("len", &len).finish()
    }
}
