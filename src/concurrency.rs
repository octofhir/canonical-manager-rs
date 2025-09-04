//! Concurrency abstraction for concurrent maps
//!
//! A thin wrapper to allow swapping backends without touching call sites.

use std::hash::Hash;

pub struct ConcurrentMap<K, V> {
    inner: papaya::HashMap<K, V>,
}

impl<K, V> ConcurrentMap<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        Self {
            inner: papaya::HashMap::new(),
        }
    }

    pub fn with_capacity(_capacity: usize) -> Self {
        Self {
            inner: papaya::HashMap::new(),
        }
    }

    pub fn get_cloned(&self, key: &K) -> Option<V> {
        let guard = self.inner.guard();
        self.inner.get(key, &guard).cloned()
    }

    pub fn insert(&self, key: K, value: V) {
        let guard = self.inner.guard();
        let _ = self.inner.insert(key, value, &guard);
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        let guard = self.inner.guard();
        self.inner.remove(key, &guard).cloned()
    }

    pub fn len(&self) -> usize {
        let guard = self.inner.guard();
        self.inner.iter(&guard).count()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        let guard = self.inner.guard();
        let keys: Vec<K> = self.inner.iter(&guard).map(|(k, _)| k.clone()).collect();
        drop(guard);
        for k in keys {
            let guard2 = self.inner.guard();
            let _ = self.inner.remove(&k, &guard2);
        }
    }

    pub fn keys(&self) -> Vec<K> {
        let guard = self.inner.guard();
        self.inner.iter(&guard).map(|(k, _)| k.clone()).collect()
    }

    pub fn iter_cloned(&self) -> Vec<(K, V)> {
        let guard = self.inner.guard();
        self.inner
            .iter(&guard)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    pub fn modify<F: FnOnce(V) -> V>(&self, key: &K, f: F) {
        if let Some(current) = self.get_cloned(key) {
            self.insert(key.clone(), f(current));
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
        let guard = self.inner.guard();
        let len = self.inner.iter(&guard).count();
        f.debug_struct("ConcurrentMap").field("len", &len).finish()
    }
}
