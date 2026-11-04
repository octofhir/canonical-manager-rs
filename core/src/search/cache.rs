//! Bounded oldest-write-first cache. Reads do not refresh order or TTL.

use super::SearchResult;
use lru::LruCache;
use parking_lot::RwLock;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CAPACITY: NonZeroUsize = match NonZeroUsize::new(1000) {
    Some(capacity) => capacity,
    None => unreachable!(),
};

type Entries = LruCache<String, (Arc<SearchResult>, Instant)>;

pub(super) struct SearchCache {
    entries: RwLock<Entries>,
    ttl: Duration,
}

impl SearchCache {
    pub(super) fn new(ttl: Duration) -> Self {
        Self {
            entries: RwLock::new(LruCache::new(CAPACITY)),
            ttl,
        }
    }

    pub(super) fn get(&self, key: &str) -> Option<SearchResult> {
        let result = {
            let entries = self.entries.read();
            // peek preserves the previous cache's oldest-write eviction policy
            // and allows concurrent readers without an exclusive LRU touch.
            let (result, timestamp) = entries.peek(key)?;
            if timestamp.elapsed() >= self.ttl {
                return None;
            }
            Arc::clone(result)
        };
        // Preserve the owned public API, but clone JSON outside the lock.
        Some(result.as_ref().clone())
    }

    pub(super) fn insert(&self, key: &str, result: &SearchResult) {
        let key = key.to_owned();
        let result = Arc::new(result.clone());
        // push atomically enforces the capacity and returns the replaced or
        // evicted entry. Never clone or scan other cached search results.
        let evicted = self.entries.write().push(key, (result, Instant::now()));
        // Potentially large JSON trees are also destroyed outside the lock.
        drop(evicted);
    }

    pub(super) fn clear(&self) {
        let empty = LruCache::new(CAPACITY);
        let old = std::mem::replace(&mut *self.entries.write(), empty);
        drop(old);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(total_count: usize) -> SearchResult {
        SearchResult {
            resources: Vec::new(),
            total_count,
            query_time: Duration::from_micros(42),
        }
    }

    fn small_cache() -> SearchCache {
        SearchCache {
            entries: RwLock::new(LruCache::new(NonZeroUsize::new(2).unwrap())),
            ttl: Duration::from_secs(300),
        }
    }

    #[test]
    fn cache_hit_returns_an_independent_result() {
        let cache = small_cache();
        let mut original = result(7);
        cache.insert("first", &original);
        original.total_count = 99;
        let mut hit = cache.get("first").unwrap();
        assert_eq!(hit.total_count, 7);
        assert_eq!(hit.query_time, Duration::from_micros(42));
        hit.total_count = 12;
        assert_eq!(cache.get("first").unwrap().total_count, 7);
        assert!(cache.get("missing").is_none());
    }

    #[test]
    fn cache_evicts_oldest_write_even_if_recently_read() {
        let cache = small_cache();
        cache.insert("first", &result(1));
        cache.insert("second", &result(2));
        assert!(cache.get("first").is_some());
        let survivor = Arc::clone(&cache.entries.read().peek("second").unwrap().0);
        cache.insert("third", &result(3));
        assert_eq!(cache.entries.read().len(), 2);
        assert!(cache.get("first").is_none());
        assert!(cache.get("third").is_some());
        assert!(Arc::ptr_eq(
            &survivor,
            &cache.entries.read().peek("second").unwrap().0
        ));
    }

    #[test]
    fn cache_replacement_refreshes_order_without_evicting_other_keys() {
        let cache = small_cache();
        cache.insert("first", &result(1));
        cache.insert("second", &result(2));
        cache.insert("first", &result(11));
        assert_eq!(cache.entries.read().len(), 2);
        assert_eq!(cache.get("second").unwrap().total_count, 2);
        cache.insert("third", &result(3));
        assert!(cache.get("second").is_none());
        assert_eq!(cache.get("first").unwrap().total_count, 11);
    }

    #[test]
    fn cache_expiry_is_based_on_write_time() {
        let cache = small_cache();
        cache.insert("first", &result(1));
        let timestamp = cache.entries.read().peek("first").unwrap().1;
        assert!(cache.get("first").is_some());
        assert_eq!(cache.entries.read().peek("first").unwrap().1, timestamp);
        cache.entries.write().peek_mut("first").unwrap().1 = Instant::now() - cache.ttl;
        assert!(cache.get("first").is_none());
        cache.insert("first", &result(2));
        assert_eq!(cache.get("first").unwrap().total_count, 2);
    }

    #[test]
    fn cache_zero_ttl_never_returns_a_hit() {
        let cache = SearchCache::new(Duration::ZERO);
        cache.insert("first", &result(1));
        assert!(cache.get("first").is_none());
    }

    #[test]
    fn cache_clear_releases_payloads_and_accepts_new_entries() {
        let cache = SearchCache::new(Duration::from_secs(300));
        cache.insert("first", &result(1));
        let payload = Arc::downgrade(&cache.entries.read().peek("first").unwrap().0);
        cache.clear();
        assert!(payload.upgrade().is_none());
        assert!(cache.entries.read().is_empty());
        assert!(cache.get("first").is_none());
        cache.insert("second", &result(2));
        assert_eq!(cache.get("second").unwrap().total_count, 2);
    }

    #[test]
    fn cache_capacity_is_strict_under_concurrent_writes() {
        let cache = SearchCache::new(Duration::from_secs(300));
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            for worker in 0..8 {
                let cache = &cache;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    for entry in 0..512 {
                        let key = format!("{worker}/{entry}");
                        cache.insert(&key, &result(entry));
                        assert!(cache.entries.read().len() <= CAPACITY.get());
                        // Another writer may evict the entry before this read.
                        if let Some(hit) = cache.get(&key) {
                            assert_eq!(hit.total_count, entry);
                        }
                    }
                });
            }
        });
        assert_eq!(cache.entries.read().len(), CAPACITY.get());
    }
}
