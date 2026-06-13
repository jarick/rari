//! Pluggable cache handler abstraction.
//!
//! Handlers are byte-agnostic: the trait operates on `Vec<u8>`. Typed
//! wrappers (e.g. `ResponseCache`) serialize their domain value to
//! bytes before calling the handler. Tags are first-class — handlers
//! that don't support tags can no-op `set_with_tags` /
//! `invalidate_by_tag`.
//!
//! Trait is `async_trait`-based (so `Arc<dyn CacheHandler>` works).
//! Native `async fn in dyn trait` is not yet stable; when it lands
//! the macro can be dropped.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use lru::LruCache;
use parking_lot::Mutex;

#[allow(unused_imports)]
pub use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialize(String),

    #[error("deserialization error: {0}")]
    Deserialize(String),

    #[error("backend error: {0}")]
    Backend(String),
}

#[async_trait::async_trait]
pub trait CacheHandler: Send + Sync + fmt::Debug {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError>;

    async fn set(&self, key: &str, value: Vec<u8>, ttl_secs: u64) -> Result<(), CacheError>;

    async fn set_with_tags(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: u64,
        tags: &[String],
    ) -> Result<(), CacheError>;

    async fn invalidate(&self, key: &str) -> Result<(), CacheError>;

    async fn invalidate_by_tag(&self, tag: &str) -> Result<(), CacheError>;

    async fn clear(&self) -> Result<(), CacheError>;

    fn get_all_keys(&self) -> Vec<String>;
}

#[derive(Debug)]
struct MemEntry {
    bytes: Vec<u8>,
    expires_at: Option<Instant>,
    tags: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct MemoryConfig {
    pub max_entries: usize,
    pub default_ttl: u64,
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self { max_entries: 1000, default_ttl: 31536000 }
    }
}

#[derive(Debug)]
pub struct MemoryCacheHandler {
    cache: DashMap<String, MemEntry>,
    lru: Mutex<LruCache<String, ()>>,
    tag_index: DashMap<String, Vec<String>>,
    max_entries: usize,
}

impl MemoryCacheHandler {
    pub fn with_config(config: MemoryConfig) -> Self {
        let max_entries =
            std::num::NonZeroUsize::new(config.max_entries.max(1)).expect("clamped to >= 1");
        tracing::debug!(
            max_entries = max_entries.get(),
            default_ttl_secs = config.default_ttl,
            "memory cache handler initialized"
        );
        Self {
            cache: DashMap::new(),
            lru: Mutex::new(LruCache::new(max_entries)),
            tag_index: DashMap::new(),
            max_entries: max_entries.get(),
        }
    }

    pub fn len(&self) -> usize {
        self.cache.len()
    }

    pub fn is_empty(&self) -> bool {
        self.cache.is_empty()
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    fn evict_lru(&self) -> Option<String> {
        let key = {
            let mut lru = self.lru.lock();
            lru.pop_lru().map(|(k, ())| k)
        }?;
        if let Some((_k, entry)) = self.cache.remove(&key) {
            self.remove_from_tag_index(&key, &entry.tags);
        }
        tracing::debug!(key = %key, "memory cache LRU eviction");
        Some(key)
    }

    fn remove_from_tag_index(&self, key: &str, tags: &[String]) {
        for tag in tags {
            if let Some(mut keys) = self.tag_index.get_mut(tag) {
                keys.retain(|k| k != key);
                if keys.is_empty() {
                    drop(keys);
                    self.tag_index.remove(tag);
                }
            }
        }
    }
}

impl Default for MemoryCacheHandler {
    fn default() -> Self {
        Self::with_config(MemoryConfig::default())
    }
}

#[async_trait::async_trait]
impl CacheHandler for MemoryCacheHandler {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let snapshot = match self.cache.get(key) {
            Some(entry) => {
                let expired = match entry.expires_at {
                    Some(t) => Instant::now() >= t,
                    None => false,
                };
                if expired { None } else { Some(entry.bytes.clone()) }
            }
            None => None,
        };

        match snapshot {
            Some(bytes) => {
                let mut lru = self.lru.lock();
                lru.promote(key);
                tracing::debug!(key = %key, size_bytes = bytes.len(), "memory cache hit");
                Ok(Some(bytes))
            }
            None => {
                let existed = self.cache.contains_key(key);
                if existed {
                    tracing::debug!(key = %key, "memory cache miss (expired); evicting");
                } else {
                    tracing::debug!(key = %key, "memory cache miss (not present)");
                }
                self.invalidate(key).await?;
                Ok(None)
            }
        }
    }

    async fn set(&self, key: &str, value: Vec<u8>, ttl_secs: u64) -> Result<(), CacheError> {
        self.set_with_tags(key, value, ttl_secs, &[]).await
    }

    async fn set_with_tags(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: u64,
        tags: &[String],
    ) -> Result<(), CacheError> {
        tracing::debug!(
            key = %key,
            size_bytes = value.len(),
            ttl_secs,
            tag_count = tags.len(),
            "memory cache set_with_tags"
        );
        // Drop old tag-index entries before overwriting, so a smaller
        // tag set on update doesn't leave dangling references.
        if let Some(old) = self.cache.remove(key) {
            self.remove_from_tag_index(key, &old.1.tags);
        }

        let needs_evict = {
            let lru = self.lru.lock();
            lru.len() >= self.max_entries
        };
        if needs_evict {
            self.evict_lru();
        }

        let expires_at = if ttl_secs == 0 {
            Some(Instant::now())
        } else {
            Instant::now().checked_add(Duration::from_secs(ttl_secs))
        };

        let entry = MemEntry { bytes: value, expires_at, tags: tags.to_vec() };
        self.cache.insert(key.to_string(), entry);

        for tag in tags {
            self.tag_index.entry(tag.clone()).or_insert_with(Vec::new).push(key.to_string());
        }

        let mut lru = self.lru.lock();
        lru.put(key.to_string(), ());

        Ok(())
    }

    async fn invalidate(&self, key: &str) -> Result<(), CacheError> {
        if let Some((_k, entry)) = self.cache.remove(key) {
            self.remove_from_tag_index(key, &entry.tags);
            let mut lru = self.lru.lock();
            lru.pop(key);
            tracing::debug!(key = %key, "memory cache invalidate");
        } else {
            tracing::debug!(key = %key, "memory cache invalidate (no-op, not present)");
        }
        Ok(())
    }

    async fn invalidate_by_tag(&self, tag: &str) -> Result<(), CacheError> {
        let keys: Vec<String> =
            self.tag_index.get(tag).map(|e| e.value().clone()).unwrap_or_default();
        tracing::debug!(tag = %tag, key_count = keys.len(), "memory cache invalidate_by_tag");
        for key in keys {
            self.invalidate(&key).await?;
        }
        self.tag_index.remove(tag);
        Ok(())
    }

    async fn clear(&self) -> Result<(), CacheError> {
        let n = self.cache.len();
        self.cache.clear();
        self.tag_index.clear();
        let mut lru = self.lru.lock();
        lru.clear();
        tracing::debug!(cleared_entries = n, "memory cache clear");
        Ok(())
    }

    fn get_all_keys(&self) -> Vec<String> {
        self.cache.iter().map(|e| e.key().clone()).collect()
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpCacheHandler;

#[async_trait::async_trait]
impl CacheHandler for NoOpCacheHandler {
    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        Ok(None)
    }

    async fn set(&self, _key: &str, _value: Vec<u8>, _ttl_secs: u64) -> Result<(), CacheError> {
        Ok(())
    }

    async fn set_with_tags(
        &self,
        _key: &str,
        _value: Vec<u8>,
        _ttl_secs: u64,
        _tags: &[String],
    ) -> Result<(), CacheError> {
        Ok(())
    }

    async fn invalidate(&self, _key: &str) -> Result<(), CacheError> {
        Ok(())
    }

    async fn invalidate_by_tag(&self, _tag: &str) -> Result<(), CacheError> {
        Ok(())
    }

    async fn clear(&self) -> Result<(), CacheError> {
        Ok(())
    }

    fn get_all_keys(&self) -> Vec<String> {
        Vec::new()
    }
}

#[derive(Debug, Default)]
pub struct CacheHandlerRegistry {
    handlers: DashMap<String, Arc<dyn CacheHandler>>,
}

impl CacheHandlerRegistry {
    pub fn new() -> Self {
        Self { handlers: DashMap::new() }
    }

    pub fn register(&self, name: &str, h: Arc<dyn CacheHandler>) {
        self.handlers.insert(name.to_string(), h);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn CacheHandler>> {
        self.handlers.get(name).map(|e| Arc::clone(e.value()))
    }

    /// Pre-registers `memory` and `noop` with their default
    /// configurations.
    pub fn default_with_memory() -> Self {
        let r = Self::new();
        r.register("memory", Arc::new(MemoryCacheHandler::default()));
        r.register("noop", Arc::new(NoOpCacheHandler));
        r
    }

    pub fn len(&self) -> usize {
        self.handlers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    pub fn names(&self) -> Vec<String> {
        self.handlers.iter().map(|e| e.key().clone()).collect()
    }

    /// Build a registry from the process environment. Always
    /// pre-registers `memory` and `noop`. Reads
    /// `RARI_CACHE_HANDLER_{RESPONSE,IMAGE,OG,LAYOUT,MODULE,FETCH}`;
    /// unknown names produce a warning and fall back to `memory`.
    /// Users plug their own (Redis, SQLite, S3, …) via
    /// `register()` after this call.
    pub fn from_env() -> Self {
        let reg = Self::default_with_memory();
        for var in [
            "RARI_CACHE_HANDLER_RESPONSE",
            "RARI_CACHE_HANDLER_IMAGE",
            "RARI_CACHE_HANDLER_OG",
            "RARI_CACHE_HANDLER_LAYOUT",
            "RARI_CACHE_HANDLER_MODULE",
            "RARI_CACHE_HANDLER_FETCH",
        ] {
            if let Ok(name) = std::env::var(var)
                && reg.get(&name).is_none()
            {
                tracing::warn!(
                    "{var}={name} is set but no handler named \"{name}\" is registered; ignoring (available: {:?})",
                    reg.names()
                );
            }
        }
        reg
    }

    /// Resolve a handler by env var name, falling back to
    /// `default_name` if the env-supplied name is unset or
    /// unregistered.
    pub fn resolve(&self, env_var: &str, default_name: &str) -> Arc<dyn CacheHandler> {
        std::env::var(env_var)
            .ok()
            .and_then(|name| self.get(&name))
            .unwrap_or_else(|| self.get(default_name).expect("default handler must be registered"))
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_memory_get_set() {
        let h = MemoryCacheHandler::default();
        h.set("k", b"hello".to_vec(), 60).await.unwrap();
        let got = h.get("k").await.unwrap();
        assert_eq!(got, Some(b"hello".to_vec()));
    }

    #[tokio::test]
    async fn test_memory_ttl_expiry() {
        let h = MemoryCacheHandler::default();
        h.set("k", b"hello".to_vec(), 0).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(h.get("k").await.unwrap(), None);
        assert_eq!(h.get("k").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_memory_lru_eviction() {
        let h = MemoryCacheHandler::with_config(MemoryConfig { max_entries: 2, default_ttl: 60 });
        h.set("a", b"1".to_vec(), 60).await.unwrap();
        h.set("b", b"2".to_vec(), 60).await.unwrap();
        let _ = h.get("a").await.unwrap();
        h.set("c", b"3".to_vec(), 60).await.unwrap();
        assert_eq!(h.get("a").await.unwrap(), Some(b"1".to_vec()));
        assert_eq!(h.get("b").await.unwrap(), None);
        assert_eq!(h.get("c").await.unwrap(), Some(b"3".to_vec()));
    }

    #[tokio::test]
    async fn test_memory_invalidate() {
        let h = MemoryCacheHandler::default();
        h.set("k", b"v".to_vec(), 60).await.unwrap();
        assert!(h.get("k").await.unwrap().is_some());
        h.invalidate("k").await.unwrap();
        assert_eq!(h.get("k").await.unwrap(), None);
        h.invalidate("k").await.unwrap();
    }

    #[tokio::test]
    async fn test_memory_invalidate_by_tag() {
        let h = MemoryCacheHandler::default();
        h.set_with_tags("k1", b"a".to_vec(), 60, &["t".to_string()]).await.unwrap();
        h.set_with_tags("k2", b"b".to_vec(), 60, &["t".to_string()]).await.unwrap();
        h.set("k3", b"c".to_vec(), 60).await.unwrap();

        h.invalidate_by_tag("t").await.unwrap();

        assert_eq!(h.get("k1").await.unwrap(), None);
        assert_eq!(h.get("k2").await.unwrap(), None);
        assert_eq!(h.get("k3").await.unwrap(), Some(b"c".to_vec()));
    }

    #[tokio::test]
    async fn test_memory_get_all_keys() {
        let h = MemoryCacheHandler::default();
        h.set("a", b"1".to_vec(), 60).await.unwrap();
        h.set("b", b"2".to_vec(), 60).await.unwrap();

        let mut keys = h.get_all_keys();
        keys.sort();
        assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);

        h.invalidate("a").await.unwrap();
        let mut keys = h.get_all_keys();
        keys.sort();
        assert_eq!(keys, vec!["b".to_string()]);
    }

    #[tokio::test]
    async fn test_noop_get_all_keys() {
        let h = NoOpCacheHandler;
        h.set("k", b"v".to_vec(), 60).await.unwrap();
        assert!(h.get_all_keys().is_empty());
    }

    #[tokio::test]
    async fn test_memory_tag_index_cleanup() {
        let h = MemoryCacheHandler::default();
        h.set_with_tags("k1", b"a".to_vec(), 60, &["alpha".to_string(), "shared".to_string()])
            .await
            .unwrap();

        assert!(h.tag_index.contains_key("alpha"));
        assert!(h.tag_index.contains_key("shared"));

        h.invalidate("k1").await.unwrap();

        assert!(!h.tag_index.contains_key("alpha"));
        assert!(!h.tag_index.contains_key("shared"));
    }

    #[tokio::test]
    async fn test_noop_returns_none() {
        let h = NoOpCacheHandler;
        assert_eq!(h.get("anything").await.unwrap(), None);
        h.set("k", b"v".to_vec(), 60).await.unwrap();
        assert_eq!(h.get("k").await.unwrap(), None);
        h.set_with_tags("k", b"v".to_vec(), 60, &["t".to_string()]).await.unwrap();
        h.invalidate("k").await.unwrap();
        h.invalidate_by_tag("t").await.unwrap();
        h.clear().await.unwrap();
    }

    #[tokio::test]
    async fn test_registry_get_and_register() {
        let r = CacheHandlerRegistry::default_with_memory();
        let mem = r.get("memory").expect("memory must be registered");
        let noop = r.get("noop").expect("noop must be registered");
        mem.set("k", b"v".to_vec(), 60).await.unwrap();
        assert_eq!(mem.get("k").await.unwrap(), Some(b"v".to_vec()));
        assert_eq!(noop.get("k").await.unwrap(), None);

        let custom: Arc<dyn CacheHandler> = Arc::new(NoOpCacheHandler);
        r.register("custom", Arc::clone(&custom));
        assert!(r.get("custom").is_some());
    }

    #[tokio::test]
    async fn test_registry_unknown_returns_none() {
        let r = CacheHandlerRegistry::new();
        assert!(r.get("nope").is_none());
    }
}
