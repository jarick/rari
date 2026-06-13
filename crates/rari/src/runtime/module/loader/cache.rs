use super::config::CacheStats;
use crate::error::RariError;
use crate::server::cache::handler::CacheHandler;
use crate::server::cache::{CacheHandlerRegistry, MemoryCacheHandler, MemoryConfig};
use crate::server::config::CacheLayerConfig;
use dashmap::DashMap;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::warn;

pub struct ModuleCaching {
    handler: Arc<dyn CacheHandler>,
    max_age_secs: u64,
    hit_count: AtomicUsize,
    miss_count: AtomicUsize,
    eviction_count: AtomicUsize,
    size: AtomicUsize,
    component_source_paths: DashMap<String, String>,
}

impl std::fmt::Debug for ModuleCaching {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModuleCaching")
            .field("max_age_secs", &self.max_age_secs)
            .field("hit_count", &self.hit_count)
            .field("miss_count", &self.miss_count)
            .field("eviction_count", &self.eviction_count)
            .field("size", &self.size)
            .field("component_source_paths", &self.component_source_paths)
            .finish_non_exhaustive()
    }
}

impl ModuleCaching {
    pub fn new(cache_size: usize) -> Self {
        Self::with_handler(
            cache_size,
            3600,
            Arc::new(MemoryCacheHandler::with_config(MemoryConfig {
                max_entries: cache_size.max(1),
                default_ttl: 3600,
            })),
        )
    }

    pub fn with_handler(
        cache_size: usize,
        max_age_secs: u64,
        handler: Arc<dyn CacheHandler>,
    ) -> Self {
        let _ = cache_size;
        Self {
            handler,
            max_age_secs,
            hit_count: AtomicUsize::new(0),
            miss_count: AtomicUsize::new(0),
            eviction_count: AtomicUsize::new(0),
            size: AtomicUsize::new(0),
            component_source_paths: DashMap::new(),
        }
    }

    #[allow(dead_code)]
    pub fn from_config(layer: &CacheLayerConfig, registry: &CacheHandlerRegistry) -> Self {
        let handler = registry.get(&layer.handler).unwrap_or_else(|| {
            warn!(handler = %layer.handler, "unknown cache handler, falling back to memory");
            registry
                .get("memory")
                .expect("memory handler is pre-registered by CacheHandlerRegistry::new()")
        });
        Self::with_handler(layer.max_entries, layer.default_ttl_secs, handler)
    }

    pub fn get_cache_stats(&self) -> CacheStats {
        let size = self.size.load(Ordering::Relaxed);
        CacheStats {
            hits: self.hit_count.load(Ordering::Relaxed),
            misses: self.miss_count.load(Ordering::Relaxed),
            evictions: self.eviction_count.load(Ordering::Relaxed),
            size,
            memory_bytes: size * 64,
        }
    }

    pub async fn get(&self, key: &str) -> Option<JsonValue> {
        let bytes = match self.handler.get(key).await {
            Ok(Some(b)) => b,
            Ok(None) => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            Err(_) => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);
                return None;
            }
        };
        match serde_json::from_slice(&bytes) {
            Ok(v) => {
                self.hit_count.fetch_add(1, Ordering::Relaxed);
                Some(v)
            }
            Err(_) => {
                self.miss_count.fetch_add(1, Ordering::Relaxed);
                let _ = self.handler.invalidate(key).await;
                None
            }
        }
    }

    pub async fn insert(&self, key: String, value: JsonValue) -> Result<(), RariError> {
        let bytes = serde_json::to_vec(&value)
            .map_err(|e| RariError::cache(format!("json serialize: {e}")))?;
        self.handler
            .set(&key, bytes, self.max_age_secs)
            .await
            .map_err(|e| RariError::cache(format!("cache set: {e}")))?;
        self.size.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub async fn invalidate(&self, key: &str) {
        match self.handler.invalidate(key).await {
            Ok(()) => {
                self.size.fetch_sub(1, Ordering::Relaxed);
            }
            Err(e) => {
                tracing::debug!("module_caching.invalidate({}) failed: {}", key, e);
            }
        }
    }

    #[cfg(test)]
    pub fn set_component_source_path(&self, component_id: String, path: String) {
        self.component_source_paths.insert(component_id, path);
    }

    pub fn get_component_source_path(&self, component_id: &str) -> Option<String> {
        self.component_source_paths.get(component_id).map(|entry| entry.value().clone())
    }

    pub fn remove_component_source_path(&self, component_id: &str) {
        self.component_source_paths.remove(component_id);
    }

    pub async fn clear(&self) {
        if let Err(e) = self.handler.clear().await {
            tracing::debug!("module_caching.clear failed: {}", e);
        }
        self.size.store(0, Ordering::Relaxed);
        self.component_source_paths.clear();
    }

    pub fn clear_component(&self, component_id: &str) {
        self.component_source_paths.remove(component_id);
    }
}

impl Default for ModuleCaching {
    fn default() -> Self {
        Self::new(5000)
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cache_basic_operations() {
        let cache = ModuleCaching::new(10);

        cache.insert("key1".to_string(), serde_json::json!({"value": 1})).await.unwrap();
        assert_eq!(cache.get("key1").await, Some(serde_json::json!({"value": 1})));
        assert!(cache.get("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn test_cache_lru_eviction() {
        let cache = ModuleCaching::new(2);

        cache.insert("key1".to_string(), serde_json::json!(1)).await.unwrap();
        cache.insert("key2".to_string(), serde_json::json!(2)).await.unwrap();
        cache.insert("key3".to_string(), serde_json::json!(3)).await.unwrap();

        assert!(cache.get("key1").await.is_none());
        assert!(cache.get("key2").await.is_some());
        assert!(cache.get("key3").await.is_some());
    }

    #[tokio::test]
    async fn test_cache_stats() {
        let cache = ModuleCaching::new(10);

        cache.insert("key1".to_string(), serde_json::json!(1)).await.unwrap();
        cache.get("key1").await;
        cache.get("key2").await;

        let stats = cache.get_cache_stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[tokio::test]
    async fn test_module_caching() {
        let caching = ModuleCaching::new(10);

        caching.set_component_source_path("comp1".to_string(), "/path/to/comp1".to_string());
        assert_eq!(caching.get_component_source_path("comp1"), Some("/path/to/comp1".to_string()));

        caching.insert("module1".to_string(), serde_json::json!({"data": "test"})).await.unwrap();
        assert!(caching.get("module1").await.is_some());
    }

    #[tokio::test]
    async fn test_handler_round_trip() {
        let cache = ModuleCaching::new(10);
        let value = serde_json::json!({"nested": {"a": [1, 2, 3], "b": "x"}});

        cache.insert("k".to_string(), value.clone()).await.unwrap();
        assert_eq!(cache.get("k").await, Some(value));

        let stats = cache.get_cache_stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 0);
    }

    #[tokio::test]
    async fn test_invalidate_via_custom_handler() {
        let handler = Arc::new(MemoryCacheHandler::default());
        let cache = ModuleCaching::with_handler(4, 60, handler.clone());

        cache.insert("k".to_string(), serde_json::json!({"v": 1})).await.unwrap();
        assert!(cache.get("k").await.is_some());

        cache.invalidate("k").await;

        assert!(cache.get("k").await.is_none());
        assert_eq!(cache.get_cache_stats().size, 0);
    }
}
