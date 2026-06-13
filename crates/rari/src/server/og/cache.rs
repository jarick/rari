use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::server::cache::handler::{CacheHandler, MemoryCacheHandler};

const OG_TTL_SECS: u64 = 60 * 60 * 24 * 365 * 10;

pub struct OgImageCache {
    handler: Arc<dyn CacheHandler>,
    cache_dir: PathBuf,
}

impl OgImageCache {
    pub fn new(memory_capacity: usize, project_path: &Path) -> Self {
        let handler =
            MemoryCacheHandler::with_config(crate::server::cache::handler::MemoryConfig {
                max_entries: memory_capacity.max(1),
                default_ttl: 0,
            });
        Self::with_handler(Arc::new(handler), project_path)
    }

    pub fn with_handler(handler: Arc<dyn CacheHandler>, project_path: &Path) -> Self {
        let cache_dir = Self::resolve_cache_dir(project_path);
        Self { handler, cache_dir }
    }

    fn resolve_cache_dir(project_path: &Path) -> PathBuf {
        let is_production = std::env::var("NODE_ENV").map(|v| v == "production").unwrap_or(false);

        if is_production {
            PathBuf::from("/tmp/rari-og-cache")
        } else {
            project_path.join(".cache").join("og")
        }
    }

    fn ensure_cache_dir(&self) {
        if let Err(e) = std::fs::create_dir_all(&self.cache_dir) {
            tracing::error!("Failed to create OG cache directory: {}", e);
        }
    }

    fn cache_filename(&self, key: &str) -> PathBuf {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();

        self.cache_dir.join(format!("{:x}.webp", hash))
    }

    pub async fn get(&self, key: &str) -> Option<Vec<u8>> {
        if let Ok(Some(bytes)) = self.handler.get(key).await {
            return Some(bytes);
        }

        let path = self.cache_filename(key);
        if let Ok(data) = tokio::fs::read(&path).await {
            if let Err(e) = self.handler.set(key, data.clone(), OG_TTL_SECS).await {
                tracing::debug!("OG image cache write-through to handler failed: {}", e);
            }
            return Some(data);
        }

        None
    }

    pub async fn insert(&self, key: String, value: Vec<u8>) {
        self.ensure_cache_dir();

        let path = self.cache_filename(&key);
        if let Err(e) = tokio::fs::write(&path, &value).await {
            tracing::error!("Failed to write OG image to disk cache: {}", e);
        }

        if let Err(e) = self.handler.set(&key, value, OG_TTL_SECS).await {
            tracing::error!("Failed to write OG image to handler cache: {}", e);
        }
    }

    pub async fn remove(&self, key: &str) -> Option<Vec<u8>> {
        let prev = match self.handler.get(key).await {
            Ok(Some(bytes)) => Some(bytes),
            _ => None,
        };

        if let Err(e) = self.handler.invalidate(key).await {
            tracing::error!("Failed to invalidate OG image in handler: {}", e);
        }

        let path = self.cache_filename(key);
        if let Err(e) = tokio::fs::remove_file(&path).await
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::debug!("OG cache remove_file: {}", e);
        }

        prev
    }

    pub async fn clear(&self) {
        if let Err(e) = self.handler.clear().await {
            tracing::error!("Failed to clear OG image handler cache: {}", e);
        }

        let mut entries = match tokio::fs::read_dir(&self.cache_dir).await {
            Ok(e) => e,
            Err(e) => {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::error!("Failed to read OG cache dir for clear: {}", e);
                }
                return;
            }
        };

        loop {
            let entry = match entries.next_entry().await {
                Ok(Some(e)) => e,
                Ok(None) => break,
                Err(e) => {
                    tracing::error!("Failed to iterate OG cache dir: {}", e);
                    break;
                }
            };

            if entry.path().extension().map(|e| e == "webp").unwrap_or(false) {
                let path = entry.path();
                if let Err(e) = tokio::fs::remove_file(&path).await {
                    tracing::debug!("OG cache remove_file (clear): {}", e);
                }
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::disallowed_methods)]
mod tests {
    use super::*;
    use std::env::temp_dir;

    use crate::server::cache::handler::MemoryCacheHandler;

    fn test_project_path(test_name: &str) -> PathBuf {
        temp_dir().join(format!("rari-test-og-cache-{}", test_name))
    }

    fn fresh_cache(test_name: &str, memory_capacity: usize) -> OgImageCache {
        let handler = Arc::new(MemoryCacheHandler::with_config(
            crate::server::cache::handler::MemoryConfig {
                max_entries: memory_capacity.max(1),
                default_ttl: 0,
            },
        ));
        OgImageCache::with_handler(handler, &test_project_path(test_name))
    }

    #[tokio::test]
    async fn test_cache_insert_and_get() {
        let cache = fresh_cache("basic", 5);
        let data = vec![1, 2, 3, 4, 5];

        cache.insert("/test/route".to_string(), data.clone()).await;
        assert_eq!(cache.get("/test/route").await, Some(data));
        cache.clear().await;
    }

    #[tokio::test]
    async fn test_cache_remove() {
        let cache = fresh_cache("remove", 5);
        let data = vec![1, 2, 3, 4, 5];

        cache.insert("/test/route".to_string(), data.clone()).await;
        assert_eq!(cache.remove("/test/route").await, Some(data));
        assert!(cache.get("/test/route").await.is_none());
    }

    #[tokio::test]
    async fn test_disk_persistence() {
        // Force a memory-tier eviction (capacity=1, two inserts). The
        // second insert's value is still recoverable from disk after we
        // clear the in-memory tier.
        let cache = fresh_cache("persistence", 1);
        let data = vec![10, 20, 30, 40, 50];

        cache.insert("/route1".to_string(), data.clone()).await;
        cache.insert("/route2".to_string(), vec![1, 2, 3]).await;

        assert_eq!(cache.get("/route1").await, Some(data));
        cache.clear().await;
    }

    #[tokio::test]
    async fn test_handler_round_trip() {
        let cache = fresh_cache("handler-round-trip", 8);
        let payload = b"webp-bytes".to_vec();

        cache.insert("k1".to_string(), payload.clone()).await;
        assert_eq!(cache.get("k1").await, Some(payload));
        cache.clear().await;
    }

    #[tokio::test]
    async fn test_handler_fallback_to_disk() {
        let project_path = test_project_path("fallback-to-disk");
        let _ = std::fs::remove_dir_all(&project_path);

        let handler_a = Arc::new(MemoryCacheHandler::with_config(
            crate::server::cache::handler::MemoryConfig { max_entries: 8, default_ttl: 0 },
        ));
        let cache_a = OgImageCache::with_handler(handler_a, &project_path);

        let payload = vec![0xDE, 0xAD, 0xBE, 0xEF];
        cache_a.insert("/persistent".to_string(), payload.clone()).await;
        let _ = cache_a.get("/persistent").await.expect("cache_a in-memory hit");
        drop(cache_a);

        let handler_b = Arc::new(MemoryCacheHandler::with_config(
            crate::server::cache::handler::MemoryConfig { max_entries: 8, default_ttl: 0 },
        ));
        let cache_b = OgImageCache::with_handler(
            Arc::clone(&handler_b) as Arc<dyn CacheHandler>,
            &project_path,
        );

        let from_disk = cache_b.get("/persistent").await;
        assert_eq!(from_disk, Some(payload.clone()), "expected disk-fallback hit");

        let in_new_handler = handler_b.get("/persistent").await.unwrap();
        assert_eq!(in_new_handler, Some(payload.clone()), "write-through to new handler missing");

        cache_b.clear().await;
        let _ = std::fs::remove_dir_all(&project_path);
    }
}
