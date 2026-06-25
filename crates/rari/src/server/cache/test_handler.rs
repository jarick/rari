#![cfg(all(feature = "redis", feature = "redb"))]

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use super::handler::{CacheError, CacheHandler, SetOutcome};
use super::redb_handler::RedbCacheHandler;
use super::redis_handler::RedisCacheHandler;
use crate::server::config::CacheLayerConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TestCacheBackend {
    Redis,
    Redb,
}

static BACKEND: Mutex<Option<TestCacheBackend>> = Mutex::new(None);

pub fn set_test_backend(backend: TestCacheBackend) {
    *BACKEND.lock() = Some(backend);
}

pub fn reset_test_backend() {
    *BACKEND.lock() = None;
}

pub fn current_backend() -> Option<TestCacheBackend> {
    *BACKEND.lock()
}

#[derive(Debug)]
pub struct TestCacheHandler {
    layer: CacheLayerConfig,
    inner: tokio::sync::Mutex<Option<Arc<dyn CacheHandler>>>,
}

impl TestCacheHandler {
    pub fn from_config(layer: &CacheLayerConfig) -> Self {
        Self {
            layer: layer.clone(),
            inner: tokio::sync::Mutex::new(None),
        }
    }

    async fn resolve_inner(&self) -> Result<Arc<dyn CacheHandler>, CacheError> {
        let mut guard = self.inner.lock().await;
        if let Some(handler) = guard.as_ref() {
            return Ok(Arc::clone(handler));
        }
        let backend = current_backend().ok_or_else(|| {
            CacheError::Backend(
                "test cache backend not set; call set_test_backend() before use".into(),
            )
        })?;
        let handler: Arc<dyn CacheHandler> = match backend {
            TestCacheBackend::Redis => Arc::new(RedisCacheHandler::from_config(&self.layer)),
            TestCacheBackend::Redb => Arc::new(RedbCacheHandler::from_config(&self.layer)),
        };
        *guard = Some(Arc::clone(&handler));
        Ok(handler)
    }
}

#[async_trait]
impl CacheHandler for TestCacheHandler {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        self.resolve_inner().await?.get(key).await
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: u64,
    ) -> Result<SetOutcome, CacheError> {
        self.resolve_inner().await?.set(key, value, ttl_secs).await
    }

    async fn set_with_tags(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: u64,
        tags: &[String],
    ) -> Result<SetOutcome, CacheError> {
        self.resolve_inner()
            .await?
            .set_with_tags(key, value, ttl_secs, tags)
            .await
    }

    async fn invalidate(&self, key: &str) -> Result<bool, CacheError> {
        self.resolve_inner().await?.invalidate(key).await
    }

    async fn clear_prefix(&self, prefix: &str) -> Result<usize, CacheError> {
        self.resolve_inner().await?.clear_prefix(prefix).await
    }

    fn get_all_keys(&self) -> Vec<String> {
        Vec::new()
    }
}
