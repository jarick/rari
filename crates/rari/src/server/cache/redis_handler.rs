#![cfg(feature = "redis")]

use std::time::Duration;

use async_trait::async_trait;
use redis::AsyncCommands;
use tokio::sync::Mutex;

use super::handler::{CacheError, CacheHandler, SetOutcome};
use crate::server::config::CacheLayerConfig;

const REDIS_TIMEOUT: Duration = Duration::from_secs(2);

fn redis_error(action: &str, error: impl std::fmt::Display) -> CacheError {
    CacheError::Backend(format!("redis {action}: {error}"))
}

async fn redis_timeout<T>(
    action: &'static str,
    fut: impl Future<Output = redis::RedisResult<T>>,
) -> Result<T, CacheError> {
    tokio::time::timeout(REDIS_TIMEOUT, fut)
        .await
        .map_err(|_| CacheError::Backend(format!("redis {action} timeout")))?
        .map_err(|e| redis_error(action, e))
}

async fn delete_keys(
    conn: &mut redis::aio::MultiplexedConnection,
    keys: Vec<String>,
) -> Result<usize, CacheError> {
    let count = keys.len();
    if count > 0 {
        redis_timeout("del", conn.del::<_, ()>(keys)).await?;
    }
    Ok(count)
}

#[derive(Debug)]
pub struct RedisCacheHandler {
    url: Option<String>,
    connection: Mutex<Option<redis::aio::MultiplexedConnection>>,
}

impl RedisCacheHandler {
    pub fn from_config(layer: &CacheLayerConfig) -> Self {
        Self {
            url: layer.url.clone(),
            connection: Mutex::new(None),
        }
    }

    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: Some(url.into()),
            connection: Mutex::new(None),
        }
    }

    async fn connection(&self) -> Result<redis::aio::MultiplexedConnection, CacheError> {
        let Some(url) = self.url.as_deref() else {
            return Err(CacheError::Backend("redis url is not configured".into()));
        };

        let mut guard = self.connection.lock().await;
        if let Some(conn) = guard.as_ref() {
            return Ok(conn.clone());
        }

        let client = redis::Client::open(url).map_err(|e| redis_error("client open", e))?;
        let new_connection =
            redis_timeout("connect", client.get_multiplexed_async_connection()).await?;
        *guard = Some(new_connection.clone());
        Ok(new_connection)
    }
}

#[async_trait]
impl CacheHandler for RedisCacheHandler {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let mut conn = self.connection().await?;
        redis_timeout("get", conn.get(key)).await
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: u64,
    ) -> Result<SetOutcome, CacheError> {
        let mut conn = self.connection().await?;
        let fut: redis::RedisFuture<'_, ()> = if ttl_secs == 0 {
            conn.set::<_, _, ()>(key, value)
        } else {
            conn.set_ex::<_, _, ()>(key, value, ttl_secs)
        };
        redis_timeout("set", fut).await?;
        Ok(SetOutcome::default())
    }

    async fn set_with_tags(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: u64,
        _tags: &[String],
    ) -> Result<SetOutcome, CacheError> {
        self.set(key, value, ttl_secs).await
    }

    async fn invalidate(&self, key: &str) -> Result<bool, CacheError> {
        let mut conn = self.connection().await?;
        let removed: i64 = redis_timeout("del", conn.del(key)).await?;
        Ok(removed > 0)
    }

    async fn clear_prefix(&self, prefix: &str) -> Result<usize, CacheError> {
        let mut conn = self.connection().await?;
        let pattern = format!("{prefix}*");
        let keys: Vec<String> = redis_timeout("keys", conn.keys(&pattern)).await?;
        delete_keys(&mut conn, keys).await
    }

    fn get_all_keys(&self) -> Vec<String> {
        Vec::new()
    }
}
