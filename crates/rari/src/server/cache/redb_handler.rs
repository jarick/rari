#![cfg(feature = "redb")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use parking_lot::Mutex;
use redb::{Database, ReadableDatabase, ReadableTable};
use tokio::task;

use super::handler::{CacheError, CacheHandler, SetOutcome};
use crate::server::config::CacheLayerConfig;

const TABLE_NAME: &str = "cache_entries";
const EXPIRY_NEVER: u64 = 0;

const TABLE_DEFINITION: redb::TableDefinition<&str, &[u8]> = redb::TableDefinition::new(TABLE_NAME);

#[derive(Debug)]
pub struct RedbCacheHandler {
    path: PathBuf,
    database: Mutex<Option<Arc<Database>>>,
    default_ttl_secs: u64,
}

impl RedbCacheHandler {
    pub fn from_config(layer: &CacheLayerConfig) -> Self {
        let path = layer
            .url
            .clone()
            .unwrap_or_else(|| "./.rari/cache.redb".to_string());
        Self {
            path: PathBuf::from(path),
            database: Mutex::new(None),
            default_ttl_secs: layer.default_ttl_secs,
        }
    }

    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            database: Mutex::new(None),
            default_ttl_secs: 3600,
        }
    }

    fn database(&self) -> Result<Arc<Database>, CacheError> {
        let mut guard = self.database.lock();
        if let Some(db) = guard.as_ref() {
            return Ok(Arc::clone(db));
        }

        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }

        let db = Database::create(&self.path)
            .map_err(|e| CacheError::Backend(format!("redb create: {e}")))?;
        let arc = Arc::new(db);
        *guard = Some(Arc::clone(&arc));
        Ok(arc)
    }
}

fn now_ms() -> Result<u64, CacheError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .map_err(|e| CacheError::Backend(format!("system time: {e}")))
}

fn encode_entry(expires_at_ms: u64, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + payload.len());
    buf.extend_from_slice(&expires_at_ms.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

fn decode_entry(bytes: &[u8]) -> Option<(u64, &[u8])> {
    if bytes.len() < 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[..8]);
    Some((u64::from_be_bytes(arr), &bytes[8..]))
}

fn redb_error(action: &str, error: impl std::fmt::Display) -> CacheError {
    CacheError::Backend(format!("redb {action}: {error}"))
}

async fn blocking<F, T>(f: F) -> Result<T, CacheError>
where
    F: FnOnce() -> Result<T, CacheError> + Send + 'static,
    T: Send + 'static,
{
    task::spawn_blocking(f)
        .await
        .map_err(|e| CacheError::Backend(format!("redb join: {e}")))?
}

fn begin_read(db: &Database) -> Result<redb::ReadTransaction, CacheError> {
    db.begin_read().map_err(|e| redb_error("read", e))
}

fn begin_write(db: &Database) -> Result<redb::WriteTransaction, CacheError> {
    db.begin_write().map_err(|e| redb_error("write", e))
}

fn commit(tx: redb::WriteTransaction) -> Result<(), CacheError> {
    tx.commit().map_err(|e| redb_error("commit", e))
}

fn table_keys_matching(
    table: &redb::Table<'_, &str, &[u8]>,
    predicate: impl Fn(&str) -> bool,
) -> Result<Vec<String>, CacheError> {
    table
        .iter()
        .map_err(|e| redb_error("iter", e))?
        .filter_map(|entry| match entry {
            Ok((key, _)) => {
                let key = key.value();
                predicate(key).then(|| Ok(key.to_string()))
            }
            Err(e) => Some(Err(redb_error("entry", e))),
        })
        .collect()
}

fn remove_keys(
    table: &mut redb::Table<'_, &str, &[u8]>,
    keys: &[String],
) -> Result<(), CacheError> {
    for key in keys {
        let _ = table
            .remove(key.as_str())
            .map_err(|e| redb_error("remove", e))?;
    }
    Ok(())
}

#[async_trait]
impl CacheHandler for RedbCacheHandler {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, CacheError> {
        let db = self.database()?;
        let key = key.to_string();
        blocking(move || {
            let tx = begin_read(&db)?;
            let table = match tx.open_table(TABLE_DEFINITION) {
                Ok(table) => table,
                Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
                Err(e) => return Err(redb_error("table", e)),
            };
            let Some(raw) = table.get(key.as_str()).map_err(|e| redb_error("get", e))? else {
                return Ok(None);
            };
            let bytes = raw.value();
            let Some((expires_at_ms, payload)) = decode_entry(bytes) else {
                return Ok(None);
            };
            if expires_at_ms != EXPIRY_NEVER && now_ms()? >= expires_at_ms {
                Ok(None)
            } else {
                Ok(Some(payload.to_vec()))
            }
        })
        .await
    }

    async fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: u64,
    ) -> Result<SetOutcome, CacheError> {
        self.set_with_tags(key, value, ttl_secs, &[]).await
    }

    async fn set_with_tags(
        &self,
        key: &str,
        value: Vec<u8>,
        ttl_secs: u64,
        _tags: &[String],
    ) -> Result<SetOutcome, CacheError> {
        let db = self.database()?;
        let effective_ttl = if ttl_secs == 0 {
            self.default_ttl_secs
        } else {
            ttl_secs
        };
        let expires_at_ms = if effective_ttl == 0 {
            EXPIRY_NEVER
        } else {
            now_ms()?.saturating_add(effective_ttl.saturating_mul(1000))
        };
        let key = key.to_string();
        let replaced = blocking(move || {
            let encoded = encode_entry(expires_at_ms, &value);
            let tx = begin_write(&db)?;
            let existed = {
                let mut table = tx
                    .open_table(TABLE_DEFINITION)
                    .map_err(|e| redb_error("table", e))?;
                let existed = table
                    .get(key.as_str())
                    .map_err(|e| redb_error("get", e))?
                    .is_some();
                table
                    .insert(key.as_str(), encoded.as_slice())
                    .map_err(|e| redb_error("insert", e))?;
                existed
            };
            commit(tx)?;
            Ok(existed)
        })
        .await?;
        Ok(SetOutcome {
            replaced,
            evicted: 0,
            evicted_bytes: 0,
        })
    }

    async fn invalidate(&self, key: &str) -> Result<bool, CacheError> {
        let db = self.database()?;
        let key = key.to_string();
        let existed = blocking(move || {
            let tx = begin_write(&db)?;
            let existed = {
                let mut table = tx
                    .open_table(TABLE_DEFINITION)
                    .map_err(|e| redb_error("table", e))?;
                let prev = table
                    .remove(key.as_str())
                    .map_err(|e| redb_error("remove", e))?;
                prev.is_some()
            };
            commit(tx)?;
            Ok(existed)
        })
        .await?;
        Ok(existed)
    }

    async fn clear(&self) -> Result<(), CacheError> {
        {
            let mut guard = self.database.lock();
            *guard = None;
        }
        let db = self.database()?;
        blocking(move || {
            let tx = begin_write(&db)?;
            let mut table = tx
                .open_table(TABLE_DEFINITION)
                .map_err(|e| redb_error("table", e))?;
            let keys = table_keys_matching(&table, |_| true)?;
            remove_keys(&mut table, &keys)?;
            drop(table);
            commit(tx)?;
            Ok(())
        })
        .await
    }

    async fn clear_prefix(&self, prefix: &str) -> Result<usize, CacheError> {
        let db = self.database()?;
        let prefix = prefix.to_string();
        let removed = blocking(move || {
            let tx = begin_write(&db)?;
            let removed = {
                let mut table = tx
                    .open_table(TABLE_DEFINITION)
                    .map_err(|e| redb_error("table", e))?;
                let keys = table_keys_matching(&table, |key| key.starts_with(&prefix))?;
                let count = keys.len();
                remove_keys(&mut table, &keys)?;
                count
            };
            commit(tx)?;
            Ok(removed)
        })
        .await?;
        Ok(removed)
    }

    fn get_all_keys(&self) -> Vec<String> {
        Vec::new()
    }
}
