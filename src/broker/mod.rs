pub(crate) mod runtime;
mod storage;

#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

pub use runtime::connection::{BrokerLife, MqttHandler};

use self::runtime::config::BrokerConfig;
use self::runtime::session_registry::BrokerState;
use self::storage::{BrokerStorage, InMemoryStorage, MysqlStorage, SqliteStorage};

#[derive(Clone)]
pub struct Broker {
    inner: Arc<BrokerInner>,
}

struct BrokerInner {
    next_generated_client_id: AtomicU64,
    storage: Arc<dyn BrokerStorage>,
    config: BrokerConfig,
}

impl Broker {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new() -> Self {
        Self::with_config(BrokerConfig::default())
    }

    pub(crate) fn with_config(config: BrokerConfig) -> Self {
        Self::with_storage(Arc::new(InMemoryStorage::default()), config)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_sqlite(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        Self::with_sqlite_and_config(path, BrokerConfig::default())
    }

    pub(crate) fn with_sqlite_and_config(
        path: impl AsRef<Path>,
        config: BrokerConfig,
    ) -> rusqlite::Result<Self> {
        Ok(Self::with_storage(
            Arc::new(SqliteStorage::open(path)?),
            config,
        ))
    }

    pub(crate) fn with_mysql_and_config(url: &str, config: BrokerConfig) -> mysql::Result<Self> {
        Ok(Self::with_storage(
            Arc::new(MysqlStorage::open(url)?),
            config,
        ))
    }

    pub(in crate::broker) fn with_storage(
        storage: Arc<dyn BrokerStorage>,
        config: BrokerConfig,
    ) -> Self {
        Self {
            inner: Arc::new(BrokerInner {
                next_generated_client_id: AtomicU64::new(1),
                storage,
                config,
            }),
        }
    }

    pub(crate) fn config(&self) -> &BrokerConfig {
        &self.inner.config
    }

    pub(in crate::broker) fn generated_client_id(&self) -> String {
        let id = self
            .inner
            .next_generated_client_id
            .fetch_add(1, Ordering::Relaxed);
        format!("pulse-{id}")
    }

    pub(in crate::broker) fn with_state<R>(
        &self,
        operation: impl FnOnce(&mut BrokerState) -> R,
    ) -> R {
        let mut operation = Some(operation);
        let mut result = None;
        self.inner.storage.with_state(&mut |state| {
            let operation = operation.take().expect("storage operation called once");
            result = Some(operation(state));
            state.record_metrics();
        });
        result.expect("storage operation completed")
    }
}
