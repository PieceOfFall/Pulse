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

use self::runtime::session_registry::BrokerState;
use self::storage::{BrokerStorage, InMemoryStorage, SqliteStorage};

#[derive(Clone)]
pub struct Broker {
    inner: Arc<BrokerInner>,
}

struct BrokerInner {
    next_generated_client_id: AtomicU64,
    storage: Arc<dyn BrokerStorage>,
}

impl Broker {
    pub fn new() -> Self {
        Self::with_storage(Arc::new(InMemoryStorage::default()))
    }

    pub fn with_sqlite(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        Ok(Self::with_storage(Arc::new(SqliteStorage::open(path)?)))
    }

    pub(in crate::broker) fn with_storage(storage: Arc<dyn BrokerStorage>) -> Self {
        Self {
            inner: Arc::new(BrokerInner {
                next_generated_client_id: AtomicU64::new(1),
                storage,
            }),
        }
    }

    pub(in crate::broker) fn generated_client_id(&self) -> String {
        let id = self
            .inner
            .next_generated_client_id
            .fetch_add(1, Ordering::Relaxed);
        format!("mqtt-rs-{id}")
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
        });
        result.expect("storage operation completed")
    }
}
