pub(crate) mod runtime;
mod storage;
pub(crate) mod vnext;

#[cfg(test)]
mod tests;

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::{path::Path, time::Duration};

pub use runtime::connection::{BrokerLife, MqttHandler};

use rs_netty::codec::{DisconnectPacket, MqttPacket};

use self::runtime::auth::{Authentication, Authenticator, ConfiguredAuthenticator};
use self::runtime::config::BrokerConfig;
use self::runtime::reason::reason_properties;
use self::runtime::session_registry::BrokerState;
use self::storage::{BrokerStorage, InMemoryStorage, MysqlStorage, SqliteStorage};
use crate::protocol;

#[derive(Clone)]
pub struct Broker {
    inner: Arc<BrokerInner>,
}

struct BrokerInner {
    next_generated_client_id: AtomicU64,
    shutting_down: AtomicBool,
    storage: Arc<dyn BrokerStorage>,
    authenticator: Arc<dyn Authenticator>,
    config: BrokerConfig,
}

impl Broker {
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn new() -> Self {
        Self::with_config(BrokerConfig::default())
    }

    pub(crate) fn with_config(config: BrokerConfig) -> Self {
        Self::with_config_and_auth(config, Arc::new(ConfiguredAuthenticator::default()))
    }

    pub(crate) fn with_config_and_auth(
        config: BrokerConfig,
        authenticator: Arc<dyn Authenticator>,
    ) -> Self {
        Self::with_storage(Arc::new(InMemoryStorage::default()), config, authenticator)
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
            Arc::new(ConfiguredAuthenticator::default()),
        ))
    }

    #[allow(dead_code)]
    pub(crate) fn with_mysql_and_config(url: &str, config: BrokerConfig) -> mysql::Result<Self> {
        Ok(Self::with_storage(
            Arc::new(MysqlStorage::open(url)?),
            config,
            Arc::new(ConfiguredAuthenticator::default()),
        ))
    }

    pub(crate) fn with_sqlite_auth_and_config(
        path: impl AsRef<Path>,
        config: BrokerConfig,
        authenticator: Arc<dyn Authenticator>,
    ) -> rusqlite::Result<Self> {
        Ok(Self::with_storage(
            Arc::new(SqliteStorage::open(path)?),
            config,
            authenticator,
        ))
    }

    pub(crate) fn with_mysql_auth_and_config(
        url: &str,
        config: BrokerConfig,
        authenticator: Arc<dyn Authenticator>,
    ) -> mysql::Result<Self> {
        Ok(Self::with_storage(
            Arc::new(MysqlStorage::open(url)?),
            config,
            authenticator,
        ))
    }

    pub(in crate::broker) fn with_storage(
        storage: Arc<dyn BrokerStorage>,
        config: BrokerConfig,
        authenticator: Arc<dyn Authenticator>,
    ) -> Self {
        Self {
            inner: Arc::new(BrokerInner {
                next_generated_client_id: AtomicU64::new(1),
                shutting_down: AtomicBool::new(false),
                storage,
                authenticator,
                config,
            }),
        }
    }

    pub(crate) fn config(&self) -> &BrokerConfig {
        &self.inner.config
    }

    pub(in crate::broker) fn authenticate(
        &self,
        username: Option<&str>,
        password: Option<&[u8]>,
    ) -> Result<Authentication, u8> {
        self.inner.authenticator.authenticate(username, password)
    }

    pub(in crate::broker) fn authorize_publish(
        &self,
        connection_id: u64,
        topic_name: &str,
    ) -> bool {
        self.read_state(|state| {
            let principal = state
                .clients_by_connection
                .get(&connection_id)
                .and_then(|client| client.principal.as_deref());
            self.inner
                .authenticator
                .authorize_publish(principal, topic_name)
        })
    }

    pub(in crate::broker) fn begin_shutdown(&self) {
        self.inner.shutting_down.store(true, Ordering::SeqCst);
    }

    pub(in crate::broker) fn is_shutting_down(&self) -> bool {
        self.inner.shutting_down.load(Ordering::SeqCst)
    }

    pub(crate) async fn shutdown_active_sessions(&self, drain_timeout: Duration) {
        self.begin_shutdown();
        let channels = self.with_state(|state| {
            state
                .clients_by_connection
                .values()
                .map(|client| client.channel.clone())
                .collect::<Vec<_>>()
        });
        if channels.is_empty() {
            return;
        }

        let packet = MqttPacket::Disconnect(DisconnectPacket {
            reason_code: protocol::SERVER_SHUTTING_DOWN,
            properties: reason_properties(protocol::SERVER_SHUTTING_DOWN),
        });
        let mut tasks = tokio::task::JoinSet::new();
        for channel in channels {
            let packet = packet.clone();
            tasks.spawn(async move {
                let _ = channel.write_and_flush(packet).await;
                let _ = channel.close().await;
            });
        }

        let drain = async { while tasks.join_next().await.is_some() {} };
        let _ = tokio::time::timeout(drain_timeout, drain).await;
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

    pub(in crate::broker) fn with_transient_state<R>(
        &self,
        operation: impl FnOnce(&mut BrokerState) -> R,
    ) -> R {
        let mut operation = Some(operation);
        let mut result = None;
        self.inner.storage.with_transient_state(&mut |state| {
            let operation = operation.take().expect("storage operation called once");
            result = Some(operation(state));
            state.record_metrics();
        });
        result.expect("storage operation completed")
    }

    pub(in crate::broker) fn read_state<R>(&self, operation: impl FnOnce(&BrokerState) -> R) -> R {
        let mut operation = Some(operation);
        let mut result = None;
        self.inner.storage.read_state(&mut |state| {
            let operation = operation.take().expect("storage operation called once");
            result = Some(operation(state));
        });
        result.expect("storage operation completed")
    }
}
