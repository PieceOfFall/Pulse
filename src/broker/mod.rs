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

use rs_netty::{
    Channel,
    codec::{DisconnectPacket, MqttPacket, Will},
};

use self::runtime::auth::{Authentication, Authenticator, ConfiguredAuthenticator};
use self::runtime::config::BrokerConfig;
use self::runtime::reason::reason_properties;
use self::runtime::session_registry::BrokerState;
use self::runtime::time::now_ms;
use self::runtime::write::BrokerWrite;
use self::storage::{BinaryStorage, BrokerStorage, InMemoryStorage, MysqlStorage, SqliteStorage};
use self::vnext::CommitPolicy;
use crate::{observability::metrics, protocol};

const KEEP_ALIVE_MONITOR_INTERVAL: Duration = Duration::from_millis(100);
const METRICS_RECORD_INTERVAL_MS: u64 = 1_000;

#[derive(Clone)]
pub struct Broker {
    inner: Arc<BrokerInner>,
}

struct BrokerInner {
    next_generated_client_id: AtomicU64,
    last_metrics_record_ms: AtomicU64,
    shutting_down: AtomicBool,
    keep_alive_monitor_started: AtomicBool,
    storage: Arc<dyn BrokerStorage>,
    authenticator: Arc<dyn Authenticator>,
    config: BrokerConfig,
}

struct ExpiredKeepAliveClient {
    connection_id: u64,
    channel: Channel<BrokerWrite>,
    will: Option<Will>,
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

    pub(crate) fn with_binary_auth_and_config(
        dir: impl AsRef<Path>,
        commit_policy: CommitPolicy,
        config: BrokerConfig,
        authenticator: Arc<dyn Authenticator>,
    ) -> std::io::Result<Self> {
        Ok(Self::with_storage(
            Arc::new(BinaryStorage::open(dir, commit_policy)?),
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
                last_metrics_record_ms: AtomicU64::new(0),
                shutting_down: AtomicBool::new(false),
                keep_alive_monitor_started: AtomicBool::new(false),
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

    pub(in crate::broker) fn ensure_keep_alive_monitor(&self) {
        if self
            .inner
            .keep_alive_monitor_started
            .swap(true, Ordering::SeqCst)
        {
            return;
        }

        let broker = self.clone();
        tokio::spawn(async move {
            run_keep_alive_monitor(broker).await;
        });
    }

    fn expire_keep_alive_clients(&self, now_ms: u64) -> Vec<ExpiredKeepAliveClient> {
        self.with_state(|state| {
            let expired_connections = state
                .clients_by_connection
                .iter()
                .filter_map(|(connection_id, client)| {
                    let deadline_ms = client.keep_alive_deadline_ms.load(Ordering::Relaxed);
                    (deadline_ms != 0 && deadline_ms <= now_ms).then_some(*connection_id)
                })
                .collect::<Vec<_>>();

            let mut expired = Vec::new();
            for connection_id in expired_connections {
                let Some(client) = state.remove_connection_state(connection_id, false) else {
                    continue;
                };
                state.connection_by_client_id.remove(&client.client_id);
                expired.push(ExpiredKeepAliveClient {
                    connection_id,
                    channel: client.channel,
                    will: client.will,
                });
            }
            expired
        })
    }

    fn has_keep_alive_deadlines(&self) -> bool {
        self.read_state(|state| {
            state
                .clients_by_connection
                .values()
                .any(|client| client.keep_alive_deadline_ms.load(Ordering::Relaxed) != 0)
        })
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
                let _ = channel.write_and_flush(packet.into()).await;
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
            self.record_metrics_if_due(state);
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
            self.record_metrics_if_due(state);
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

    fn record_metrics_if_due(&self, state: &BrokerState) {
        let now = now_ms();
        let last = self.inner.last_metrics_record_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < METRICS_RECORD_INTERVAL_MS {
            return;
        }

        if self
            .inner
            .last_metrics_record_ms
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            state.record_metrics();
        }
    }
}

async fn run_keep_alive_monitor(broker: Broker) {
    loop {
        tokio::time::sleep(KEEP_ALIVE_MONITOR_INTERVAL).await;

        let expired = broker.expire_keep_alive_clients(now_ms());
        for client in expired {
            metrics::connection_closed("keep_alive_timeout");
            if let Some(will) = client.will {
                broker.publish_will(client.connection_id, will).await;
            }
            let _ = client.channel.close().await;
        }

        if !broker.has_keep_alive_deadlines() {
            broker
                .inner
                .keep_alive_monitor_started
                .store(false, Ordering::SeqCst);
            if !broker.has_keep_alive_deadlines() {
                return;
            }
            if broker
                .inner
                .keep_alive_monitor_started
                .swap(true, Ordering::SeqCst)
            {
                return;
            }
        }
    }
}
