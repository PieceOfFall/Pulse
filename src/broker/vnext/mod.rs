#![allow(dead_code)]

pub(crate) mod durable_log;
pub(crate) mod routing;
pub(crate) mod session;
pub(crate) mod shard;

use std::{io, path::Path};

pub(crate) use durable_log::{CommitPolicy, DurableLog, DurableLogEvent};
pub(crate) use routing::{RouteSubscription, RouterIndex};
pub(crate) use session::SessionTable;
pub(crate) use shard::ShardRuntime;

#[derive(Debug)]
pub(crate) struct BrokerCore {
    shards: Vec<ShardRuntime>,
    router: RouterIndex,
    sessions: SessionTable,
    durable_log: Option<DurableLog>,
}

impl BrokerCore {
    pub(crate) fn in_memory(worker_threads: usize) -> Self {
        Self {
            shards: build_shards(worker_threads),
            router: RouterIndex::default(),
            sessions: SessionTable::default(),
            durable_log: None,
        }
    }

    pub(crate) fn with_wal(
        worker_threads: usize,
        path: impl AsRef<Path>,
        commit_policy: CommitPolicy,
    ) -> io::Result<Self> {
        let path = path.as_ref();
        let recovered = DurableLog::replay(path)?;
        let durable_log = DurableLog::open(path, commit_policy)?;
        let mut core = Self {
            shards: build_shards(worker_threads),
            router: RouterIndex::default(),
            sessions: SessionTable::from_recovered(&recovered),
            durable_log: Some(durable_log),
        };
        for subscription in recovered.subscriptions.values() {
            core.router.insert(subscription.clone());
        }
        Ok(core)
    }

    pub(crate) fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub(crate) fn shard_for_client(&self, client_id: &str) -> usize {
        stable_hash(client_id) % self.shards.len()
    }

    pub(crate) fn upsert_subscription(
        &mut self,
        subscription: RouteSubscription,
    ) -> io::Result<()> {
        if let Some(durable_log) = &mut self.durable_log {
            durable_log.append(&DurableLogEvent::SubscriptionUpsert {
                client_id: subscription.client_id.clone(),
                filter: subscription.filter.clone(),
                match_filter: subscription.match_filter.clone(),
                shared_group: subscription.shared_group.clone(),
                subscription_identifier: subscription.subscription_identifier,
            })?;
        }
        self.router.insert(subscription);
        Ok(())
    }

    pub(crate) fn delete_subscription(&mut self, client_id: &str, filter: &str) -> io::Result<()> {
        if let Some(durable_log) = &mut self.durable_log {
            durable_log.append(&DurableLogEvent::SubscriptionDelete {
                client_id: client_id.to_string(),
                filter: filter.to_string(),
            })?;
        }
        self.router.remove(client_id, filter);
        Ok(())
    }

    pub(crate) fn match_publish(&self, topic_name: &str) -> Vec<RouteSubscription> {
        self.router.matches(topic_name)
    }

    pub(crate) fn flush_storage(&mut self) -> io::Result<()> {
        if let Some(durable_log) = &mut self.durable_log {
            durable_log.flush()?;
        }
        Ok(())
    }

    pub(crate) fn sessions(&self) -> &SessionTable {
        &self.sessions
    }
}

fn build_shards(worker_threads: usize) -> Vec<ShardRuntime> {
    let worker_threads = worker_threads.max(1);
    (0..worker_threads).map(ShardRuntime::new).collect()
}

fn stable_hash(value: &str) -> usize {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in value.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash as usize
}

#[cfg(test)]
mod tests {
    use super::{BrokerCore, CommitPolicy, RouteSubscription};

    #[test]
    fn core_routes_recovered_wal_subscriptions() {
        let path =
            std::env::temp_dir().join(format!("pulse-vnext-core-{}-{}.wal", std::process::id(), 1));
        let _ = std::fs::remove_file(&path);
        {
            let mut core =
                BrokerCore::with_wal(2, &path, CommitPolicy::Balanced).expect("open broker core");
            core.upsert_subscription(RouteSubscription::new("client-a", "devices/+/temp", None))
                .expect("upsert subscription");
            core.flush_storage().expect("flush wal");
        }

        let core =
            BrokerCore::with_wal(2, &path, CommitPolicy::Balanced).expect("reopen broker core");
        let matches = core.match_publish("devices/alpha/temp");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].client_id, "client-a");
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn shard_selection_is_stable() {
        let core = BrokerCore::in_memory(4);
        assert_eq!(
            core.shard_for_client("client-a"),
            core.shard_for_client("client-a")
        );
        assert!(core.shard_for_client("client-a") < core.shard_count());
    }
}
