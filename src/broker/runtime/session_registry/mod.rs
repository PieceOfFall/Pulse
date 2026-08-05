use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    sync::{Arc, atomic::AtomicU64},
};

use rs_netty::{Channel, codec::Will};

use super::{
    message::PendingPublish, retained_store::RetainedStore, subscription_tree::SubscriptionEntry,
    time::now_ms, write::BrokerWrite,
};
use crate::observability::metrics;

#[derive(Default)]
pub(in crate::broker) struct BrokerState {
    pub(in crate::broker) clients_by_connection: HashMap<u64, ClientEntry>,
    pub(in crate::broker) connection_by_client_id: HashMap<String, u64>,
    pub(in crate::broker) sessions_by_client_id: HashMap<String, SessionEntry>,
    pub(in crate::broker) subscriptions: Vec<SubscriptionEntry>,
    pub(in crate::broker) retained: RetainedStore,
    pub(in crate::broker) qos2_inflight: HashMap<(u64, u16), PendingPublish>,
    pub(in crate::broker) shared_subscription_cursors: HashMap<String, usize>,
    client_persistence_changes: BTreeMap<String, ClientPersistenceChange>,
    retained_persistence_changes: BTreeSet<String>,
}

impl BrokerState {
    pub(in crate::broker) fn is_client_durable(&self, client_id: &str) -> bool {
        self.connection_by_client_id
            .get(client_id)
            .and_then(|connection_id| self.clients_by_connection.get(connection_id))
            .map_or_else(
                || {
                    self.sessions_by_client_id
                        .get(client_id)
                        .is_some_and(|session| session.session_expiry_interval != 0)
                },
                |client| client.persistent_session,
            )
    }

    pub(in crate::broker) fn mark_client_changed(&mut self, client_id: impl Into<String>) {
        self.client_persistence_changes
            .entry(client_id.into())
            .or_insert(ClientPersistenceChange::Changed);
    }

    pub(in crate::broker) fn mark_client_changed_if_durable(
        &mut self,
        client_id: impl Into<String>,
    ) {
        let client_id = client_id.into();
        if self.is_client_durable(&client_id) {
            self.mark_client_changed(client_id);
        }
    }

    pub(in crate::broker) fn mark_client_reset(&mut self, client_id: impl Into<String>) {
        self.client_persistence_changes
            .insert(client_id.into(), ClientPersistenceChange::Reset);
    }

    pub(in crate::broker) fn mark_retained_changed(&mut self, topic_name: impl Into<String>) {
        self.retained_persistence_changes.insert(topic_name.into());
    }

    pub(in crate::broker) fn persistence_changes(&self) -> Vec<PersistenceChange> {
        let client_changes = self
            .client_persistence_changes
            .iter()
            .map(|(client_id, change)| match change {
                ClientPersistenceChange::Changed => {
                    PersistenceChange::ClientChanged(client_id.clone())
                }
                ClientPersistenceChange::Reset => PersistenceChange::ClientReset(client_id.clone()),
            });
        let retained_changes = self
            .retained_persistence_changes
            .iter()
            .cloned()
            .map(PersistenceChange::RetainedTopic);
        client_changes.chain(retained_changes).collect()
    }

    pub(in crate::broker) fn take_persistence_changes(&mut self) -> Vec<PersistenceChange> {
        let client_changes = std::mem::take(&mut self.client_persistence_changes)
            .into_iter()
            .map(|(client_id, change)| match change {
                ClientPersistenceChange::Changed => PersistenceChange::ClientChanged(client_id),
                ClientPersistenceChange::Reset => PersistenceChange::ClientReset(client_id),
            });
        let retained_changes = std::mem::take(&mut self.retained_persistence_changes)
            .into_iter()
            .map(PersistenceChange::RetainedTopic);
        client_changes.chain(retained_changes).collect()
    }

    pub(in crate::broker) fn record_metrics(&self) {
        let mut queue_size = 0;
        let mut qos1_inflight = 0;
        let mut qos2_inflight = self.qos2_inflight.len();

        for session in self.sessions_by_client_id.values() {
            queue_size += session.offline_queue.len();
            qos1_inflight += session.outbound_qos1.len();
            qos2_inflight +=
                session.outbound_qos2_publish.len() + session.outbound_qos2_pubrel.len();
        }

        metrics::set_subscriptions_current(self.subscriptions.len());
        metrics::set_session_queue_size(queue_size);
        metrics::set_retained_messages_current(self.retained.len());
        metrics::set_qos1_inflight_current(qos1_inflight);
        metrics::set_qos2_inflight_current(qos2_inflight);
    }

    pub(in crate::broker) fn expire_sessions(&mut self, now_ms: u64) {
        let expired: Vec<String> = self
            .sessions_by_client_id
            .iter()
            .filter_map(|(client_id, session)| {
                if session
                    .expires_at_ms
                    .is_some_and(|expires_at| expires_at <= now_ms)
                    && !self.connection_by_client_id.contains_key(client_id)
                {
                    Some(client_id.clone())
                } else {
                    None
                }
            })
            .collect();

        for client_id in expired {
            self.sessions_by_client_id.remove(&client_id);
            self.subscriptions
                .retain(|subscription| subscription.client_id != client_id);
            self.mark_client_reset(client_id);
        }
    }

    pub(in crate::broker) fn remove_connection_state(
        &mut self,
        connection_id: u64,
        preserve_session: bool,
    ) -> Option<ClientEntry> {
        let client = self.clients_by_connection.remove(&connection_id)?;
        self.qos2_inflight
            .retain(|(conn_id, _), _| *conn_id != connection_id);
        if !preserve_session && client.session_expiry_interval == 0 {
            self.sessions_by_client_id.remove(&client.client_id);
            self.subscriptions
                .retain(|sub| sub.client_id != client.client_id);
            if client.persistent_session {
                self.mark_client_reset(client.client_id.clone());
            }
        } else if !preserve_session {
            let expires_at_ms = session_expires_at_ms(client.session_expiry_interval);
            self.sessions_by_client_id
                .entry(client.client_id.clone())
                .and_modify(|session| {
                    session.expires_at_ms = expires_at_ms;
                    session.session_expiry_interval = client.session_expiry_interval;
                })
                .or_insert_with(|| {
                    SessionEntry::disconnected(client.session_expiry_interval, expires_at_ms)
                });
            self.mark_client_changed_if_durable(client.client_id.clone());
        }
        Some(client)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::broker) enum PersistenceChange {
    ClientChanged(String),
    ClientReset(String),
    RetainedTopic(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ClientPersistenceChange {
    Changed,
    Reset,
}

#[derive(Clone)]
pub(in crate::broker) struct QueuedPublish {
    pub(in crate::broker) sequence: u64,
    pub(in crate::broker) pending: PendingPublish,
}

pub(in crate::broker) struct SessionEntry {
    pub(in crate::broker) expires_at_ms: Option<u64>,
    pub(in crate::broker) session_expiry_interval: u32,
    pub(in crate::broker) next_packet_id: u16,
    pub(in crate::broker) outbound_qos1: HashMap<u16, PendingPublish>,
    pub(in crate::broker) outbound_qos2_publish: HashMap<u16, PendingPublish>,
    pub(in crate::broker) outbound_qos2_pubrel: HashSet<u16>,
    pub(in crate::broker) offline_queue: VecDeque<QueuedPublish>,
    pub(in crate::broker) next_offline_sequence: u64,
}

impl SessionEntry {
    pub(in crate::broker) fn connected(session_expiry_interval: u32) -> Self {
        Self {
            expires_at_ms: None,
            session_expiry_interval,
            next_packet_id: 1,
            outbound_qos1: HashMap::new(),
            outbound_qos2_publish: HashMap::new(),
            outbound_qos2_pubrel: HashSet::new(),
            offline_queue: VecDeque::new(),
            next_offline_sequence: 0,
        }
    }

    pub(in crate::broker) fn disconnected(
        session_expiry_interval: u32,
        expires_at_ms: Option<u64>,
    ) -> Self {
        Self {
            expires_at_ms,
            session_expiry_interval,
            next_packet_id: 1,
            outbound_qos1: HashMap::new(),
            outbound_qos2_publish: HashMap::new(),
            outbound_qos2_pubrel: HashSet::new(),
            offline_queue: VecDeque::new(),
            next_offline_sequence: 0,
        }
    }
}

pub(in crate::broker) struct ClientEntry {
    pub(in crate::broker) client_id: String,
    pub(in crate::broker) channel: Channel<BrokerWrite>,
    pub(in crate::broker) will: Option<Will>,
    pub(in crate::broker) principal: Option<String>,
    pub(in crate::broker) session_expiry_interval: u32,
    pub(in crate::broker) receive_maximum: u16,
    pub(in crate::broker) maximum_packet_size: u32,
    pub(in crate::broker) keep_alive_deadline_ms: Arc<AtomicU64>,
    pub(in crate::broker) persistent_session: bool,
    pub(in crate::broker) subscription_count: usize,
}

impl ClientEntry {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::broker) fn new(
        client_id: String,
        channel: Channel<BrokerWrite>,
        will: Option<Will>,
        principal: Option<String>,
        session_expiry_interval: u32,
        receive_maximum: u16,
        maximum_packet_size: u32,
        persistent_session: bool,
        subscription_count: usize,
    ) -> Self {
        Self {
            client_id,
            channel,
            will,
            principal,
            session_expiry_interval,
            receive_maximum,
            maximum_packet_size,
            keep_alive_deadline_ms: Arc::new(AtomicU64::new(0)),
            persistent_session,
            subscription_count,
        }
    }
}

fn session_expires_at_ms(session_expiry_interval: u32) -> Option<u64> {
    if session_expiry_interval == u32::MAX {
        None
    } else {
        Some(now_ms().saturating_add(u64::from(session_expiry_interval) * 1_000))
    }
}

#[cfg(test)]
mod tests {
    use super::{BrokerState, PersistenceChange, SessionEntry};

    #[test]
    fn persistence_changes_are_deduplicated_and_reset_wins() {
        let mut state = BrokerState::default();
        state.mark_client_changed("client-b");
        state.mark_client_reset("client-b");
        state.mark_client_changed("client-b");
        state.mark_client_changed("client-a");
        state.mark_retained_changed("topic/b");
        state.mark_retained_changed("topic/a");
        state.mark_retained_changed("topic/a");

        let expected = vec![
            PersistenceChange::ClientChanged("client-a".to_string()),
            PersistenceChange::ClientReset("client-b".to_string()),
            PersistenceChange::RetainedTopic("topic/a".to_string()),
            PersistenceChange::RetainedTopic("topic/b".to_string()),
        ];
        assert_eq!(state.persistence_changes(), expected);
        assert_eq!(state.persistence_changes(), expected);
        assert_eq!(state.take_persistence_changes(), expected);
        assert!(state.take_persistence_changes().is_empty());
    }

    #[test]
    fn expiring_session_marks_client_for_projection_reset() {
        let mut state = BrokerState::default();
        state.sessions_by_client_id.insert(
            "expired".to_string(),
            SessionEntry::disconnected(60, Some(10)),
        );

        state.expire_sessions(10);

        assert!(!state.sessions_by_client_id.contains_key("expired"));
        assert_eq!(
            state.take_persistence_changes(),
            vec![PersistenceChange::ClientReset("expired".to_string())]
        );
    }

    #[test]
    fn transient_session_is_not_marked_for_persistence() {
        let mut state = BrokerState::default();
        state
            .sessions_by_client_id
            .insert("transient".to_string(), SessionEntry::connected(0));

        state.mark_client_changed_if_durable("transient");

        assert!(state.take_persistence_changes().is_empty());
    }
}
