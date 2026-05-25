use std::{
    collections::{HashMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

use rs_netty::{
    Channel,
    codec::{
        MqttPacket, MqttProperty, PublishPacket, QoS, Subscription, SubscriptionOptions, Will,
    },
};

#[derive(Default)]
pub(super) struct BrokerState {
    pub(super) clients_by_connection: HashMap<u64, ClientEntry>,
    pub(super) connection_by_client_id: HashMap<String, u64>,
    pub(super) sessions_by_client_id: HashMap<String, SessionEntry>,
    pub(super) subscriptions: Vec<SubscriptionEntry>,
    pub(super) retained: HashMap<String, RetainedMessage>,
    pub(super) qos2_inflight: HashMap<(u64, u16), PublishPacket>,
}

impl BrokerState {
    pub(super) fn expire_sessions(&mut self, now_ms: u64) {
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
        }
    }

    pub(super) fn remove_connection_state(
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
        } else if !preserve_session {
            let expires_at_ms = session_expires_at_ms(client.session_expiry_interval);
            self.sessions_by_client_id.insert(
                client.client_id.clone(),
                SessionEntry {
                    expires_at_ms,
                    _expiry_interval: client.session_expiry_interval,
                },
            );
        }
        Some(client)
    }
}

pub(super) struct SessionEntry {
    pub(super) expires_at_ms: Option<u64>,
    _expiry_interval: u32,
}

impl SessionEntry {
    pub(super) fn connected(session_expiry_interval: u32) -> Self {
        Self {
            expires_at_ms: None,
            _expiry_interval: session_expiry_interval,
        }
    }
}

pub(super) struct ClientEntry {
    pub(super) client_id: String,
    pub(super) channel: Channel<MqttPacket>,
    pub(super) will: Option<Will>,
    pub(super) session_expiry_interval: u32,
    pub(super) next_packet_id: u16,
    pub(super) outbound_qos1: HashSet<u16>,
    pub(super) outbound_qos2_publish: HashSet<u16>,
    pub(super) outbound_qos2_pubrel: HashSet<u16>,
}

impl ClientEntry {
    pub(super) fn new(
        client_id: String,
        channel: Channel<MqttPacket>,
        will: Option<Will>,
        session_expiry_interval: u32,
    ) -> Self {
        Self {
            client_id,
            channel,
            will,
            session_expiry_interval,
            next_packet_id: 1,
            outbound_qos1: HashSet::new(),
            outbound_qos2_publish: HashSet::new(),
            outbound_qos2_pubrel: HashSet::new(),
        }
    }
}

#[derive(Clone)]
pub(super) struct SubscriptionEntry {
    pub(super) client_id: String,
    pub(super) filter: String,
    pub(super) options: SubscriptionOptions,
}

#[derive(Clone)]
pub(super) struct RetainedMessage {
    pub(super) qos: QoS,
    pub(super) topic_name: String,
    pub(super) properties: Vec<MqttProperty>,
    pub(super) payload: bytes::Bytes,
}

pub(super) fn retain_publish(state: &mut BrokerState, packet: &PublishPacket) {
    if !packet.retain {
        return;
    }

    if packet.payload.is_empty() {
        state.retained.remove(&packet.topic_name);
    } else {
        state.retained.insert(
            packet.topic_name.clone(),
            RetainedMessage {
                qos: packet.qos,
                topic_name: packet.topic_name.clone(),
                properties: packet.properties.clone(),
                payload: packet.payload.clone(),
            },
        );
    }
}

pub(super) fn upsert_subscription(
    subscriptions: &mut Vec<SubscriptionEntry>,
    client_id: &str,
    subscription: Subscription,
) -> usize {
    if let Some(index) = subscriptions
        .iter_mut()
        .position(|sub| sub.client_id == client_id && sub.filter == subscription.topic_filter)
    {
        subscriptions[index].options = subscription.options;
        return index;
    }

    subscriptions.push(SubscriptionEntry {
        client_id: client_id.to_string(),
        filter: subscription.topic_filter,
        options: subscription.options,
    });
    subscriptions.len() - 1
}

fn session_expires_at_ms(session_expiry_interval: u32) -> Option<u64> {
    if session_expiry_interval == u32::MAX {
        None
    } else {
        Some(now_ms().saturating_add(u64::from(session_expiry_interval) * 1_000))
    }
}

pub(super) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
