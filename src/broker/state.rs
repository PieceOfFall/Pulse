use std::{
    collections::{HashMap, HashSet, VecDeque},
    time::{SystemTime, UNIX_EPOCH},
};

use rs_netty::{
    Channel,
    codec::{
        MqttPacket, MqttProperty, PublishPacket, QoS, Subscription, SubscriptionOptions, Will,
    },
};

pub(super) const MAX_OFFLINE_QUEUE_LEN: usize = 1024;
pub(super) const MAX_RETAINED_MESSAGES: usize = 1024;
pub(super) const MAX_RETAINED_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Default)]
pub(super) struct BrokerState {
    pub(super) clients_by_connection: HashMap<u64, ClientEntry>,
    pub(super) connection_by_client_id: HashMap<String, u64>,
    pub(super) sessions_by_client_id: HashMap<String, SessionEntry>,
    pub(super) subscriptions: Vec<SubscriptionEntry>,
    pub(super) retained: HashMap<String, RetainedMessage>,
    pub(super) qos2_inflight: HashMap<(u64, u16), PendingPublish>,
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
            self.sessions_by_client_id
                .entry(client.client_id.clone())
                .and_modify(|session| {
                    session.expires_at_ms = expires_at_ms;
                    session.session_expiry_interval = client.session_expiry_interval;
                })
                .or_insert_with(|| {
                    SessionEntry::disconnected(client.session_expiry_interval, expires_at_ms)
                });
        }
        Some(client)
    }
}

pub(super) struct SessionEntry {
    pub(super) expires_at_ms: Option<u64>,
    pub(super) session_expiry_interval: u32,
    pub(super) next_packet_id: u16,
    pub(super) outbound_qos1: HashMap<u16, PendingPublish>,
    pub(super) outbound_qos2_publish: HashMap<u16, PendingPublish>,
    pub(super) outbound_qos2_pubrel: HashSet<u16>,
    pub(super) offline_queue: VecDeque<PendingPublish>,
}

impl SessionEntry {
    pub(super) fn connected(session_expiry_interval: u32) -> Self {
        Self {
            expires_at_ms: None,
            session_expiry_interval,
            next_packet_id: 1,
            outbound_qos1: HashMap::new(),
            outbound_qos2_publish: HashMap::new(),
            outbound_qos2_pubrel: HashSet::new(),
            offline_queue: VecDeque::new(),
        }
    }

    pub(super) fn disconnected(session_expiry_interval: u32, expires_at_ms: Option<u64>) -> Self {
        Self {
            expires_at_ms,
            session_expiry_interval,
            next_packet_id: 1,
            outbound_qos1: HashMap::new(),
            outbound_qos2_publish: HashMap::new(),
            outbound_qos2_pubrel: HashSet::new(),
            offline_queue: VecDeque::new(),
        }
    }
}

pub(super) struct ClientEntry {
    pub(super) client_id: String,
    pub(super) channel: Channel<MqttPacket>,
    pub(super) will: Option<Will>,
    pub(super) session_expiry_interval: u32,
    pub(super) receive_maximum: u16,
    pub(super) maximum_packet_size: u32,
}

impl ClientEntry {
    pub(super) fn new(
        client_id: String,
        channel: Channel<MqttPacket>,
        will: Option<Will>,
        session_expiry_interval: u32,
        receive_maximum: u16,
        maximum_packet_size: u32,
    ) -> Self {
        Self {
            client_id,
            channel,
            will,
            session_expiry_interval,
            receive_maximum,
            maximum_packet_size,
        }
    }
}

#[derive(Clone)]
pub(super) struct SubscriptionEntry {
    pub(super) client_id: String,
    pub(super) filter: String,
    pub(super) options: SubscriptionOptions,
    pub(super) subscription_identifier: Option<u32>,
}

#[derive(Clone)]
pub(super) struct RetainedMessage {
    pub(super) qos: QoS,
    pub(super) topic_name: String,
    pub(super) properties: Vec<MqttProperty>,
    pub(super) payload: bytes::Bytes,
    pub(super) expires_at_ms: Option<u64>,
}

#[derive(Clone)]
pub(super) struct PendingPublish {
    pub(super) packet: PublishPacket,
    pub(super) expires_at_ms: Option<u64>,
}

pub(super) fn retain_publish(state: &mut BrokerState, packet: &PublishPacket) {
    if !packet.retain {
        return;
    }

    let now_ms = now_ms();
    let expires_at_ms = message_expires_at_ms(packet, now_ms);
    if packet.payload.is_empty() || is_message_expired(expires_at_ms, now_ms) {
        state.retained.remove(&packet.topic_name);
    } else if can_store_retained(state, packet) {
        state.retained.insert(
            packet.topic_name.clone(),
            RetainedMessage {
                qos: packet.qos,
                topic_name: packet.topic_name.clone(),
                properties: packet.properties.clone(),
                payload: packet.payload.clone(),
                expires_at_ms,
            },
        );
    }
}

pub(super) fn upsert_subscription(
    subscriptions: &mut Vec<SubscriptionEntry>,
    client_id: &str,
    subscription: Subscription,
    subscription_identifier: Option<u32>,
) -> UpsertSubscriptionResult {
    if let Some(index) = subscriptions
        .iter_mut()
        .position(|sub| sub.client_id == client_id && sub.filter == subscription.topic_filter)
    {
        subscriptions[index].options = subscription.options;
        subscriptions[index].subscription_identifier = subscription_identifier;
        return UpsertSubscriptionResult {
            index,
            inserted: false,
        };
    }

    subscriptions.push(SubscriptionEntry {
        client_id: client_id.to_string(),
        filter: subscription.topic_filter,
        options: subscription.options,
        subscription_identifier,
    });
    UpsertSubscriptionResult {
        index: subscriptions.len() - 1,
        inserted: true,
    }
}

pub(super) struct UpsertSubscriptionResult {
    pub(super) index: usize,
    pub(super) inserted: bool,
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

pub(super) fn message_expires_at_ms(packet: &PublishPacket, now_ms: u64) -> Option<u64> {
    packet
        .properties
        .iter()
        .find_map(|property| match property {
            MqttProperty::MessageExpiryInterval(seconds) => {
                Some(now_ms.saturating_add(u64::from(*seconds) * 1_000))
            }
            _ => None,
        })
}

pub(super) fn is_message_expired(expires_at_ms: Option<u64>, now_ms: u64) -> bool {
    expires_at_ms.is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
}

fn can_store_retained(state: &BrokerState, packet: &PublishPacket) -> bool {
    if !state.retained.contains_key(&packet.topic_name)
        && state.retained.len() >= MAX_RETAINED_MESSAGES
    {
        return false;
    }

    let retained_payload_bytes: usize = state
        .retained
        .iter()
        .filter(|(topic_name, _)| *topic_name != &packet.topic_name)
        .map(|(_, message)| message.payload.len())
        .sum();
    retained_payload_bytes.saturating_add(packet.payload.len()) <= MAX_RETAINED_PAYLOAD_BYTES
}
