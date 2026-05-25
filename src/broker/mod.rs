mod delivery;
mod handler;
mod life;
mod state;
mod storage;

#[cfg(test)]
mod tests;

use std::path::Path;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use rs_netty::{
    Channel, CloseReason,
    codec::{
        MqttPacket, PublishPacket, SubAckPacket, SubscribePacket, UnsubAckPacket,
        UnsubscribePacket, Will,
    },
};

pub use handler::MqttHandler;
pub use life::BrokerLife;

use crate::protocol;

use self::delivery::{
    Delivery, deliveries_for_publish, flush_deliveries, packet_size, queued_deliveries_for_client,
    redeliveries_for_client, retained_for_subscription,
};
use self::state::{
    BrokerState, ClientEntry, PendingPublish, SessionEntry, is_message_expired,
    message_expires_at_ms, now_ms, retain_publish, upsert_subscription,
};
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

    fn generated_client_id(&self) -> String {
        let id = self
            .inner
            .next_generated_client_id
            .fetch_add(1, Ordering::Relaxed);
        format!("mqtt-rs-{id}")
    }

    fn with_state<R>(&self, operation: impl FnOnce(&mut BrokerState) -> R) -> R {
        let mut operation = Some(operation);
        let mut result = None;
        self.inner.storage.with_state(&mut |state| {
            let operation = operation.take().expect("storage operation called once");
            result = Some(operation(state));
        });
        result.expect("storage operation completed")
    }

    fn connect(
        &self,
        connection_id: u64,
        requested_client_id: String,
        channel: Channel<MqttPacket>,
        will: Option<Will>,
        options: ConnectOptions,
    ) -> ConnectOutcome {
        let client_id = if requested_client_id.is_empty() {
            self.generated_client_id()
        } else {
            requested_client_id
        };

        self.with_state(|state| {
            state.expire_sessions(now_ms());
            let had_session = state.sessions_by_client_id.contains_key(&client_id);
            if options.clean_start {
                state.sessions_by_client_id.remove(&client_id);
                state.subscriptions.retain(|sub| sub.client_id != client_id);
            }

            let replaced_channel = if let Some(previous_connection_id) = state
                .connection_by_client_id
                .insert(client_id.clone(), connection_id)
            {
                if previous_connection_id != connection_id {
                    let previous = state.remove_connection_state(previous_connection_id, true);
                    previous.map(|previous| previous.channel)
                } else {
                    None
                }
            } else {
                None
            };

            state
                .sessions_by_client_id
                .entry(client_id.clone())
                .and_modify(|session| {
                    session.expires_at_ms = None;
                    session.session_expiry_interval = options.session_expiry_interval;
                })
                .or_insert_with(|| SessionEntry::connected(options.session_expiry_interval));
            state.clients_by_connection.insert(
                connection_id,
                ClientEntry::new(
                    client_id.clone(),
                    channel,
                    will,
                    options.session_expiry_interval,
                    options.receive_maximum,
                    options.maximum_packet_size,
                ),
            );
            let redeliveries = redeliveries_for_client(state, &client_id);

            ConnectOutcome {
                client_id,
                session_present: !options.clean_start && had_session,
                replaced_channel,
                redeliveries,
            }
        })
    }

    fn subscribe(
        &self,
        connection_id: u64,
        packet: SubscribePacket,
    ) -> (SubAckPacket, Vec<Delivery>) {
        self.with_state(|state| {
            let Some(client_id) = state
                .clients_by_connection
                .get(&connection_id)
                .map(|client| client.client_id.clone())
            else {
                return (
                    SubAckPacket {
                        packet_id: packet.packet_id,
                        properties: Vec::new(),
                        reason_codes: vec![protocol::UNSPECIFIED_ERROR; packet.subscriptions.len()],
                    },
                    Vec::new(),
                );
            };

            let mut reason_codes = Vec::with_capacity(packet.subscriptions.len());
            let mut retained_deliveries = Vec::new();

            for subscription in packet.subscriptions {
                if !protocol::is_valid_topic_filter(&subscription.topic_filter) {
                    reason_codes.push(protocol::TOPIC_FILTER_INVALID);
                    continue;
                }

                let upsert = upsert_subscription(
                    &mut state.subscriptions,
                    &client_id,
                    subscription,
                    subscription_identifier(&packet.properties),
                );
                let stored = state.subscriptions[upsert.index].clone();
                reason_codes.push(protocol::granted_qos_code(stored.options.maximum_qos));

                if should_send_retained_on_subscribe(
                    stored.options.retain_handling,
                    upsert.inserted,
                ) {
                    retained_deliveries.extend(retained_for_subscription(state, &stored));
                }
            }

            (
                SubAckPacket {
                    packet_id: packet.packet_id,
                    properties: Vec::new(),
                    reason_codes,
                },
                retained_deliveries,
            )
        })
    }

    fn unsubscribe(&self, connection_id: u64, packet: UnsubscribePacket) -> UnsubAckPacket {
        self.with_state(|state| {
            if let Some(client_id) = state
                .clients_by_connection
                .get(&connection_id)
                .map(|client| client.client_id.clone())
            {
                for filter in &packet.topic_filters {
                    state
                        .subscriptions
                        .retain(|sub| !(sub.client_id == client_id && sub.filter == *filter));
                }
            }

            UnsubAckPacket {
                packet_id: packet.packet_id,
                properties: Vec::new(),
                reason_codes: vec![protocol::SUCCESS; packet.topic_filters.len()],
            }
        })
    }

    fn publish(&self, publisher_connection_id: u64, packet: &PublishPacket) -> Vec<Delivery> {
        self.with_state(|state| {
            state.expire_sessions(now_ms());
            retain_publish(state, packet);
            deliveries_for_publish(state, publisher_connection_id, packet)
        })
    }

    fn store_qos2_publish(&self, connection_id: u64, packet_id: u16, packet: PublishPacket) -> u8 {
        self.with_state(|state| {
            if state
                .qos2_inflight
                .keys()
                .filter(|(conn_id, _)| *conn_id == connection_id)
                .count()
                >= usize::from(protocol::SERVER_RECEIVE_MAXIMUM)
            {
                return protocol::RECEIVE_MAXIMUM_EXCEEDED;
            }

            let key = (connection_id, packet_id);
            if state.qos2_inflight.contains_key(&key) {
                return protocol::PACKET_IDENTIFIER_IN_USE;
            }

            state.qos2_inflight.insert(
                key,
                PendingPublish {
                    expires_at_ms: message_expires_at_ms(&packet, now_ms()),
                    packet,
                },
            );
            protocol::SUCCESS
        })
    }

    fn complete_qos2_publish(&self, connection_id: u64, packet_id: u16) -> Option<Vec<Delivery>> {
        self.with_state(|state| {
            let pending = state.qos2_inflight.remove(&(connection_id, packet_id))?;

            let now_ms = now_ms();
            state.expire_sessions(now_ms);
            if is_message_expired(pending.expires_at_ms, now_ms) {
                return Some(Vec::new());
            }
            retain_publish(state, &pending.packet);

            Some(deliveries_for_publish(
                state,
                connection_id,
                &pending.packet,
            ))
        })
    }

    fn complete_outbound_qos1(&self, connection_id: u64, packet_id: u16) -> Option<Vec<Delivery>> {
        self.with_state(|state| {
            let client_id = state
                .clients_by_connection
                .get(&connection_id)
                .map(|client| client.client_id.clone())?;
            let removed = state
                .sessions_by_client_id
                .get_mut(&client_id)
                .is_some_and(|session| session.outbound_qos1.remove(&packet_id).is_some());
            removed.then(|| queued_deliveries_for_client(state, &client_id))
        })
    }

    fn receive_outbound_qos2(&self, connection_id: u64, packet_id: u16) -> Option<Vec<Delivery>> {
        self.with_state(|state| {
            let client_id = state
                .clients_by_connection
                .get(&connection_id)
                .map(|client| client.client_id.clone())?;
            let session = state.sessions_by_client_id.get_mut(&client_id)?;

            if session.outbound_qos2_publish.remove(&packet_id).is_some() {
                session.outbound_qos2_pubrel.insert(packet_id);
                Some(queued_deliveries_for_client(state, &client_id))
            } else {
                None
            }
        })
    }

    fn packet_exceeds_server_maximum(&self, packet: &MqttPacket) -> bool {
        packet_size(packet).is_some_and(|size| size > protocol::SERVER_MAXIMUM_PACKET_SIZE as usize)
    }

    fn complete_outbound_qos2(&self, connection_id: u64, packet_id: u16) -> bool {
        self.with_state(|state| {
            let Some(client_id) = state
                .clients_by_connection
                .get(&connection_id)
                .map(|client| client.client_id.clone())
            else {
                return false;
            };
            state
                .sessions_by_client_id
                .get_mut(&client_id)
                .is_some_and(|session| session.outbound_qos2_pubrel.remove(&packet_id))
        })
    }

    fn remove_connection(&self, connection_id: u64) -> Option<Will> {
        self.with_state(|state| {
            let client = state.remove_connection_state(connection_id, false)?;
            state.connection_by_client_id.remove(&client.client_id);
            client.will
        })
    }

    async fn publish_will(&self, connection_id: u64, will: Will) {
        if !protocol::is_valid_topic_name(&will.topic) {
            return;
        }

        let packet = PublishPacket {
            dup: false,
            qos: will.qos,
            retain: will.retain,
            topic_name: will.topic,
            packet_id: None,
            properties: will.properties,
            payload: will.payload,
        };
        let deliveries = self.publish(connection_id, &packet);
        flush_deliveries(deliveries).await;
    }
}

struct ConnectOutcome {
    client_id: String,
    session_present: bool,
    replaced_channel: Option<Channel<MqttPacket>>,
    redeliveries: Vec<Delivery>,
}

struct ConnectOptions {
    clean_start: bool,
    session_expiry_interval: u32,
    receive_maximum: u16,
    maximum_packet_size: u32,
}

fn should_publish_will(reason: CloseReason) -> bool {
    !matches!(
        reason,
        CloseReason::HandlerClosed | CloseReason::LocalClosed
    )
}

fn should_send_retained_on_subscribe(retain_handling: u8, inserted: bool) -> bool {
    match retain_handling {
        1 => inserted,
        2 => false,
        _ => true,
    }
}

fn subscription_identifier(properties: &[rs_netty::codec::MqttProperty]) -> Option<u32> {
    properties.iter().find_map(|property| match property {
        rs_netty::codec::MqttProperty::SubscriptionIdentifier(value) => Some(*value),
        _ => None,
    })
}
