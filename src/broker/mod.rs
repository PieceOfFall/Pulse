mod delivery;
mod handler;
mod life;
mod state;
mod storage;

#[cfg(test)]
mod tests;

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
    Delivery, deliveries_for_publish, flush_deliveries, retained_for_subscription,
};
use self::state::{
    BrokerState, ClientEntry, SessionEntry, now_ms, retain_publish, upsert_subscription,
};
use self::storage::{BrokerStorage, InMemoryStorage};

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
        clean_start: bool,
        session_expiry_interval: u32,
    ) -> ConnectOutcome {
        let client_id = if requested_client_id.is_empty() {
            self.generated_client_id()
        } else {
            requested_client_id
        };

        self.with_state(|state| {
            state.expire_sessions(now_ms());
            let had_session = state.sessions_by_client_id.contains_key(&client_id);
            if clean_start {
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

            state.sessions_by_client_id.insert(
                client_id.clone(),
                SessionEntry::connected(session_expiry_interval),
            );
            state.clients_by_connection.insert(
                connection_id,
                ClientEntry::new(client_id.clone(), channel, will, session_expiry_interval),
            );

            ConnectOutcome {
                client_id,
                session_present: !clean_start && had_session,
                replaced_channel,
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

                let stored_index =
                    upsert_subscription(&mut state.subscriptions, &client_id, subscription);
                let stored = state.subscriptions[stored_index].clone();
                reason_codes.push(protocol::granted_qos_code(stored.options.maximum_qos));

                if stored.options.retain_handling != 2 {
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
            retain_publish(state, packet);
            deliveries_for_publish(state, publisher_connection_id, packet)
        })
    }

    fn store_qos2_publish(
        &self,
        connection_id: u64,
        packet_id: u16,
        packet: PublishPacket,
    ) -> bool {
        self.with_state(|state| {
            let key = (connection_id, packet_id);
            if state.qos2_inflight.contains_key(&key) {
                return false;
            }

            state.qos2_inflight.insert(key, packet);
            true
        })
    }

    fn complete_qos2_publish(&self, connection_id: u64, packet_id: u16) -> Option<Vec<Delivery>> {
        self.with_state(|state| {
            let packet = state.qos2_inflight.remove(&(connection_id, packet_id))?;

            retain_publish(state, &packet);

            Some(deliveries_for_publish(state, connection_id, &packet))
        })
    }

    fn complete_outbound_qos1(&self, connection_id: u64, packet_id: u16) -> bool {
        self.with_state(|state| {
            state
                .clients_by_connection
                .get_mut(&connection_id)
                .is_some_and(|client| client.outbound_qos1.remove(&packet_id))
        })
    }

    fn receive_outbound_qos2(&self, connection_id: u64, packet_id: u16) -> bool {
        self.with_state(|state| {
            let Some(client) = state.clients_by_connection.get_mut(&connection_id) else {
                return false;
            };

            if client.outbound_qos2_publish.remove(&packet_id) {
                client.outbound_qos2_pubrel.insert(packet_id);
                true
            } else {
                false
            }
        })
    }

    fn complete_outbound_qos2(&self, connection_id: u64, packet_id: u16) -> bool {
        self.with_state(|state| {
            state
                .clients_by_connection
                .get_mut(&connection_id)
                .is_some_and(|client| client.outbound_qos2_pubrel.remove(&packet_id))
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
}

fn should_publish_will(reason: CloseReason) -> bool {
    !matches!(
        reason,
        CloseReason::HandlerClosed | CloseReason::LocalClosed
    )
}
