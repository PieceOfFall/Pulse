mod delivery;
mod handler;
mod life;

#[cfg(test)]
mod tests;

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use rs_netty::{
    Channel, CloseReason,
    codec::{
        MqttPacket, MqttProperty, PublishPacket, QoS, SubAckPacket, SubscribePacket, Subscription,
        UnsubAckPacket, UnsubscribePacket, Will,
    },
};

pub use handler::MqttHandler;
pub use life::BrokerLife;

use crate::protocol;

use self::delivery::{
    Delivery, deliveries_for_publish, flush_deliveries, retained_for_subscription,
};

#[derive(Clone)]
pub struct Broker {
    inner: Arc<BrokerInner>,
}

struct BrokerInner {
    next_generated_client_id: AtomicU64,
    state: Mutex<BrokerState>,
}

#[derive(Default)]
struct BrokerState {
    clients_by_connection: HashMap<u64, ClientEntry>,
    connection_by_client_id: HashMap<String, u64>,
    subscriptions: Vec<SubscriptionEntry>,
    retained: HashMap<String, RetainedMessage>,
    qos2_inflight: HashMap<(u64, u16), PublishPacket>,
}

struct ClientEntry {
    client_id: String,
    channel: Channel<MqttPacket>,
    will: Option<Will>,
    next_packet_id: u16,
    outbound_qos1: HashSet<u16>,
    outbound_qos2_publish: HashSet<u16>,
    outbound_qos2_pubrel: HashSet<u16>,
}

#[derive(Clone)]
struct SubscriptionEntry {
    connection_id: u64,
    filter: String,
    options: rs_netty::codec::SubscriptionOptions,
}

#[derive(Clone)]
struct RetainedMessage {
    qos: QoS,
    topic_name: String,
    properties: Vec<MqttProperty>,
    payload: bytes::Bytes,
}

impl Broker {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(BrokerInner {
                next_generated_client_id: AtomicU64::new(1),
                state: Mutex::new(BrokerState::default()),
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

    fn connect(
        &self,
        connection_id: u64,
        requested_client_id: String,
        channel: Channel<MqttPacket>,
        will: Option<Will>,
    ) -> ConnectOutcome {
        let client_id = if requested_client_id.is_empty() {
            self.generated_client_id()
        } else {
            requested_client_id
        };

        let mut state = self.inner.state.lock().expect("broker state lock poisoned");
        let replaced_channel = if let Some(previous_connection_id) = state
            .connection_by_client_id
            .insert(client_id.clone(), connection_id)
        {
            if previous_connection_id != connection_id {
                let previous = state.clients_by_connection.remove(&previous_connection_id);
                state
                    .subscriptions
                    .retain(|sub| sub.connection_id != previous_connection_id);
                state
                    .qos2_inflight
                    .retain(|(conn_id, _), _| *conn_id != previous_connection_id);
                previous.map(|previous| previous.channel)
            } else {
                None
            }
        } else {
            None
        };

        state.clients_by_connection.insert(
            connection_id,
            ClientEntry {
                client_id: client_id.clone(),
                channel,
                will,
                next_packet_id: 1,
                outbound_qos1: HashSet::new(),
                outbound_qos2_publish: HashSet::new(),
                outbound_qos2_pubrel: HashSet::new(),
            },
        );

        ConnectOutcome {
            client_id,
            session_present: false,
            replaced_channel,
        }
    }

    fn subscribe(
        &self,
        connection_id: u64,
        packet: SubscribePacket,
    ) -> (SubAckPacket, Vec<Delivery>) {
        let mut state = self.inner.state.lock().expect("broker state lock poisoned");
        if !state.clients_by_connection.contains_key(&connection_id) {
            return (
                SubAckPacket {
                    packet_id: packet.packet_id,
                    properties: Vec::new(),
                    reason_codes: vec![protocol::UNSPECIFIED_ERROR; packet.subscriptions.len()],
                },
                Vec::new(),
            );
        }

        let mut reason_codes = Vec::with_capacity(packet.subscriptions.len());
        let mut retained_deliveries = Vec::new();

        for subscription in packet.subscriptions {
            if !protocol::is_valid_topic_filter(&subscription.topic_filter) {
                reason_codes.push(protocol::TOPIC_FILTER_INVALID);
                continue;
            }

            let stored_index =
                upsert_subscription(&mut state.subscriptions, connection_id, subscription);
            let stored = state.subscriptions[stored_index].clone();
            reason_codes.push(protocol::granted_qos_code(stored.options.maximum_qos));

            if stored.options.retain_handling != 2 {
                retained_deliveries.extend(retained_for_subscription(&mut state, &stored));
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
    }

    fn unsubscribe(&self, connection_id: u64, packet: UnsubscribePacket) -> UnsubAckPacket {
        let mut state = self.inner.state.lock().expect("broker state lock poisoned");
        for filter in &packet.topic_filters {
            state
                .subscriptions
                .retain(|sub| !(sub.connection_id == connection_id && sub.filter == *filter));
        }

        UnsubAckPacket {
            packet_id: packet.packet_id,
            properties: Vec::new(),
            reason_codes: vec![protocol::SUCCESS; packet.topic_filters.len()],
        }
    }

    fn publish(&self, publisher_connection_id: u64, packet: &PublishPacket) -> Vec<Delivery> {
        let mut state = self.inner.state.lock().expect("broker state lock poisoned");

        retain_publish(&mut state, packet);

        deliveries_for_publish(&mut state, publisher_connection_id, packet)
    }

    fn store_qos2_publish(&self, connection_id: u64, packet_id: u16, packet: PublishPacket) {
        let mut state = self.inner.state.lock().expect("broker state lock poisoned");
        state
            .qos2_inflight
            .insert((connection_id, packet_id), packet);
    }

    fn complete_qos2_publish(&self, connection_id: u64, packet_id: u16) -> Vec<Delivery> {
        let mut state = self.inner.state.lock().expect("broker state lock poisoned");
        let Some(packet) = state.qos2_inflight.remove(&(connection_id, packet_id)) else {
            return Vec::new();
        };

        retain_publish(&mut state, &packet);

        deliveries_for_publish(&mut state, connection_id, &packet)
    }

    fn complete_outbound_qos1(&self, connection_id: u64, packet_id: u16) {
        let mut state = self.inner.state.lock().expect("broker state lock poisoned");
        if let Some(client) = state.clients_by_connection.get_mut(&connection_id) {
            client.outbound_qos1.remove(&packet_id);
        }
    }

    fn receive_outbound_qos2(&self, connection_id: u64, packet_id: u16) -> bool {
        let mut state = self.inner.state.lock().expect("broker state lock poisoned");
        let Some(client) = state.clients_by_connection.get_mut(&connection_id) else {
            return false;
        };

        if client.outbound_qos2_publish.remove(&packet_id) {
            client.outbound_qos2_pubrel.insert(packet_id);
            true
        } else {
            false
        }
    }

    fn complete_outbound_qos2(&self, connection_id: u64, packet_id: u16) {
        let mut state = self.inner.state.lock().expect("broker state lock poisoned");
        if let Some(client) = state.clients_by_connection.get_mut(&connection_id) {
            client.outbound_qos2_pubrel.remove(&packet_id);
        }
    }

    fn remove_connection(&self, connection_id: u64) -> Option<Will> {
        let mut state = self.inner.state.lock().expect("broker state lock poisoned");
        let client = state.clients_by_connection.remove(&connection_id)?;
        state.connection_by_client_id.remove(&client.client_id);
        state
            .subscriptions
            .retain(|sub| sub.connection_id != connection_id);
        state
            .qos2_inflight
            .retain(|(conn_id, _), _| *conn_id != connection_id);
        client.will
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

fn retain_publish(state: &mut BrokerState, packet: &PublishPacket) {
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

fn upsert_subscription(
    subscriptions: &mut Vec<SubscriptionEntry>,
    connection_id: u64,
    subscription: Subscription,
) -> usize {
    if let Some(index) = subscriptions.iter_mut().position(|sub| {
        sub.connection_id == connection_id && sub.filter == subscription.topic_filter
    }) {
        subscriptions[index].options = subscription.options;
        return index;
    }

    subscriptions.push(SubscriptionEntry {
        connection_id,
        filter: subscription.topic_filter,
        options: subscription.options,
    });
    subscriptions.len() - 1
}

fn should_publish_will(reason: CloseReason) -> bool {
    !matches!(
        reason,
        CloseReason::HandlerClosed | CloseReason::LocalClosed
    )
}
