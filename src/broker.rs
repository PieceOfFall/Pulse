use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use rs_netty::{
    Channel, CloseReason, ConnInfo, Context, Handler, Life, Result,
    codec::{
        ConnAckPacket, DisconnectPacket, MqttPacket, MqttProperty, PublishPacket, QoS,
        SubAckPacket, SubscribePacket, Subscription, UnsubAckPacket, UnsubscribePacket, Will,
        mqtt::AckPacket,
    },
};

use crate::protocol;

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
}

#[derive(Clone)]
struct SubscriptionEntry {
    connection_id: u64,
    filter: String,
    options: rs_netty::codec::SubscriptionOptions,
}

#[derive(Clone)]
struct RetainedMessage {
    topic_name: String,
    properties: Vec<MqttProperty>,
    payload: bytes::Bytes,
}

#[derive(Clone)]
struct Delivery {
    channel: Channel<MqttPacket>,
    packet: MqttPacket,
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
        if let Some(previous_connection_id) = state
            .connection_by_client_id
            .insert(client_id.clone(), connection_id)
        {
            if previous_connection_id != connection_id {
                if let Some(previous) = state.clients_by_connection.remove(&previous_connection_id)
                {
                    let _ = previous.channel;
                }
                state
                    .subscriptions
                    .retain(|sub| sub.connection_id != previous_connection_id);
                state
                    .qos2_inflight
                    .retain(|(conn_id, _), _| *conn_id != previous_connection_id);
            }
        }

        state.clients_by_connection.insert(
            connection_id,
            ClientEntry {
                client_id: client_id.clone(),
                channel,
                will,
            },
        );

        ConnectOutcome {
            client_id,
            session_present: false,
        }
    }

    fn subscribe(
        &self,
        connection_id: u64,
        packet: SubscribePacket,
    ) -> (SubAckPacket, Vec<Delivery>) {
        let mut state = self.inner.state.lock().expect("broker state lock poisoned");
        let Some(client) = state.clients_by_connection.get(&connection_id) else {
            return (
                SubAckPacket {
                    packet_id: packet.packet_id,
                    properties: Vec::new(),
                    reason_codes: vec![protocol::UNSPECIFIED_ERROR; packet.subscriptions.len()],
                },
                Vec::new(),
            );
        };

        let channel = client.channel.clone();
        let mut reason_codes = Vec::with_capacity(packet.subscriptions.len());
        let mut retained_deliveries = Vec::new();

        for subscription in packet.subscriptions {
            if !protocol::is_valid_topic_filter(&subscription.topic_filter) {
                reason_codes.push(protocol::TOPIC_FILTER_INVALID);
                continue;
            }

            upsert_subscription(&mut state.subscriptions, connection_id, subscription);
            let stored = state
                .subscriptions
                .last()
                .expect("subscription was inserted");
            reason_codes.push(protocol::granted_qos_code(stored.options.maximum_qos));

            if stored.options.retain_handling != 2 {
                retained_deliveries.extend(retained_for_subscription(&state, &channel, stored));
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

        if packet.retain {
            if packet.payload.is_empty() {
                state.retained.remove(&packet.topic_name);
            } else {
                state.retained.insert(
                    packet.topic_name.clone(),
                    RetainedMessage {
                        topic_name: packet.topic_name.clone(),
                        properties: packet.properties.clone(),
                        payload: packet.payload.clone(),
                    },
                );
            }
        }

        deliveries_for_publish(&state, publisher_connection_id, packet)
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

        if packet.retain {
            if packet.payload.is_empty() {
                state.retained.remove(&packet.topic_name);
            } else {
                state.retained.insert(
                    packet.topic_name.clone(),
                    RetainedMessage {
                        topic_name: packet.topic_name.clone(),
                        properties: packet.properties.clone(),
                        payload: packet.payload.clone(),
                    },
                );
            }
        }

        deliveries_for_publish(&state, connection_id, &packet)
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
}

pub struct MqttHandler {
    broker: Broker,
    connected: bool,
    client_id: Option<String>,
}

impl MqttHandler {
    pub fn new(broker: Broker) -> Self {
        Self {
            broker,
            connected: false,
            client_id: None,
        }
    }

    async fn disconnect(&mut self, ctx: &mut Context<MqttPacket>, reason_code: u8) -> Result<()> {
        ctx.write(MqttPacket::Disconnect(DisconnectPacket {
            reason_code,
            properties: Vec::new(),
        }))
        .await?;
        ctx.close().await
    }
}

impl Handler<MqttPacket> for MqttHandler {
    type Write = MqttPacket;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: MqttPacket) -> Result<()> {
        match msg {
            MqttPacket::Connect(packet) => {
                if self.connected {
                    return self.disconnect(ctx, protocol::PROTOCOL_ERROR).await;
                }

                let assigned_client_id = packet.client_id.is_empty();
                let outcome =
                    self.broker
                        .connect(ctx.id(), packet.client_id, ctx.channel(), packet.will);
                self.connected = true;
                self.client_id = Some(outcome.client_id.clone());

                let mut properties = vec![
                    MqttProperty::ReceiveMaximum(1024),
                    MqttProperty::MaximumQoS(2),
                    MqttProperty::RetainAvailable(1),
                    MqttProperty::WildcardSubscriptionAvailable(1),
                    MqttProperty::SubscriptionIdentifierAvailable(0),
                    MqttProperty::SharedSubscriptionAvailable(0),
                ];
                if assigned_client_id {
                    properties.push(MqttProperty::AssignedClientIdentifier(outcome.client_id));
                }

                ctx.write(MqttPacket::ConnAck(ConnAckPacket {
                    session_present: outcome.session_present,
                    reason_code: protocol::SUCCESS,
                    properties,
                }))
                .await
            }
            packet if !self.connected => {
                let reason = match packet {
                    MqttPacket::Auth(_) => protocol::BAD_AUTHENTICATION_METHOD,
                    _ => protocol::PROTOCOL_ERROR,
                };
                self.disconnect(ctx, reason).await
            }
            MqttPacket::PingReq => ctx.write(MqttPacket::PingResp).await,
            MqttPacket::Disconnect(_) => {
                self.broker.remove_connection(ctx.id());
                self.connected = false;
                ctx.close().await
            }
            MqttPacket::Subscribe(packet) => {
                let (suback, retained) = self.broker.subscribe(ctx.id(), packet);
                ctx.write(MqttPacket::SubAck(suback)).await?;
                flush_deliveries(retained).await;
                Ok(())
            }
            MqttPacket::Unsubscribe(packet) => {
                let unsuback = self.broker.unsubscribe(ctx.id(), packet);
                ctx.write(MqttPacket::UnsubAck(unsuback)).await
            }
            MqttPacket::Publish(packet) => {
                if !protocol::is_valid_topic_name(&packet.topic_name) {
                    return self.disconnect(ctx, protocol::TOPIC_NAME_INVALID).await;
                }

                match packet.qos {
                    QoS::AtMostOnce => {}
                    QoS::AtLeastOnce => {
                        if let Some(packet_id) = packet.packet_id {
                            ctx.write(MqttPacket::PubAck(AckPacket::new(
                                packet_id,
                                protocol::SUCCESS,
                            )))
                            .await?;
                        } else {
                            return self.disconnect(ctx, protocol::MALFORMED_PACKET).await;
                        }
                    }
                    QoS::ExactlyOnce => {
                        if let Some(packet_id) = packet.packet_id {
                            self.broker.store_qos2_publish(ctx.id(), packet_id, packet);
                            ctx.write(MqttPacket::PubRec(AckPacket::new(
                                packet_id,
                                protocol::SUCCESS,
                            )))
                            .await?;
                            return Ok(());
                        } else {
                            return self.disconnect(ctx, protocol::MALFORMED_PACKET).await;
                        }
                    }
                }

                let deliveries = self.broker.publish(ctx.id(), &packet);
                flush_deliveries(deliveries).await;
                Ok(())
            }
            MqttPacket::PubRel(packet) => {
                let deliveries = self
                    .broker
                    .complete_qos2_publish(ctx.id(), packet.packet_id);
                flush_deliveries(deliveries).await;
                ctx.write(MqttPacket::PubComp(AckPacket::new(
                    packet.packet_id,
                    protocol::SUCCESS,
                )))
                .await
            }
            MqttPacket::PubAck(_) | MqttPacket::PubRec(_) | MqttPacket::PubComp(_) => Ok(()),
            MqttPacket::Auth(_) => {
                self.disconnect(ctx, protocol::BAD_AUTHENTICATION_METHOD)
                    .await
            }
            MqttPacket::ConnAck(_)
            | MqttPacket::SubAck(_)
            | MqttPacket::UnsubAck(_)
            | MqttPacket::PingResp => self.disconnect(ctx, protocol::PROTOCOL_ERROR).await,
        }
    }
}

#[derive(Clone)]
pub struct BrokerLife {
    broker: Broker,
}

impl BrokerLife {
    pub fn new(broker: Broker) -> Self {
        Self { broker }
    }
}

impl Life for BrokerLife {
    async fn tcp_connection_closed(&self, info: ConnInfo, reason: CloseReason) -> Result<()> {
        let will = self.broker.remove_connection(info.id());
        if let Some(will) = will {
            if should_publish_will(reason) {
                self.broker.publish_will(info.id(), will).await;
            }
        }
        Ok(())
    }
}

fn upsert_subscription(
    subscriptions: &mut Vec<SubscriptionEntry>,
    connection_id: u64,
    subscription: Subscription,
) {
    if let Some(existing) = subscriptions
        .iter_mut()
        .find(|sub| sub.connection_id == connection_id && sub.filter == subscription.topic_filter)
    {
        existing.options = subscription.options;
        return;
    }

    subscriptions.push(SubscriptionEntry {
        connection_id,
        filter: subscription.topic_filter,
        options: subscription.options,
    });
}

fn deliveries_for_publish(
    state: &BrokerState,
    publisher_connection_id: u64,
    packet: &PublishPacket,
) -> Vec<Delivery> {
    state
        .subscriptions
        .iter()
        .filter(|sub| {
            protocol::topic_matches(&sub.filter, &packet.topic_name)
                && !(sub.options.no_local && sub.connection_id == publisher_connection_id)
        })
        .filter_map(|sub| {
            let client = state.clients_by_connection.get(&sub.connection_id)?;
            Some(Delivery {
                channel: client.channel.clone(),
                packet: MqttPacket::Publish(PublishPacket {
                    dup: false,
                    qos: QoS::AtMostOnce,
                    retain: sub.options.retain_as_published && packet.retain,
                    topic_name: packet.topic_name.clone(),
                    packet_id: None,
                    properties: packet.properties.clone(),
                    payload: packet.payload.clone(),
                }),
            })
        })
        .collect()
}

fn retained_for_subscription(
    state: &BrokerState,
    channel: &Channel<MqttPacket>,
    subscription: &SubscriptionEntry,
) -> Vec<Delivery> {
    state
        .retained
        .values()
        .filter(|message| protocol::topic_matches(&subscription.filter, &message.topic_name))
        .map(|message| Delivery {
            channel: channel.clone(),
            packet: MqttPacket::Publish(PublishPacket {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: true,
                topic_name: message.topic_name.clone(),
                packet_id: None,
                properties: message.properties.clone(),
                payload: message.payload.clone(),
            }),
        })
        .collect()
}

async fn flush_deliveries(deliveries: Vec<Delivery>) {
    for delivery in deliveries {
        let _ = delivery.channel.write(delivery.packet).await;
    }
}

fn should_publish_will(reason: CloseReason) -> bool {
    !matches!(
        reason,
        CloseReason::HandlerClosed | CloseReason::LocalClosed
    )
}
