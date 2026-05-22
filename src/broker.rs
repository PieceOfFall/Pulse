use std::{
    collections::{HashMap, HashSet},
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

        if packet.retain {
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

        if packet.retain {
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
                if let Some(replaced_channel) = outcome.replaced_channel {
                    let _ = replaced_channel.close().await;
                }
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
            MqttPacket::PubAck(packet) => {
                self.broker
                    .complete_outbound_qos1(ctx.id(), packet.packet_id);
                Ok(())
            }
            MqttPacket::PubRec(packet) => {
                if self
                    .broker
                    .receive_outbound_qos2(ctx.id(), packet.packet_id)
                {
                    ctx.write(MqttPacket::PubRel(AckPacket::new(
                        packet.packet_id,
                        protocol::SUCCESS,
                    )))
                    .await
                } else {
                    Ok(())
                }
            }
            MqttPacket::PubComp(packet) => {
                self.broker
                    .complete_outbound_qos2(ctx.id(), packet.packet_id);
                Ok(())
            }
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
fn deliveries_for_publish(
    state: &mut BrokerState,
    publisher_connection_id: u64,
    packet: &PublishPacket,
) -> Vec<Delivery> {
    let matches: Vec<SubscriptionEntry> = state
        .subscriptions
        .iter()
        .filter(|sub| {
            protocol::topic_matches(&sub.filter, &packet.topic_name)
                && !(sub.options.no_local && sub.connection_id == publisher_connection_id)
        })
        .cloned()
        .collect();

    matches
        .into_iter()
        .filter_map(|sub| {
            let client = state.clients_by_connection.get_mut(&sub.connection_id)?;
            Some(delivery_for_client(
                client,
                packet,
                sub.options.maximum_qos,
                sub.options.retain_as_published && packet.retain,
            ))
        })
        .collect()
}

fn retained_for_subscription(
    state: &mut BrokerState,
    subscription: &SubscriptionEntry,
) -> Vec<Delivery> {
    let retained: Vec<RetainedMessage> = state
        .retained
        .values()
        .filter(|message| protocol::topic_matches(&subscription.filter, &message.topic_name))
        .cloned()
        .collect();

    let Some(client) = state
        .clients_by_connection
        .get_mut(&subscription.connection_id)
    else {
        return Vec::new();
    };

    retained
        .into_iter()
        .map(|message| {
            delivery_for_client(
                client,
                &PublishPacket {
                    dup: false,
                    qos: message.qos,
                    retain: true,
                    topic_name: message.topic_name,
                    packet_id: None,
                    properties: message.properties,
                    payload: message.payload,
                },
                subscription.options.maximum_qos,
                true,
            )
        })
        .collect()
}

fn delivery_for_client(
    client: &mut ClientEntry,
    packet: &PublishPacket,
    maximum_qos: QoS,
    retain: bool,
) -> Delivery {
    let qos = effective_qos(packet.qos, maximum_qos);
    let packet_id = match qos {
        QoS::AtMostOnce => None,
        QoS::AtLeastOnce => {
            let packet_id = next_packet_id(client);
            client.outbound_qos1.insert(packet_id);
            Some(packet_id)
        }
        QoS::ExactlyOnce => {
            let packet_id = next_packet_id(client);
            client.outbound_qos2_publish.insert(packet_id);
            Some(packet_id)
        }
    };

    Delivery {
        channel: client.channel.clone(),
        packet: MqttPacket::Publish(PublishPacket {
            dup: false,
            qos,
            retain,
            topic_name: packet.topic_name.clone(),
            packet_id,
            properties: packet.properties.clone(),
            payload: packet.payload.clone(),
        }),
    }
}

fn effective_qos(publish_qos: QoS, maximum_qos: QoS) -> QoS {
    if qos_rank(publish_qos) <= qos_rank(maximum_qos) {
        publish_qos
    } else {
        maximum_qos
    }
}

fn qos_rank(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

fn next_packet_id(client: &mut ClientEntry) -> u16 {
    loop {
        let packet_id = client.next_packet_id;
        client.next_packet_id = if client.next_packet_id == u16::MAX {
            1
        } else {
            client.next_packet_id + 1
        };

        if !client.outbound_qos1.contains(&packet_id)
            && !client.outbound_qos2_publish.contains(&packet_id)
            && !client.outbound_qos2_pubrel.contains(&packet_id)
        {
            return packet_id;
        }
    }
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};
    use rs_netty::{
        TcpServer,
        codec::{
            ConnectPacket, Decoder, Encoder, MqttCodec, MqttPacket, PublishPacket, QoS,
            SubscribePacket, Subscription, SubscriptionOptions, mqtt::AckPacket,
        },
        pipeline,
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    use super::{
        Broker, BrokerLife, MqttHandler, SubscriptionEntry, protocol, upsert_subscription,
    };

    #[tokio::test]
    async fn duplicate_client_id_closes_previous_connection() -> rs_netty::Result<()> {
        let broker = Broker::new();
        let server = TcpServer::bind("127.0.0.1:0")
            .life(BrokerLife::new(broker.clone()))
            .pipeline(move || {
                pipeline()
                    .codec(MqttCodec::with_max_packet_size(1024 * 1024))
                    .handler(MqttHandler::new(broker.clone()))
            })
            .start()
            .await?;

        let mut first = TcpStream::connect(server.local_addr()).await?;
        first.write_all(&connect_packet("same-client")).await?;
        read_connack(&mut first).await?;

        let mut second = TcpStream::connect(server.local_addr()).await?;
        second.write_all(&connect_packet("same-client")).await?;
        read_connack(&mut second).await?;

        let mut buf = [0; 1];
        let read = tokio::time::timeout(Duration::from_millis(200), first.read(&mut buf))
            .await
            .expect("previous connection should close")?;
        assert_eq!(read, 0);

        server.shutdown();
        server.wait().await
    }

    #[test]
    fn upsert_subscription_returns_updated_subscription_index() {
        let mut subscriptions = vec![
            SubscriptionEntry {
                connection_id: 1,
                filter: "devices/one".to_string(),
                options: SubscriptionOptions::default(),
            },
            SubscriptionEntry {
                connection_id: 2,
                filter: "devices/two".to_string(),
                options: SubscriptionOptions::default(),
            },
        ];

        let index = upsert_subscription(
            &mut subscriptions,
            1,
            Subscription {
                topic_filter: "devices/one".to_string(),
                options: SubscriptionOptions {
                    maximum_qos: QoS::ExactlyOnce,
                    ..SubscriptionOptions::default()
                },
            },
        );

        assert_eq!(index, 0);
        assert_eq!(subscriptions.len(), 2);
        assert_eq!(subscriptions[index].options.maximum_qos, QoS::ExactlyOnce);
    }

    #[tokio::test]
    async fn qos1_publish_is_delivered_with_qos1_and_acknowledged() -> rs_netty::Result<()> {
        let broker = Broker::new();
        let server = TcpServer::bind("127.0.0.1:0")
            .life(BrokerLife::new(broker.clone()))
            .pipeline(move || {
                pipeline()
                    .codec(MqttCodec::with_max_packet_size(1024 * 1024))
                    .handler(MqttHandler::new(broker.clone()))
            })
            .start()
            .await?;

        let mut subscriber = TcpStream::connect(server.local_addr()).await?;
        let mut subscriber_buf = BytesMut::new();
        write_packet(&mut subscriber, connect("subscriber")).await?;
        assert!(matches!(
            read_packet_with_buf(&mut subscriber, &mut subscriber_buf).await?,
            MqttPacket::ConnAck(_)
        ));
        write_packet(
            &mut subscriber,
            subscribe(1, "devices/one", QoS::AtLeastOnce),
        )
        .await?;
        assert!(matches!(
            read_packet(&mut subscriber).await?,
            MqttPacket::SubAck(_)
        ));

        let mut publisher = TcpStream::connect(server.local_addr()).await?;
        write_packet(&mut publisher, connect("publisher")).await?;
        assert!(matches!(
            read_packet(&mut publisher).await?,
            MqttPacket::ConnAck(_)
        ));
        write_packet(
            &mut publisher,
            publish("devices/one", QoS::AtLeastOnce, Some(7), "hello"),
        )
        .await?;
        assert!(matches!(
            read_packet(&mut publisher).await?,
            MqttPacket::PubAck(packet) if packet.packet_id == 7
        ));

        let delivered = read_packet(&mut subscriber).await?;
        let MqttPacket::Publish(packet) = delivered else {
            panic!("expected publish, got {delivered:?}");
        };
        assert_eq!(packet.qos, QoS::AtLeastOnce);
        assert_eq!(packet.packet_id, Some(1));
        assert_eq!(packet.payload, Bytes::from_static(b"hello"));
        write_packet(
            &mut subscriber,
            MqttPacket::PubAck(AckPacket::new(packet.packet_id.unwrap(), protocol::SUCCESS)),
        )
        .await?;

        server.shutdown();
        server.wait().await
    }

    #[tokio::test]
    async fn qos2_publish_completes_both_handshakes() -> rs_netty::Result<()> {
        let broker = Broker::new();
        let server = TcpServer::bind("127.0.0.1:0")
            .life(BrokerLife::new(broker.clone()))
            .pipeline(move || {
                pipeline()
                    .codec(MqttCodec::with_max_packet_size(1024 * 1024))
                    .handler(MqttHandler::new(broker.clone()))
            })
            .start()
            .await?;

        let mut subscriber = TcpStream::connect(server.local_addr()).await?;
        write_packet(&mut subscriber, connect("subscriber")).await?;
        assert!(matches!(
            read_packet(&mut subscriber).await?,
            MqttPacket::ConnAck(_)
        ));
        write_packet(
            &mut subscriber,
            subscribe(1, "devices/two", QoS::ExactlyOnce),
        )
        .await?;
        assert!(matches!(
            read_packet(&mut subscriber).await?,
            MqttPacket::SubAck(_)
        ));

        let mut publisher = TcpStream::connect(server.local_addr()).await?;
        write_packet(&mut publisher, connect("publisher")).await?;
        assert!(matches!(
            read_packet(&mut publisher).await?,
            MqttPacket::ConnAck(_)
        ));
        write_packet(
            &mut publisher,
            publish("devices/two", QoS::ExactlyOnce, Some(9), "hello"),
        )
        .await?;
        assert!(matches!(
            read_packet(&mut publisher).await?,
            MqttPacket::PubRec(packet) if packet.packet_id == 9
        ));
        write_packet(
            &mut publisher,
            MqttPacket::PubRel(AckPacket::new(9, protocol::SUCCESS)),
        )
        .await?;
        assert!(matches!(
            read_packet(&mut publisher).await?,
            MqttPacket::PubComp(packet) if packet.packet_id == 9
        ));

        let delivered = read_packet(&mut subscriber).await?;
        let MqttPacket::Publish(packet) = delivered else {
            panic!("expected publish, got {delivered:?}");
        };
        assert_eq!(packet.qos, QoS::ExactlyOnce);
        assert_eq!(packet.packet_id, Some(1));
        assert_eq!(packet.payload, Bytes::from_static(b"hello"));
        write_packet(
            &mut subscriber,
            MqttPacket::PubRec(AckPacket::new(packet.packet_id.unwrap(), protocol::SUCCESS)),
        )
        .await?;
        assert!(matches!(
            read_packet(&mut subscriber).await?,
            MqttPacket::PubRel(packet) if packet.packet_id == 1
        ));
        write_packet(
            &mut subscriber,
            MqttPacket::PubComp(AckPacket::new(1, protocol::SUCCESS)),
        )
        .await?;

        server.shutdown();
        server.wait().await
    }

    #[tokio::test]
    async fn retained_qos2_publish_replays_at_subscriber_qos() -> rs_netty::Result<()> {
        let broker = Broker::new();
        let server = TcpServer::bind("127.0.0.1:0")
            .life(BrokerLife::new(broker.clone()))
            .pipeline(move || {
                pipeline()
                    .codec(MqttCodec::with_max_packet_size(1024 * 1024))
                    .handler(MqttHandler::new(broker.clone()))
            })
            .start()
            .await?;

        let mut publisher = TcpStream::connect(server.local_addr()).await?;
        write_packet(&mut publisher, connect("publisher")).await?;
        assert!(matches!(
            read_packet(&mut publisher).await?,
            MqttPacket::ConnAck(_)
        ));
        write_packet(
            &mut publisher,
            publish_with_retain(
                "devices/retained",
                QoS::ExactlyOnce,
                Some(11),
                "sticky",
                true,
            ),
        )
        .await?;
        assert!(matches!(
            read_packet(&mut publisher).await?,
            MqttPacket::PubRec(packet) if packet.packet_id == 11
        ));
        write_packet(
            &mut publisher,
            MqttPacket::PubRel(AckPacket::new(11, protocol::SUCCESS)),
        )
        .await?;
        assert!(matches!(
            read_packet(&mut publisher).await?,
            MqttPacket::PubComp(packet) if packet.packet_id == 11
        ));

        let mut subscriber = TcpStream::connect(server.local_addr()).await?;
        let mut subscriber_buf = BytesMut::new();
        write_packet(&mut subscriber, connect("subscriber")).await?;
        assert!(matches!(
            read_packet_with_buf(&mut subscriber, &mut subscriber_buf).await?,
            MqttPacket::ConnAck(_)
        ));
        write_packet(
            &mut subscriber,
            subscribe(1, "devices/retained", QoS::ExactlyOnce),
        )
        .await?;
        assert!(matches!(
            read_packet_with_buf(&mut subscriber, &mut subscriber_buf).await?,
            MqttPacket::SubAck(_)
        ));

        let delivered = read_packet_with_buf(&mut subscriber, &mut subscriber_buf).await?;
        let MqttPacket::Publish(packet) = delivered else {
            panic!("expected retained publish, got {delivered:?}");
        };
        assert_eq!(packet.qos, QoS::ExactlyOnce);
        assert!(packet.retain);
        assert_eq!(packet.packet_id, Some(1));
        assert_eq!(packet.payload, Bytes::from_static(b"sticky"));

        server.shutdown();
        server.wait().await
    }

    fn connect_packet(client_id: &str) -> Vec<u8> {
        let mut packet = Vec::new();
        packet.push(0x10);
        let remaining_len = 13 + client_id.len();
        encode_remaining_len(remaining_len, &mut packet);
        packet.extend_from_slice(&[0x00, 0x04]);
        packet.extend_from_slice(b"MQTT");
        packet.extend_from_slice(&[0x05, 0x02, 0x00, 0x3c, 0x00]);
        packet.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
        packet.extend_from_slice(client_id.as_bytes());
        packet
    }

    fn encode_remaining_len(mut len: usize, dst: &mut Vec<u8>) {
        loop {
            let mut byte = (len % 128) as u8;
            len /= 128;
            if len > 0 {
                byte |= 0x80;
            }
            dst.push(byte);
            if len == 0 {
                break;
            }
        }
    }

    async fn read_connack(stream: &mut TcpStream) -> rs_netty::Result<()> {
        let mut fixed = [0; 2];
        stream.read_exact(&mut fixed).await?;
        assert_eq!(fixed[0], 0x20);
        let mut rest = vec![0; fixed[1] as usize];
        stream.read_exact(&mut rest).await?;
        assert_eq!(rest[0], 0);
        assert_eq!(rest[1], 0);
        Ok(())
    }

    fn connect(client_id: &str) -> MqttPacket {
        MqttPacket::Connect(ConnectPacket {
            clean_start: true,
            keep_alive: 60,
            properties: Vec::new(),
            client_id: client_id.to_string(),
            will: None,
            username: None,
            password: None,
        })
    }

    fn subscribe(packet_id: u16, topic_filter: &str, maximum_qos: QoS) -> MqttPacket {
        MqttPacket::Subscribe(SubscribePacket {
            packet_id,
            properties: Vec::new(),
            subscriptions: vec![Subscription {
                topic_filter: topic_filter.to_string(),
                options: SubscriptionOptions {
                    maximum_qos,
                    ..SubscriptionOptions::default()
                },
            }],
        })
    }

    fn publish(topic_name: &str, qos: QoS, packet_id: Option<u16>, payload: &str) -> MqttPacket {
        publish_with_retain(topic_name, qos, packet_id, payload, false)
    }

    fn publish_with_retain(
        topic_name: &str,
        qos: QoS,
        packet_id: Option<u16>,
        payload: &str,
        retain: bool,
    ) -> MqttPacket {
        MqttPacket::Publish(PublishPacket {
            dup: false,
            qos,
            retain,
            topic_name: topic_name.to_string(),
            packet_id,
            properties: Vec::new(),
            payload: Bytes::copy_from_slice(payload.as_bytes()),
        })
    }

    async fn write_packet(stream: &mut TcpStream, packet: MqttPacket) -> rs_netty::Result<()> {
        let mut codec = MqttCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(packet, &mut buf)?;
        stream.write_all(&buf).await?;
        Ok(())
    }

    async fn read_packet(stream: &mut TcpStream) -> rs_netty::Result<MqttPacket> {
        let mut buf = BytesMut::new();
        read_packet_with_buf(stream, &mut buf).await
    }

    async fn read_packet_with_buf(
        stream: &mut TcpStream,
        buf: &mut BytesMut,
    ) -> rs_netty::Result<MqttPacket> {
        let mut codec = MqttCodec::new();
        loop {
            if let Some(packet) = codec.decode(buf)? {
                return Ok(packet);
            }

            let mut chunk = [0; 1024];
            let read = stream.read(&mut chunk).await?;
            assert_ne!(read, 0, "connection closed before next MQTT packet");
            buf.extend_from_slice(&chunk[..read]);
        }
    }
}
