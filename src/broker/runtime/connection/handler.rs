use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use rs_netty::{
    Channel, Context, Handler, Result,
    codec::{ConnAckPacket, DisconnectPacket, MqttPacket, MqttProperty, QoS, mqtt::AckPacket},
};
use tokio::task::JoinHandle;

use crate::protocol;

use super::{ConnectOptions, connack_capabilities, topic_alias::TopicAliases};
use crate::{
    broker::{
        Broker,
        runtime::{
            delivery::flush_deliveries,
            reason::{ack_packet, reason_properties},
            time::now_ms,
        },
    },
    observability::metrics,
};
use tracing::{debug, info, warn};

pub struct MqttHandler {
    broker: Broker,
    connected: bool,
    client_id: Option<String>,
    keep_alive: Option<KeepAliveHandle>,
    retransmit: Option<JoinHandle<()>>,
    topic_aliases: TopicAliases,
}

struct KeepAliveHandle {
    deadline_ms: Arc<AtomicU64>,
    interval_ms: u64,
    task: JoinHandle<()>,
}

impl MqttHandler {
    pub fn new(broker: Broker) -> Self {
        let topic_alias_maximum = broker.config().server_topic_alias_maximum;
        Self {
            broker,
            connected: false,
            client_id: None,
            keep_alive: None,
            retransmit: None,
            topic_aliases: TopicAliases::new(topic_alias_maximum),
        }
    }

    fn refresh_keep_alive(&self) {
        if let Some(keep_alive) = &self.keep_alive {
            keep_alive.deadline_ms.store(
                now_ms().saturating_add(keep_alive.interval_ms),
                Ordering::Relaxed,
            );
        }
    }

    fn start_keep_alive(
        &mut self,
        connection_id: u64,
        keep_alive_seconds: u16,
        channel: Channel<MqttPacket>,
    ) {
        self.stop_keep_alive();
        if keep_alive_seconds == 0 {
            return;
        }

        let interval_ms = u64::from(keep_alive_seconds).saturating_mul(1500);
        let deadline_ms = Arc::new(AtomicU64::new(now_ms().saturating_add(interval_ms)));
        let task_deadline = deadline_ms.clone();
        let broker = self.broker.clone();
        let task = tokio::spawn(async move {
            run_keep_alive_watchdog(broker, connection_id, channel, task_deadline).await;
        });

        self.keep_alive = Some(KeepAliveHandle {
            deadline_ms,
            interval_ms,
            task,
        });
    }

    fn stop_keep_alive(&mut self) {
        if let Some(keep_alive) = self.keep_alive.take() {
            keep_alive.task.abort();
        }
    }

    fn start_retransmit(&mut self, connection_id: u64) {
        self.stop_retransmit();
        let interval_ms = self.broker.config().inflight_retransmit_interval_ms;
        if interval_ms == 0 {
            return;
        }

        let broker = self.broker.clone();
        self.retransmit = Some(tokio::spawn(async move {
            run_inflight_retransmitter(broker, connection_id, interval_ms).await;
        }));
    }

    fn stop_retransmit(&mut self) {
        if let Some(retransmit) = self.retransmit.take() {
            retransmit.abort();
        }
    }

    async fn reject_connect(
        &mut self,
        ctx: &mut Context<MqttPacket>,
        reason_code: u8,
    ) -> Result<()> {
        if is_auth_failure(reason_code) {
            metrics::auth_failed(auth_failure_reason(reason_code));
        }
        warn!(connection_id = ctx.id(), reason_code, "rejecting connect");
        ctx.write_and_flush(MqttPacket::ConnAck(ConnAckPacket {
            session_present: false,
            reason_code,
            properties: reason_properties(reason_code),
        }))
        .await?;
        ctx.close().await
    }

    async fn disconnect(&mut self, ctx: &mut Context<MqttPacket>, reason_code: u8) -> Result<()> {
        self.disconnect_with_policy(ctx, reason_code, true).await
    }

    async fn disconnect_without_will(
        &mut self,
        ctx: &mut Context<MqttPacket>,
        reason_code: u8,
    ) -> Result<()> {
        self.disconnect_with_policy(ctx, reason_code, false).await
    }

    async fn disconnect_with_policy(
        &mut self,
        ctx: &mut Context<MqttPacket>,
        reason_code: u8,
        publish_will: bool,
    ) -> Result<()> {
        if is_auth_failure(reason_code) {
            metrics::auth_failed(auth_failure_reason(reason_code));
        }
        warn!(
            connection_id = ctx.id(),
            client_id = self.client_id.as_deref(),
            reason_code,
            "disconnecting client"
        );
        ctx.write_and_flush(MqttPacket::Disconnect(DisconnectPacket {
            reason_code,
            properties: reason_properties(reason_code),
        }))
        .await?;

        self.stop_keep_alive();
        self.stop_retransmit();
        if self.connected {
            self.connected = false;
            if let Some(outcome) = self.broker.remove_connection(ctx.id()) {
                metrics::connection_closed("broker_disconnect");
                self.client_id = Some(outcome.client_id);
                if let Some(will) = outcome.will
                    && publish_will
                {
                    self.broker.publish_will(ctx.id(), will).await;
                }
            }
        }

        ctx.close().await
    }
}

impl Handler<MqttPacket> for MqttHandler {
    type Write = MqttPacket;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: MqttPacket) -> Result<()> {
        if self.connected {
            self.refresh_keep_alive();
        }

        match msg {
            MqttPacket::Connect(packet) => {
                if self.connected {
                    return self.disconnect(ctx, protocol::PROTOCOL_ERROR).await;
                }
                if let Some(reason_code) = validate_connect(&packet) {
                    return self.reject_connect(ctx, reason_code).await;
                }
                if self.broker.is_shutting_down() {
                    return self.reject_connect(ctx, protocol::SERVER_UNAVAILABLE).await;
                }
                let authentication = match self.broker.authenticate(
                    packet.username.as_deref(),
                    packet.password.as_ref().map(|password| password.as_ref()),
                ) {
                    Ok(authentication) => authentication,
                    Err(reason_code) => return self.reject_connect(ctx, reason_code).await,
                };

                let assigned_client_id = packet.client_id.is_empty();
                let outcome = self.broker.connect(
                    ctx.id(),
                    packet.client_id,
                    ctx.channel(),
                    packet.will,
                    authentication.principal,
                    ConnectOptions::from_properties(packet.clean_start, &packet.properties),
                );
                if let Some(replaced_channel) = outcome.replaced_channel {
                    metrics::connection_closed("session_replaced");
                    let _ = replaced_channel.close().await;
                }
                self.start_keep_alive(ctx.id(), packet.keep_alive, ctx.channel());
                self.start_retransmit(ctx.id());
                self.connected = true;
                self.client_id = Some(outcome.client_id.clone());
                metrics::connection_opened();
                info!(
                    connection_id = ctx.id(),
                    client_id = outcome.client_id,
                    session_present = outcome.session_present,
                    "client connected"
                );

                let mut properties = connack_capabilities(self.broker.config());
                if assigned_client_id {
                    properties.push(MqttProperty::AssignedClientIdentifier(outcome.client_id));
                }

                ctx.write_and_flush(MqttPacket::ConnAck(ConnAckPacket {
                    session_present: outcome.session_present,
                    reason_code: protocol::SUCCESS,
                    properties,
                }))
                .await?;
                flush_deliveries(outcome.redeliveries).await;
                Ok(())
            }
            packet if !self.connected => {
                let reason = match packet {
                    MqttPacket::Auth(_) => protocol::BAD_AUTHENTICATION_METHOD,
                    _ => protocol::PROTOCOL_ERROR,
                };
                self.disconnect(ctx, reason).await
            }
            MqttPacket::PingReq => ctx.write_and_flush(MqttPacket::PingResp).await,
            MqttPacket::Disconnect(_) => {
                self.stop_keep_alive();
                self.stop_retransmit();
                if self.broker.remove_connection(ctx.id()).is_some() {
                    metrics::connection_closed("client_disconnect");
                }
                self.connected = false;
                info!(
                    connection_id = ctx.id(),
                    client_id = self.client_id.as_deref(),
                    "client disconnected"
                );
                ctx.close().await
            }
            MqttPacket::Subscribe(packet) => {
                let (suback, retained) = self.broker.subscribe(ctx.id(), packet);
                ctx.write_and_flush(MqttPacket::SubAck(suback)).await?;
                flush_deliveries(retained).await;
                Ok(())
            }
            MqttPacket::Unsubscribe(packet) => {
                let unsuback = self.broker.unsubscribe(ctx.id(), packet);
                ctx.write_and_flush(MqttPacket::UnsubAck(unsuback)).await
            }
            MqttPacket::Publish(mut packet) => {
                let started_at = Instant::now();
                metrics::publish_received(qos_name(packet.qos));
                if !self.topic_aliases.resolve_publish(&mut packet) {
                    return self.disconnect(ctx, protocol::TOPIC_ALIAS_INVALID).await;
                }
                if !protocol::is_valid_topic_name(&packet.topic_name) {
                    return self.disconnect(ctx, protocol::TOPIC_NAME_INVALID).await;
                }
                if self
                    .broker
                    .packet_exceeds_server_maximum(&MqttPacket::Publish(packet.clone()))
                {
                    return self.disconnect(ctx, protocol::PACKET_TOO_LARGE).await;
                }

                match packet.qos {
                    QoS::AtMostOnce => {
                        if !self.broker.authorize_publish(ctx.id(), &packet.topic_name) {
                            return self
                                .disconnect_without_will(ctx, protocol::NOT_AUTHORIZED)
                                .await;
                        }
                    }
                    QoS::AtLeastOnce => {
                        if let Some(packet_id) = packet.packet_id {
                            if !self.broker.authorize_publish(ctx.id(), &packet.topic_name) {
                                return ctx
                                    .write_and_flush(MqttPacket::PubAck(ack_packet(
                                        packet_id,
                                        protocol::NOT_AUTHORIZED,
                                    )))
                                    .await;
                            }
                            ctx.write_and_flush(MqttPacket::PubAck(AckPacket::new(
                                packet_id,
                                protocol::SUCCESS,
                            )))
                            .await?;
                        } else {
                            return self.disconnect(ctx, protocol::MALFORMED_PACKET).await;
                        }
                    }
                    QoS::ExactlyOnce => {
                        return if let Some(packet_id) = packet.packet_id {
                            if !self.broker.authorize_publish(ctx.id(), &packet.topic_name) {
                                ctx.write_and_flush(MqttPacket::PubRec(ack_packet(
                                    packet_id,
                                    protocol::NOT_AUTHORIZED,
                                )))
                                .await?;
                                return Ok(());
                            }
                            let reason_code =
                                self.broker.store_qos2_publish(ctx.id(), packet_id, packet);
                            ctx.write_and_flush(MqttPacket::PubRec(ack_packet(
                                packet_id,
                                reason_code,
                            )))
                            .await?;
                            Ok(())
                        } else {
                            self.disconnect(ctx, protocol::MALFORMED_PACKET).await
                        };
                    }
                }

                let deliveries = self.broker.publish(ctx.id(), &packet);
                flush_deliveries(deliveries).await;
                metrics::publish_latency(started_at.elapsed());
                Ok(())
            }
            MqttPacket::PubRel(packet) => {
                let Some(deliveries) = self
                    .broker
                    .complete_qos2_publish(ctx.id(), packet.packet_id)
                else {
                    return ctx
                        .write_and_flush(MqttPacket::PubComp(ack_packet(
                            packet.packet_id,
                            protocol::PACKET_IDENTIFIER_NOT_FOUND,
                        )))
                        .await;
                };

                flush_deliveries(deliveries).await;
                ctx.write_and_flush(MqttPacket::PubComp(ack_packet(
                    packet.packet_id,
                    protocol::SUCCESS,
                )))
                .await
            }
            MqttPacket::PubAck(packet) => {
                let Some(deliveries) = self
                    .broker
                    .complete_outbound_qos1(ctx.id(), packet.packet_id)
                else {
                    return self
                        .disconnect(ctx, protocol::PACKET_IDENTIFIER_NOT_FOUND)
                        .await;
                };
                flush_deliveries(deliveries).await;
                Ok(())
            }
            MqttPacket::PubRec(packet) => {
                if let Some(deliveries) = self
                    .broker
                    .receive_outbound_qos2(ctx.id(), packet.packet_id)
                {
                    ctx.write_and_flush(MqttPacket::PubRel(AckPacket::new(
                        packet.packet_id,
                        protocol::SUCCESS,
                    )))
                    .await?;
                    flush_deliveries(deliveries).await;
                    Ok(())
                } else {
                    self.disconnect(ctx, protocol::PACKET_IDENTIFIER_NOT_FOUND)
                        .await
                }
            }
            MqttPacket::PubComp(packet) => {
                if !self
                    .broker
                    .complete_outbound_qos2(ctx.id(), packet.packet_id)
                {
                    return self
                        .disconnect(ctx, protocol::PACKET_IDENTIFIER_NOT_FOUND)
                        .await;
                }
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

impl Drop for MqttHandler {
    fn drop(&mut self) {
        self.stop_keep_alive();
        self.stop_retransmit();
    }
}

fn validate_connect(packet: &rs_netty::codec::ConnectPacket) -> Option<u8> {
    if packet.client_id.is_empty() && !packet.clean_start {
        return Some(protocol::CLIENT_IDENTIFIER_NOT_VALID);
    }
    if packet.client_id.as_bytes().contains(&0) {
        return Some(protocol::MALFORMED_PACKET);
    }

    if let Some(will) = &packet.will {
        if !protocol::is_valid_topic_name(&will.topic) {
            return Some(protocol::TOPIC_NAME_INVALID);
        }
        if will.properties.iter().any(is_invalid_payload_format) {
            return Some(protocol::PAYLOAD_FORMAT_INVALID);
        }
    }

    let mut has_authentication_method = false;
    let mut has_authentication_data = false;
    for property in &packet.properties {
        match property {
            MqttProperty::AuthenticationMethod(_) => has_authentication_method = true,
            MqttProperty::AuthenticationData(_) => has_authentication_data = true,
            MqttProperty::ReceiveMaximum(0) | MqttProperty::MaximumPacketSize(0) => {
                return Some(protocol::MALFORMED_PACKET);
            }
            MqttProperty::RequestProblemInformation(value)
            | MqttProperty::RequestResponseInformation(value) => {
                if !matches!(*value, 0 | 1) {
                    return Some(protocol::MALFORMED_PACKET);
                }
            }
            _ => {}
        }
    }

    if has_authentication_method || has_authentication_data {
        return Some(protocol::BAD_AUTHENTICATION_METHOD);
    }

    None
}

fn is_auth_failure(reason_code: u8) -> bool {
    matches!(
        reason_code,
        protocol::BAD_USER_NAME_OR_PASSWORD | protocol::BAD_AUTHENTICATION_METHOD
    )
}

fn auth_failure_reason(reason_code: u8) -> &'static str {
    match reason_code {
        protocol::BAD_USER_NAME_OR_PASSWORD => "bad_username_or_password",
        protocol::BAD_AUTHENTICATION_METHOD => "bad_authentication_method",
        _ => "unknown",
    }
}

fn is_invalid_payload_format(property: &MqttProperty) -> bool {
    matches!(property, MqttProperty::PayloadFormatIndicator(value) if !matches!(*value, 0 | 1))
}

async fn run_keep_alive_watchdog(
    broker: Broker,
    connection_id: u64,
    channel: Channel<MqttPacket>,
    deadline_ms: Arc<AtomicU64>,
) {
    loop {
        let deadline = deadline_ms.load(Ordering::Relaxed);
        let now = now_ms();

        if now >= deadline {
            debug!(connection_id, "keep alive deadline reached");
            if let Some(outcome) = broker.remove_connection(connection_id) {
                metrics::connection_closed("keep_alive_timeout");
                if let Some(will) = outcome.will {
                    broker.publish_will(connection_id, will).await;
                }
            }
            let _ = channel.close().await;
            return;
        }

        tokio::time::sleep(Duration::from_millis(deadline - now)).await;
    }
}

async fn run_inflight_retransmitter(broker: Broker, connection_id: u64, interval_ms: u64) {
    let interval = Duration::from_millis(interval_ms);
    loop {
        tokio::time::sleep(interval).await;
        let Some(deliveries) = broker.retransmit_outbound(connection_id) else {
            return;
        };
        flush_deliveries(deliveries).await;
    }
}

fn qos_name(qos: QoS) -> &'static str {
    match qos {
        QoS::AtMostOnce => "0",
        QoS::AtLeastOnce => "1",
        QoS::ExactlyOnce => "2",
    }
}
