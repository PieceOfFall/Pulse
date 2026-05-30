use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use rs_netty::{
    Context, Handler, Result,
    codec::{
        ConnAckPacket, DisconnectPacket, MqttPacket, MqttProperty, QoS, SubAckPacket,
        mqtt::AckPacket,
    },
};
use tokio::task::JoinHandle;

use crate::protocol;

use super::{ConnectOptions, ConnectionIdMap, connack_capabilities, topic_alias::TopicAliases};
use crate::broker::runtime::write::{BrokerWrite, preencoded_single_suback};
use crate::{
    broker::{
        Broker,
        runtime::{
            delivery::{flush_deliveries, flush_deliveries_to_context},
            reason::{ack_packet, reason_properties},
            time::now_ms,
        },
    },
    observability::metrics,
};
use tracing::{info, warn};

pub struct MqttHandler {
    broker: Broker,
    connection_ids: ConnectionIdMap,
    connected: bool,
    client_id: Option<String>,
    keep_alive: Option<KeepAliveState>,
    retransmit: Option<JoinHandle<()>>,
    topic_aliases: TopicAliases,
}

struct KeepAliveState {
    deadline_ms: Arc<AtomicU64>,
    interval_ms: u64,
}

impl MqttHandler {
    #[allow(dead_code)]
    pub fn new(broker: Broker) -> Self {
        Self::with_connection_ids(broker, ConnectionIdMap::default())
    }

    pub(crate) fn with_connection_ids(broker: Broker, connection_ids: ConnectionIdMap) -> Self {
        let topic_alias_maximum = broker.config().server_topic_alias_maximum;
        Self {
            broker,
            connection_ids,
            connected: false,
            client_id: None,
            keep_alive: None,
            retransmit: None,
            topic_aliases: TopicAliases::new(topic_alias_maximum),
        }
    }

    fn broker_connection_id(&self, local_connection_id: u64) -> u64 {
        self.connection_ids.broker_id(local_connection_id)
    }

    fn refresh_keep_alive(&self) {
        if let Some(keep_alive) = &self.keep_alive {
            keep_alive.deadline_ms.store(
                now_ms().saturating_add(keep_alive.interval_ms),
                Ordering::Relaxed,
            );
        }
    }

    fn start_keep_alive(&mut self, deadline_ms: Arc<AtomicU64>, keep_alive_seconds: u16) {
        self.stop_keep_alive();
        if keep_alive_seconds == 0 {
            return;
        }

        let interval_ms = u64::from(keep_alive_seconds).saturating_mul(1500);
        deadline_ms.store(now_ms().saturating_add(interval_ms), Ordering::Relaxed);
        self.keep_alive = Some(KeepAliveState {
            deadline_ms,
            interval_ms,
        });
        self.broker.ensure_keep_alive_monitor();
    }

    fn stop_keep_alive(&mut self) {
        if let Some(keep_alive) = self.keep_alive.take() {
            keep_alive.deadline_ms.store(0, Ordering::Relaxed);
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
        ctx: &mut Context<BrokerWrite>,
        reason_code: u8,
    ) -> Result<()> {
        if is_auth_failure(reason_code) {
            metrics::auth_failed(auth_failure_reason(reason_code));
        }
        warn!(connection_id = ctx.id(), reason_code, "rejecting connect");
        ctx.write_and_flush(
            MqttPacket::ConnAck(ConnAckPacket {
                session_present: false,
                reason_code,
                properties: reason_properties(reason_code),
            })
            .into(),
        )
        .await?;
        ctx.close().await
    }

    async fn disconnect(&mut self, ctx: &mut Context<BrokerWrite>, reason_code: u8) -> Result<()> {
        self.disconnect_with_policy(ctx, reason_code, true).await
    }

    async fn disconnect_without_will(
        &mut self,
        ctx: &mut Context<BrokerWrite>,
        reason_code: u8,
    ) -> Result<()> {
        self.disconnect_with_policy(ctx, reason_code, false).await
    }

    async fn disconnect_with_policy(
        &mut self,
        ctx: &mut Context<BrokerWrite>,
        reason_code: u8,
        publish_will: bool,
    ) -> Result<()> {
        if is_auth_failure(reason_code) {
            metrics::auth_failed(auth_failure_reason(reason_code));
        }
        let connection_id = if self.connected {
            self.broker_connection_id(ctx.id())
        } else {
            ctx.id()
        };
        warn!(
            connection_id,
            client_id = self.client_id.as_deref(),
            reason_code,
            "disconnecting client"
        );
        ctx.write_and_flush(
            MqttPacket::Disconnect(DisconnectPacket {
                reason_code,
                properties: reason_properties(reason_code),
            })
            .into(),
        )
        .await?;

        self.stop_keep_alive();
        self.stop_retransmit();
        if self.connected {
            self.connected = false;
            if let Some(outcome) = self.broker.remove_connection(connection_id) {
                metrics::connection_closed("broker_disconnect");
                self.client_id = Some(outcome.client_id);
                if let Some(will) = outcome.will
                    && publish_will
                {
                    self.broker.publish_will(connection_id, will).await;
                }
            }
            self.connection_ids.remove(ctx.id());
        }

        ctx.close().await
    }
}

impl Handler<MqttPacket> for MqttHandler {
    type Write = BrokerWrite;

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
                let connection_id = self.broker_connection_id(ctx.id());
                let outcome = self.broker.connect(
                    connection_id,
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
                self.start_keep_alive(outcome.keep_alive_deadline_ms.clone(), packet.keep_alive);
                self.start_retransmit(connection_id);
                self.connected = true;
                self.client_id = Some(outcome.client_id.clone());
                metrics::connection_opened();
                info!(
                    connection_id,
                    client_id = outcome.client_id,
                    session_present = outcome.session_present,
                    "client connected"
                );

                let mut properties = connack_capabilities(self.broker.config());
                if assigned_client_id {
                    properties.push(MqttProperty::AssignedClientIdentifier(outcome.client_id));
                }

                ctx.write_and_flush(
                    MqttPacket::ConnAck(ConnAckPacket {
                        session_present: outcome.session_present,
                        reason_code: protocol::SUCCESS,
                        properties,
                    })
                    .into(),
                )
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
            MqttPacket::PingReq => ctx.write_and_flush(MqttPacket::PingResp.into()).await,
            MqttPacket::Disconnect(_) => {
                self.stop_keep_alive();
                self.stop_retransmit();
                let connection_id = self.broker_connection_id(ctx.id());
                if self.broker.remove_connection(connection_id).is_some() {
                    metrics::connection_closed("client_disconnect");
                }
                self.connection_ids.remove(ctx.id());
                self.connected = false;
                info!(
                    connection_id,
                    client_id = self.client_id.as_deref(),
                    "client disconnected"
                );
                ctx.close().await
            }
            MqttPacket::Subscribe(packet) => {
                let connection_id = self.broker_connection_id(ctx.id());
                let (suback, retained) = self.broker.subscribe(connection_id, packet);
                if retained.is_empty() {
                    ctx.write_and_flush(suback_write(suback)).await?;
                } else {
                    ctx.write(suback_write(suback)).await?;
                    flush_deliveries_to_context(connection_id, ctx, retained).await?;
                }
                Ok(())
            }
            MqttPacket::Unsubscribe(packet) => {
                let connection_id = self.broker_connection_id(ctx.id());
                let unsuback = self.broker.unsubscribe(connection_id, packet);
                ctx.write_and_flush(MqttPacket::UnsubAck(unsuback).into())
                    .await
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

                let connection_id = self.broker_connection_id(ctx.id());
                match packet.qos {
                    QoS::AtMostOnce => {
                        if let Some(result) = self
                            .broker
                            .publish_qos0_fast_authorized(connection_id, &packet)
                        {
                            match result {
                                Ok(deliveries) => {
                                    flush_deliveries(deliveries).await;
                                    metrics::publish_latency(started_at.elapsed());
                                    return Ok(());
                                }
                                Err(reason_code) => {
                                    return self.disconnect_without_will(ctx, reason_code).await;
                                }
                            }
                        }
                        if !self
                            .broker
                            .authorize_publish(connection_id, &packet.topic_name)
                        {
                            return self
                                .disconnect_without_will(ctx, protocol::NOT_AUTHORIZED)
                                .await;
                        }
                    }
                    QoS::AtLeastOnce => {
                        if let Some(packet_id) = packet.packet_id {
                            if !self
                                .broker
                                .authorize_publish(connection_id, &packet.topic_name)
                            {
                                return ctx
                                    .write_and_flush(
                                        MqttPacket::PubAck(ack_packet(
                                            packet_id,
                                            protocol::NOT_AUTHORIZED,
                                        ))
                                        .into(),
                                    )
                                    .await;
                            }
                            ctx.write_and_flush(
                                MqttPacket::PubAck(AckPacket::new(packet_id, protocol::SUCCESS))
                                    .into(),
                            )
                            .await?;
                        } else {
                            return self.disconnect(ctx, protocol::MALFORMED_PACKET).await;
                        }
                    }
                    QoS::ExactlyOnce => {
                        return if let Some(packet_id) = packet.packet_id {
                            if !self
                                .broker
                                .authorize_publish(connection_id, &packet.topic_name)
                            {
                                ctx.write_and_flush(
                                    MqttPacket::PubRec(ack_packet(
                                        packet_id,
                                        protocol::NOT_AUTHORIZED,
                                    ))
                                    .into(),
                                )
                                .await?;
                                return Ok(());
                            }
                            let reason_code =
                                self.broker
                                    .store_qos2_publish(connection_id, packet_id, packet);
                            ctx.write_and_flush(
                                MqttPacket::PubRec(ack_packet(packet_id, reason_code)).into(),
                            )
                            .await?;
                            Ok(())
                        } else {
                            self.disconnect(ctx, protocol::MALFORMED_PACKET).await
                        };
                    }
                }

                let deliveries = self.broker.publish(connection_id, &packet);
                flush_deliveries(deliveries).await;
                metrics::publish_latency(started_at.elapsed());
                Ok(())
            }
            MqttPacket::PubRel(packet) => {
                let connection_id = self.broker_connection_id(ctx.id());
                let Some(deliveries) = self
                    .broker
                    .complete_qos2_publish(connection_id, packet.packet_id)
                else {
                    return ctx
                        .write_and_flush(
                            MqttPacket::PubComp(ack_packet(
                                packet.packet_id,
                                protocol::PACKET_IDENTIFIER_NOT_FOUND,
                            ))
                            .into(),
                        )
                        .await;
                };

                flush_deliveries(deliveries).await;
                ctx.write_and_flush(
                    MqttPacket::PubComp(ack_packet(packet.packet_id, protocol::SUCCESS)).into(),
                )
                .await
            }
            MqttPacket::PubAck(packet) => {
                let connection_id = self.broker_connection_id(ctx.id());
                let Some(deliveries) = self
                    .broker
                    .complete_outbound_qos1(connection_id, packet.packet_id)
                else {
                    return self
                        .disconnect(ctx, protocol::PACKET_IDENTIFIER_NOT_FOUND)
                        .await;
                };
                flush_deliveries(deliveries).await;
                Ok(())
            }
            MqttPacket::PubRec(packet) => {
                let connection_id = self.broker_connection_id(ctx.id());
                if let Some(deliveries) = self
                    .broker
                    .receive_outbound_qos2(connection_id, packet.packet_id)
                {
                    ctx.write_and_flush(
                        MqttPacket::PubRel(AckPacket::new(packet.packet_id, protocol::SUCCESS))
                            .into(),
                    )
                    .await?;
                    flush_deliveries(deliveries).await;
                    Ok(())
                } else {
                    self.disconnect(ctx, protocol::PACKET_IDENTIFIER_NOT_FOUND)
                        .await
                }
            }
            MqttPacket::PubComp(packet) => {
                let connection_id = self.broker_connection_id(ctx.id());
                if !self
                    .broker
                    .complete_outbound_qos2(connection_id, packet.packet_id)
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

fn suback_write(packet: SubAckPacket) -> BrokerWrite {
    if packet.properties.is_empty() && packet.reason_codes.len() == 1 {
        preencoded_single_suback(packet.packet_id, packet.reason_codes[0])
    } else {
        MqttPacket::SubAck(packet).into()
    }
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
