use rs_netty::{
    Context, Handler, Result,
    codec::{ConnAckPacket, DisconnectPacket, MqttPacket, MqttProperty, QoS, mqtt::AckPacket},
};

use crate::protocol;

use super::{Broker, delivery::flush_deliveries};

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

    async fn reject_connect(
        &mut self,
        ctx: &mut Context<MqttPacket>,
        reason_code: u8,
    ) -> Result<()> {
        ctx.write(MqttPacket::ConnAck(ConnAckPacket {
            session_present: false,
            reason_code,
            properties: Vec::new(),
        }))
        .await?;
        ctx.close().await
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
                if let Some(reason_code) = validate_connect(&packet) {
                    return self.reject_connect(ctx, reason_code).await;
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
                        return if let Some(packet_id) = packet.packet_id {
                            self.broker.store_qos2_publish(ctx.id(), packet_id, packet);
                            ctx.write(MqttPacket::PubRec(AckPacket::new(
                                packet_id,
                                protocol::SUCCESS,
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
    if packet.username.is_some() || packet.password.is_some() {
        return Some(protocol::BAD_USER_NAME_OR_PASSWORD);
    }

    None
}

fn is_invalid_payload_format(property: &MqttProperty) -> bool {
    matches!(property, MqttProperty::PayloadFormatIndicator(value) if !matches!(*value, 0 | 1))
}
