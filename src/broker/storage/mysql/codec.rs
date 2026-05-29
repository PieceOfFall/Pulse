use bytes::{Bytes, BytesMut};
use rs_netty::codec::{Decoder, Encoder, MqttCodec, MqttPacket, PublishPacket, QoS};

use crate::broker::runtime::retained_store::RetainedMessage;

pub(super) fn encode_retained(message: &RetainedMessage) -> Vec<u8> {
    let mut codec = MqttCodec::new();
    let mut buffer = BytesMut::new();
    let packet_id = if message.qos == QoS::AtMostOnce {
        None
    } else {
        Some(1)
    };
    codec
        .encode(
            MqttPacket::Publish(PublishPacket {
                dup: false,
                qos: message.qos,
                retain: true,
                topic_name: message.topic_name.clone(),
                packet_id,
                properties: message.properties.clone(),
                payload: message.payload.clone(),
            }),
            &mut buffer,
        )
        .expect("encode retained publish");
    buffer.to_vec()
}

pub(super) fn encode_publish(packet: &PublishPacket) -> Vec<u8> {
    let mut codec = MqttCodec::new();
    let mut buffer = BytesMut::new();
    let mut packet = packet.clone();
    if packet.qos != QoS::AtMostOnce && packet.packet_id.is_none() {
        packet.packet_id = Some(1);
    }
    codec
        .encode(MqttPacket::Publish(packet), &mut buffer)
        .expect("encode publish");
    buffer.to_vec()
}

pub(super) fn decode_retained(packet: &[u8]) -> Option<RetainedMessage> {
    let mut codec = MqttCodec::new();
    let mut buffer = BytesMut::from(packet);
    let packet = codec.decode(&mut buffer).ok().flatten()?;
    let MqttPacket::Publish(packet) = packet else {
        return None;
    };

    Some(RetainedMessage::new(
        packet.qos,
        packet.topic_name,
        packet.properties,
        Bytes::copy_from_slice(&packet.payload),
        None,
    ))
}

pub(super) fn decode_publish(packet: &[u8]) -> Option<PublishPacket> {
    let mut codec = MqttCodec::new();
    let mut buffer = BytesMut::from(packet);
    let packet = codec.decode(&mut buffer).ok().flatten()?;
    let MqttPacket::Publish(packet) = packet else {
        return None;
    };
    Some(packet)
}

pub(super) fn qos_to_u8(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

pub(super) fn qos_from_u8(value: u8) -> QoS {
    match value {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

pub(super) fn bool_to_u8(value: bool) -> u8 {
    if value { 1 } else { 0 }
}
