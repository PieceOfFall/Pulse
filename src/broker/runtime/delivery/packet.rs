use bytes::BytesMut;
use rs_netty::codec::{Encoder, MqttCodec, MqttPacket, MqttProperty, PublishPacket, QoS};

pub(super) fn pending_publish(
    packet: &PublishPacket,
    qos: QoS,
    retain: bool,
    packet_id: Option<u16>,
    dup: bool,
    subscription_identifier: Option<u32>,
) -> PublishPacket {
    let mut properties = packet.properties.clone();
    if let Some(subscription_identifier) = subscription_identifier {
        properties.push(MqttProperty::SubscriptionIdentifier(
            subscription_identifier,
        ));
    }

    PublishPacket {
        dup,
        qos,
        retain,
        topic_name: packet.topic_name.clone(),
        packet_id,
        properties,
        payload: packet.payload.clone(),
    }
}

pub(in crate::broker) fn packet_size(packet: &MqttPacket) -> Option<usize> {
    let mut codec = MqttCodec::new();
    let mut buffer = BytesMut::new();
    codec.encode(packet.clone(), &mut buffer).ok()?;
    Some(buffer.len())
}

pub(super) fn fits_maximum_packet_size(packet: &PublishPacket, maximum_packet_size: u32) -> bool {
    packet_size(&MqttPacket::Publish(packet.clone()))
        .is_some_and(|size| size <= maximum_packet_size as usize)
}

pub(super) fn effective_qos(publish_qos: QoS, maximum_qos: QoS) -> QoS {
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
