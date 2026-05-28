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

pub(super) fn fits_qos0_publish_fast(
    packet: &PublishPacket,
    subscription_identifier: Option<u32>,
    maximum_packet_size: u32,
) -> bool {
    if packet.properties.is_empty() && subscription_identifier.is_none() {
        let remaining_len = 2usize
            .saturating_add(packet.topic_name.len())
            .saturating_add(1)
            .saturating_add(packet.payload.len());
        let packet_len = 1usize
            .saturating_add(varint_len(remaining_len))
            .saturating_add(remaining_len);
        return packet_len <= maximum_packet_size as usize;
    }

    let publish = pending_publish(
        packet,
        QoS::AtMostOnce,
        false,
        None,
        false,
        subscription_identifier,
    );
    fits_maximum_packet_size(&publish, maximum_packet_size)
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

fn varint_len(value: usize) -> usize {
    match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rs_netty::codec::{PublishPacket, QoS};

    use super::{fits_maximum_packet_size, fits_qos0_publish_fast};

    #[test]
    fn qos0_fast_size_check_matches_codec_for_plain_publish() {
        let packet = PublishPacket {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: false,
            topic_name: "devices/a".to_string(),
            packet_id: None,
            properties: Vec::new(),
            payload: Bytes::from_static(b"hello"),
        };

        assert_eq!(
            fits_qos0_publish_fast(&packet, None, 32),
            fits_maximum_packet_size(&packet, 32)
        );
        assert_eq!(
            fits_qos0_publish_fast(&packet, None, 8),
            fits_maximum_packet_size(&packet, 8)
        );
    }
}
