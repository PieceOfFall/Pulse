use rs_netty::codec::{MqttProperty, PublishPacket};

#[derive(Clone)]
pub(in crate::broker) struct PendingPublish {
    pub(in crate::broker) packet: PublishPacket,
    pub(in crate::broker) expires_at_ms: Option<u64>,
}

pub(in crate::broker) fn message_expires_at_ms(packet: &PublishPacket, now_ms: u64) -> Option<u64> {
    packet
        .properties
        .iter()
        .find_map(|property| match property {
            MqttProperty::MessageExpiryInterval(seconds) => {
                Some(now_ms.saturating_add(u64::from(*seconds) * 1_000))
            }
            _ => None,
        })
}

pub(in crate::broker) fn is_message_expired(expires_at_ms: Option<u64>, now_ms: u64) -> bool {
    expires_at_ms.is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
}
