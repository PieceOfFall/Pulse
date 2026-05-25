use rs_netty::codec::{MqttProperty, PublishPacket, QoS};

use super::{
    config::BrokerConfig,
    message::{is_message_expired, message_expires_at_ms},
    session_registry::BrokerState,
    time::now_ms,
};

#[derive(Clone)]
pub(in crate::broker) struct RetainedMessage {
    pub(in crate::broker) qos: QoS,
    pub(in crate::broker) topic_name: String,
    pub(in crate::broker) properties: Vec<MqttProperty>,
    pub(in crate::broker) payload: bytes::Bytes,
    pub(in crate::broker) expires_at_ms: Option<u64>,
}

pub(in crate::broker) fn retain_publish(
    state: &mut BrokerState,
    packet: &PublishPacket,
    config: &BrokerConfig,
) {
    if !packet.retain {
        return;
    }

    let now_ms = now_ms();
    let expires_at_ms = message_expires_at_ms(packet, now_ms);
    if packet.payload.is_empty() || is_message_expired(expires_at_ms, now_ms) {
        state.retained.remove(&packet.topic_name);
    } else if can_store_retained(state, packet, config) {
        state.retained.insert(
            packet.topic_name.clone(),
            RetainedMessage {
                qos: packet.qos,
                topic_name: packet.topic_name.clone(),
                properties: packet.properties.clone(),
                payload: packet.payload.clone(),
                expires_at_ms,
            },
        );
    }
}

fn can_store_retained(state: &BrokerState, packet: &PublishPacket, config: &BrokerConfig) -> bool {
    if !state.retained.contains_key(&packet.topic_name)
        && state.retained.len() >= config.max_retained_messages
    {
        return false;
    }

    let retained_payload_bytes: usize = state
        .retained
        .iter()
        .filter(|(topic_name, _)| *topic_name != &packet.topic_name)
        .map(|(_, message)| message.payload.len())
        .sum();
    retained_payload_bytes.saturating_add(packet.payload.len()) <= config.max_retained_payload_bytes
}
