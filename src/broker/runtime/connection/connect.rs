use rs_netty::codec::MqttProperty;

use crate::broker::runtime::config::{
    SERVER_MAXIMUM_PACKET_SIZE, SERVER_RECEIVE_MAXIMUM, SERVER_TOPIC_ALIAS_MAXIMUM,
};

pub(in crate::broker) struct ConnectOptions {
    pub(in crate::broker) clean_start: bool,
    pub(in crate::broker) session_expiry_interval: u32,
    pub(in crate::broker) receive_maximum: u16,
    pub(in crate::broker) maximum_packet_size: u32,
}

impl ConnectOptions {
    pub(in crate::broker) fn from_properties(
        clean_start: bool,
        properties: &[MqttProperty],
    ) -> Self {
        Self {
            clean_start,
            session_expiry_interval: session_expiry_interval(properties),
            receive_maximum: receive_maximum(properties),
            maximum_packet_size: maximum_packet_size(properties),
        }
    }
}

pub(in crate::broker) fn connack_capabilities() -> Vec<MqttProperty> {
    vec![
        MqttProperty::ReceiveMaximum(SERVER_RECEIVE_MAXIMUM),
        MqttProperty::MaximumPacketSize(SERVER_MAXIMUM_PACKET_SIZE),
        MqttProperty::TopicAliasMaximum(SERVER_TOPIC_ALIAS_MAXIMUM),
        MqttProperty::MaximumQoS(2),
        MqttProperty::RetainAvailable(1),
        MqttProperty::WildcardSubscriptionAvailable(1),
        MqttProperty::SubscriptionIdentifierAvailable(1),
        MqttProperty::SharedSubscriptionAvailable(1),
    ]
}

fn session_expiry_interval(properties: &[MqttProperty]) -> u32 {
    properties
        .iter()
        .find_map(|property| match property {
            MqttProperty::SessionExpiryInterval(value) => Some(*value),
            _ => None,
        })
        .unwrap_or(0)
}

fn receive_maximum(properties: &[MqttProperty]) -> u16 {
    properties
        .iter()
        .find_map(|property| match property {
            MqttProperty::ReceiveMaximum(value) => Some(*value),
            _ => None,
        })
        .unwrap_or(u16::MAX)
}

fn maximum_packet_size(properties: &[MqttProperty]) -> u32 {
    properties
        .iter()
        .find_map(|property| match property {
            MqttProperty::MaximumPacketSize(value) => Some(*value),
            _ => None,
        })
        .unwrap_or(u32::MAX)
}
