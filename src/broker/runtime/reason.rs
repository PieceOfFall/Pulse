use rs_netty::codec::{MqttProperty, mqtt::AckPacket};

use crate::protocol;

pub(in crate::broker) fn reason_properties(reason_code: u8) -> Vec<MqttProperty> {
    reason_string(reason_code).into_iter().collect()
}

pub(in crate::broker) fn ack_packet(packet_id: u16, reason_code: u8) -> AckPacket {
    AckPacket {
        packet_id,
        reason_code,
        properties: reason_properties(reason_code),
    }
}

pub(in crate::broker) fn reason_string(reason_code: u8) -> Option<MqttProperty> {
    let reason = match reason_code {
        protocol::MALFORMED_PACKET => "malformed packet",
        protocol::PROTOCOL_ERROR => "protocol error",
        protocol::CLIENT_IDENTIFIER_NOT_VALID => "client identifier not valid",
        protocol::BAD_USER_NAME_OR_PASSWORD => "bad user name or password",
        protocol::NOT_AUTHORIZED => "not authorized",
        protocol::SERVER_UNAVAILABLE => "server unavailable",
        protocol::SERVER_SHUTTING_DOWN => "server shutting down",
        protocol::BAD_AUTHENTICATION_METHOD => "bad authentication method",
        protocol::TOPIC_FILTER_INVALID => "topic filter invalid",
        protocol::TOPIC_NAME_INVALID => "topic name invalid",
        protocol::PACKET_IDENTIFIER_IN_USE => "packet identifier in use",
        protocol::PACKET_IDENTIFIER_NOT_FOUND => "packet identifier not found",
        protocol::RECEIVE_MAXIMUM_EXCEEDED => "receive maximum exceeded",
        protocol::TOPIC_ALIAS_INVALID => "topic alias invalid",
        protocol::PACKET_TOO_LARGE => "packet too large",
        protocol::QUOTA_EXCEEDED => "quota exceeded",
        protocol::PAYLOAD_FORMAT_INVALID => "payload format invalid",
        _ => return None,
    };
    Some(MqttProperty::ReasonString(reason.to_string()))
}
