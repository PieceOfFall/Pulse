use rs_netty::codec::QoS;

pub const SUCCESS: u8 = 0x00;
pub const QOS_0_GRANTED: u8 = 0x00;
pub const QOS_1_GRANTED: u8 = 0x01;
pub const QOS_2_GRANTED: u8 = 0x02;
pub const UNSPECIFIED_ERROR: u8 = 0x80;
pub const MALFORMED_PACKET: u8 = 0x81;
pub const PROTOCOL_ERROR: u8 = 0x82;
pub const CLIENT_IDENTIFIER_NOT_VALID: u8 = 0x85;
pub const BAD_USER_NAME_OR_PASSWORD: u8 = 0x86;
pub const BAD_AUTHENTICATION_METHOD: u8 = 0x8c;
pub const TOPIC_FILTER_INVALID: u8 = 0x8f;
pub const TOPIC_NAME_INVALID: u8 = 0x90;
pub const PACKET_IDENTIFIER_IN_USE: u8 = 0x91;
pub const PACKET_IDENTIFIER_NOT_FOUND: u8 = 0x92;
pub const RECEIVE_MAXIMUM_EXCEEDED: u8 = 0x93;
pub const PACKET_TOO_LARGE: u8 = 0x95;
pub const PAYLOAD_FORMAT_INVALID: u8 = 0x99;

pub const SERVER_RECEIVE_MAXIMUM: u16 = 1024;
pub const SERVER_MAXIMUM_PACKET_SIZE: u32 = 16 * 1024 * 1024;

pub fn granted_qos_code(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => QOS_0_GRANTED,
        QoS::AtLeastOnce => QOS_1_GRANTED,
        QoS::ExactlyOnce => QOS_2_GRANTED,
    }
}

pub fn is_valid_topic_name(topic: &str) -> bool {
    !topic.is_empty() && !topic.contains('+') && !topic.contains('#')
}

pub fn is_valid_topic_filter(filter: &str) -> bool {
    if filter.is_empty() {
        return false;
    }

    let levels: Vec<&str> = filter.split('/').collect();
    for (index, level) in levels.iter().enumerate() {
        if level.contains('#') && (*level != "#" || index + 1 != levels.len()) {
            return false;
        }
        if level.contains('+') && *level != "+" {
            return false;
        }
    }

    true
}

pub fn topic_matches(filter: &str, topic: &str) -> bool {
    if !is_valid_topic_filter(filter) || !is_valid_topic_name(topic) {
        return false;
    }

    if topic.starts_with('$') && !filter.starts_with('$') {
        return false;
    }

    let filter_levels = filter.split('/');
    let mut topic_levels = topic.split('/').peekable();

    for filter_level in filter_levels {
        match filter_level {
            "#" => return true,
            "+" => {
                if topic_levels.next().is_none() {
                    return false;
                }
            }
            literal => {
                if topic_levels.next() != Some(literal) {
                    return false;
                }
            }
        }
    }

    topic_levels.next().is_none()
}

#[cfg(test)]
mod tests {
    use super::{is_valid_topic_filter, topic_matches};

    #[test]
    fn validates_topic_filters() {
        assert!(is_valid_topic_filter("sensors/+/temperature"));
        assert!(is_valid_topic_filter("devices/#"));
        assert!(is_valid_topic_filter("#"));
        assert!(!is_valid_topic_filter("devices/#/state"));
        assert!(!is_valid_topic_filter("devices/foo#"));
        assert!(!is_valid_topic_filter("devices/+foo"));
    }

    #[test]
    fn matches_topic_filters() {
        assert!(topic_matches(
            "sensors/+/temperature",
            "sensors/a/temperature"
        ));
        assert!(topic_matches("devices/#", "devices/a/state"));
        assert!(topic_matches("devices/#", "devices"));
        assert!(topic_matches("$SYS/#", "$SYS/broker/uptime"));
        assert!(!topic_matches("#", "$SYS/broker/uptime"));
        assert!(!topic_matches("+/broker/uptime", "$SYS/broker/uptime"));
        assert!(!topic_matches(
            "sensors/+/temperature",
            "sensors/a/humidity"
        ));
        assert!(!topic_matches("devices/+", "devices/a/state"));
    }
}
