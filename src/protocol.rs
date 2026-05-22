use rs_netty::codec::QoS;

pub const SUCCESS: u8 = 0x00;
pub const QOS_0_GRANTED: u8 = 0x00;
pub const QOS_1_GRANTED: u8 = 0x01;
pub const QOS_2_GRANTED: u8 = 0x02;
pub const UNSPECIFIED_ERROR: u8 = 0x80;
pub const MALFORMED_PACKET: u8 = 0x81;
pub const PROTOCOL_ERROR: u8 = 0x82;
pub const BAD_AUTHENTICATION_METHOD: u8 = 0x8c;
pub const TOPIC_FILTER_INVALID: u8 = 0x8f;
pub const TOPIC_NAME_INVALID: u8 = 0x90;

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

    let mut filter_levels = filter.split('/').peekable();
    let mut topic_levels = topic.split('/').peekable();

    while let Some(filter_level) = filter_levels.next() {
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
        assert!(!topic_matches(
            "sensors/+/temperature",
            "sensors/a/humidity"
        ));
        assert!(!topic_matches("devices/+", "devices/a/state"));
    }
}
