pub(crate) const SERVER_RECEIVE_MAXIMUM: u16 = 1024;
pub(crate) const SERVER_MAXIMUM_PACKET_SIZE: u32 = 16 * 1024 * 1024;
pub(crate) const SERVER_TOPIC_ALIAS_MAXIMUM: u16 = 1024;

pub(crate) const MAX_SUBSCRIPTIONS_PER_CLIENT: usize = 1024;
pub(crate) const MAX_OFFLINE_QUEUE_LEN: usize = 1024;
pub(crate) const MAX_RETAINED_MESSAGES: usize = 1024;
pub(crate) const MAX_RETAINED_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
pub(crate) struct BrokerConfig {
    pub(crate) server_receive_maximum: u16,
    pub(crate) server_maximum_packet_size: u32,
    pub(crate) server_topic_alias_maximum: u16,
    pub(crate) max_subscriptions_per_client: usize,
    pub(crate) max_offline_queue_len: usize,
    pub(crate) max_retained_messages: usize,
    pub(crate) max_retained_payload_bytes: usize,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            server_receive_maximum: SERVER_RECEIVE_MAXIMUM,
            server_maximum_packet_size: SERVER_MAXIMUM_PACKET_SIZE,
            server_topic_alias_maximum: SERVER_TOPIC_ALIAS_MAXIMUM,
            max_subscriptions_per_client: MAX_SUBSCRIPTIONS_PER_CLIENT,
            max_offline_queue_len: MAX_OFFLINE_QUEUE_LEN,
            max_retained_messages: MAX_RETAINED_MESSAGES,
            max_retained_payload_bytes: MAX_RETAINED_PAYLOAD_BYTES,
        }
    }
}
