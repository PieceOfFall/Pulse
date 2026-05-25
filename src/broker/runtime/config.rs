pub(crate) const SERVER_RECEIVE_MAXIMUM: u16 = 1024;
pub(crate) const SERVER_MAXIMUM_PACKET_SIZE: u32 = 16 * 1024 * 1024;
pub(crate) const SERVER_TOPIC_ALIAS_MAXIMUM: u16 = 1024;

pub(crate) const MAX_SUBSCRIPTIONS_PER_CLIENT: usize = 1024;
pub(crate) const MAX_OFFLINE_QUEUE_LEN: usize = 1024;
pub(crate) const MAX_RETAINED_MESSAGES: usize = 1024;
pub(crate) const MAX_RETAINED_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
