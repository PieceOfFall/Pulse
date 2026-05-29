use std::{
    cmp::{Ordering, Reverse},
    collections::{BTreeMap, BinaryHeap, HashMap},
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use rs_netty::codec::{MqttProperty, PublishPacket, QoS};

use super::{
    config::BrokerConfig,
    message::{is_message_expired, message_expires_at_ms},
    session_registry::BrokerState,
    time::now_ms,
};
#[cfg(test)]
use crate::protocol;

#[derive(Clone, Debug, PartialEq)]
pub(in crate::broker) struct RetainedMessage {
    pub(in crate::broker) qos: QoS,
    pub(in crate::broker) topic_name: String,
    pub(in crate::broker) properties: Vec<MqttProperty>,
    pub(in crate::broker) payload: Bytes,
    pub(in crate::broker) expires_at_ms: Option<u64>,
    pub(in crate::broker) preencoded_qos0: Option<Bytes>,
}

impl RetainedMessage {
    pub(in crate::broker) fn new(
        qos: QoS,
        topic_name: String,
        properties: Vec<MqttProperty>,
        payload: Bytes,
        expires_at_ms: Option<u64>,
    ) -> Self {
        let preencoded_qos0 = if qos == QoS::AtMostOnce && properties.is_empty() {
            encode_plain_qos0_retained(&topic_name, &payload)
        } else {
            None
        };

        Self {
            qos,
            topic_name,
            properties,
            payload,
            expires_at_ms,
            preencoded_qos0,
        }
    }
}

fn encode_plain_qos0_retained(topic_name: &str, payload: &Bytes) -> Option<Bytes> {
    if topic_name.len() > u16::MAX as usize {
        return None;
    }

    let remaining_len = 2usize
        .checked_add(topic_name.len())?
        .checked_add(1)?
        .checked_add(payload.len())?;
    if remaining_len > 268_435_455 {
        return None;
    }

    let mut buffer =
        BytesMut::with_capacity(1 + remaining_len_varint_len(remaining_len) + remaining_len);
    buffer.extend_from_slice(&[0x31]);
    write_remaining_len(remaining_len, &mut buffer);
    buffer.extend_from_slice(&(topic_name.len() as u16).to_be_bytes());
    buffer.extend_from_slice(topic_name.as_bytes());
    buffer.extend_from_slice(&[0]);
    buffer.extend_from_slice(payload);
    Some(buffer.freeze())
}

fn write_remaining_len(mut value: usize, buffer: &mut BytesMut) {
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        buffer.extend_from_slice(&[byte]);
        if value == 0 {
            break;
        }
    }
}

fn remaining_len_varint_len(value: usize) -> usize {
    match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}

#[derive(Default)]
pub(in crate::broker) struct RetainedStore {
    messages: HashMap<String, Arc<RetainedMessage>>,
    payload_bytes: usize,
    trie: RetainedTrieNode,
    expirations: BinaryHeap<Reverse<ExpiryEntry>>,
}

pub(in crate::broker) enum RetainedMatches {
    Empty,
    One(Arc<RetainedMessage>),
    Many(Vec<Arc<RetainedMessage>>),
}

impl RetainedMatches {
    fn many(messages: Vec<Arc<RetainedMessage>>) -> Self {
        if messages.is_empty() {
            Self::Empty
        } else {
            Self::Many(messages)
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::One(_) => 1,
            Self::Many(messages) => messages.len(),
        }
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        matches!(self, Self::Empty)
    }
}

pub(in crate::broker) enum RetainedMatchesIntoIter {
    Empty,
    One(Option<Arc<RetainedMessage>>),
    Many(std::vec::IntoIter<Arc<RetainedMessage>>),
}

impl Iterator for RetainedMatchesIntoIter {
    type Item = Arc<RetainedMessage>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Empty => None,
            Self::One(message) => message.take(),
            Self::Many(messages) => messages.next(),
        }
    }
}

impl IntoIterator for RetainedMatches {
    type Item = Arc<RetainedMessage>;
    type IntoIter = RetainedMatchesIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::Empty => RetainedMatchesIntoIter::Empty,
            Self::One(message) => RetainedMatchesIntoIter::One(Some(message)),
            Self::Many(messages) => RetainedMatchesIntoIter::Many(messages.into_iter()),
        }
    }
}

impl RetainedStore {
    pub(in crate::broker) fn insert(
        &mut self,
        topic_name: String,
        message: RetainedMessage,
    ) -> Option<RetainedMessage> {
        debug_assert_eq!(topic_name, message.topic_name);
        let previous = self.messages.insert(topic_name.clone(), Arc::new(message));
        if let Some(previous) = &previous {
            self.payload_bytes = self.payload_bytes.saturating_sub(previous.payload.len());
        } else {
            self.trie.insert(&topic_name);
        }

        let message = self
            .messages
            .get(&topic_name)
            .expect("retained message just inserted");
        self.payload_bytes = self.payload_bytes.saturating_add(message.payload.len());
        if let Some(expires_at_ms) = message.expires_at_ms {
            self.expirations.push(Reverse(ExpiryEntry {
                expires_at_ms,
                topic_name,
            }));
        }
        previous.map(|message| (*message).clone())
    }

    pub(in crate::broker) fn remove(&mut self, topic_name: &str) -> Option<RetainedMessage> {
        let removed = self.messages.remove(topic_name)?;
        self.payload_bytes = self.payload_bytes.saturating_sub(removed.payload.len());
        self.trie.remove(topic_name);
        Some((*removed).clone())
    }

    pub(in crate::broker) fn get(&self, topic_name: &str) -> Option<&RetainedMessage> {
        self.messages.get(topic_name).map(Arc::as_ref)
    }

    pub(in crate::broker) fn contains_key(&self, topic_name: &str) -> bool {
        self.messages.contains_key(topic_name)
    }

    pub(in crate::broker) fn len(&self) -> usize {
        self.messages.len()
    }

    pub(in crate::broker) fn payload_bytes(&self) -> usize {
        self.payload_bytes
    }

    pub(in crate::broker) fn iter(&self) -> impl Iterator<Item = (&String, &RetainedMessage)> {
        self.messages
            .iter()
            .map(|(topic_name, message)| (topic_name, message.as_ref()))
    }

    pub(in crate::broker) fn expire(&mut self, now_ms: u64) -> bool {
        let mut removed_any = false;
        while let Some(Reverse(expiry)) = self.expirations.peek() {
            if expiry.expires_at_ms > now_ms {
                break;
            }

            let expiry = self.expirations.pop().expect("peeked expiry").0;
            if self
                .messages
                .get(&expiry.topic_name)
                .is_some_and(|message| message.expires_at_ms == Some(expiry.expires_at_ms))
            {
                self.remove(&expiry.topic_name);
                removed_any = true;
            }
        }
        removed_any
    }

    #[cfg(test)]
    pub(in crate::broker) fn matching(&mut self, filter: &str, now_ms: u64) -> RetainedMatches {
        self.expire(now_ms);
        let filter = protocol::shared_subscription_filter(filter).unwrap_or(filter);
        if !protocol::is_valid_topic_filter(filter) {
            return RetainedMatches::Empty;
        }

        self.matching_valid_filter_after_expire(filter)
    }

    pub(in crate::broker) fn matching_valid_filter(
        &mut self,
        filter: &str,
        now_ms: u64,
    ) -> RetainedMatches {
        self.expire(now_ms);
        self.matching_valid_filter_after_expire(filter)
    }

    fn matching_valid_filter_after_expire(&self, filter: &str) -> RetainedMatches {
        if !filter.contains('+') && !filter.contains('#') {
            return self
                .messages
                .get(filter)
                .cloned()
                .map(RetainedMatches::One)
                .unwrap_or(RetainedMatches::Empty);
        }

        let mut topic_names = Vec::new();
        let levels: Vec<&str> = filter.split('/').collect();
        self.trie.collect_matches(&levels, 0, &mut topic_names);

        let filter_matches_system = filter.starts_with('$');
        let messages = topic_names
            .into_iter()
            .filter(|topic_name| filter_matches_system || !topic_name.starts_with('$'))
            .filter_map(|topic_name| self.messages.get(&topic_name).cloned())
            .collect();
        RetainedMatches::many(messages)
    }
}

#[derive(Default)]
struct RetainedTrieNode {
    literals: BTreeMap<String, RetainedTrieNode>,
    retained_topic: Option<String>,
}

impl RetainedTrieNode {
    fn insert(&mut self, topic_name: &str) {
        let mut node = self;
        for level in topic_name.split('/') {
            node = node.literals.entry(level.to_string()).or_default();
        }
        node.retained_topic = Some(topic_name.to_string());
    }

    fn remove(&mut self, topic_name: &str) {
        let levels: Vec<&str> = topic_name.split('/').collect();
        self.remove_levels(&levels);
    }

    fn remove_levels(&mut self, levels: &[&str]) -> bool {
        let Some((level, rest)) = levels.split_first() else {
            self.retained_topic = None;
            return self.literals.is_empty();
        };

        if let Some(child) = self.literals.get_mut(*level)
            && child.remove_levels(rest)
        {
            self.literals.remove(*level);
        }

        self.retained_topic.is_none() && self.literals.is_empty()
    }

    fn collect_matches(&self, filter_levels: &[&str], index: usize, matches: &mut Vec<String>) {
        let Some(filter_level) = filter_levels.get(index) else {
            if let Some(topic_name) = &self.retained_topic {
                matches.push(topic_name.clone());
            }
            return;
        };

        match *filter_level {
            "#" if index + 1 == filter_levels.len() => self.collect_all(matches),
            "+" => {
                for child in self.literals.values() {
                    child.collect_matches(filter_levels, index + 1, matches);
                }
            }
            literal => {
                if let Some(child) = self.literals.get(literal) {
                    child.collect_matches(filter_levels, index + 1, matches);
                }
            }
        }
    }

    fn collect_all(&self, matches: &mut Vec<String>) {
        if let Some(topic_name) = &self.retained_topic {
            matches.push(topic_name.clone());
        }
        for child in self.literals.values() {
            child.collect_all(matches);
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
struct ExpiryEntry {
    expires_at_ms: u64,
    topic_name: String,
}

impl Ord for ExpiryEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.expires_at_ms
            .cmp(&other.expires_at_ms)
            .then_with(|| self.topic_name.cmp(&other.topic_name))
    }
}

impl PartialOrd for ExpiryEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
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
        if state.retained.remove(&packet.topic_name).is_some() {
            state.mark_retained_changed();
        }
    } else if can_store_retained(&state.retained, packet, config) {
        state.retained.insert(
            packet.topic_name.clone(),
            RetainedMessage::new(
                packet.qos,
                packet.topic_name.clone(),
                packet.properties.clone(),
                packet.payload.clone(),
                expires_at_ms,
            ),
        );
        state.mark_retained_changed();
    }
}

fn can_store_retained(
    retained: &RetainedStore,
    packet: &PublishPacket,
    config: &BrokerConfig,
) -> bool {
    if !retained.contains_key(&packet.topic_name) && retained.len() >= config.max_retained_messages
    {
        return false;
    }

    let current_payload_bytes = retained
        .get(&packet.topic_name)
        .map_or(0, |message| message.payload.len());
    let retained_payload_bytes = retained
        .payload_bytes()
        .saturating_sub(current_payload_bytes);
    retained_payload_bytes.saturating_add(packet.payload.len()) <= config.max_retained_payload_bytes
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rs_netty::codec::{PublishPacket, QoS};

    use super::{RetainedMessage, RetainedStore, retain_publish};
    use crate::broker::runtime::{config::BrokerConfig, session_registry::BrokerState};

    fn message(topic_name: &str, payload: &'static [u8]) -> RetainedMessage {
        RetainedMessage::new(
            QoS::AtMostOnce,
            topic_name.to_string(),
            Vec::new(),
            Bytes::from_static(payload),
            None,
        )
    }

    #[test]
    fn plain_qos0_message_precomputes_retained_publish_bytes() {
        let message = message("devices/a", b"hello");
        assert_eq!(
            message.preencoded_qos0.as_deref(),
            Some(&b"\x31\x11\x00\x09devices/a\x00hello"[..])
        );
    }

    #[test]
    fn retained_message_with_properties_skips_plain_preencoding() {
        let message = RetainedMessage::new(
            QoS::AtMostOnce,
            "devices/a".to_string(),
            vec![rs_netty::codec::MqttProperty::MessageExpiryInterval(60)],
            Bytes::from_static(b"hello"),
            Some(60_000),
        );

        assert!(message.preencoded_qos0.is_none());
    }

    #[test]
    fn matches_exact_plus_hash_and_system_filters() {
        let mut store = RetainedStore::default();
        store.insert(
            "devices/a/temp".to_string(),
            message("devices/a/temp", b"a"),
        );
        store.insert(
            "devices/b/humidity".to_string(),
            message("devices/b/humidity", b"b"),
        );
        store.insert(
            "$SYS/broker/uptime".to_string(),
            message("$SYS/broker/uptime", b"sys"),
        );

        assert_eq!(store.matching("devices/a/temp", 0).len(), 1);
        assert_eq!(store.matching("devices/+/temp", 0).len(), 1);
        assert_eq!(store.matching("devices/#", 0).len(), 2);
        assert_eq!(store.matching("#", 0).len(), 2);
        assert_eq!(store.matching("$SYS/#", 0).len(), 1);
    }

    #[test]
    fn expires_messages_lazily() {
        let mut store = RetainedStore::default();
        let mut retained = message("devices/expiring", b"x");
        retained.expires_at_ms = Some(10);
        store.insert("devices/expiring".to_string(), retained);

        assert_eq!(store.matching("devices/#", 9).len(), 1);
        assert!(store.matching("devices/#", 10).is_empty());
        assert_eq!(store.len(), 0);
        assert_eq!(store.payload_bytes(), 0);
    }

    #[test]
    fn retain_publish_empty_payload_deletes_existing_message() {
        let mut state = BrokerState::default();
        let config = BrokerConfig::default();
        state.retained.insert(
            "devices/delete".to_string(),
            message("devices/delete", b"x"),
        );

        retain_publish(
            &mut state,
            &PublishPacket {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: true,
                topic_name: "devices/delete".to_string(),
                packet_id: None,
                properties: Vec::new(),
                payload: Bytes::new(),
            },
            &config,
        );

        assert!(state.retained.get("devices/delete").is_none());
    }

    #[test]
    fn enforces_message_and_payload_limits() {
        let mut state = BrokerState::default();
        let mut config = BrokerConfig::default();
        config.max_retained_messages = 1;
        config.max_retained_payload_bytes = 4;

        retain_publish(
            &mut state,
            &PublishPacket {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: true,
                topic_name: "devices/one".to_string(),
                packet_id: None,
                properties: Vec::new(),
                payload: Bytes::from_static(b"1234"),
            },
            &config,
        );
        retain_publish(
            &mut state,
            &PublishPacket {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: true,
                topic_name: "devices/two".to_string(),
                packet_id: None,
                properties: Vec::new(),
                payload: Bytes::from_static(b"x"),
            },
            &config,
        );
        retain_publish(
            &mut state,
            &PublishPacket {
                dup: false,
                qos: QoS::AtMostOnce,
                retain: true,
                topic_name: "devices/one".to_string(),
                packet_id: None,
                properties: Vec::new(),
                payload: Bytes::from_static(b"12345"),
            },
            &config,
        );

        assert_eq!(state.retained.len(), 1);
        assert_eq!(
            state.retained.get("devices/one").expect("retained").payload,
            Bytes::from_static(b"1234")
        );
    }
}
