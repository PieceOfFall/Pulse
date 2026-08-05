use rs_netty::codec::{PublishPacket, QoS};
use tracing::warn;

use super::packet::{effective_qos, pending_publish};
use crate::broker::runtime::{
    message::{PendingPublish, is_message_expired, message_expires_at_ms},
    session_registry::{BrokerState, QueuedPublish, SessionEntry},
    subscription_tree::SubscriptionEntry,
    time::now_ms,
};

pub(super) fn queue_offline_publish(
    state: &mut BrokerState,
    subscription: &SubscriptionEntry,
    packet: &PublishPacket,
    max_offline_queue_len: usize,
) -> bool {
    let Some(session) = state.sessions_by_client_id.get_mut(&subscription.client_id) else {
        return false;
    };
    if session.session_expiry_interval == 0 {
        return false;
    }

    let qos = effective_qos(packet.qos, subscription.options.maximum_qos);
    if qos == QoS::AtMostOnce {
        return false;
    }

    let now_ms = now_ms();
    let expires_at_ms = message_expires_at_ms(packet, now_ms);
    if is_message_expired(expires_at_ms, now_ms) {
        return false;
    }

    queue_pending_publish(
        session,
        packet,
        qos,
        subscription.options.retain_as_published && packet.retain,
        expires_at_ms,
        subscription.subscription_identifier,
        max_offline_queue_len,
    )
}

pub(super) fn queue_pending_publish(
    session: &mut SessionEntry,
    packet: &PublishPacket,
    qos: QoS,
    retain: bool,
    expires_at_ms: Option<u64>,
    subscription_identifier: Option<u32>,
    max_offline_queue_len: usize,
) -> bool {
    if session.offline_queue.len() >= max_offline_queue_len {
        return false;
    }

    if session.next_offline_sequence >= i64::MAX as u64 {
        warn!(
            next_offline_sequence = session.next_offline_sequence,
            "offline queue sequence exhausted; dropping publish"
        );
        return false;
    }

    let sequence = session.next_offline_sequence;
    session.next_offline_sequence += 1;
    session.offline_queue.push_back(QueuedPublish {
        sequence,
        pending: PendingPublish {
            packet: pending_publish(packet, qos, retain, None, false, subscription_identifier),
            expires_at_ms,
        },
    });
    true
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rs_netty::codec::{PublishPacket, QoS};

    use super::queue_pending_publish;
    use crate::broker::runtime::session_registry::SessionEntry;

    fn packet() -> PublishPacket {
        PublishPacket {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic_name: "devices/a".to_string(),
            packet_id: None,
            properties: Vec::new(),
            payload: Bytes::from_static(b"value"),
        }
    }

    #[test]
    fn queued_publishes_receive_stable_monotonic_sequences() {
        let mut session = SessionEntry::connected(60);

        assert!(queue_pending_publish(
            &mut session,
            &packet(),
            QoS::AtLeastOnce,
            false,
            None,
            None,
            10,
        ));
        assert!(queue_pending_publish(
            &mut session,
            &packet(),
            QoS::AtLeastOnce,
            false,
            None,
            None,
            10,
        ));

        assert_eq!(
            session
                .offline_queue
                .iter()
                .map(|queued| queued.sequence)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(session.next_offline_sequence, 2);
    }

    #[test]
    fn queue_rejects_sequence_at_sql_integer_limit() {
        let mut session = SessionEntry::connected(60);
        session.next_offline_sequence = i64::MAX as u64;

        assert!(!queue_pending_publish(
            &mut session,
            &packet(),
            QoS::AtLeastOnce,
            false,
            None,
            None,
            10,
        ));
        assert!(session.offline_queue.is_empty());
        assert_eq!(session.next_offline_sequence, i64::MAX as u64);
    }
}
