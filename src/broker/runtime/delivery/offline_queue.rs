use rs_netty::codec::{PublishPacket, QoS};

use super::packet::{effective_qos, pending_publish};
use crate::broker::runtime::{
    message::{PendingPublish, is_message_expired, message_expires_at_ms},
    session_registry::{BrokerState, SessionEntry},
    subscription_tree::SubscriptionEntry,
    time::now_ms,
};

pub(super) fn queue_offline_publish(
    state: &mut BrokerState,
    subscription: &SubscriptionEntry,
    packet: &PublishPacket,
    max_offline_queue_len: usize,
) {
    let Some(session) = state.sessions_by_client_id.get_mut(&subscription.client_id) else {
        return;
    };
    if session.session_expiry_interval == 0 {
        return;
    }

    let qos = effective_qos(packet.qos, subscription.options.maximum_qos);
    if qos == QoS::AtMostOnce {
        return;
    }

    let now_ms = now_ms();
    let expires_at_ms = message_expires_at_ms(packet, now_ms);
    if is_message_expired(expires_at_ms, now_ms) {
        return;
    }

    queue_pending_publish(
        session,
        packet,
        qos,
        subscription.options.retain_as_published && packet.retain,
        expires_at_ms,
        subscription.subscription_identifier,
        max_offline_queue_len,
    );
}

pub(super) fn queue_pending_publish(
    session: &mut SessionEntry,
    packet: &PublishPacket,
    qos: QoS,
    retain: bool,
    expires_at_ms: Option<u64>,
    subscription_identifier: Option<u32>,
    max_offline_queue_len: usize,
) {
    if session.offline_queue.len() >= max_offline_queue_len {
        return;
    }

    session.offline_queue.push_back(PendingPublish {
        packet: pending_publish(packet, qos, retain, None, false, subscription_identifier),
        expires_at_ms,
    });
}
