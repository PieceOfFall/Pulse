use rs_netty::codec::PublishPacket;

use super::{Delivery, DeliveryTarget, inflight::delivery_for_client};
use crate::{
    broker::runtime::{
        message::is_message_expired, retained_store::RetainedMessage,
        session_registry::BrokerState, subscription_tree::SubscriptionEntry, time::now_ms,
    },
    protocol,
};

pub(in crate::broker) fn retained_for_subscription(
    state: &mut BrokerState,
    subscription: &SubscriptionEntry,
) -> Vec<Delivery> {
    let now_ms = now_ms();
    state
        .retained
        .retain(|_, message| !is_message_expired(message.expires_at_ms, now_ms));

    let retained: Vec<RetainedMessage> = state
        .retained
        .values()
        .filter(|message| protocol::topic_matches(&subscription.match_filter, &message.topic_name))
        .cloned()
        .collect();

    let Some(connection_id) = state.connection_by_client_id.get(&subscription.client_id) else {
        return Vec::new();
    };
    let Some(client) = state.clients_by_connection.get(connection_id) else {
        return Vec::new();
    };
    let target = DeliveryTarget {
        channel: client.channel.clone(),
        receive_maximum: client.receive_maximum,
        maximum_packet_size: client.maximum_packet_size,
    };
    let Some(session) = state.sessions_by_client_id.get_mut(&subscription.client_id) else {
        return Vec::new();
    };

    retained
        .into_iter()
        .filter_map(|message| {
            delivery_for_client(
                session,
                target.clone(),
                &PublishPacket {
                    dup: false,
                    qos: message.qos,
                    retain: true,
                    topic_name: message.topic_name,
                    packet_id: None,
                    properties: message.properties,
                    payload: message.payload,
                },
                subscription.options.maximum_qos,
                true,
                message.expires_at_ms,
                subscription.subscription_identifier,
            )
        })
        .collect()
}
