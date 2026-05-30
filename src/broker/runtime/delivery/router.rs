use rs_netty::codec::{MqttPacket, PublishPacket, QoS};

use super::{
    Delivery, DeliveryTarget,
    inflight::delivery_for_client,
    offline_queue::queue_offline_publish,
    packet::{effective_qos, fits_qos0_publish_fast, pending_publish},
};
use crate::{
    broker::runtime::{
        config::BrokerConfig,
        message::{is_message_expired, message_expires_at_ms},
        session_registry::BrokerState,
        subscription_tree::{SubscriptionEntry, select_shared_subscriptions},
        time::now_ms,
    },
    protocol,
};

pub(in crate::broker) fn deliveries_for_publish(
    state: &mut BrokerState,
    publisher_connection_id: u64,
    packet: &PublishPacket,
    config: &BrokerConfig,
) -> Vec<Delivery> {
    let now_ms = now_ms();
    let expires_at_ms = message_expires_at_ms(packet, now_ms);
    if is_message_expired(expires_at_ms, now_ms) {
        return Vec::new();
    }

    let publisher_client_id = state
        .clients_by_connection
        .get(&publisher_connection_id)
        .map(|client| client.client_id.as_str());
    let matches: Vec<SubscriptionEntry> = state
        .subscriptions
        .iter()
        .filter(|sub| {
            protocol::topic_matches(&sub.match_filter, &packet.topic_name)
                && !(sub.options.no_local && publisher_client_id == Some(sub.client_id.as_str()))
        })
        .cloned()
        .collect();
    let matches = select_shared_subscriptions(state, matches);

    matches
        .into_iter()
        .filter_map(
            |sub| match state.connection_by_client_id.get(&sub.client_id) {
                Some(connection_id) => {
                    let client = state.clients_by_connection.get(connection_id)?;
                    let target = DeliveryTarget {
                        connection_id: *connection_id,
                        channel: client.channel.clone(),
                        receive_maximum: client.receive_maximum,
                        maximum_packet_size: client.maximum_packet_size,
                    };
                    let session = state.sessions_by_client_id.get_mut(&sub.client_id)?;
                    let delivery = delivery_for_client(
                        session,
                        target,
                        packet,
                        sub.options.maximum_qos,
                        sub.options.retain_as_published && packet.retain,
                        expires_at_ms,
                        sub.subscription_identifier,
                        config.max_offline_queue_len,
                    );
                    if effective_qos(packet.qos, sub.options.maximum_qos) != QoS::AtMostOnce {
                        state.mark_outbound_changed(sub.client_id.clone());
                        state.mark_offline_changed(sub.client_id.clone());
                    }
                    delivery
                }
                None => {
                    queue_offline_publish(state, &sub, packet, config.max_offline_queue_len);
                    state.mark_offline_changed(sub.client_id.clone());
                    None
                }
            },
        )
        .collect()
}

pub(in crate::broker) fn qos0_deliveries_for_publish_readonly(
    state: &BrokerState,
    publisher_connection_id: u64,
    packet: &PublishPacket,
) -> Option<Vec<Delivery>> {
    if packet.qos != QoS::AtMostOnce || packet.retain {
        return None;
    }

    let now_ms = now_ms();
    let expires_at_ms = message_expires_at_ms(packet, now_ms);
    if is_message_expired(expires_at_ms, now_ms) {
        return Some(Vec::new());
    }

    let publisher_client_id = state
        .clients_by_connection
        .get(&publisher_connection_id)
        .map(|client| client.client_id.as_str());
    let mut deliveries = Vec::new();

    for subscription in state.subscriptions.iter().filter(|sub| {
        protocol::topic_matches(&sub.match_filter, &packet.topic_name)
            && !(sub.options.no_local && publisher_client_id == Some(sub.client_id.as_str()))
    }) {
        if subscription.shared_group.is_some() {
            return None;
        }

        let Some(connection_id) = state.connection_by_client_id.get(&subscription.client_id) else {
            continue;
        };
        let Some(client) = state.clients_by_connection.get(connection_id) else {
            continue;
        };

        let qos = effective_qos(packet.qos, subscription.options.maximum_qos);
        debug_assert_eq!(qos, QoS::AtMostOnce);
        if !fits_qos0_publish_fast(
            packet,
            subscription.subscription_identifier,
            client.maximum_packet_size,
        ) {
            continue;
        }
        let publish = pending_publish(
            packet,
            qos,
            false,
            None,
            false,
            subscription.subscription_identifier,
        );

        deliveries.push(Delivery {
            connection_id: *connection_id,
            channel: client.channel.clone(),
            packet: MqttPacket::Publish(publish).into(),
        });
    }

    Some(deliveries)
}
