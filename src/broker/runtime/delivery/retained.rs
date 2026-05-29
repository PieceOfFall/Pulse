use rs_netty::codec::{MqttPacket, PublishPacket, QoS};

use super::{
    Delivery, DeliveryTarget,
    inflight::delivery_for_client,
    packet::{effective_qos, fits_qos0_publish_fast},
};
use crate::broker::runtime::{
    config::BrokerConfig, retained_store::RetainedMessage, session_registry::BrokerState,
    subscription_tree::SubscriptionEntry, time::now_ms,
};

pub(in crate::broker) fn retained_for_subscription(
    state: &mut BrokerState,
    subscription: &SubscriptionEntry,
    config: &BrokerConfig,
) -> Vec<Delivery> {
    let now_ms = now_ms();
    let retained = state.retained.matching(&subscription.match_filter, now_ms);

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

    let mut deliveries = Vec::new();
    let mut qos_state_changed = false;
    for message in retained {
        if let Some(delivery) = qos0_retained_delivery_fast(
            &message,
            &target,
            subscription.options.maximum_qos,
            subscription.subscription_identifier,
        ) {
            deliveries.push(delivery);
            continue;
        }

        let expires_at_ms = message.expires_at_ms;
        let packet = publish_packet(message);
        let delivery = delivery_for_client(
            session,
            target.clone(),
            &packet,
            subscription.options.maximum_qos,
            true,
            expires_at_ms,
            subscription.subscription_identifier,
            config.max_offline_queue_len,
        );
        if effective_qos(packet.qos, subscription.options.maximum_qos) != QoS::AtMostOnce {
            qos_state_changed = true;
        }
        if let Some(delivery) = delivery {
            deliveries.push(delivery);
        }
    }

    if qos_state_changed {
        state.mark_outbound_changed(subscription.client_id.clone());
        state.mark_offline_changed(subscription.client_id.clone());
    }
    deliveries
}

fn qos0_retained_delivery_fast(
    message: &RetainedMessage,
    target: &DeliveryTarget,
    maximum_qos: QoS,
    subscription_identifier: Option<u32>,
) -> Option<Delivery> {
    let qos = effective_qos(message.qos, maximum_qos);
    if qos != QoS::AtMostOnce {
        return None;
    }

    if !fits_retained_qos0_fast(message, subscription_identifier, target.maximum_packet_size) {
        return None;
    }

    let mut properties = message.properties.clone();
    if let Some(subscription_identifier) = subscription_identifier {
        properties.push(rs_netty::codec::MqttProperty::SubscriptionIdentifier(
            subscription_identifier,
        ));
    }
    Some(Delivery {
        channel: target.channel.clone(),
        packet: MqttPacket::Publish(PublishPacket {
            dup: false,
            qos,
            retain: true,
            topic_name: message.topic_name.clone(),
            packet_id: None,
            properties,
            payload: message.payload.clone(),
        }),
    })
}

fn fits_retained_qos0_fast(
    message: &RetainedMessage,
    subscription_identifier: Option<u32>,
    maximum_packet_size: u32,
) -> bool {
    if message.properties.is_empty() && subscription_identifier.is_none() {
        let remaining_len = 2usize
            .saturating_add(message.topic_name.len())
            .saturating_add(1)
            .saturating_add(message.payload.len());
        let packet_len = 1usize
            .saturating_add(varint_len(remaining_len))
            .saturating_add(remaining_len);
        return packet_len <= maximum_packet_size as usize;
    }

    let packet = PublishPacket {
        dup: false,
        qos: QoS::AtMostOnce,
        retain: true,
        topic_name: message.topic_name.clone(),
        packet_id: None,
        properties: message.properties.clone(),
        payload: message.payload.clone(),
    };
    fits_qos0_publish_fast(&packet, subscription_identifier, maximum_packet_size)
}

fn varint_len(value: usize) -> usize {
    match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}

fn publish_packet(message: RetainedMessage) -> PublishPacket {
    PublishPacket {
        dup: false,
        qos: message.qos,
        retain: true,
        topic_name: message.topic_name,
        packet_id: None,
        properties: message.properties,
        payload: message.payload,
    }
}
