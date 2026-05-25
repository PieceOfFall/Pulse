use bytes::BytesMut;
use rs_netty::{
    Channel,
    codec::{Encoder, MqttCodec, MqttPacket, MqttProperty, PublishPacket, QoS},
};

use crate::protocol;

use super::state::{
    BrokerState, MAX_OFFLINE_QUEUE_LEN, PendingPublish, RetainedMessage, SessionEntry,
    SubscriptionEntry, is_message_expired, message_expires_at_ms, now_ms,
};

#[derive(Clone)]
pub(super) struct Delivery {
    channel: Channel<MqttPacket>,
    packet: MqttPacket,
}

#[derive(Clone)]
struct DeliveryTarget {
    channel: Channel<MqttPacket>,
    receive_maximum: u16,
    maximum_packet_size: u32,
}

pub(super) fn deliveries_for_publish(
    state: &mut BrokerState,
    publisher_connection_id: u64,
    packet: &PublishPacket,
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
                        channel: client.channel.clone(),
                        receive_maximum: client.receive_maximum,
                        maximum_packet_size: client.maximum_packet_size,
                    };
                    let session = state.sessions_by_client_id.get_mut(&sub.client_id)?;
                    delivery_for_client(
                        session,
                        target,
                        packet,
                        sub.options.maximum_qos,
                        sub.options.retain_as_published && packet.retain,
                        expires_at_ms,
                        sub.subscription_identifier,
                    )
                }
                None => {
                    queue_offline_publish(state, &sub, packet);
                    None
                }
            },
        )
        .collect()
}

pub(super) fn retained_for_subscription(
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

pub(super) fn redeliveries_for_client(state: &mut BrokerState, client_id: &str) -> Vec<Delivery> {
    let Some(connection_id) = state.connection_by_client_id.get(client_id) else {
        return Vec::new();
    };
    let Some(client) = state.clients_by_connection.get(connection_id) else {
        return Vec::new();
    };
    let channel = client.channel.clone();
    let receive_maximum = client.receive_maximum;
    let maximum_packet_size = client.maximum_packet_size;
    let Some(session) = state.sessions_by_client_id.get_mut(client_id) else {
        return Vec::new();
    };

    let now_ms = now_ms();
    session
        .outbound_qos1
        .retain(|_, pending| !is_message_expired(pending.expires_at_ms, now_ms));
    session
        .outbound_qos2_publish
        .retain(|_, pending| !is_message_expired(pending.expires_at_ms, now_ms));

    let mut redeliveries: Vec<Delivery> = session
        .outbound_qos1
        .iter()
        .chain(session.outbound_qos2_publish.iter())
        .take(usize::from(receive_maximum))
        .filter_map(|(packet_id, pending)| {
            let mut packet = pending.packet.clone();
            packet.dup = true;
            packet.packet_id = Some(*packet_id);
            fits_maximum_packet_size(&packet, maximum_packet_size).then(|| Delivery {
                channel: channel.clone(),
                packet: MqttPacket::Publish(packet),
            })
        })
        .collect();

    redeliveries.extend(flush_queued_for_session(
        session,
        channel,
        receive_maximum,
        maximum_packet_size,
    ));

    redeliveries
}

pub(super) fn queued_deliveries_for_client(
    state: &mut BrokerState,
    client_id: &str,
) -> Vec<Delivery> {
    let Some(connection_id) = state.connection_by_client_id.get(client_id) else {
        return Vec::new();
    };
    let Some(client) = state.clients_by_connection.get(connection_id) else {
        return Vec::new();
    };
    let channel = client.channel.clone();
    let receive_maximum = client.receive_maximum;
    let maximum_packet_size = client.maximum_packet_size;
    let Some(session) = state.sessions_by_client_id.get_mut(client_id) else {
        return Vec::new();
    };

    flush_queued_for_session(session, channel, receive_maximum, maximum_packet_size)
}

fn delivery_for_client(
    session: &mut SessionEntry,
    target: DeliveryTarget,
    packet: &PublishPacket,
    maximum_qos: QoS,
    retain: bool,
    expires_at_ms: Option<u64>,
    subscription_identifier: Option<u32>,
) -> Option<Delivery> {
    let now_ms = now_ms();
    if is_message_expired(expires_at_ms, now_ms) {
        return None;
    }

    let qos = effective_qos(packet.qos, maximum_qos);
    if qos != QoS::AtMostOnce && inflight_count(session) >= usize::from(target.receive_maximum) {
        queue_pending_publish(
            session,
            packet,
            qos,
            retain,
            expires_at_ms,
            subscription_identifier,
        );
        return None;
    }

    if qos == QoS::AtMostOnce
        && !fits_maximum_packet_size(
            &pending_publish(packet, qos, retain, None, false, subscription_identifier),
            target.maximum_packet_size,
        )
    {
        return None;
    }

    let packet_id = match qos {
        QoS::AtMostOnce => None,
        QoS::AtLeastOnce => {
            let packet_id = next_packet_id(session);
            let publish = pending_publish(
                packet,
                qos,
                retain,
                Some(packet_id),
                false,
                subscription_identifier,
            );
            if !fits_maximum_packet_size(&publish, target.maximum_packet_size) {
                return None;
            }
            session.outbound_qos1.insert(
                packet_id,
                PendingPublish {
                    packet: publish,
                    expires_at_ms,
                },
            );
            Some(packet_id)
        }
        QoS::ExactlyOnce => {
            let packet_id = next_packet_id(session);
            let publish = pending_publish(
                packet,
                qos,
                retain,
                Some(packet_id),
                false,
                subscription_identifier,
            );
            if !fits_maximum_packet_size(&publish, target.maximum_packet_size) {
                return None;
            }
            session.outbound_qos2_publish.insert(
                packet_id,
                PendingPublish {
                    packet: publish,
                    expires_at_ms,
                },
            );
            Some(packet_id)
        }
    };

    Some(Delivery {
        channel: target.channel,
        packet: MqttPacket::Publish(pending_publish(
            packet,
            qos,
            retain,
            packet_id,
            false,
            subscription_identifier,
        )),
    })
}

fn select_shared_subscriptions(
    state: &mut BrokerState,
    subscriptions: Vec<SubscriptionEntry>,
) -> Vec<SubscriptionEntry> {
    let mut selected = Vec::new();
    let mut shared: std::collections::HashMap<String, Vec<SubscriptionEntry>> =
        std::collections::HashMap::new();

    for subscription in subscriptions {
        if let Some(group) = &subscription.shared_group {
            shared
                .entry(format!("{group}/{}", subscription.match_filter))
                .or_default()
                .push(subscription);
        } else {
            selected.push(subscription);
        }
    }

    for (key, mut group) in shared {
        group.retain(|subscription| {
            state
                .connection_by_client_id
                .contains_key(&subscription.client_id)
        });
        if group.is_empty() {
            continue;
        }
        group.sort_by(|left, right| left.client_id.cmp(&right.client_id));
        let cursor = state.shared_subscription_cursors.entry(key).or_default();
        let index = *cursor % group.len();
        *cursor = cursor.wrapping_add(1);
        selected.push(group.swap_remove(index));
    }

    selected
}

fn pending_publish(
    packet: &PublishPacket,
    qos: QoS,
    retain: bool,
    packet_id: Option<u16>,
    dup: bool,
    subscription_identifier: Option<u32>,
) -> PublishPacket {
    let mut properties = packet.properties.clone();
    if let Some(subscription_identifier) = subscription_identifier {
        properties.push(MqttProperty::SubscriptionIdentifier(
            subscription_identifier,
        ));
    }

    PublishPacket {
        dup,
        qos,
        retain,
        topic_name: packet.topic_name.clone(),
        packet_id,
        properties,
        payload: packet.payload.clone(),
    }
}

fn queue_offline_publish(
    state: &mut BrokerState,
    subscription: &SubscriptionEntry,
    packet: &PublishPacket,
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
    );
}

fn effective_qos(publish_qos: QoS, maximum_qos: QoS) -> QoS {
    if qos_rank(publish_qos) <= qos_rank(maximum_qos) {
        publish_qos
    } else {
        maximum_qos
    }
}

fn qos_rank(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

fn next_packet_id(session: &mut SessionEntry) -> u16 {
    loop {
        let packet_id = session.next_packet_id;
        session.next_packet_id = if session.next_packet_id == u16::MAX {
            1
        } else {
            session.next_packet_id + 1
        };

        if !session.outbound_qos1.contains_key(&packet_id)
            && !session.outbound_qos2_publish.contains_key(&packet_id)
            && !session.outbound_qos2_pubrel.contains(&packet_id)
        {
            return packet_id;
        }
    }
}

fn flush_queued_for_session(
    session: &mut SessionEntry,
    channel: Channel<MqttPacket>,
    receive_maximum: u16,
    maximum_packet_size: u32,
) -> Vec<Delivery> {
    let mut deliveries = Vec::new();
    let now_ms = now_ms();
    while inflight_count(session) < usize::from(receive_maximum) {
        let Some(pending) = session.offline_queue.pop_front() else {
            break;
        };
        if is_message_expired(pending.expires_at_ms, now_ms) {
            continue;
        }

        let qos = pending.packet.qos;
        let packet_id = match qos {
            QoS::AtMostOnce => None,
            QoS::AtLeastOnce => {
                let packet_id = next_packet_id(session);
                let mut publish = pending.packet.clone();
                publish.packet_id = Some(packet_id);
                if !fits_maximum_packet_size(&publish, maximum_packet_size) {
                    continue;
                }
                session.outbound_qos1.insert(
                    packet_id,
                    PendingPublish {
                        packet: publish,
                        expires_at_ms: pending.expires_at_ms,
                    },
                );
                Some(packet_id)
            }
            QoS::ExactlyOnce => {
                let packet_id = next_packet_id(session);
                let mut publish = pending.packet.clone();
                publish.packet_id = Some(packet_id);
                if !fits_maximum_packet_size(&publish, maximum_packet_size) {
                    continue;
                }
                session.outbound_qos2_publish.insert(
                    packet_id,
                    PendingPublish {
                        packet: publish,
                        expires_at_ms: pending.expires_at_ms,
                    },
                );
                Some(packet_id)
            }
        };

        let mut packet = pending.packet;
        packet.packet_id = packet_id;
        if !fits_maximum_packet_size(&packet, maximum_packet_size) {
            continue;
        }
        deliveries.push(Delivery {
            channel: channel.clone(),
            packet: MqttPacket::Publish(packet),
        });
    }

    deliveries
}

fn queue_pending_publish(
    session: &mut SessionEntry,
    packet: &PublishPacket,
    qos: QoS,
    retain: bool,
    expires_at_ms: Option<u64>,
    subscription_identifier: Option<u32>,
) {
    if session.offline_queue.len() >= MAX_OFFLINE_QUEUE_LEN {
        return;
    }

    session.offline_queue.push_back(PendingPublish {
        packet: pending_publish(packet, qos, retain, None, false, subscription_identifier),
        expires_at_ms,
    });
}

fn inflight_count(session: &SessionEntry) -> usize {
    session.outbound_qos1.len() + session.outbound_qos2_publish.len()
}

pub(super) fn packet_size(packet: &MqttPacket) -> Option<usize> {
    let mut codec = MqttCodec::new();
    let mut buffer = BytesMut::new();
    codec.encode(packet.clone(), &mut buffer).ok()?;
    Some(buffer.len())
}

fn fits_maximum_packet_size(packet: &PublishPacket, maximum_packet_size: u32) -> bool {
    packet_size(&MqttPacket::Publish(packet.clone()))
        .is_some_and(|size| size <= maximum_packet_size as usize)
}

pub(super) async fn flush_deliveries(deliveries: Vec<Delivery>) {
    for delivery in deliveries {
        let _ = delivery.channel.write(delivery.packet).await;
    }
}
