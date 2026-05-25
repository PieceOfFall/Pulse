use rs_netty::{
    Channel,
    codec::{MqttPacket, PublishPacket, QoS},
};

use crate::protocol;

use super::state::{BrokerState, RetainedMessage, SessionEntry, SubscriptionEntry};

#[derive(Clone)]
pub(super) struct Delivery {
    channel: Channel<MqttPacket>,
    packet: MqttPacket,
}

pub(super) fn deliveries_for_publish(
    state: &mut BrokerState,
    publisher_connection_id: u64,
    packet: &PublishPacket,
) -> Vec<Delivery> {
    let publisher_client_id = state
        .clients_by_connection
        .get(&publisher_connection_id)
        .map(|client| client.client_id.as_str());
    let matches: Vec<SubscriptionEntry> = state
        .subscriptions
        .iter()
        .filter(|sub| {
            protocol::topic_matches(&sub.filter, &packet.topic_name)
                && !(sub.options.no_local && publisher_client_id == Some(sub.client_id.as_str()))
        })
        .cloned()
        .collect();

    matches
        .into_iter()
        .filter_map(|sub| {
            let connection_id = state.connection_by_client_id.get(&sub.client_id)?;
            let channel = state
                .clients_by_connection
                .get(connection_id)?
                .channel
                .clone();
            let session = state.sessions_by_client_id.get_mut(&sub.client_id)?;
            Some(delivery_for_client(
                session,
                channel,
                packet,
                sub.options.maximum_qos,
                sub.options.retain_as_published && packet.retain,
            ))
        })
        .collect()
}

pub(super) fn retained_for_subscription(
    state: &mut BrokerState,
    subscription: &SubscriptionEntry,
) -> Vec<Delivery> {
    let retained: Vec<RetainedMessage> = state
        .retained
        .values()
        .filter(|message| protocol::topic_matches(&subscription.filter, &message.topic_name))
        .cloned()
        .collect();

    let Some(connection_id) = state.connection_by_client_id.get(&subscription.client_id) else {
        return Vec::new();
    };
    let Some(channel) = state
        .clients_by_connection
        .get(connection_id)
        .map(|client| client.channel.clone())
    else {
        return Vec::new();
    };
    let Some(session) = state.sessions_by_client_id.get_mut(&subscription.client_id) else {
        return Vec::new();
    };

    retained
        .into_iter()
        .map(|message| {
            delivery_for_client(
                session,
                channel.clone(),
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
            )
        })
        .collect()
}

pub(super) fn redeliveries_for_client(state: &mut BrokerState, client_id: &str) -> Vec<Delivery> {
    let Some(connection_id) = state.connection_by_client_id.get(client_id) else {
        return Vec::new();
    };
    let Some(channel) = state
        .clients_by_connection
        .get(connection_id)
        .map(|client| client.channel.clone())
    else {
        return Vec::new();
    };
    let Some(session) = state.sessions_by_client_id.get(client_id) else {
        return Vec::new();
    };

    session
        .outbound_qos1
        .iter()
        .chain(session.outbound_qos2_publish.iter())
        .map(|(packet_id, packet)| {
            let mut packet = packet.clone();
            packet.dup = true;
            packet.packet_id = Some(*packet_id);
            Delivery {
                channel: channel.clone(),
                packet: MqttPacket::Publish(packet),
            }
        })
        .collect()
}

fn delivery_for_client(
    session: &mut SessionEntry,
    channel: Channel<MqttPacket>,
    packet: &PublishPacket,
    maximum_qos: QoS,
    retain: bool,
) -> Delivery {
    let qos = effective_qos(packet.qos, maximum_qos);
    let packet_id = match qos {
        QoS::AtMostOnce => None,
        QoS::AtLeastOnce => {
            let packet_id = next_packet_id(session);
            session.outbound_qos1.insert(
                packet_id,
                pending_publish(packet, qos, retain, Some(packet_id), false),
            );
            Some(packet_id)
        }
        QoS::ExactlyOnce => {
            let packet_id = next_packet_id(session);
            session.outbound_qos2_publish.insert(
                packet_id,
                pending_publish(packet, qos, retain, Some(packet_id), false),
            );
            Some(packet_id)
        }
    };

    Delivery {
        channel,
        packet: MqttPacket::Publish(pending_publish(packet, qos, retain, packet_id, false)),
    }
}

fn pending_publish(
    packet: &PublishPacket,
    qos: QoS,
    retain: bool,
    packet_id: Option<u16>,
    dup: bool,
) -> PublishPacket {
    PublishPacket {
        dup,
        qos,
        retain,
        topic_name: packet.topic_name.clone(),
        packet_id,
        properties: packet.properties.clone(),
        payload: packet.payload.clone(),
    }
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

pub(super) async fn flush_deliveries(deliveries: Vec<Delivery>) {
    for delivery in deliveries {
        let _ = delivery.channel.write(delivery.packet).await;
    }
}
