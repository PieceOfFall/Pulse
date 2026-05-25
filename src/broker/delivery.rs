use rs_netty::{
    Channel,
    codec::{MqttPacket, PublishPacket, QoS},
};

use crate::protocol;

use super::{BrokerState, ClientEntry, RetainedMessage, SubscriptionEntry};

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
    let matches: Vec<SubscriptionEntry> = state
        .subscriptions
        .iter()
        .filter(|sub| {
            protocol::topic_matches(&sub.filter, &packet.topic_name)
                && !(sub.options.no_local && sub.connection_id == publisher_connection_id)
        })
        .cloned()
        .collect();

    matches
        .into_iter()
        .filter_map(|sub| {
            let client = state.clients_by_connection.get_mut(&sub.connection_id)?;
            Some(delivery_for_client(
                client,
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

    let Some(client) = state
        .clients_by_connection
        .get_mut(&subscription.connection_id)
    else {
        return Vec::new();
    };

    retained
        .into_iter()
        .map(|message| {
            delivery_for_client(
                client,
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

fn delivery_for_client(
    client: &mut ClientEntry,
    packet: &PublishPacket,
    maximum_qos: QoS,
    retain: bool,
) -> Delivery {
    let qos = effective_qos(packet.qos, maximum_qos);
    let packet_id = match qos {
        QoS::AtMostOnce => None,
        QoS::AtLeastOnce => {
            let packet_id = next_packet_id(client);
            client.outbound_qos1.insert(packet_id);
            Some(packet_id)
        }
        QoS::ExactlyOnce => {
            let packet_id = next_packet_id(client);
            client.outbound_qos2_publish.insert(packet_id);
            Some(packet_id)
        }
    };

    Delivery {
        channel: client.channel.clone(),
        packet: MqttPacket::Publish(PublishPacket {
            dup: false,
            qos,
            retain,
            topic_name: packet.topic_name.clone(),
            packet_id,
            properties: packet.properties.clone(),
            payload: packet.payload.clone(),
        }),
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

fn next_packet_id(client: &mut ClientEntry) -> u16 {
    loop {
        let packet_id = client.next_packet_id;
        client.next_packet_id = if client.next_packet_id == u16::MAX {
            1
        } else {
            client.next_packet_id + 1
        };

        if !client.outbound_qos1.contains(&packet_id)
            && !client.outbound_qos2_publish.contains(&packet_id)
            && !client.outbound_qos2_pubrel.contains(&packet_id)
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
