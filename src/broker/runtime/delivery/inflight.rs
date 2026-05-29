use rs_netty::{
    Channel,
    codec::{MqttPacket, PublishPacket, QoS, mqtt::AckPacket},
};

use super::{
    Delivery, DeliveryTarget,
    offline_queue::queue_pending_publish,
    packet::{effective_qos, fits_maximum_packet_size, pending_publish},
};
use crate::broker::runtime::{
    message::{PendingPublish, is_message_expired},
    session_registry::{BrokerState, SessionEntry},
    time::now_ms,
    write::BrokerWrite,
};

pub(in crate::broker) fn redeliveries_for_client(
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
                packet: MqttPacket::Publish(packet).into(),
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

pub(in crate::broker) fn queued_deliveries_for_client(
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

pub(in crate::broker) fn retransmissions_for_connection(
    state: &mut BrokerState,
    connection_id: u64,
) -> Option<Vec<Delivery>> {
    let client = state.clients_by_connection.get(&connection_id)?;
    let client_id = client.client_id.clone();
    let channel = client.channel.clone();
    let receive_maximum = client.receive_maximum;
    let maximum_packet_size = client.maximum_packet_size;
    let session = state.sessions_by_client_id.get_mut(&client_id)?;

    let now_ms = now_ms();
    session
        .outbound_qos1
        .retain(|_, pending| !is_message_expired(pending.expires_at_ms, now_ms));
    session
        .outbound_qos2_publish
        .retain(|_, pending| !is_message_expired(pending.expires_at_ms, now_ms));

    let mut deliveries: Vec<Delivery> = session
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
                packet: MqttPacket::Publish(packet).into(),
            })
        })
        .collect();

    deliveries.extend(
        session
            .outbound_qos2_pubrel
            .iter()
            .map(|packet_id| Delivery {
                channel: channel.clone(),
                packet: MqttPacket::PubRel(AckPacket::new(*packet_id, crate::protocol::SUCCESS))
                    .into(),
            }),
    );

    Some(deliveries)
}

pub(super) fn delivery_for_client(
    session: &mut SessionEntry,
    target: DeliveryTarget,
    packet: &PublishPacket,
    maximum_qos: QoS,
    retain: bool,
    expires_at_ms: Option<u64>,
    subscription_identifier: Option<u32>,
    max_offline_queue_len: usize,
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
            max_offline_queue_len,
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
        ))
        .into(),
    })
}

fn flush_queued_for_session(
    session: &mut SessionEntry,
    channel: Channel<BrokerWrite>,
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
            packet: MqttPacket::Publish(packet).into(),
        });
    }

    deliveries
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

fn inflight_count(session: &SessionEntry) -> usize {
    session.outbound_qos1.len() + session.outbound_qos2_publish.len()
}
