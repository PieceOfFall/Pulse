use rs_netty::codec::{MqttPacket, PublishPacket, Will};

use super::{
    Delivery, deliveries_for_publish, flush_deliveries, packet_size, queued_deliveries_for_client,
};
use crate::{
    broker::{
        Broker,
        runtime::{
            message::{PendingPublish, is_message_expired, message_expires_at_ms},
            retained_store::retain_publish,
            time::now_ms,
        },
    },
    protocol,
};

impl Broker {
    pub(in crate::broker) fn publish(
        &self,
        publisher_connection_id: u64,
        packet: &PublishPacket,
    ) -> Vec<Delivery> {
        let config = *self.config();
        self.with_state(|state| {
            state.expire_sessions(now_ms());
            retain_publish(state, packet, &config);
            deliveries_for_publish(state, publisher_connection_id, packet, &config)
        })
    }

    pub(in crate::broker) fn store_qos2_publish(
        &self,
        connection_id: u64,
        packet_id: u16,
        packet: PublishPacket,
    ) -> u8 {
        let receive_maximum = self.config().server_receive_maximum;
        self.with_state(|state| {
            let key = (connection_id, packet_id);
            if state.qos2_inflight.contains_key(&key) {
                return protocol::SUCCESS;
            }

            if state
                .qos2_inflight
                .keys()
                .filter(|(conn_id, _)| *conn_id == connection_id)
                .count()
                >= usize::from(receive_maximum)
            {
                return protocol::RECEIVE_MAXIMUM_EXCEEDED;
            }

            state.qos2_inflight.insert(
                key,
                PendingPublish {
                    expires_at_ms: message_expires_at_ms(&packet, now_ms()),
                    packet,
                },
            );
            protocol::SUCCESS
        })
    }

    pub(in crate::broker) fn complete_qos2_publish(
        &self,
        connection_id: u64,
        packet_id: u16,
    ) -> Option<Vec<Delivery>> {
        let config = *self.config();
        self.with_state(|state| {
            let pending = state.qos2_inflight.remove(&(connection_id, packet_id))?;

            let now_ms = now_ms();
            state.expire_sessions(now_ms);
            if is_message_expired(pending.expires_at_ms, now_ms) {
                return Some(Vec::new());
            }
            retain_publish(state, &pending.packet, &config);

            Some(deliveries_for_publish(
                state,
                connection_id,
                &pending.packet,
                &config,
            ))
        })
    }

    pub(in crate::broker) fn complete_outbound_qos1(
        &self,
        connection_id: u64,
        packet_id: u16,
    ) -> Option<Vec<Delivery>> {
        self.with_state(|state| {
            let client_id = state
                .clients_by_connection
                .get(&connection_id)
                .map(|client| client.client_id.clone())?;
            let removed = state
                .sessions_by_client_id
                .get_mut(&client_id)
                .is_some_and(|session| session.outbound_qos1.remove(&packet_id).is_some());
            removed.then(|| queued_deliveries_for_client(state, &client_id))
        })
    }

    pub(in crate::broker) fn receive_outbound_qos2(
        &self,
        connection_id: u64,
        packet_id: u16,
    ) -> Option<Vec<Delivery>> {
        self.with_state(|state| {
            let client_id = state
                .clients_by_connection
                .get(&connection_id)
                .map(|client| client.client_id.clone())?;
            let session = state.sessions_by_client_id.get_mut(&client_id)?;

            if session.outbound_qos2_publish.remove(&packet_id).is_some() {
                session.outbound_qos2_pubrel.insert(packet_id);
                Some(queued_deliveries_for_client(state, &client_id))
            } else {
                None
            }
        })
    }

    pub(in crate::broker) fn packet_exceeds_server_maximum(&self, packet: &MqttPacket) -> bool {
        packet_size(packet)
            .is_some_and(|size| size > self.config().server_maximum_packet_size as usize)
    }

    pub(in crate::broker) fn complete_outbound_qos2(
        &self,
        connection_id: u64,
        packet_id: u16,
    ) -> bool {
        self.with_state(|state| {
            let Some(client_id) = state
                .clients_by_connection
                .get(&connection_id)
                .map(|client| client.client_id.clone())
            else {
                return false;
            };
            state
                .sessions_by_client_id
                .get_mut(&client_id)
                .is_some_and(|session| session.outbound_qos2_pubrel.remove(&packet_id))
        })
    }

    pub(in crate::broker) async fn publish_will(&self, connection_id: u64, will: Will) {
        if !protocol::is_valid_topic_name(&will.topic) {
            return;
        }

        let packet = PublishPacket {
            dup: false,
            qos: will.qos,
            retain: will.retain,
            topic_name: will.topic,
            packet_id: None,
            properties: will.properties,
            payload: will.payload,
        };
        let deliveries = self.publish(connection_id, &packet);
        flush_deliveries(deliveries).await;
    }
}
