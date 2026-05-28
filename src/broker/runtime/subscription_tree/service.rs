use rs_netty::codec::{SubAckPacket, SubscribePacket, UnsubAckPacket, UnsubscribePacket};

use super::{is_new_subscription, upsert_subscription};
use crate::{
    broker::{
        Broker,
        runtime::{
            delivery::{Delivery, retained_for_subscription},
            reason,
        },
    },
    protocol,
};

impl Broker {
    pub(in crate::broker) fn subscribe(
        &self,
        connection_id: u64,
        packet: SubscribePacket,
    ) -> (SubAckPacket, Vec<Delivery>) {
        let config = *self.config();
        self.with_state(|state| {
            let Some(client_id) = state
                .clients_by_connection
                .get(&connection_id)
                .map(|client| client.client_id.clone())
            else {
                return (
                    SubAckPacket {
                        packet_id: packet.packet_id,
                        properties: Vec::new(),
                        reason_codes: vec![protocol::UNSPECIFIED_ERROR; packet.subscriptions.len()],
                    },
                    Vec::new(),
                );
            };
            let principal = state
                .clients_by_connection
                .get(&connection_id)
                .and_then(|client| client.principal.as_deref())
                .map(str::to_string);

            let mut reason_codes = Vec::with_capacity(packet.subscriptions.len());
            let mut retained_deliveries = Vec::new();
            let current_subscription_count = state
                .subscriptions
                .iter()
                .filter(|subscription| subscription.client_id == client_id)
                .count();
            let mut inserted_count = 0;

            for subscription in packet.subscriptions {
                if !protocol::is_valid_topic_filter(&subscription.topic_filter) {
                    reason_codes.push(protocol::TOPIC_FILTER_INVALID);
                    continue;
                }
                if !self
                    .inner
                    .authenticator
                    .authorize_subscribe(principal.as_deref(), &subscription.topic_filter)
                {
                    reason_codes.push(protocol::NOT_AUTHORIZED);
                    continue;
                }
                if is_new_subscription(&state.subscriptions, &client_id, &subscription.topic_filter)
                    && current_subscription_count + inserted_count
                        >= config.max_subscriptions_per_client
                {
                    reason_codes.push(protocol::QUOTA_EXCEEDED);
                    continue;
                }

                let upsert = upsert_subscription(
                    &mut state.subscriptions,
                    &client_id,
                    subscription,
                    subscription_identifier(&packet.properties),
                );
                let stored = state.subscriptions[upsert.index].clone();
                reason_codes.push(protocol::granted_qos_code(stored.options.maximum_qos));
                if upsert.inserted {
                    inserted_count += 1;
                }

                if should_send_retained_on_subscribe(
                    stored.options.retain_handling,
                    upsert.inserted,
                ) {
                    retained_deliveries.extend(retained_for_subscription(state, &stored, &config));
                }
            }

            (
                SubAckPacket {
                    packet_id: packet.packet_id,
                    properties: reason_codes
                        .iter()
                        .find_map(|reason_code| reason::reason_string(*reason_code))
                        .into_iter()
                        .collect(),
                    reason_codes,
                },
                retained_deliveries,
            )
        })
    }

    pub(in crate::broker) fn unsubscribe(
        &self,
        connection_id: u64,
        packet: UnsubscribePacket,
    ) -> UnsubAckPacket {
        self.with_state(|state| {
            if let Some(client_id) = state
                .clients_by_connection
                .get(&connection_id)
                .map(|client| client.client_id.clone())
            {
                for filter in &packet.topic_filters {
                    state
                        .subscriptions
                        .retain(|sub| !(sub.client_id == client_id && sub.filter == *filter));
                }
            }

            UnsubAckPacket {
                packet_id: packet.packet_id,
                properties: Vec::new(),
                reason_codes: vec![protocol::SUCCESS; packet.topic_filters.len()],
            }
        })
    }
}

fn should_send_retained_on_subscribe(retain_handling: u8, inserted: bool) -> bool {
    match retain_handling {
        1 => inserted,
        2 => false,
        _ => true,
    }
}

fn subscription_identifier(properties: &[rs_netty::codec::MqttProperty]) -> Option<u32> {
    properties.iter().find_map(|property| match property {
        rs_netty::codec::MqttProperty::SubscriptionIdentifier(value) => Some(*value),
        _ => None,
    })
}
