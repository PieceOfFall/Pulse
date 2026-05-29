use rs_netty::codec::{SubAckPacket, SubscribePacket, UnsubAckPacket, UnsubscribePacket};

use super::upsert_subscription_at;
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
            let Some(client) = state.clients_by_connection.get(&connection_id) else {
                return (
                    SubAckPacket {
                        packet_id: packet.packet_id,
                        properties: Vec::new(),
                        reason_codes: vec![protocol::UNSPECIFIED_ERROR; packet.subscriptions.len()],
                    },
                    Vec::new(),
                );
            };
            let client_id = client.client_id.clone();
            let principal = client.principal.clone();
            let persistent_session = client.persistent_session;
            let mut subscription_count = client.subscription_count;

            let mut reason_codes = Vec::with_capacity(packet.subscriptions.len());
            let mut retained_deliveries = Vec::new();
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
                let known_subscription_count = subscription_count + inserted_count;
                let existing_index = if known_subscription_count == 0 {
                    None
                } else {
                    subscription_position(
                        &state.subscriptions,
                        &client_id,
                        &subscription.topic_filter,
                    )
                };
                if existing_index.is_none()
                    && known_subscription_count >= config.max_subscriptions_per_client
                {
                    reason_codes.push(protocol::QUOTA_EXCEEDED);
                    continue;
                }

                let upsert = upsert_subscription_at(
                    &mut state.subscriptions,
                    &client_id,
                    subscription,
                    subscription_identifier(&packet.properties),
                    existing_index,
                );
                let stored = state.subscriptions[upsert.index].clone();
                if persistent_session {
                    state.mark_subscriptions_changed();
                }
                reason_codes.push(protocol::granted_qos_code(stored.options.maximum_qos));
                if upsert.inserted {
                    inserted_count += 1;
                }

                if should_send_retained_on_subscribe(
                    stored.options.retain_handling,
                    upsert.inserted,
                ) {
                    let retained = retained_for_subscription(state, &stored, &config);
                    if retained_deliveries.is_empty() {
                        retained_deliveries = retained;
                    } else {
                        retained_deliveries.extend(retained);
                    }
                }
            }
            if inserted_count != 0 {
                subscription_count += inserted_count;
                if let Some(client) = state.clients_by_connection.get_mut(&connection_id) {
                    client.subscription_count = subscription_count;
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
            if let Some((client_id, persistent_session)) = state
                .clients_by_connection
                .get(&connection_id)
                .map(|client| (client.client_id.clone(), client.persistent_session))
            {
                let subscription_count = state.subscriptions.len();
                for filter in &packet.topic_filters {
                    state
                        .subscriptions
                        .retain(|sub| !(sub.client_id == client_id && sub.filter == *filter));
                }
                let removed_count = subscription_count.saturating_sub(state.subscriptions.len());
                if removed_count != 0 {
                    if let Some(client) = state.clients_by_connection.get_mut(&connection_id) {
                        client.subscription_count =
                            client.subscription_count.saturating_sub(removed_count);
                    }
                    if persistent_session {
                        state.mark_subscriptions_changed();
                    }
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

fn subscription_position(
    subscriptions: &[super::SubscriptionEntry],
    client_id: &str,
    topic_filter: &str,
) -> Option<usize> {
    for (index, subscription) in subscriptions.iter().enumerate() {
        if subscription.client_id == client_id && subscription.filter == topic_filter {
            return Some(index);
        }
    }
    None
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
