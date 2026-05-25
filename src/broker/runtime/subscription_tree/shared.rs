use std::collections::HashMap;

use super::SubscriptionEntry;
use crate::broker::runtime::session_registry::BrokerState;

pub(in crate::broker) fn select_shared_subscriptions(
    state: &mut BrokerState,
    subscriptions: Vec<SubscriptionEntry>,
) -> Vec<SubscriptionEntry> {
    let mut selected = Vec::new();
    let mut shared: HashMap<String, Vec<SubscriptionEntry>> = HashMap::new();

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
