use rs_netty::codec::{Subscription, SubscriptionOptions};

mod service;
mod shared;

pub(in crate::broker) use shared::select_shared_subscriptions;

#[derive(Clone)]
pub(in crate::broker) struct SubscriptionEntry {
    pub(in crate::broker) client_id: String,
    pub(in crate::broker) filter: String,
    pub(in crate::broker) match_filter: String,
    pub(in crate::broker) shared_group: Option<String>,
    pub(in crate::broker) options: SubscriptionOptions,
    pub(in crate::broker) subscription_identifier: Option<u32>,
}

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::broker) fn upsert_subscription(
    subscriptions: &mut Vec<SubscriptionEntry>,
    client_id: &str,
    subscription: Subscription,
    subscription_identifier: Option<u32>,
) -> UpsertSubscriptionResult {
    let existing_index = subscriptions
        .iter()
        .position(|sub| sub.client_id == client_id && sub.filter == subscription.topic_filter);
    upsert_subscription_at(
        subscriptions,
        client_id,
        subscription,
        subscription_identifier,
        existing_index,
    )
}

pub(in crate::broker) fn upsert_subscription_at(
    subscriptions: &mut Vec<SubscriptionEntry>,
    client_id: &str,
    subscription: Subscription,
    subscription_identifier: Option<u32>,
    existing_index: Option<usize>,
) -> UpsertSubscriptionResult {
    let match_filter = crate::protocol::shared_subscription_filter(&subscription.topic_filter)
        .unwrap_or(&subscription.topic_filter)
        .to_string();
    let shared_group =
        crate::protocol::shared_subscription_group(&subscription.topic_filter).map(str::to_string);

    if let Some(index) = existing_index {
        subscriptions[index].options = subscription.options;
        subscriptions[index].subscription_identifier = subscription_identifier;
        subscriptions[index].match_filter = match_filter;
        subscriptions[index].shared_group = shared_group;
        return UpsertSubscriptionResult {
            index,
            inserted: false,
        };
    }

    subscriptions.push(SubscriptionEntry {
        client_id: client_id.to_string(),
        filter: subscription.topic_filter,
        match_filter,
        shared_group,
        options: subscription.options,
        subscription_identifier,
    });
    UpsertSubscriptionResult {
        index: subscriptions.len() - 1,
        inserted: true,
    }
}

pub(in crate::broker) struct UpsertSubscriptionResult {
    pub(in crate::broker) index: usize,
    pub(in crate::broker) inserted: bool,
}
