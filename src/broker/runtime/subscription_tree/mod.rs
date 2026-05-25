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

pub(in crate::broker) fn upsert_subscription(
    subscriptions: &mut Vec<SubscriptionEntry>,
    client_id: &str,
    subscription: Subscription,
    subscription_identifier: Option<u32>,
) -> UpsertSubscriptionResult {
    let match_filter = crate::protocol::shared_subscription_filter(&subscription.topic_filter)
        .unwrap_or(&subscription.topic_filter)
        .to_string();
    let shared_group =
        crate::protocol::shared_subscription_group(&subscription.topic_filter).map(str::to_string);

    if let Some(index) = subscriptions
        .iter_mut()
        .position(|sub| sub.client_id == client_id && sub.filter == subscription.topic_filter)
    {
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

pub(in crate::broker) fn is_new_subscription(
    subscriptions: &[SubscriptionEntry],
    client_id: &str,
    topic_filter: &str,
) -> bool {
    !subscriptions.iter().any(|subscription| {
        subscription.client_id == client_id && subscription.filter == topic_filter
    })
}
