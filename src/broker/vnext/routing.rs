use std::collections::BTreeMap;

use crate::protocol;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RouteSubscription {
    pub(crate) client_id: String,
    pub(crate) filter: String,
    pub(crate) match_filter: String,
    pub(crate) shared_group: Option<String>,
    pub(crate) subscription_identifier: Option<u32>,
}

impl RouteSubscription {
    pub(crate) fn new(
        client_id: impl Into<String>,
        filter: impl Into<String>,
        subscription_identifier: Option<u32>,
    ) -> Self {
        let filter = filter.into();
        let match_filter = protocol::shared_subscription_filter(&filter)
            .unwrap_or(&filter)
            .to_string();
        let shared_group = protocol::shared_subscription_group(&filter).map(str::to_string);

        Self {
            client_id: client_id.into(),
            filter,
            match_filter,
            shared_group,
            subscription_identifier,
        }
    }

    fn key_matches(&self, client_id: &str, filter: &str) -> bool {
        self.client_id == client_id && self.filter == filter
    }
}

#[derive(Default, Debug)]
pub(crate) struct RouterIndex {
    root: TrieNode,
}

impl RouterIndex {
    pub(crate) fn insert(&mut self, subscription: RouteSubscription) {
        if !protocol::is_valid_topic_filter(&subscription.filter) {
            return;
        }
        let match_filter = subscription.match_filter.clone();
        self.root.insert(&match_filter, subscription);
    }

    pub(crate) fn remove(&mut self, client_id: &str, filter: &str) {
        let match_filter = protocol::shared_subscription_filter(filter).unwrap_or(filter);
        self.root.remove(match_filter, client_id, filter);
    }

    pub(crate) fn matches(&self, topic_name: &str) -> Vec<RouteSubscription> {
        if !protocol::is_valid_topic_name(topic_name) {
            return Vec::new();
        }

        let topic_is_system = topic_name.starts_with('$');
        let levels: Vec<&str> = topic_name.split('/').collect();
        let mut matches = Vec::new();
        self.root.collect(&levels, 0, &mut matches);
        matches
            .retain(|subscription| !topic_is_system || subscription.match_filter.starts_with('$'));
        matches
    }
}

#[derive(Default, Debug)]
struct TrieNode {
    literals: BTreeMap<String, TrieNode>,
    plus: Option<Box<TrieNode>>,
    exact: Vec<RouteSubscription>,
    hash: Vec<RouteSubscription>,
}

impl TrieNode {
    fn insert(&mut self, filter: &str, subscription: RouteSubscription) {
        let levels: Vec<&str> = filter.split('/').collect();
        self.insert_levels(&levels, subscription);
    }

    fn insert_levels(&mut self, levels: &[&str], subscription: RouteSubscription) {
        let Some((level, rest)) = levels.split_first() else {
            upsert_subscription(&mut self.exact, subscription);
            return;
        };

        match *level {
            "#" if rest.is_empty() => upsert_subscription(&mut self.hash, subscription),
            "+" => self
                .plus
                .get_or_insert_with(Box::default)
                .insert_levels(rest, subscription),
            literal => self
                .literals
                .entry(literal.to_string())
                .or_default()
                .insert_levels(rest, subscription),
        }
    }

    fn remove(&mut self, filter: &str, client_id: &str, original_filter: &str) {
        let levels: Vec<&str> = filter.split('/').collect();
        self.remove_levels(&levels, client_id, original_filter);
    }

    fn remove_levels(&mut self, levels: &[&str], client_id: &str, original_filter: &str) {
        let Some((level, rest)) = levels.split_first() else {
            self.exact
                .retain(|subscription| !subscription.key_matches(client_id, original_filter));
            return;
        };

        match *level {
            "#" if rest.is_empty() => self
                .hash
                .retain(|subscription| !subscription.key_matches(client_id, original_filter)),
            "+" => {
                if let Some(plus) = self.plus.as_mut() {
                    plus.remove_levels(rest, client_id, original_filter);
                }
            }
            literal => {
                if let Some(node) = self.literals.get_mut(literal) {
                    node.remove_levels(rest, client_id, original_filter);
                }
            }
        }
    }

    fn collect(&self, topic_levels: &[&str], index: usize, matches: &mut Vec<RouteSubscription>) {
        matches.extend(self.hash.iter().cloned());

        if index == topic_levels.len() {
            matches.extend(self.exact.iter().cloned());
            return;
        }

        let level = topic_levels[index];
        if let Some(literal) = self.literals.get(level) {
            literal.collect(topic_levels, index + 1, matches);
        }
        if let Some(plus) = &self.plus {
            plus.collect(topic_levels, index + 1, matches);
        }
    }
}

fn upsert_subscription(
    subscriptions: &mut Vec<RouteSubscription>,
    subscription: RouteSubscription,
) {
    if let Some(existing) = subscriptions
        .iter_mut()
        .find(|existing| existing.key_matches(&subscription.client_id, &subscription.filter))
    {
        *existing = subscription;
    } else {
        subscriptions.push(subscription);
    }
}

#[cfg(test)]
mod tests {
    use super::{RouteSubscription, RouterIndex};

    #[test]
    fn matches_exact_plus_and_hash_filters() {
        let mut index = RouterIndex::default();
        index.insert(RouteSubscription::new("exact", "devices/a/temp", None));
        index.insert(RouteSubscription::new("plus", "devices/+/temp", None));
        index.insert(RouteSubscription::new("hash", "devices/#", None));

        let matches = index.matches("devices/a/temp");
        let clients: Vec<&str> = matches.iter().map(|sub| sub.client_id.as_str()).collect();
        assert!(clients.contains(&"exact"));
        assert!(clients.contains(&"plus"));
        assert!(clients.contains(&"hash"));
    }

    #[test]
    fn hash_filter_matches_parent_level() {
        let mut index = RouterIndex::default();
        index.insert(RouteSubscription::new("hash", "devices/#", None));

        let matches = index.matches("devices");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].client_id, "hash");
    }

    #[test]
    fn system_topics_only_match_system_filters() {
        let mut index = RouterIndex::default();
        index.insert(RouteSubscription::new("all", "#", None));
        index.insert(RouteSubscription::new("sys", "$SYS/#", None));

        let matches = index.matches("$SYS/broker/uptime");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].client_id, "sys");
    }

    #[test]
    fn shared_subscription_keeps_original_filter_and_match_filter() {
        let mut index = RouterIndex::default();
        index.insert(RouteSubscription::new(
            "member-a",
            "$share/group/devices/+",
            Some(7),
        ));

        let matches = index.matches("devices/a");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].filter, "$share/group/devices/+");
        assert_eq!(matches[0].match_filter, "devices/+");
        assert_eq!(matches[0].shared_group.as_deref(), Some("group"));
        assert_eq!(matches[0].subscription_identifier, Some(7));
    }

    #[test]
    fn remove_deletes_only_matching_client_filter_pair() {
        let mut index = RouterIndex::default();
        index.insert(RouteSubscription::new("a", "devices/+", None));
        index.insert(RouteSubscription::new("b", "devices/+", None));
        index.remove("a", "devices/+");

        let matches = index.matches("devices/one");
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].client_id, "b");
    }
}
