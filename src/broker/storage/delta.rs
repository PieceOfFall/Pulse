use std::collections::{BTreeMap, BTreeSet};

use rs_netty::codec::{PublishPacket, QoS, SubscriptionOptions};

use crate::broker::runtime::{
    message::PendingPublish,
    retained_store::RetainedMessage,
    session_registry::{BrokerState, PersistenceChange, QueuedPublish, SessionEntry},
    subscription_tree::SubscriptionEntry,
    time::now_ms,
};
use crate::protocol;

pub(crate) const CLIENT_PATCH_VERSION: u8 = 1;

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct PersistentProjection {
    pub(crate) clients: BTreeMap<String, ClientSnapshot>,
    pub(crate) retained: BTreeMap<String, RetainedMessage>,
}

impl PersistentProjection {
    pub(crate) fn from_state(state: &BrokerState) -> Self {
        let clients = state
            .sessions_by_client_id
            .iter()
            .filter(|(client_id, _)| state.is_client_durable(client_id))
            .map(|(client_id, session)| {
                (
                    client_id.clone(),
                    ClientSnapshot::from_state(state, client_id, session),
                )
            })
            .collect();
        let retained = state
            .retained
            .iter()
            .map(|(topic_name, message)| (topic_name.clone(), message.clone()))
            .collect();
        Self { clients, retained }
    }

    pub(crate) fn into_state(self) -> BrokerState {
        let mut state = BrokerState::default();
        for (client_id, client) in self.clients {
            if client.session.session_expiry_interval == 0 {
                continue;
            }
            let expires_at_ms = recovered_expiry(&client.session);
            let mut session =
                SessionEntry::disconnected(client.session.session_expiry_interval, expires_at_ms);
            session.next_packet_id = client.session.next_packet_id;
            session.next_offline_sequence = client.session.next_offline_sequence;
            session.offline_queue = client
                .offline
                .into_iter()
                .map(|(sequence, pending)| QueuedPublish {
                    sequence,
                    pending: pending.into_pending(),
                })
                .collect();
            session.outbound_qos1 = client
                .qos1
                .into_iter()
                .map(|(packet_id, pending)| (packet_id, pending.into_pending()))
                .collect();
            session.outbound_qos2_publish = client
                .qos2_publish
                .into_iter()
                .map(|(packet_id, pending)| (packet_id, pending.into_pending()))
                .collect();
            session.outbound_qos2_pubrel = client.qos2_pubrel.into_iter().collect();
            state.sessions_by_client_id.insert(client_id, session);
            state.subscriptions.extend(
                client
                    .subscriptions
                    .into_values()
                    .map(SubscriptionSnapshot::into_subscription),
            );
        }
        for (topic_name, message) in self.retained {
            state.retained.insert(topic_name, message);
        }
        state
    }

    pub(crate) fn canonicalize_for_offline_recovery(&mut self) -> bool {
        let now_ms = now_ms();
        let previous_len = self.clients.len();
        self.clients.retain(|_, client| {
            client.session.session_expiry_interval != 0
                && client
                    .session
                    .expires_at_ms
                    .is_none_or(|expires_at_ms| expires_at_ms > now_ms)
        });
        let mut changed = self.clients.len() != previous_len;
        for client in self.clients.values_mut() {
            let expires_at_ms = recovered_expiry_at(&client.session, now_ms);
            if client.session.expires_at_ms != expires_at_ms {
                client.session.expires_at_ms = expires_at_ms;
                changed = true;
            }
        }
        changed
    }

    pub(crate) fn apply_patch(&mut self, patch: &StoragePatch) -> Result<(), &'static str> {
        patch.validate()?;
        match patch {
            StoragePatch::Client(patch) => self.apply_client_patch(patch),
            StoragePatch::Retained(patch) => {
                if let Some(message) = &patch.message {
                    self.retained
                        .insert(patch.topic_name.clone(), message.clone());
                } else {
                    self.retained.remove(&patch.topic_name);
                }
                Ok(())
            }
        }
    }

    fn apply_client_patch(&mut self, patch: &ClientPatch) -> Result<(), &'static str> {
        patch.validate()?;
        match patch.mode {
            ClientPatchMode::Delete => {
                self.clients.remove(&patch.client_id);
                Ok(())
            }
            ClientPatchMode::Reset => {
                let client = ClientSnapshot::from_reset_patch(patch)?;
                self.clients.insert(patch.client_id.clone(), client);
                Ok(())
            }
            ClientPatchMode::Merge => {
                let client = self
                    .clients
                    .get_mut(&patch.client_id)
                    .ok_or("client merge patch has no persisted session")?;
                let mut next = client.clone();
                next.apply_merge(patch)?;
                next.validate(&patch.client_id)?;
                *client = next;
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum StoragePatch {
    Client(ClientPatch),
    Retained(RetainedPatch),
}

impl StoragePatch {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        match self {
            Self::Client(patch) => patch.validate(),
            Self::Retained(patch) => patch.validate(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ClientPatchMode {
    Merge,
    Reset,
    Delete,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ClientPatch {
    pub(crate) version: u8,
    pub(crate) client_id: String,
    pub(crate) mode: ClientPatchMode,
    pub(crate) session: Option<SessionSnapshot>,
    pub(crate) subscription_upserts: Vec<SubscriptionSnapshot>,
    pub(crate) subscription_deletes: Vec<String>,
    pub(crate) offline_remove_through: Option<u64>,
    pub(crate) offline_append: Vec<QueuedSnapshot>,
    pub(crate) qos1_upserts: BTreeMap<u16, PendingSnapshot>,
    pub(crate) qos1_deletes: BTreeSet<u16>,
    pub(crate) qos2_publish_upserts: BTreeMap<u16, PendingSnapshot>,
    pub(crate) qos2_publish_deletes: BTreeSet<u16>,
    pub(crate) pubrel_add: BTreeSet<u16>,
    pub(crate) pubrel_remove: BTreeSet<u16>,
}

impl ClientPatch {
    fn empty(client_id: String, mode: ClientPatchMode) -> Self {
        Self {
            version: CLIENT_PATCH_VERSION,
            client_id,
            mode,
            session: None,
            subscription_upserts: Vec::new(),
            subscription_deletes: Vec::new(),
            offline_remove_through: None,
            offline_append: Vec::new(),
            qos1_upserts: BTreeMap::new(),
            qos1_deletes: BTreeSet::new(),
            qos2_publish_upserts: BTreeMap::new(),
            qos2_publish_deletes: BTreeSet::new(),
            pubrel_add: BTreeSet::new(),
            pubrel_remove: BTreeSet::new(),
        }
    }

    pub(crate) fn reset(client_id: String, client: &ClientSnapshot) -> Self {
        let mut patch = Self::empty(client_id, ClientPatchMode::Reset);
        patch.session = Some(client.session.clone());
        patch.subscription_upserts = client.subscriptions.values().cloned().collect();
        patch.offline_append = client
            .offline
            .iter()
            .map(|(sequence, pending)| QueuedSnapshot {
                sequence: *sequence,
                pending: pending.clone(),
            })
            .collect();
        patch.qos1_upserts = client.qos1.clone();
        patch.qos2_publish_upserts = client.qos2_publish.clone();
        patch.pubrel_add = client.qos2_pubrel.clone();
        patch
    }

    pub(crate) fn delete(client_id: String) -> Self {
        Self::empty(client_id, ClientPatchMode::Delete)
    }

    fn between(
        client_id: String,
        previous: Option<&ClientSnapshot>,
        next: Option<&ClientSnapshot>,
        force_reset: bool,
    ) -> Option<Self> {
        match (previous, next) {
            (_, None) if force_reset || previous.is_some() => Some(Self::delete(client_id)),
            (_, None) => None,
            (_, Some(next)) if force_reset || previous.is_none() => {
                Some(Self::reset(client_id, next))
            }
            (Some(previous), Some(next)) if previous == next => None,
            (Some(previous), Some(next)) => Self::merge(client_id.clone(), previous, next)
                .or_else(|| Some(Self::reset(client_id, next))),
            (None, Some(_)) => unreachable!("handled missing previous client"),
        }
    }

    fn merge(client_id: String, previous: &ClientSnapshot, next: &ClientSnapshot) -> Option<Self> {
        let mut patch = Self::empty(client_id, ClientPatchMode::Merge);
        if previous.session != next.session {
            patch.session = Some(next.session.clone());
        }

        for (filter, subscription) in &next.subscriptions {
            if previous.subscriptions.get(filter) != Some(subscription) {
                patch.subscription_upserts.push(subscription.clone());
            }
        }
        patch.subscription_deletes.extend(
            previous
                .subscriptions
                .keys()
                .filter(|filter| !next.subscriptions.contains_key(*filter))
                .cloned(),
        );

        if !diff_offline(previous, next, &mut patch) {
            return None;
        }
        diff_map(
            &previous.qos1,
            &next.qos1,
            &mut patch.qos1_upserts,
            &mut patch.qos1_deletes,
        );
        diff_map(
            &previous.qos2_publish,
            &next.qos2_publish,
            &mut patch.qos2_publish_upserts,
            &mut patch.qos2_publish_deletes,
        );
        patch.pubrel_add = next
            .qos2_pubrel
            .difference(&previous.qos2_pubrel)
            .copied()
            .collect();
        patch.pubrel_remove = previous
            .qos2_pubrel
            .difference(&next.qos2_pubrel)
            .copied()
            .collect();
        Some(patch)
    }

    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.version != CLIENT_PATCH_VERSION {
            return Err("unsupported client patch version");
        }
        if self.client_id.is_empty() {
            return Err("client patch has an empty client id");
        }
        if self.session.as_ref().is_some_and(|session| {
            session.next_packet_id == 0 || session.next_offline_sequence > i64::MAX as u64
        }) {
            return Err("client patch contains an invalid persistent session");
        }
        let mut subscription_upserts = BTreeSet::new();
        for subscription in &self.subscription_upserts {
            subscription.validate(&self.client_id)?;
            if !subscription_upserts.insert(subscription.filter.as_str()) {
                return Err("duplicate subscription upsert");
            }
        }
        if self
            .subscription_deletes
            .iter()
            .any(|filter| !protocol::is_valid_topic_filter(filter))
        {
            return Err("invalid subscription delete");
        }
        let subscription_deletes = self
            .subscription_deletes
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if subscription_deletes.len() != self.subscription_deletes.len()
            || !subscription_upserts.is_disjoint(&subscription_deletes)
        {
            return Err("conflicting subscription changes");
        }
        if self.offline_remove_through > Some(i64::MAX as u64) {
            return Err("client patch contains an invalid offline removal");
        }
        let mut previous_sequence = None;
        for queued in &self.offline_append {
            if queued.sequence > i64::MAX as u64
                || previous_sequence.is_some_and(|previous| queued.sequence <= previous)
                || queued.pending.packet.packet_id.is_some()
                || queued.pending.packet.qos == QoS::AtMostOnce
                || !protocol::is_valid_topic_name(&queued.pending.packet.topic_name)
            {
                return Err("client patch contains an invalid offline append");
            }
            previous_sequence = Some(queued.sequence);
        }
        if let Some(last_sequence) = previous_sequence
            && self
                .session
                .as_ref()
                .is_none_or(|session| session.next_offline_sequence <= last_sequence)
        {
            return Err("offline append is not covered by the session counter");
        }
        if self
            .qos1_upserts
            .keys()
            .chain(self.qos1_deletes.iter())
            .chain(self.qos2_publish_upserts.keys())
            .chain(self.qos2_publish_deletes.iter())
            .chain(self.pubrel_add.iter())
            .chain(self.pubrel_remove.iter())
            .any(|packet_id| *packet_id == 0)
        {
            return Err("client patch contains packet identifier zero");
        }
        if !self.qos1_upserts.iter().all(|(packet_id, pending)| {
            valid_outbound_pending(*packet_id, pending, QoS::AtLeastOnce)
        }) || !self
            .qos2_publish_upserts
            .iter()
            .all(|(packet_id, pending)| {
                valid_outbound_pending(*packet_id, pending, QoS::ExactlyOnce)
            })
        {
            return Err("client patch contains an invalid outbound publish");
        }
        if let Some(remove_through) = self.offline_remove_through
            && self
                .offline_append
                .first()
                .is_some_and(|queued| queued.sequence <= remove_through)
        {
            return Err("client patch appends an already removed offline sequence");
        }
        if self
            .qos1_upserts
            .keys()
            .any(|packet_id| self.qos1_deletes.contains(packet_id))
            || self
                .qos2_publish_upserts
                .keys()
                .any(|packet_id| self.qos2_publish_deletes.contains(packet_id))
            || !self.pubrel_add.is_disjoint(&self.pubrel_remove)
        {
            return Err("client patch contains conflicting outbound changes");
        }
        if self.qos1_upserts.keys().any(|packet_id| {
            self.qos2_publish_upserts.contains_key(packet_id) || self.pubrel_add.contains(packet_id)
        }) || self
            .qos2_publish_upserts
            .keys()
            .any(|packet_id| self.pubrel_add.contains(packet_id))
        {
            return Err("client patch reuses an outbound packet identifier");
        }
        match self.mode {
            ClientPatchMode::Delete if !self.is_empty_body() => {
                Err("delete client patch contains state")
            }
            ClientPatchMode::Reset if self.session.is_none() || self.has_deletes() => {
                Err("reset client patch is incomplete")
            }
            ClientPatchMode::Merge if self.session.is_none() && self.is_empty_delta() => {
                Err("empty client merge patch")
            }
            _ => Ok(()),
        }
    }

    fn has_deletes(&self) -> bool {
        !self.subscription_deletes.is_empty()
            || self.offline_remove_through.is_some()
            || !self.qos1_deletes.is_empty()
            || !self.qos2_publish_deletes.is_empty()
            || !self.pubrel_remove.is_empty()
    }

    fn is_empty_delta(&self) -> bool {
        self.subscription_upserts.is_empty()
            && self.subscription_deletes.is_empty()
            && self.offline_remove_through.is_none()
            && self.offline_append.is_empty()
            && self.qos1_upserts.is_empty()
            && self.qos1_deletes.is_empty()
            && self.qos2_publish_upserts.is_empty()
            && self.qos2_publish_deletes.is_empty()
            && self.pubrel_add.is_empty()
            && self.pubrel_remove.is_empty()
    }

    fn is_empty_body(&self) -> bool {
        self.session.is_none() && self.is_empty_delta()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct RetainedPatch {
    pub(crate) topic_name: String,
    pub(crate) message: Option<RetainedMessage>,
}

impl RetainedPatch {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if !protocol::is_valid_topic_name(&self.topic_name)
            || self
                .message
                .as_ref()
                .is_some_and(|message| message.topic_name != self.topic_name)
        {
            return Err("invalid retained patch topic");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ClientSnapshot {
    pub(crate) session: SessionSnapshot,
    pub(crate) subscriptions: BTreeMap<String, SubscriptionSnapshot>,
    pub(crate) offline: BTreeMap<u64, PendingSnapshot>,
    pub(crate) qos1: BTreeMap<u16, PendingSnapshot>,
    pub(crate) qos2_publish: BTreeMap<u16, PendingSnapshot>,
    pub(crate) qos2_pubrel: BTreeSet<u16>,
}

impl ClientSnapshot {
    fn from_state(state: &BrokerState, client_id: &str, session: &SessionEntry) -> Self {
        Self {
            session: SessionSnapshot::from_session(session),
            subscriptions: state
                .subscriptions
                .iter()
                .filter(|subscription| subscription.client_id == client_id)
                .map(|subscription| {
                    (
                        subscription.filter.clone(),
                        SubscriptionSnapshot::from_subscription(subscription),
                    )
                })
                .collect(),
            offline: session
                .offline_queue
                .iter()
                .map(|queued| {
                    (
                        queued.sequence,
                        PendingSnapshot::from_pending(&queued.pending),
                    )
                })
                .collect(),
            qos1: session
                .outbound_qos1
                .iter()
                .map(|(packet_id, pending)| (*packet_id, PendingSnapshot::from_pending(pending)))
                .collect(),
            qos2_publish: session
                .outbound_qos2_publish
                .iter()
                .map(|(packet_id, pending)| (*packet_id, PendingSnapshot::from_pending(pending)))
                .collect(),
            qos2_pubrel: session.outbound_qos2_pubrel.iter().copied().collect(),
        }
    }

    fn from_reset_patch(patch: &ClientPatch) -> Result<Self, &'static str> {
        let mut client = Self {
            session: patch
                .session
                .clone()
                .ok_or("reset client patch has no session")?,
            subscriptions: BTreeMap::new(),
            offline: BTreeMap::new(),
            qos1: patch.qos1_upserts.clone(),
            qos2_publish: patch.qos2_publish_upserts.clone(),
            qos2_pubrel: patch.pubrel_add.clone(),
        };
        for subscription in &patch.subscription_upserts {
            if subscription.client_id != patch.client_id {
                return Err("subscription belongs to another client");
            }
            if client
                .subscriptions
                .insert(subscription.filter.clone(), subscription.clone())
                .is_some()
            {
                return Err("duplicate subscription in reset patch");
            }
        }
        for queued in &patch.offline_append {
            if client
                .offline
                .insert(queued.sequence, queued.pending.clone())
                .is_some()
            {
                return Err("duplicate offline sequence in reset patch");
            }
        }
        client.validate(&patch.client_id)?;
        Ok(client)
    }

    fn apply_merge(&mut self, patch: &ClientPatch) -> Result<(), &'static str> {
        if patch.session.as_ref().is_some_and(|session| {
            session.next_offline_sequence < self.session.next_offline_sequence
        }) {
            return Err("offline sequence counter moved backwards");
        }
        if patch
            .offline_remove_through
            .is_some_and(|sequence| sequence >= self.session.next_offline_sequence)
        {
            return Err("offline removal exceeds the issued sequence range");
        }
        if patch
            .offline_append
            .first()
            .is_some_and(|queued| queued.sequence < self.session.next_offline_sequence)
        {
            return Err("offline append reuses an issued sequence");
        }
        if let Some(session) = &patch.session {
            self.session = session.clone();
        }
        for filter in &patch.subscription_deletes {
            self.subscriptions.remove(filter);
        }
        for subscription in &patch.subscription_upserts {
            if subscription.client_id != patch.client_id {
                return Err("subscription belongs to another client");
            }
            self.subscriptions
                .insert(subscription.filter.clone(), subscription.clone());
        }
        if let Some(remove_through) = patch.offline_remove_through {
            self.offline
                .retain(|sequence, _| *sequence > remove_through);
        }
        for queued in &patch.offline_append {
            if let Some(existing) = self.offline.get(&queued.sequence) {
                if existing == &queued.pending {
                    continue;
                }
                return Err("offline append conflicts with an existing sequence");
            }
            self.offline.insert(queued.sequence, queued.pending.clone());
        }
        apply_map_delta(&mut self.qos1, &patch.qos1_upserts, &patch.qos1_deletes);
        apply_map_delta(
            &mut self.qos2_publish,
            &patch.qos2_publish_upserts,
            &patch.qos2_publish_deletes,
        );
        for packet_id in &patch.pubrel_remove {
            self.qos2_pubrel.remove(packet_id);
        }
        self.qos2_pubrel.extend(&patch.pubrel_add);
        Ok(())
    }

    fn validate(&self, client_id: &str) -> Result<(), &'static str> {
        if self.session.next_packet_id == 0 || self.session.next_offline_sequence > i64::MAX as u64
        {
            return Err("client snapshot contains an invalid session");
        }
        if self.subscriptions.iter().any(|(filter, subscription)| {
            filter != &subscription.filter || subscription.validate(client_id).is_err()
        }) {
            return Err("client snapshot contains an invalid subscription");
        }
        if self.offline.iter().any(|(sequence, pending)| {
            *sequence >= self.session.next_offline_sequence
                || pending.packet.packet_id.is_some()
                || pending.packet.qos == QoS::AtMostOnce
                || !protocol::is_valid_topic_name(&pending.packet.topic_name)
        }) {
            return Err("client snapshot contains an invalid offline queue");
        }
        if !self.qos1.iter().all(|(packet_id, pending)| {
            valid_outbound_pending(*packet_id, pending, QoS::AtLeastOnce)
        }) || !self.qos2_publish.iter().all(|(packet_id, pending)| {
            valid_outbound_pending(*packet_id, pending, QoS::ExactlyOnce)
        }) {
            return Err("client snapshot contains an invalid outbound publish");
        }
        if self.qos1.keys().any(|packet_id| {
            self.qos2_publish.contains_key(packet_id) || self.qos2_pubrel.contains(packet_id)
        }) || self
            .qos2_publish
            .keys()
            .any(|packet_id| self.qos2_pubrel.contains(packet_id))
        {
            return Err("client snapshot reuses an outbound packet identifier");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SessionSnapshot {
    pub(crate) session_expiry_interval: u32,
    pub(crate) expires_at_ms: Option<u64>,
    pub(crate) next_packet_id: u16,
    pub(crate) next_offline_sequence: u64,
}

impl SessionSnapshot {
    fn from_session(session: &SessionEntry) -> Self {
        Self {
            session_expiry_interval: session.session_expiry_interval,
            expires_at_ms: session.expires_at_ms,
            next_packet_id: session.next_packet_id,
            next_offline_sequence: session.next_offline_sequence,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SubscriptionSnapshot {
    pub(crate) client_id: String,
    pub(crate) filter: String,
    pub(crate) match_filter: String,
    pub(crate) shared_group: Option<String>,
    pub(crate) maximum_qos: QoS,
    pub(crate) no_local: bool,
    pub(crate) retain_as_published: bool,
    pub(crate) retain_handling: u8,
    pub(crate) subscription_identifier: Option<u32>,
}

impl SubscriptionSnapshot {
    pub(crate) fn from_subscription(subscription: &SubscriptionEntry) -> Self {
        Self {
            client_id: subscription.client_id.clone(),
            filter: subscription.filter.clone(),
            match_filter: subscription.match_filter.clone(),
            shared_group: subscription.shared_group.clone(),
            maximum_qos: subscription.options.maximum_qos,
            no_local: subscription.options.no_local,
            retain_as_published: subscription.options.retain_as_published,
            retain_handling: subscription.options.retain_handling,
            subscription_identifier: subscription.subscription_identifier,
        }
    }

    pub(crate) fn into_subscription(self) -> SubscriptionEntry {
        SubscriptionEntry {
            client_id: self.client_id,
            filter: self.filter,
            match_filter: self.match_filter,
            shared_group: self.shared_group,
            options: SubscriptionOptions {
                maximum_qos: self.maximum_qos,
                no_local: self.no_local,
                retain_as_published: self.retain_as_published,
                retain_handling: self.retain_handling,
            },
            subscription_identifier: self.subscription_identifier,
        }
    }

    fn validate(&self, client_id: &str) -> Result<(), &'static str> {
        if self.client_id != client_id || !protocol::is_valid_topic_filter(&self.filter) {
            return Err("invalid persistent subscription");
        }
        let match_filter =
            protocol::shared_subscription_filter(&self.filter).unwrap_or(&self.filter);
        let shared_group = protocol::shared_subscription_group(&self.filter);
        if self.match_filter != match_filter
            || self.shared_group.as_deref() != shared_group
            || self.retain_handling > 2
            || self
                .subscription_identifier
                .is_some_and(|identifier| identifier == 0 || identifier > 268_435_455)
        {
            return Err("inconsistent persistent subscription");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct PendingSnapshot {
    pub(crate) packet: PublishPacket,
    pub(crate) expires_at_ms: Option<u64>,
}

impl PendingSnapshot {
    pub(crate) fn from_pending(pending: &PendingPublish) -> Self {
        Self {
            packet: pending.packet.clone(),
            expires_at_ms: pending.expires_at_ms,
        }
    }

    pub(crate) fn into_pending(self) -> PendingPublish {
        PendingPublish {
            packet: self.packet,
            expires_at_ms: self.expires_at_ms,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QueuedSnapshot {
    pub(crate) sequence: u64,
    pub(crate) pending: PendingSnapshot,
}

pub(crate) fn prepare_patches(
    projection: &PersistentProjection,
    state: &BrokerState,
    changes: &[PersistenceChange],
) -> Vec<StoragePatch> {
    let mut clients = BTreeMap::<String, bool>::new();
    let mut retained = BTreeSet::new();
    for change in changes {
        match change {
            PersistenceChange::ClientChanged(client_id) => {
                clients.entry(client_id.clone()).or_insert(false);
            }
            PersistenceChange::ClientReset(client_id) => {
                clients.insert(client_id.clone(), true);
            }
            PersistenceChange::RetainedTopic(topic_name) => {
                retained.insert(topic_name.clone());
            }
        }
    }

    let mut patches = Vec::new();
    for (client_id, force_reset) in clients {
        prepare_client_patch(projection, state, client_id, force_reset, &mut patches);
    }
    for topic_name in retained {
        let previous = projection.retained.get(&topic_name);
        let next = state.retained.get(&topic_name);
        if previous != next {
            patches.push(StoragePatch::Retained(RetainedPatch {
                topic_name,
                message: next.cloned(),
            }));
        }
    }
    patches
}

fn prepare_client_patch(
    projection: &PersistentProjection,
    state: &BrokerState,
    client_id: String,
    force_reset: bool,
    patches: &mut Vec<StoragePatch>,
) {
    let previous = projection.clients.get(&client_id);
    let next = state
        .sessions_by_client_id
        .get(&client_id)
        .filter(|_| state.is_client_durable(&client_id))
        .map(|session| ClientSnapshot::from_state(state, &client_id, session));
    if let Some(patch) = ClientPatch::between(client_id, previous, next.as_ref(), force_reset) {
        patches.push(StoragePatch::Client(patch));
    }
}

fn diff_offline(previous: &ClientSnapshot, next: &ClientSnapshot, patch: &mut ClientPatch) -> bool {
    let removed = previous
        .offline
        .keys()
        .filter(|sequence| !next.offline.contains_key(*sequence))
        .copied()
        .collect::<Vec<_>>();
    if previous.offline.iter().any(|(sequence, pending)| {
        next.offline
            .get(sequence)
            .is_some_and(|next| next != pending)
    }) {
        return false;
    }
    if !removed.is_empty() {
        let prefix = previous
            .offline
            .keys()
            .take(removed.len())
            .copied()
            .collect::<Vec<_>>();
        if prefix != removed {
            return false;
        }
        patch.offline_remove_through = removed.last().copied();
    }
    let previous_last = previous
        .offline
        .last_key_value()
        .map(|(sequence, _)| *sequence);
    for (sequence, pending) in &next.offline {
        if !previous.offline.contains_key(sequence) {
            if previous_last.is_some_and(|last| *sequence <= last) {
                return false;
            }
            patch.offline_append.push(QueuedSnapshot {
                sequence: *sequence,
                pending: pending.clone(),
            });
        }
    }
    true
}

fn diff_map<K: Copy + Ord, V: Clone + PartialEq>(
    previous: &BTreeMap<K, V>,
    next: &BTreeMap<K, V>,
    upserts: &mut BTreeMap<K, V>,
    deletes: &mut BTreeSet<K>,
) {
    for (key, value) in next {
        if previous.get(key) != Some(value) {
            upserts.insert(*key, value.clone());
        }
    }
    deletes.extend(
        previous
            .keys()
            .filter(|key| !next.contains_key(*key))
            .copied(),
    );
}

fn apply_map_delta<K: Copy + Ord, V: Clone>(
    target: &mut BTreeMap<K, V>,
    upserts: &BTreeMap<K, V>,
    deletes: &BTreeSet<K>,
) {
    for key in deletes {
        target.remove(key);
    }
    target.extend(upserts.iter().map(|(key, value)| (*key, value.clone())));
}

fn valid_outbound_pending(packet_id: u16, pending: &PendingSnapshot, qos: QoS) -> bool {
    pending.packet.qos == qos
        && pending.packet.packet_id == Some(packet_id)
        && protocol::is_valid_topic_name(&pending.packet.topic_name)
}

fn recovered_expiry(session: &SessionSnapshot) -> Option<u64> {
    recovered_expiry_at(session, now_ms())
}

fn recovered_expiry_at(session: &SessionSnapshot, now_ms: u64) -> Option<u64> {
    session.expires_at_ms.or_else(|| {
        (session.session_expiry_interval != u32::MAX)
            .then(|| now_ms.saturating_add(u64::from(session.session_expiry_interval) * 1_000))
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    fn pending(payload: &'static [u8]) -> PendingSnapshot {
        PendingSnapshot {
            packet: PublishPacket {
                dup: false,
                qos: QoS::AtLeastOnce,
                retain: false,
                topic_name: "test".to_string(),
                packet_id: None,
                properties: Vec::new(),
                payload: Bytes::from_static(payload),
            },
            expires_at_ms: None,
        }
    }

    fn client() -> ClientSnapshot {
        let mut qos1 = pending(b"qos1");
        qos1.packet.packet_id = Some(1);
        ClientSnapshot {
            session: SessionSnapshot {
                session_expiry_interval: 60,
                expires_at_ms: None,
                next_packet_id: 1,
                next_offline_sequence: 3,
            },
            subscriptions: BTreeMap::new(),
            offline: [(0, pending(b"zero")), (1, pending(b"one"))]
                .into_iter()
                .collect(),
            qos1: [(1, qos1)].into_iter().collect(),
            qos2_publish: BTreeMap::new(),
            qos2_pubrel: BTreeSet::new(),
        }
    }

    #[test]
    fn ack_and_queue_handoff_is_a_constant_size_merge() {
        let previous = client();
        let mut next = previous.clone();
        next.offline.remove(&0);
        next.offline.insert(2, pending(b"two"));
        next.qos1.remove(&1);
        let mut qos1 = pending(b"next");
        qos1.packet.packet_id = Some(2);
        next.qos1.insert(2, qos1);
        next.session.next_packet_id = 3;

        let patch = ClientPatch::between("client".to_string(), Some(&previous), Some(&next), false)
            .expect("client patch");
        assert_eq!(patch.mode, ClientPatchMode::Merge);
        assert_eq!(patch.offline_remove_through, Some(0));
        assert_eq!(patch.offline_append.len(), 1);
        assert_eq!(patch.qos1_deletes, BTreeSet::from([1]));
        assert_eq!(patch.qos1_upserts.len(), 1);
    }

    #[test]
    fn projection_is_updated_only_when_patch_is_applied() {
        let client = client();
        let mut projection = PersistentProjection::default();
        let patch = StoragePatch::Client(ClientPatch::reset("client".to_string(), &client));
        assert!(!projection.clients.contains_key("client"));
        projection.apply_patch(&patch).expect("apply patch");
        assert_eq!(projection.clients.get("client"), Some(&client));
    }

    #[test]
    fn reset_rejects_packet_identifier_reused_across_qos_states() {
        let mut client = client();
        let mut qos2 = pending(b"qos2");
        qos2.packet.qos = QoS::ExactlyOnce;
        qos2.packet.packet_id = Some(1);
        client.qos2_publish.insert(1, qos2);
        let patch = StoragePatch::Client(ClientPatch::reset("client".to_string(), &client));
        let mut projection = PersistentProjection::default();

        assert!(patch.validate().is_err());
        assert!(projection.apply_patch(&patch).is_err());
        assert!(projection.clients.is_empty());
    }

    #[test]
    fn patches_reject_semantically_invalid_topics_and_subscriptions() {
        let mut invalid_outbound = client();
        invalid_outbound.qos1.get_mut(&1).unwrap().packet.topic_name = "devices/+".to_string();
        assert!(
            ClientPatch::reset("client".to_string(), &invalid_outbound)
                .validate()
                .is_err()
        );

        let mut invalid_subscription = ClientPatch::reset("client".to_string(), &client());
        invalid_subscription
            .subscription_upserts
            .push(SubscriptionSnapshot {
                client_id: "client".to_string(),
                filter: "$share/group/devices/#".to_string(),
                match_filter: "wrong/#".to_string(),
                shared_group: Some("group".to_string()),
                maximum_qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                subscription_identifier: Some(1),
            });
        assert!(invalid_subscription.validate().is_err());

        let retained = RetainedMessage::new(
            QoS::AtMostOnce,
            "devices/#".to_string(),
            Vec::new(),
            Bytes::from_static(b"invalid"),
            None,
        );
        assert!(
            RetainedPatch {
                topic_name: "devices/#".to_string(),
                message: Some(retained),
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn merge_rejects_offline_sequence_regression_without_mutating_projection() {
        let client = client();
        let mut projection = PersistentProjection::default();
        projection
            .clients
            .insert("client".to_string(), client.clone());

        let mut counter_regression =
            ClientPatch::empty("client".to_string(), ClientPatchMode::Merge);
        let mut session = client.session.clone();
        session.next_offline_sequence = 2;
        counter_regression.session = Some(session);
        assert!(counter_regression.validate().is_ok());
        assert!(
            projection
                .apply_patch(&StoragePatch::Client(counter_regression))
                .is_err()
        );
        assert_eq!(projection.clients.get("client"), Some(&client));

        let mut reused_sequence = ClientPatch::empty("client".to_string(), ClientPatchMode::Merge);
        let mut session = client.session.clone();
        session.next_offline_sequence = 4;
        reused_sequence.session = Some(session);
        reused_sequence.offline_append.push(QueuedSnapshot {
            sequence: 2,
            pending: pending(b"reused"),
        });
        assert!(reused_sequence.validate().is_ok());
        assert!(
            projection
                .apply_patch(&StoragePatch::Client(reused_sequence))
                .is_err()
        );
        assert_eq!(projection.clients.get("client"), Some(&client));
    }

    #[test]
    fn merge_rejects_packet_identifier_conflicting_with_existing_state() {
        let client = client();
        let mut projection = PersistentProjection::default();
        projection
            .clients
            .insert("client".to_string(), client.clone());
        let mut patch = ClientPatch::empty("client".to_string(), ClientPatchMode::Merge);
        patch.pubrel_add.insert(1);

        assert!(patch.validate().is_ok());
        assert!(
            projection
                .apply_patch(&StoragePatch::Client(patch))
                .is_err()
        );
        assert_eq!(projection.clients.get("client"), Some(&client));
    }

    #[test]
    fn offline_recovery_drops_zero_expiry_sessions() {
        let mut projection = PersistentProjection::default();
        let mut client = client();
        client.session.session_expiry_interval = 0;
        projection.clients.insert("transient".to_string(), client);
        projection.canonicalize_for_offline_recovery();
        assert!(!projection.clients.contains_key("transient"));
        assert!(projection.into_state().sessions_by_client_id.is_empty());
    }

    #[test]
    fn offline_recovery_drops_sessions_with_elapsed_deadlines() {
        let mut projection = PersistentProjection::default();
        let mut expired = client();
        expired.session.expires_at_ms = Some(now_ms().saturating_sub(1));
        projection.clients.insert("expired".to_string(), expired);

        assert!(projection.canonicalize_for_offline_recovery());
        assert!(!projection.clients.contains_key("expired"));
    }

    #[test]
    fn offline_recovery_starts_finite_expiry_but_preserves_never_expire() {
        let before = now_ms();
        let mut projection = PersistentProjection::default();
        let mut finite = client();
        finite.session.session_expiry_interval = 60;
        finite.session.expires_at_ms = None;
        let mut forever = client();
        forever.session.session_expiry_interval = u32::MAX;
        forever.session.expires_at_ms = None;
        projection.clients.insert("finite".to_string(), finite);
        projection.clients.insert("forever".to_string(), forever);

        projection.canonicalize_for_offline_recovery();
        let after = now_ms();
        let finite_expiry = projection.clients["finite"]
            .session
            .expires_at_ms
            .expect("finite recovery expiry");
        assert!(finite_expiry >= before + 60_000);
        assert!(finite_expiry <= after + 60_000);
        assert_eq!(projection.clients["forever"].session.expires_at_ms, None);
    }
}
