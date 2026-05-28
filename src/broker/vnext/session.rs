use std::collections::BTreeMap;

use super::durable_log::RecoveredDurableState;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SessionActor {
    pub(crate) client_id: String,
    pub(crate) shard_id: usize,
    pub(crate) connected: bool,
    pub(crate) session_expiry_interval: u32,
    pub(crate) expires_at_ms: Option<u64>,
    pub(crate) next_packet_id: u16,
    pub(crate) outbound_queue_len: usize,
    pub(crate) inflight_len: usize,
}

impl SessionActor {
    fn disconnected(
        client_id: String,
        shard_id: usize,
        session_expiry_interval: u32,
        expires_at_ms: Option<u64>,
        next_packet_id: u16,
    ) -> Self {
        Self {
            client_id,
            shard_id,
            connected: false,
            session_expiry_interval,
            expires_at_ms,
            next_packet_id,
            outbound_queue_len: 0,
            inflight_len: 0,
        }
    }
}

#[derive(Default, Debug)]
pub(crate) struct SessionTable {
    sessions: BTreeMap<String, SessionActor>,
}

impl SessionTable {
    pub(crate) fn from_recovered(recovered: &RecoveredDurableState) -> Self {
        let mut table = Self::default();
        for (client_id, session) in &recovered.sessions {
            table.sessions.insert(
                client_id.clone(),
                SessionActor::disconnected(
                    client_id.clone(),
                    0,
                    session.session_expiry_interval,
                    session.expires_at_ms,
                    session.next_packet_id,
                ),
            );
        }
        table
    }

    pub(crate) fn upsert_connected(
        &mut self,
        client_id: impl Into<String>,
        shard_id: usize,
        session_expiry_interval: u32,
    ) -> &mut SessionActor {
        let client_id = client_id.into();
        self.sessions
            .entry(client_id.clone())
            .and_modify(|session| {
                session.connected = true;
                session.shard_id = shard_id;
                session.session_expiry_interval = session_expiry_interval;
                session.expires_at_ms = None;
            })
            .or_insert_with(|| SessionActor {
                client_id,
                shard_id,
                connected: true,
                session_expiry_interval,
                expires_at_ms: None,
                next_packet_id: 1,
                outbound_queue_len: 0,
                inflight_len: 0,
            })
    }

    pub(crate) fn disconnect(&mut self, client_id: &str, expires_at_ms: Option<u64>) {
        if let Some(session) = self.sessions.get_mut(client_id) {
            session.connected = false;
            session.expires_at_ms = expires_at_ms;
        }
    }

    pub(crate) fn get(&self, client_id: &str) -> Option<&SessionActor> {
        self.sessions.get(client_id)
    }

    pub(crate) fn len(&self) -> usize {
        self.sessions.len()
    }
}

#[cfg(test)]
mod tests {
    use super::SessionTable;

    #[test]
    fn session_table_reuses_existing_session_on_reconnect() {
        let mut table = SessionTable::default();
        table.upsert_connected("client-a", 1, 60).next_packet_id = 42;
        table.disconnect("client-a", Some(123));

        let session = table.upsert_connected("client-a", 2, 120);
        assert_eq!(session.next_packet_id, 42);
        assert_eq!(session.shard_id, 2);
        assert_eq!(session.session_expiry_interval, 120);
        assert!(session.connected);
        assert!(session.expires_at_ms.is_none());
    }
}
