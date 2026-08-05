use std::{path::Path, sync::Mutex};

use rs_netty::codec::{QoS, SubscriptionOptions};
use rusqlite::{Connection, Transaction, params};

use super::{
    BrokerStorage,
    delta::{
        ClientPatch, ClientPatchMode, PendingSnapshot, PersistentProjection, RetainedPatch,
        SessionSnapshot, StoragePatch, SubscriptionSnapshot, prepare_patches,
    },
};
use crate::broker::runtime::{
    message::PendingPublish,
    session_registry::{BrokerState, QueuedPublish, SessionEntry},
    subscription_tree::SubscriptionEntry,
};

mod codec;
mod schema;

use self::{
    codec::{
        bool_to_u8, decode_publish, decode_retained, encode_publish, encode_retained, qos_from_u8,
        qos_to_u8,
    },
    schema::{configure_connection, migrate},
};

pub(crate) struct SqliteStorage {
    connection: Mutex<Connection>,
    state: Mutex<BrokerState>,
    projection: Mutex<PersistentProjection>,
}

impl SqliteStorage {
    pub(crate) fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let mut connection = Connection::open(path)?;
        configure_connection(&connection)?;
        migrate(&mut connection)?;
        let loaded_state = load_state(&connection)?;
        let projection = PersistentProjection::from_state(&loaded_state);
        let state = projection.clone().into_state();

        Ok(Self {
            connection: Mutex::new(connection),
            state: Mutex::new(state),
            projection: Mutex::new(projection),
        })
    }
}

impl BrokerStorage for SqliteStorage {
    fn with_state(&self, operation: &mut dyn FnMut(&mut BrokerState)) {
        let mut state = self.state.lock().expect("broker state lock poisoned");
        operation(&mut state);

        let changes = state.persistence_changes();
        if changes.is_empty() {
            return;
        }
        let mut projection = self
            .projection
            .lock()
            .expect("sqlite projection lock poisoned");
        let patches = prepare_patches(&projection, &state, &changes);
        if patches.is_empty() {
            state.take_persistence_changes();
            return;
        }
        let mut connection = self.connection.lock().expect("sqlite lock poisoned");
        for patch in patches {
            persist_patch(&mut connection, &patch).expect("persist broker patch to sqlite");
            projection
                .apply_patch(&patch)
                .expect("apply persisted sqlite patch");
        }
        state.take_persistence_changes();
    }

    fn read_state(&self, operation: &mut dyn FnMut(&BrokerState)) {
        let state = self.state.lock().expect("broker state lock poisoned");
        operation(&state);
    }
}

fn load_state(connection: &Connection) -> rusqlite::Result<BrokerState> {
    let mut state = BrokerState::default();
    load_sessions(connection, &mut state)?;
    load_subscriptions(connection, &mut state)?;
    load_retained(connection, &mut state)?;
    load_outbound_inflight(connection, &mut state)?;
    load_outbound_pubrel(connection, &mut state)?;
    load_offline_queue(connection, &mut state)?;
    Ok(state)
}

fn load_sessions(connection: &Connection, state: &mut BrokerState) -> rusqlite::Result<()> {
    let mut statement = connection.prepare(
        "SELECT client_id, session_expiry_interval, expires_at_ms, next_packet_id, next_offline_sequence FROM sessions",
    )?;
    let rows = statement.query_map([], |row| {
        let client_id: String = row.get(0)?;
        let session_expiry_interval: u32 = row.get(1)?;
        let expires_at_ms: Option<i64> = row.get(2)?;
        let next_packet_id: u16 = row.get(3)?;
        let next_offline_sequence: i64 = row.get(4)?;
        let next_offline_sequence = u64::try_from(next_offline_sequence)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, next_offline_sequence))?;
        let mut session = SessionEntry::disconnected(
            session_expiry_interval,
            expires_at_ms.map(|value| value as u64),
        );
        session.next_packet_id = next_packet_id;
        session.next_offline_sequence = next_offline_sequence;
        Ok((client_id, session))
    })?;

    for row in rows {
        let (client_id, session) = row?;
        state.sessions_by_client_id.insert(client_id, session);
    }
    Ok(())
}

fn load_outbound_inflight(
    connection: &Connection,
    state: &mut BrokerState,
) -> rusqlite::Result<()> {
    let mut statement = connection.prepare(
        "SELECT client_id, packet_id, qos, packet, expires_at_ms FROM outbound_inflight",
    )?;
    let rows = statement.query_map([], |row| {
        let client_id: String = row.get(0)?;
        let packet_id: u16 = row.get(1)?;
        let qos: u8 = row.get(2)?;
        let packet: Vec<u8> = row.get(3)?;
        let expires_at_ms: Option<i64> = row.get(4)?;
        Ok((
            client_id,
            packet_id,
            qos,
            packet,
            expires_at_ms.map(|value| value as u64),
        ))
    })?;

    for row in rows {
        let (client_id, packet_id, qos, packet, expires_at_ms) = row?;
        let Some(packet) = decode_publish(&packet) else {
            continue;
        };
        let Some(session) = state.sessions_by_client_id.get_mut(&client_id) else {
            continue;
        };
        match qos_from_u8(qos) {
            QoS::AtLeastOnce => {
                session.outbound_qos1.insert(
                    packet_id,
                    PendingPublish {
                        packet,
                        expires_at_ms,
                    },
                );
            }
            QoS::ExactlyOnce => {
                session.outbound_qos2_publish.insert(
                    packet_id,
                    PendingPublish {
                        packet,
                        expires_at_ms,
                    },
                );
            }
            QoS::AtMostOnce => {}
        }
    }
    Ok(())
}

fn load_outbound_pubrel(connection: &Connection, state: &mut BrokerState) -> rusqlite::Result<()> {
    let mut statement = connection.prepare("SELECT client_id, packet_id FROM outbound_pubrel")?;
    let rows = statement.query_map([], |row| {
        let client_id: String = row.get(0)?;
        let packet_id: u16 = row.get(1)?;
        Ok((client_id, packet_id))
    })?;

    for row in rows {
        let (client_id, packet_id) = row?;
        if let Some(session) = state.sessions_by_client_id.get_mut(&client_id) {
            session.outbound_qos2_pubrel.insert(packet_id);
        }
    }
    Ok(())
}

fn load_offline_queue(connection: &Connection, state: &mut BrokerState) -> rusqlite::Result<()> {
    let mut statement = connection.prepare(
        "SELECT client_id, sequence, packet, expires_at_ms FROM offline_queue ORDER BY client_id, sequence",
    )?;
    let rows = statement.query_map([], |row| {
        let client_id: String = row.get(0)?;
        let sequence: i64 = row.get(1)?;
        let sequence = u64::try_from(sequence)
            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(1, sequence))?;
        let packet: Vec<u8> = row.get(2)?;
        let expires_at_ms: Option<i64> = row.get(3)?;
        Ok((
            client_id,
            sequence,
            packet,
            expires_at_ms.map(|value| value as u64),
        ))
    })?;

    for row in rows {
        let (client_id, sequence, packet, expires_at_ms) = row?;
        let Some(mut packet) = decode_publish(&packet) else {
            continue;
        };
        packet.packet_id = None;
        if let Some(session) = state.sessions_by_client_id.get_mut(&client_id) {
            session.offline_queue.push_back(QueuedPublish {
                sequence,
                pending: PendingPublish {
                    packet,
                    expires_at_ms,
                },
            });
        }
    }
    Ok(())
}

fn load_subscriptions(connection: &Connection, state: &mut BrokerState) -> rusqlite::Result<()> {
    let mut statement = connection.prepare(
        r#"
        SELECT client_id, topic_filter, maximum_qos, no_local, retain_as_published, retain_handling, subscription_identifier, match_filter, shared_group
        FROM subscriptions
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        let maximum_qos: u8 = row.get(2)?;
        let no_local: u8 = row.get(3)?;
        let retain_as_published: u8 = row.get(4)?;
        let filter: String = row.get(1)?;
        let persisted_match_filter: String = row.get(7)?;
        let match_filter = if persisted_match_filter.is_empty() {
            crate::protocol::shared_subscription_filter(&filter)
                .unwrap_or(&filter)
                .to_string()
        } else {
            persisted_match_filter
        };
        Ok(SubscriptionEntry {
            client_id: row.get(0)?,
            filter,
            match_filter,
            shared_group: row.get(8)?,
            options: SubscriptionOptions {
                maximum_qos: qos_from_u8(maximum_qos),
                no_local: no_local != 0,
                retain_as_published: retain_as_published != 0,
                retain_handling: row.get(5)?,
            },
            subscription_identifier: row.get::<_, Option<u32>>(6)?,
        })
    })?;

    for row in rows {
        state.subscriptions.push(row?);
    }
    Ok(())
}

fn load_retained(connection: &Connection, state: &mut BrokerState) -> rusqlite::Result<()> {
    let mut statement =
        connection.prepare("SELECT topic_name, packet, expires_at_ms FROM retained_messages")?;
    let rows = statement.query_map([], |row| {
        let topic_name: String = row.get(0)?;
        let packet: Vec<u8> = row.get(1)?;
        let expires_at_ms: Option<i64> = row.get(2)?;
        Ok((topic_name, packet, expires_at_ms.map(|value| value as u64)))
    })?;

    for row in rows {
        let (topic_name, packet, expires_at_ms) = row?;
        if let Some(mut message) = decode_retained(&packet) {
            message.expires_at_ms = expires_at_ms;
            state.retained.insert(topic_name, message);
        }
    }
    Ok(())
}

fn persist_patch(connection: &mut Connection, patch: &StoragePatch) -> rusqlite::Result<()> {
    match patch {
        StoragePatch::Client(patch) => persist_client_patch(connection, patch),
        StoragePatch::Retained(patch) => persist_retained_patch(connection, patch),
    }
}

fn persist_client_patch(connection: &mut Connection, patch: &ClientPatch) -> rusqlite::Result<()> {
    patch.validate().expect("valid sqlite client patch");
    let transaction = connection.transaction()?;

    match patch.mode {
        ClientPatchMode::Delete => {
            delete_session(&transaction, &patch.client_id)?;
        }
        ClientPatchMode::Reset => {
            delete_session(&transaction, &patch.client_id)?;
            persist_client_delta(&transaction, patch)?;
        }
        ClientPatchMode::Merge => persist_client_delta(&transaction, patch)?,
    }

    transaction.commit()
}

fn delete_session(transaction: &Transaction<'_>, client_id: &str) -> rusqlite::Result<()> {
    transaction.execute(
        "DELETE FROM sessions WHERE client_id = ?1",
        params![client_id],
    )?;
    Ok(())
}

fn persist_client_delta(
    transaction: &Transaction<'_>,
    patch: &ClientPatch,
) -> rusqlite::Result<()> {
    if let Some(session) = &patch.session {
        upsert_session(transaction, &patch.client_id, session)?;
    }

    delete_subscriptions(transaction, &patch.client_id, &patch.subscription_deletes)?;
    upsert_subscriptions(transaction, &patch.subscription_upserts)?;

    if let Some(sequence) = patch.offline_remove_through {
        transaction.execute(
            "DELETE FROM offline_queue WHERE client_id = ?1 AND sequence <= ?2",
            params![patch.client_id, sequence as i64],
        )?;
    }
    upsert_offline(transaction, &patch.client_id, &patch.offline_append)?;

    delete_inflight(
        transaction,
        &patch.client_id,
        QoS::AtLeastOnce,
        &patch.qos1_deletes,
    )?;
    upsert_inflight(
        transaction,
        &patch.client_id,
        QoS::AtLeastOnce,
        &patch.qos1_upserts,
    )?;
    delete_inflight(
        transaction,
        &patch.client_id,
        QoS::ExactlyOnce,
        &patch.qos2_publish_deletes,
    )?;
    upsert_inflight(
        transaction,
        &patch.client_id,
        QoS::ExactlyOnce,
        &patch.qos2_publish_upserts,
    )?;

    {
        let mut statement = transaction
            .prepare("DELETE FROM outbound_pubrel WHERE client_id = ?1 AND packet_id = ?2")?;
        for packet_id in &patch.pubrel_remove {
            statement.execute(params![patch.client_id, packet_id])?;
        }
    }
    {
        let mut statement = transaction.prepare(
            r#"
            INSERT INTO outbound_pubrel (client_id, packet_id) VALUES (?1, ?2)
            ON CONFLICT(client_id, packet_id) DO NOTHING
            "#,
        )?;
        for packet_id in &patch.pubrel_add {
            statement.execute(params![patch.client_id, packet_id])?;
        }
    }
    Ok(())
}

fn upsert_session(
    transaction: &Transaction<'_>,
    client_id: &str,
    session: &SessionSnapshot,
) -> rusqlite::Result<()> {
    transaction.execute(
        r#"
        INSERT INTO sessions (
            client_id,
            session_expiry_interval,
            expires_at_ms,
            next_packet_id,
            next_offline_sequence
        ) VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(client_id) DO UPDATE SET
            session_expiry_interval = excluded.session_expiry_interval,
            expires_at_ms = excluded.expires_at_ms,
            next_packet_id = excluded.next_packet_id,
            next_offline_sequence = excluded.next_offline_sequence
        "#,
        params![
            client_id,
            session.session_expiry_interval,
            session.expires_at_ms.map(|value| value as i64),
            session.next_packet_id,
            session.next_offline_sequence as i64,
        ],
    )?;
    Ok(())
}

fn delete_subscriptions(
    transaction: &Transaction<'_>,
    client_id: &str,
    filters: &[String],
) -> rusqlite::Result<()> {
    let mut statement = transaction
        .prepare("DELETE FROM subscriptions WHERE client_id = ?1 AND topic_filter = ?2")?;
    for filter in filters {
        statement.execute(params![client_id, filter])?;
    }
    Ok(())
}

fn upsert_subscriptions(
    transaction: &Transaction<'_>,
    subscriptions: &[SubscriptionSnapshot],
) -> rusqlite::Result<()> {
    let mut statement = transaction.prepare(
        r#"
        INSERT INTO subscriptions (
            client_id,
            topic_filter,
            match_filter,
            shared_group,
            maximum_qos,
            no_local,
            retain_as_published,
            retain_handling,
            subscription_identifier
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT(client_id, topic_filter) DO UPDATE SET
            match_filter = excluded.match_filter,
            shared_group = excluded.shared_group,
            maximum_qos = excluded.maximum_qos,
            no_local = excluded.no_local,
            retain_as_published = excluded.retain_as_published,
            retain_handling = excluded.retain_handling,
            subscription_identifier = excluded.subscription_identifier
        "#,
    )?;
    for subscription in subscriptions {
        statement.execute(params![
            subscription.client_id,
            subscription.filter,
            subscription.match_filter,
            subscription.shared_group,
            qos_to_u8(subscription.maximum_qos),
            bool_to_u8(subscription.no_local),
            bool_to_u8(subscription.retain_as_published),
            subscription.retain_handling,
            subscription.subscription_identifier,
        ])?;
    }
    Ok(())
}

fn upsert_offline(
    transaction: &Transaction<'_>,
    client_id: &str,
    queued: &[super::delta::QueuedSnapshot],
) -> rusqlite::Result<()> {
    let mut statement = transaction.prepare(
        r#"
        INSERT INTO offline_queue (client_id, sequence, packet, expires_at_ms)
        VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT(client_id, sequence) DO UPDATE SET
            packet = excluded.packet,
            expires_at_ms = excluded.expires_at_ms
        "#,
    )?;
    for queued in queued {
        statement.execute(params![
            client_id,
            queued.sequence as i64,
            encode_publish(&queued.pending.packet),
            queued.pending.expires_at_ms.map(|value| value as i64),
        ])?;
    }
    Ok(())
}

fn delete_inflight(
    transaction: &Transaction<'_>,
    client_id: &str,
    qos: QoS,
    packet_ids: &std::collections::BTreeSet<u16>,
) -> rusqlite::Result<()> {
    let mut statement = transaction.prepare(
        "DELETE FROM outbound_inflight WHERE client_id = ?1 AND packet_id = ?2 AND qos = ?3",
    )?;
    for packet_id in packet_ids {
        statement.execute(params![client_id, packet_id, qos_to_u8(qos)])?;
    }
    Ok(())
}

fn upsert_inflight(
    transaction: &Transaction<'_>,
    client_id: &str,
    qos: QoS,
    entries: &std::collections::BTreeMap<u16, PendingSnapshot>,
) -> rusqlite::Result<()> {
    let mut statement = transaction.prepare(
        r#"
        INSERT INTO outbound_inflight (client_id, packet_id, qos, packet, expires_at_ms)
        VALUES (?1, ?2, ?3, ?4, ?5)
        ON CONFLICT(client_id, packet_id, qos) DO UPDATE SET
            packet = excluded.packet,
            expires_at_ms = excluded.expires_at_ms
        "#,
    )?;
    for (packet_id, pending) in entries {
        statement.execute(params![
            client_id,
            packet_id,
            qos_to_u8(qos),
            encode_publish(&pending.packet),
            pending.expires_at_ms.map(|value| value as i64),
        ])?;
    }
    Ok(())
}

fn persist_retained_patch(
    connection: &mut Connection,
    patch: &RetainedPatch,
) -> rusqlite::Result<()> {
    let transaction = connection.transaction()?;
    if let Some(message) = &patch.message {
        transaction.execute(
            r#"
            INSERT INTO retained_messages (topic_name, packet, expires_at_ms)
            VALUES (?1, ?2, ?3)
            ON CONFLICT(topic_name) DO UPDATE SET
                packet = excluded.packet,
                expires_at_ms = excluded.expires_at_ms
            "#,
            params![
                patch.topic_name,
                encode_retained(message),
                message.expires_at_ms.map(|value| value as i64),
            ],
        )?;
    } else {
        transaction.execute(
            "DELETE FROM retained_messages WHERE topic_name = ?1",
            params![patch.topic_name],
        )?;
    }
    transaction.commit()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::runtime::retained_store::RetainedMessage;
    use bytes::Bytes;
    use rs_netty::codec::PublishPacket;

    fn sqlite_path(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "pulse-sqlite-storage-{}-{label}.db",
            std::process::id()
        ))
    }

    fn remove_sqlite_files(path: &Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    fn pending(
        qos: QoS,
        topic_name: &str,
        packet_id: Option<u16>,
        payload: &'static [u8],
    ) -> PendingPublish {
        PendingPublish {
            packet: PublishPacket {
                dup: false,
                qos,
                retain: false,
                topic_name: topic_name.to_string(),
                packet_id,
                properties: Vec::new(),
                payload: Bytes::from_static(payload),
            },
            expires_at_ms: None,
        }
    }

    fn subscription(client_id: &str, filter: &str) -> SubscriptionEntry {
        SubscriptionEntry {
            client_id: client_id.to_string(),
            filter: filter.to_string(),
            match_filter: filter.to_string(),
            shared_group: None,
            options: SubscriptionOptions {
                maximum_qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
            },
            subscription_identifier: None,
        }
    }

    #[test]
    fn sqlite_storage_loads_persisted_sessions_subscriptions_and_retained_messages() {
        let path = sqlite_path("roundtrip");
        remove_sqlite_files(&path);

        let storage = SqliteStorage::open(&path).expect("open sqlite storage");
        storage.with_state(&mut |state| {
            state.sessions_by_client_id.insert(
                "client".to_string(),
                SessionEntry::disconnected(60, Some(i64::MAX as u64)),
            );
            let session = state
                .sessions_by_client_id
                .get_mut("client")
                .expect("session");
            session.next_packet_id = 7;
            session.outbound_qos1.insert(
                1,
                PendingPublish {
                    packet: PublishPacket {
                        dup: false,
                        qos: QoS::AtLeastOnce,
                        retain: false,
                        topic_name: "devices/inflight".to_string(),
                        packet_id: Some(1),
                        properties: Vec::new(),
                        payload: Bytes::from_static(b"inflight"),
                    },
                    expires_at_ms: Some(456),
                },
            );
            session.offline_queue.push_back(QueuedPublish {
                sequence: 3,
                pending: PendingPublish {
                    packet: PublishPacket {
                        dup: false,
                        qos: QoS::AtLeastOnce,
                        retain: false,
                        topic_name: "devices/offline".to_string(),
                        packet_id: None,
                        properties: Vec::new(),
                        payload: Bytes::from_static(b"offline"),
                    },
                    expires_at_ms: Some(789),
                },
            });
            session.next_offline_sequence = 4;
            state.subscriptions.push(SubscriptionEntry {
                client_id: "client".to_string(),
                filter: "devices/one".to_string(),
                match_filter: "devices/one".to_string(),
                shared_group: None,
                options: SubscriptionOptions {
                    maximum_qos: QoS::ExactlyOnce,
                    no_local: true,
                    retain_as_published: true,
                    retain_handling: 1,
                },
                subscription_identifier: Some(42),
            });
            state.retained.insert(
                "devices/one".to_string(),
                RetainedMessage::new(
                    QoS::AtLeastOnce,
                    "devices/one".to_string(),
                    Vec::new(),
                    Bytes::from_static(b"hello"),
                    Some(999),
                ),
            );
            state.mark_client_reset("client");
            state.mark_retained_changed("devices/one");
        });
        drop(storage);

        let storage = SqliteStorage::open(&path).expect("reopen sqlite storage");
        storage.with_state(&mut |state| {
            let session = state
                .sessions_by_client_id
                .get("client")
                .expect("persisted session");
            assert_eq!(session.session_expiry_interval, 60);
            assert_eq!(session.expires_at_ms, Some(i64::MAX as u64));
            assert_eq!(session.next_packet_id, 7);
            let inflight = session.outbound_qos1.get(&1).expect("persisted inflight");
            assert_eq!(inflight.packet.payload, Bytes::from_static(b"inflight"));
            assert_eq!(inflight.expires_at_ms, Some(456));
            let offline = session.offline_queue.front().expect("persisted offline");
            assert_eq!(offline.sequence, 3);
            assert_eq!(
                offline.pending.packet.payload,
                Bytes::from_static(b"offline")
            );
            assert_eq!(offline.pending.packet.packet_id, None);
            assert_eq!(offline.pending.expires_at_ms, Some(789));
            assert_eq!(session.next_offline_sequence, 4);

            let subscription = state
                .subscriptions
                .iter()
                .find(|subscription| subscription.client_id == "client")
                .expect("persisted subscription");
            assert_eq!(subscription.filter, "devices/one");
            assert_eq!(subscription.match_filter, "devices/one");
            assert_eq!(subscription.shared_group, None);
            assert_eq!(subscription.options.maximum_qos, QoS::ExactlyOnce);
            assert!(subscription.options.no_local);
            assert!(subscription.options.retain_as_published);
            assert_eq!(subscription.options.retain_handling, 1);
            assert_eq!(subscription.subscription_identifier, Some(42));

            let retained = state
                .retained
                .get("devices/one")
                .expect("persisted retained");
            assert_eq!(retained.qos, QoS::AtLeastOnce);
            assert_eq!(retained.payload, Bytes::from_static(b"hello"));
            assert_eq!(retained.expires_at_ms, Some(999));
        });

        remove_sqlite_files(&path);
    }

    #[test]
    fn sqlite_merge_is_scoped_and_delete_cascades_one_client() {
        let path = sqlite_path("incremental-client-patch");
        remove_sqlite_files(&path);
        let storage = SqliteStorage::open(&path).expect("open sqlite storage");

        storage.with_state(&mut |state| {
            let mut client_a = SessionEntry::disconnected(60, None);
            client_a.next_packet_id = 3;
            client_a.next_offline_sequence = 2;
            client_a.offline_queue.push_back(QueuedPublish {
                sequence: 0,
                pending: pending(QoS::AtLeastOnce, "offline/zero", None, b"zero"),
            });
            client_a.offline_queue.push_back(QueuedPublish {
                sequence: 1,
                pending: pending(QoS::AtLeastOnce, "offline/one", None, b"one"),
            });
            client_a.outbound_qos1.insert(
                1,
                pending(QoS::AtLeastOnce, "inflight/qos1", Some(1), b"qos1"),
            );
            client_a.outbound_qos2_publish.insert(
                2,
                pending(QoS::ExactlyOnce, "inflight/qos2", Some(2), b"qos2"),
            );
            client_a.outbound_qos2_pubrel.insert(3);
            state
                .sessions_by_client_id
                .insert("client-a".to_string(), client_a);
            let mut client_b = SessionEntry::disconnected(60, None);
            client_b.next_offline_sequence = 1;
            client_b.offline_queue.push_back(QueuedPublish {
                sequence: 0,
                pending: pending(QoS::AtLeastOnce, "b/offline", None, b"protected"),
            });
            client_b.outbound_qos1.insert(
                10,
                pending(QoS::AtLeastOnce, "b/qos1", Some(10), b"protected"),
            );
            client_b.outbound_qos2_publish.insert(
                11,
                pending(QoS::ExactlyOnce, "b/qos2", Some(11), b"protected"),
            );
            client_b.outbound_qos2_pubrel.insert(12);
            state
                .sessions_by_client_id
                .insert("client-b".to_string(), client_b);
            state.subscriptions.push(subscription("client-a", "a/old"));
            state.subscriptions.push(subscription("client-a", "a/keep"));
            state
                .subscriptions
                .push(subscription("client-b", "b/protected"));
            state.mark_client_reset("client-a");
            state.mark_client_reset("client-b");
        });

        {
            let connection = storage.connection.lock().expect("sqlite lock");
            for table in [
                "sessions",
                "subscriptions",
                "offline_queue",
                "outbound_inflight",
                "outbound_pubrel",
            ] {
                connection
                    .execute_batch(&format!(
                        r#"
                        CREATE TRIGGER protect_client_b_{table}_insert
                        BEFORE INSERT ON {table}
                        WHEN NEW.client_id = 'client-b'
                        BEGIN
                            SELECT RAISE(ABORT, 'unrelated row was inserted');
                        END;
                        CREATE TRIGGER protect_client_b_{table}_update
                        BEFORE UPDATE ON {table}
                        WHEN OLD.client_id = 'client-b' OR NEW.client_id = 'client-b'
                        BEGIN
                            SELECT RAISE(ABORT, 'unrelated row was updated');
                        END;
                        CREATE TRIGGER protect_client_b_{table}_delete
                        BEFORE DELETE ON {table}
                        WHEN OLD.client_id = 'client-b'
                        BEGIN
                            SELECT RAISE(ABORT, 'unrelated row was deleted');
                        END;
                        "#
                    ))
                    .expect("protect unrelated rows");
            }
        }

        storage.with_state(&mut |state| {
            let client_a = state
                .sessions_by_client_id
                .get_mut("client-a")
                .expect("client-a session");
            client_a.next_packet_id = 7;
            client_a.next_offline_sequence = 3;
            client_a.offline_queue.pop_front();
            client_a.offline_queue.push_back(QueuedPublish {
                sequence: 2,
                pending: pending(QoS::AtLeastOnce, "offline/two", None, b"two"),
            });
            client_a.outbound_qos1.remove(&1);
            client_a.outbound_qos1.insert(
                4,
                pending(QoS::AtLeastOnce, "inflight/qos1-new", Some(4), b"four"),
            );
            client_a.outbound_qos2_publish.remove(&2);
            client_a.outbound_qos2_publish.insert(
                5,
                pending(QoS::ExactlyOnce, "inflight/qos2-new", Some(5), b"five"),
            );
            client_a.outbound_qos2_pubrel.remove(&3);
            client_a.outbound_qos2_pubrel.insert(6);
            state.subscriptions.retain(|subscription| {
                subscription.client_id != "client-a" || subscription.filter != "a/old"
            });
            state.subscriptions.push(subscription("client-a", "a/new"));
            state.mark_client_changed("client-a");
        });

        {
            let connection = storage.connection.lock().expect("sqlite lock");
            let client_a: (u16, i64) = connection
                .query_row(
                    "SELECT next_packet_id, next_offline_sequence FROM sessions WHERE client_id = 'client-a'",
                    [],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .expect("load client-a session");
            assert_eq!(client_a, (7, 3));
            let client_b_count: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM sessions WHERE client_id = 'client-b'",
                    [],
                    |row| row.get(0),
                )
                .expect("count protected session");
            assert_eq!(client_b_count, 1);

            let offline_sequences = connection
                .prepare(
                    "SELECT sequence FROM offline_queue WHERE client_id = 'client-a' ORDER BY sequence",
                )
                .expect("prepare offline query")
                .query_map([], |row| row.get::<_, i64>(0))
                .expect("query offline")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("collect offline");
            assert_eq!(offline_sequences, vec![1, 2]);

            let inflight = connection
                .prepare(
                    "SELECT packet_id, qos FROM outbound_inflight WHERE client_id = 'client-a' ORDER BY qos",
                )
                .expect("prepare inflight query")
                .query_map([], |row| Ok((row.get::<_, u16>(0)?, row.get::<_, u8>(1)?)))
                .expect("query inflight")
                .collect::<rusqlite::Result<Vec<_>>>()
                .expect("collect inflight");
            assert_eq!(inflight, vec![(4, 1), (5, 2)]);
            let pubrel: u16 = connection
                .query_row(
                    "SELECT packet_id FROM outbound_pubrel WHERE client_id = 'client-a'",
                    [],
                    |row| row.get(0),
                )
                .expect("load pubrel");
            assert_eq!(pubrel, 6);
            let protected_subscription_count: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM subscriptions WHERE client_id = 'client-b' AND topic_filter = 'b/protected'",
                    [],
                    |row| row.get(0),
                )
                .expect("count protected subscription");
            assert_eq!(protected_subscription_count, 1);
        }

        storage.with_state(&mut |state| {
            state.sessions_by_client_id.remove("client-a");
            state
                .subscriptions
                .retain(|subscription| subscription.client_id != "client-a");
            state.mark_client_reset("client-a");
        });

        {
            let connection = storage.connection.lock().expect("sqlite lock");
            for table in [
                "sessions",
                "subscriptions",
                "offline_queue",
                "outbound_inflight",
                "outbound_pubrel",
            ] {
                let count: i64 = connection
                    .query_row(
                        &format!("SELECT COUNT(*) FROM {table} WHERE client_id = 'client-a'"),
                        [],
                        |row| row.get(0),
                    )
                    .expect("count deleted client rows");
                assert_eq!(count, 0, "{table} retained deleted client rows");
            }
            let client_b_count: i64 = connection
                .query_row(
                    "SELECT COUNT(*) FROM sessions WHERE client_id = 'client-b'",
                    [],
                    |row| row.get(0),
                )
                .expect("count protected session after delete");
            assert_eq!(client_b_count, 1);
            for (table, expected) in [
                ("subscriptions", 1),
                ("offline_queue", 1),
                ("outbound_inflight", 2),
                ("outbound_pubrel", 1),
            ] {
                let count: i64 = connection
                    .query_row(
                        &format!("SELECT COUNT(*) FROM {table} WHERE client_id = 'client-b'"),
                        [],
                        |row| row.get(0),
                    )
                    .expect("count protected client rows after delete");
                assert_eq!(count, expected, "{table} changed unrelated client rows");
            }
        }

        drop(storage);
        remove_sqlite_files(&path);
    }
}
