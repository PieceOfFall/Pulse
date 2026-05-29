use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

use rs_netty::codec::{QoS, SubscriptionOptions};
use rusqlite::{Connection, params};

use super::BrokerStorage;
use crate::broker::runtime::{
    message::PendingPublish,
    session_registry::{BrokerState, SessionEntry},
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
}

impl SqliteStorage {
    pub(crate) fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let connection = Connection::open(path)?;
        configure_connection(&connection)?;
        migrate(&connection)?;
        let state = load_state(&connection)?;

        Ok(Self {
            connection: Mutex::new(connection),
            state: Mutex::new(state),
        })
    }
}

impl BrokerStorage for SqliteStorage {
    fn with_state(&self, operation: &mut dyn FnMut(&mut BrokerState)) {
        let mut state = self.state.lock().expect("broker state lock poisoned");
        operation(&mut state);

        let mut connection = self.connection.lock().expect("sqlite lock poisoned");
        persist_state(&mut connection, &state).expect("persist broker state to sqlite");
    }

    fn with_transient_state(&self, operation: &mut dyn FnMut(&mut BrokerState)) {
        let mut state = self.state.lock().expect("broker state lock poisoned");
        operation(&mut state);
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
        "SELECT client_id, session_expiry_interval, expires_at_ms, next_packet_id FROM sessions",
    )?;
    let rows = statement.query_map([], |row| {
        let client_id: String = row.get(0)?;
        let session_expiry_interval: u32 = row.get(1)?;
        let expires_at_ms: Option<i64> = row.get(2)?;
        let next_packet_id: u16 = row.get(3)?;
        let mut session = SessionEntry::disconnected(
            session_expiry_interval,
            expires_at_ms.map(|value| value as u64),
        );
        session.next_packet_id = next_packet_id;
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
        "SELECT client_id, packet, expires_at_ms FROM offline_queue ORDER BY client_id, sequence",
    )?;
    let rows = statement.query_map([], |row| {
        let client_id: String = row.get(0)?;
        let packet: Vec<u8> = row.get(1)?;
        let expires_at_ms: Option<i64> = row.get(2)?;
        Ok((client_id, packet, expires_at_ms.map(|value| value as u64)))
    })?;

    for row in rows {
        let (client_id, packet, expires_at_ms) = row?;
        let Some(mut packet) = decode_publish(&packet) else {
            continue;
        };
        packet.packet_id = None;
        if let Some(session) = state.sessions_by_client_id.get_mut(&client_id) {
            session.offline_queue.push_back(PendingPublish {
                packet,
                expires_at_ms,
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

fn persist_state(
    connection: &mut MutexGuard<'_, Connection>,
    state: &BrokerState,
) -> rusqlite::Result<()> {
    let transaction = connection.transaction()?;
    transaction.execute("DELETE FROM subscriptions", [])?;
    transaction.execute("DELETE FROM sessions", [])?;
    transaction.execute("DELETE FROM retained_messages", [])?;
    transaction.execute("DELETE FROM outbound_inflight", [])?;
    transaction.execute("DELETE FROM outbound_pubrel", [])?;
    transaction.execute("DELETE FROM offline_queue", [])?;

    {
        let mut statement = transaction.prepare(
            "INSERT INTO sessions (client_id, session_expiry_interval, expires_at_ms, next_packet_id) VALUES (?1, ?2, ?3, ?4)",
        )?;
        for (client_id, session) in &state.sessions_by_client_id {
            statement.execute(params![
                client_id,
                session.session_expiry_interval,
                session.expires_at_ms.map(|value| value as i64),
                session.next_packet_id
            ])?;
        }
    }

    {
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
            "#,
        )?;
        for subscription in &state.subscriptions {
            statement.execute(params![
                subscription.client_id,
                subscription.filter,
                subscription.match_filter,
                subscription.shared_group,
                qos_to_u8(subscription.options.maximum_qos),
                bool_to_u8(subscription.options.no_local),
                bool_to_u8(subscription.options.retain_as_published),
                subscription.options.retain_handling,
                subscription.subscription_identifier,
            ])?;
        }
    }

    {
        let mut statement = transaction.prepare(
            "INSERT INTO retained_messages (topic_name, packet, expires_at_ms) VALUES (?1, ?2, ?3)",
        )?;
        for (topic_name, message) in state.retained.iter() {
            statement.execute(params![
                topic_name,
                encode_retained(message),
                message.expires_at_ms.map(|value| value as i64)
            ])?;
        }
    }

    {
        let mut inflight = transaction.prepare(
            "INSERT INTO outbound_inflight (client_id, packet_id, qos, packet, expires_at_ms) VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        let mut pubrel = transaction
            .prepare("INSERT INTO outbound_pubrel (client_id, packet_id) VALUES (?1, ?2)")?;
        let mut offline = transaction.prepare(
            "INSERT INTO offline_queue (client_id, sequence, packet, expires_at_ms) VALUES (?1, ?2, ?3, ?4)",
        )?;

        for (client_id, session) in &state.sessions_by_client_id {
            for (packet_id, pending) in &session.outbound_qos1 {
                inflight.execute(params![
                    client_id,
                    packet_id,
                    qos_to_u8(QoS::AtLeastOnce),
                    encode_publish(&pending.packet),
                    pending.expires_at_ms.map(|value| value as i64)
                ])?;
            }
            for (packet_id, pending) in &session.outbound_qos2_publish {
                inflight.execute(params![
                    client_id,
                    packet_id,
                    qos_to_u8(QoS::ExactlyOnce),
                    encode_publish(&pending.packet),
                    pending.expires_at_ms.map(|value| value as i64)
                ])?;
            }
            for packet_id in &session.outbound_qos2_pubrel {
                pubrel.execute(params![client_id, packet_id])?;
            }
            for (sequence, pending) in session.offline_queue.iter().enumerate() {
                offline.execute(params![
                    client_id,
                    sequence as i64,
                    encode_publish(&pending.packet),
                    pending.expires_at_ms.map(|value| value as i64)
                ])?;
            }
        }
    }

    transaction.commit()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::runtime::retained_store::RetainedMessage;
    use bytes::Bytes;
    use rs_netty::codec::PublishPacket;

    #[test]
    fn sqlite_storage_loads_persisted_sessions_subscriptions_and_retained_messages() {
        let path =
            std::env::temp_dir().join(format!("pulse-sqlite-storage-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let storage = SqliteStorage::open(&path).expect("open sqlite storage");
        storage.with_state(&mut |state| {
            state.sessions_by_client_id.insert(
                "client".to_string(),
                SessionEntry::disconnected(60, Some(123)),
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
            session.offline_queue.push_back(PendingPublish {
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
            });
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
                RetainedMessage {
                    qos: QoS::AtLeastOnce,
                    topic_name: "devices/one".to_string(),
                    properties: Vec::new(),
                    payload: Bytes::from_static(b"hello"),
                    expires_at_ms: Some(999),
                },
            );
        });
        drop(storage);

        let storage = SqliteStorage::open(&path).expect("reopen sqlite storage");
        storage.with_state(&mut |state| {
            let session = state
                .sessions_by_client_id
                .get("client")
                .expect("persisted session");
            assert_eq!(session.session_expiry_interval, 60);
            assert_eq!(session.expires_at_ms, Some(123));
            assert_eq!(session.next_packet_id, 7);
            let inflight = session.outbound_qos1.get(&1).expect("persisted inflight");
            assert_eq!(inflight.packet.payload, Bytes::from_static(b"inflight"));
            assert_eq!(inflight.expires_at_ms, Some(456));
            let offline = session.offline_queue.front().expect("persisted offline");
            assert_eq!(offline.packet.payload, Bytes::from_static(b"offline"));
            assert_eq!(offline.packet.packet_id, None);
            assert_eq!(offline.expires_at_ms, Some(789));

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

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }
}
