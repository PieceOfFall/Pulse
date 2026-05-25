use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

use bytes::{Bytes, BytesMut};
use rs_netty::codec::{
    Decoder, Encoder, MqttCodec, MqttPacket, PublishPacket, QoS, SubscriptionOptions,
};
use rusqlite::{Connection, params};

use super::BrokerStorage;
use crate::broker::state::{BrokerState, RetainedMessage, SessionEntry, SubscriptionEntry};

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
}

fn configure_connection(connection: &Connection) -> rusqlite::Result<()> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(())
}

fn migrate(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS sessions (
            client_id TEXT PRIMARY KEY,
            session_expiry_interval INTEGER NOT NULL,
            expires_at_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            client_id TEXT NOT NULL,
            topic_filter TEXT NOT NULL,
            maximum_qos INTEGER NOT NULL,
            no_local INTEGER NOT NULL,
            retain_as_published INTEGER NOT NULL,
            retain_handling INTEGER NOT NULL,
            PRIMARY KEY (client_id, topic_filter),
            FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS retained_messages (
            topic_name TEXT PRIMARY KEY,
            packet BLOB NOT NULL
        );
        "#,
    )
}

fn load_state(connection: &Connection) -> rusqlite::Result<BrokerState> {
    let mut state = BrokerState::default();
    load_sessions(connection, &mut state)?;
    load_subscriptions(connection, &mut state)?;
    load_retained(connection, &mut state)?;
    Ok(state)
}

fn load_sessions(connection: &Connection, state: &mut BrokerState) -> rusqlite::Result<()> {
    let mut statement = connection
        .prepare("SELECT client_id, session_expiry_interval, expires_at_ms FROM sessions")?;
    let rows = statement.query_map([], |row| {
        let client_id: String = row.get(0)?;
        let session_expiry_interval: u32 = row.get(1)?;
        let expires_at_ms: Option<i64> = row.get(2)?;
        Ok((
            client_id,
            SessionEntry::disconnected(
                session_expiry_interval,
                expires_at_ms.map(|value| value as u64),
            ),
        ))
    })?;

    for row in rows {
        let (client_id, session) = row?;
        state.sessions_by_client_id.insert(client_id, session);
    }
    Ok(())
}

fn load_subscriptions(connection: &Connection, state: &mut BrokerState) -> rusqlite::Result<()> {
    let mut statement = connection.prepare(
        r#"
        SELECT client_id, topic_filter, maximum_qos, no_local, retain_as_published, retain_handling
        FROM subscriptions
        "#,
    )?;
    let rows = statement.query_map([], |row| {
        let maximum_qos: u8 = row.get(2)?;
        let no_local: u8 = row.get(3)?;
        let retain_as_published: u8 = row.get(4)?;
        Ok(SubscriptionEntry {
            client_id: row.get(0)?,
            filter: row.get(1)?,
            options: SubscriptionOptions {
                maximum_qos: qos_from_u8(maximum_qos),
                no_local: no_local != 0,
                retain_as_published: retain_as_published != 0,
                retain_handling: row.get(5)?,
            },
        })
    })?;

    for row in rows {
        state.subscriptions.push(row?);
    }
    Ok(())
}

fn load_retained(connection: &Connection, state: &mut BrokerState) -> rusqlite::Result<()> {
    let mut statement = connection.prepare("SELECT topic_name, packet FROM retained_messages")?;
    let rows = statement.query_map([], |row| {
        let topic_name: String = row.get(0)?;
        let packet: Vec<u8> = row.get(1)?;
        Ok((topic_name, packet))
    })?;

    for row in rows {
        let (topic_name, packet) = row?;
        if let Some(message) = decode_retained(&packet) {
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

    {
        let mut statement = transaction.prepare(
            "INSERT INTO sessions (client_id, session_expiry_interval, expires_at_ms) VALUES (?1, ?2, ?3)",
        )?;
        for (client_id, session) in &state.sessions_by_client_id {
            statement.execute(params![
                client_id,
                session.session_expiry_interval,
                session.expires_at_ms.map(|value| value as i64)
            ])?;
        }
    }

    {
        let mut statement = transaction.prepare(
            r#"
            INSERT INTO subscriptions (
                client_id,
                topic_filter,
                maximum_qos,
                no_local,
                retain_as_published,
                retain_handling
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            "#,
        )?;
        for subscription in &state.subscriptions {
            statement.execute(params![
                subscription.client_id,
                subscription.filter,
                qos_to_u8(subscription.options.maximum_qos),
                bool_to_u8(subscription.options.no_local),
                bool_to_u8(subscription.options.retain_as_published),
                subscription.options.retain_handling,
            ])?;
        }
    }

    {
        let mut statement = transaction
            .prepare("INSERT INTO retained_messages (topic_name, packet) VALUES (?1, ?2)")?;
        for (topic_name, message) in &state.retained {
            statement.execute(params![topic_name, encode_retained(message)])?;
        }
    }

    transaction.commit()
}

fn encode_retained(message: &RetainedMessage) -> Vec<u8> {
    let mut codec = MqttCodec::new();
    let mut buffer = BytesMut::new();
    let packet_id = if message.qos == QoS::AtMostOnce {
        None
    } else {
        Some(1)
    };
    codec
        .encode(
            MqttPacket::Publish(PublishPacket {
                dup: false,
                qos: message.qos,
                retain: true,
                topic_name: message.topic_name.clone(),
                packet_id,
                properties: message.properties.clone(),
                payload: message.payload.clone(),
            }),
            &mut buffer,
        )
        .expect("encode retained publish");
    buffer.to_vec()
}

fn decode_retained(packet: &[u8]) -> Option<RetainedMessage> {
    let mut codec = MqttCodec::new();
    let mut buffer = BytesMut::from(packet);
    let packet = codec.decode(&mut buffer).ok().flatten()?;
    let MqttPacket::Publish(packet) = packet else {
        return None;
    };

    Some(RetainedMessage {
        qos: packet.qos,
        topic_name: packet.topic_name,
        properties: packet.properties,
        payload: Bytes::copy_from_slice(&packet.payload),
    })
}

fn qos_to_u8(qos: QoS) -> u8 {
    match qos {
        QoS::AtMostOnce => 0,
        QoS::AtLeastOnce => 1,
        QoS::ExactlyOnce => 2,
    }
}

fn qos_from_u8(value: u8) -> QoS {
    match value {
        1 => QoS::AtLeastOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtMostOnce,
    }
}

fn bool_to_u8(value: bool) -> u8 {
    if value { 1 } else { 0 }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_storage_loads_persisted_sessions_subscriptions_and_retained_messages() {
        let path =
            std::env::temp_dir().join(format!("mqtt-rs-sqlite-storage-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let storage = SqliteStorage::open(&path).expect("open sqlite storage");
        storage.with_state(&mut |state| {
            state.sessions_by_client_id.insert(
                "client".to_string(),
                SessionEntry::disconnected(60, Some(123)),
            );
            state.subscriptions.push(SubscriptionEntry {
                client_id: "client".to_string(),
                filter: "devices/one".to_string(),
                options: SubscriptionOptions {
                    maximum_qos: QoS::ExactlyOnce,
                    no_local: true,
                    retain_as_published: true,
                    retain_handling: 1,
                },
            });
            state.retained.insert(
                "devices/one".to_string(),
                RetainedMessage {
                    qos: QoS::AtLeastOnce,
                    topic_name: "devices/one".to_string(),
                    properties: Vec::new(),
                    payload: Bytes::from_static(b"hello"),
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

            let subscription = state
                .subscriptions
                .iter()
                .find(|subscription| subscription.client_id == "client")
                .expect("persisted subscription");
            assert_eq!(subscription.filter, "devices/one");
            assert_eq!(subscription.options.maximum_qos, QoS::ExactlyOnce);
            assert!(subscription.options.no_local);
            assert!(subscription.options.retain_as_published);
            assert_eq!(subscription.options.retain_handling, 1);

            let retained = state
                .retained
                .get("devices/one")
                .expect("persisted retained");
            assert_eq!(retained.qos, QoS::AtLeastOnce);
            assert_eq!(retained.payload, Bytes::from_static(b"hello"));
        });

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }
}
