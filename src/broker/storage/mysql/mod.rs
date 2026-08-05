use std::sync::Mutex;

use mysql::{Pool, PooledConn, Transaction, TxOpts, params, prelude::Queryable};
use rs_netty::codec::{QoS, SubscriptionOptions};

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
    schema::migrate,
};

type OutboundInflightRow = (String, u16, u8, Vec<u8>, Option<u64>);
type SubscriptionRow = (
    String,
    String,
    u8,
    u8,
    u8,
    u8,
    Option<u32>,
    String,
    Option<String>,
);

pub(crate) struct MysqlStorage {
    pool: Pool,
    state: Mutex<BrokerState>,
    projection: Mutex<PersistentProjection>,
}

impl MysqlStorage {
    pub(crate) fn open(url: &str) -> mysql::Result<Self> {
        let pool = Pool::new(url)?;
        let mut connection = pool.get_conn()?;
        migrate(&mut connection)?;
        let loaded_state = load_state(&mut connection)?;
        let projection = PersistentProjection::from_state(&loaded_state);
        let state = projection.clone().into_state();

        Ok(Self {
            pool,
            state: Mutex::new(state),
            projection: Mutex::new(projection),
        })
    }
}

impl BrokerStorage for MysqlStorage {
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
            .expect("mysql projection lock poisoned");
        let patches = prepare_patches(&projection, &state, &changes);
        if patches.is_empty() {
            state.take_persistence_changes();
            return;
        }
        let mut connection = self.pool.get_conn().expect("get mysql connection");
        for patch in patches {
            persist_patch(&mut connection, &patch).expect("persist broker patch to mysql");
            projection
                .apply_patch(&patch)
                .expect("apply persisted mysql patch");
        }
        state.take_persistence_changes();
    }

    fn read_state(&self, operation: &mut dyn FnMut(&BrokerState)) {
        let state = self.state.lock().expect("broker state lock poisoned");
        operation(&state);
    }
}

fn load_state(connection: &mut PooledConn) -> mysql::Result<BrokerState> {
    let mut state = BrokerState::default();
    load_sessions(connection, &mut state)?;
    load_subscriptions(connection, &mut state)?;
    load_retained(connection, &mut state)?;
    load_outbound_inflight(connection, &mut state)?;
    load_outbound_pubrel(connection, &mut state)?;
    load_offline_queue(connection, &mut state)?;
    Ok(state)
}

fn load_sessions(connection: &mut PooledConn, state: &mut BrokerState) -> mysql::Result<()> {
    let rows: Vec<(String, u32, Option<u64>, u16, u64)> = connection.query(
        "SELECT client_id, session_expiry_interval, expires_at_ms, next_packet_id, next_offline_sequence FROM sessions",
    )?;
    for (
        client_id,
        session_expiry_interval,
        expires_at_ms,
        next_packet_id,
        next_offline_sequence,
    ) in rows
    {
        if next_offline_sequence > i64::MAX as u64 {
            return Err(invalid_data(
                "MySQL session offline sequence exceeds SQLite range",
            ));
        }
        let mut session = SessionEntry::disconnected(session_expiry_interval, expires_at_ms);
        session.next_packet_id = next_packet_id;
        session.next_offline_sequence = next_offline_sequence;
        state.sessions_by_client_id.insert(client_id, session);
    }
    Ok(())
}

fn load_outbound_inflight(
    connection: &mut PooledConn,
    state: &mut BrokerState,
) -> mysql::Result<()> {
    let rows: Vec<OutboundInflightRow> = connection
        .query("SELECT client_id, packet_id, qos, packet, expires_at_ms FROM outbound_inflight")?;
    for (client_id, packet_id, qos, packet, expires_at_ms) in rows {
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

fn load_outbound_pubrel(connection: &mut PooledConn, state: &mut BrokerState) -> mysql::Result<()> {
    let rows: Vec<(String, u16)> =
        connection.query("SELECT client_id, packet_id FROM outbound_pubrel")?;
    for (client_id, packet_id) in rows {
        if let Some(session) = state.sessions_by_client_id.get_mut(&client_id) {
            session.outbound_qos2_pubrel.insert(packet_id);
        }
    }
    Ok(())
}

fn load_offline_queue(connection: &mut PooledConn, state: &mut BrokerState) -> mysql::Result<()> {
    let rows: Vec<(String, u64, Vec<u8>, Option<u64>)> = connection.query(
        "SELECT client_id, sequence, packet, expires_at_ms FROM offline_queue ORDER BY client_id, sequence",
    )?;
    for (client_id, sequence, packet, expires_at_ms) in rows {
        if sequence > i64::MAX as u64 {
            return Err(invalid_data("MySQL offline sequence exceeds SQLite range"));
        }
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

fn invalid_data(message: &'static str) -> mysql::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message).into()
}

fn load_subscriptions(connection: &mut PooledConn, state: &mut BrokerState) -> mysql::Result<()> {
    let rows: Vec<SubscriptionRow> = connection.query(
            r#"
            SELECT client_id, topic_filter, maximum_qos, no_local, retain_as_published, retain_handling, subscription_identifier, match_filter, shared_group
            FROM subscriptions
            "#,
        )?;
    for (
        client_id,
        filter,
        maximum_qos,
        no_local,
        retain_as_published,
        retain_handling,
        subscription_identifier,
        persisted_match_filter,
        shared_group,
    ) in rows
    {
        let match_filter = if persisted_match_filter.is_empty() {
            crate::protocol::shared_subscription_filter(&filter)
                .unwrap_or(&filter)
                .to_string()
        } else {
            persisted_match_filter
        };
        state.subscriptions.push(SubscriptionEntry {
            client_id,
            filter,
            match_filter,
            shared_group,
            options: SubscriptionOptions {
                maximum_qos: qos_from_u8(maximum_qos),
                no_local: no_local != 0,
                retain_as_published: retain_as_published != 0,
                retain_handling,
            },
            subscription_identifier,
        });
    }
    Ok(())
}

fn load_retained(connection: &mut PooledConn, state: &mut BrokerState) -> mysql::Result<()> {
    let rows: Vec<(String, Vec<u8>, Option<u64>)> =
        connection.query("SELECT topic_name, packet, expires_at_ms FROM retained_messages")?;
    for (topic_name, packet, expires_at_ms) in rows {
        if let Some(mut message) = decode_retained(&packet) {
            message.expires_at_ms = expires_at_ms;
            state.retained.insert(topic_name, message);
        }
    }
    Ok(())
}

fn persist_patch(connection: &mut PooledConn, patch: &StoragePatch) -> mysql::Result<()> {
    match patch {
        StoragePatch::Client(patch) => persist_client_patch(connection, patch),
        StoragePatch::Retained(patch) => persist_retained_patch(connection, patch),
    }
}

fn persist_client_patch(connection: &mut PooledConn, patch: &ClientPatch) -> mysql::Result<()> {
    patch.validate().expect("valid mysql client patch");
    let mut transaction = connection.start_transaction(TxOpts::default())?;

    match patch.mode {
        ClientPatchMode::Delete => {
            delete_session(&mut transaction, &patch.client_id)?;
        }
        ClientPatchMode::Reset => {
            delete_session(&mut transaction, &patch.client_id)?;
            persist_client_delta(&mut transaction, patch)?;
        }
        ClientPatchMode::Merge => persist_client_delta(&mut transaction, patch)?,
    }

    transaction.commit()
}

fn delete_session(transaction: &mut Transaction<'_>, client_id: &str) -> mysql::Result<()> {
    transaction.exec_drop(
        "DELETE FROM sessions WHERE client_id = :client_id",
        params! { "client_id" => client_id },
    )
}

fn persist_client_delta(
    transaction: &mut Transaction<'_>,
    patch: &ClientPatch,
) -> mysql::Result<()> {
    if let Some(session) = &patch.session {
        upsert_session(transaction, &patch.client_id, session)?;
    }

    for filter in &patch.subscription_deletes {
        transaction.exec_drop(
            "DELETE FROM subscriptions WHERE client_id = :client_id AND topic_filter = :topic_filter",
            params! {
                "client_id" => &patch.client_id,
                "topic_filter" => filter,
            },
        )?;
    }
    for subscription in &patch.subscription_upserts {
        upsert_subscription(transaction, subscription)?;
    }

    if let Some(sequence) = patch.offline_remove_through {
        transaction.exec_drop(
            "DELETE FROM offline_queue WHERE client_id = :client_id AND sequence <= :sequence",
            params! {
                "client_id" => &patch.client_id,
                "sequence" => sequence,
            },
        )?;
    }
    for queued in &patch.offline_append {
        transaction.exec_drop(
            r#"
            INSERT INTO offline_queue (client_id, sequence, packet, expires_at_ms)
            VALUES (:client_id, :sequence, :packet, :expires_at_ms)
            ON DUPLICATE KEY UPDATE
                packet = VALUES(packet),
                expires_at_ms = VALUES(expires_at_ms)
            "#,
            params! {
                "client_id" => &patch.client_id,
                "sequence" => queued.sequence,
                "packet" => encode_publish(&queued.pending.packet),
                "expires_at_ms" => queued.pending.expires_at_ms,
            },
        )?;
    }

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

    for packet_id in &patch.pubrel_remove {
        transaction.exec_drop(
            "DELETE FROM outbound_pubrel WHERE client_id = :client_id AND packet_id = :packet_id",
            params! {
                "client_id" => &patch.client_id,
                "packet_id" => packet_id,
            },
        )?;
    }
    for packet_id in &patch.pubrel_add {
        transaction.exec_drop(
            r#"
            INSERT INTO outbound_pubrel (client_id, packet_id)
            VALUES (:client_id, :packet_id)
            ON DUPLICATE KEY UPDATE packet_id = VALUES(packet_id)
            "#,
            params! {
                "client_id" => &patch.client_id,
                "packet_id" => packet_id,
            },
        )?;
    }
    Ok(())
}

fn upsert_session(
    transaction: &mut Transaction<'_>,
    client_id: &str,
    session: &SessionSnapshot,
) -> mysql::Result<()> {
    transaction.exec_drop(
        r#"
        INSERT INTO sessions (
            client_id,
            session_expiry_interval,
            expires_at_ms,
            next_packet_id,
            next_offline_sequence
        ) VALUES (
            :client_id,
            :session_expiry_interval,
            :expires_at_ms,
            :next_packet_id,
            :next_offline_sequence
        )
        ON DUPLICATE KEY UPDATE
            session_expiry_interval = VALUES(session_expiry_interval),
            expires_at_ms = VALUES(expires_at_ms),
            next_packet_id = VALUES(next_packet_id),
            next_offline_sequence = VALUES(next_offline_sequence)
        "#,
        params! {
            "client_id" => client_id,
            "session_expiry_interval" => session.session_expiry_interval,
            "expires_at_ms" => session.expires_at_ms,
            "next_packet_id" => session.next_packet_id,
            "next_offline_sequence" => session.next_offline_sequence,
        },
    )
}

fn upsert_subscription(
    transaction: &mut Transaction<'_>,
    subscription: &SubscriptionSnapshot,
) -> mysql::Result<()> {
    transaction.exec_drop(
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
        ) VALUES (
            :client_id,
            :topic_filter,
            :match_filter,
            :shared_group,
            :maximum_qos,
            :no_local,
            :retain_as_published,
            :retain_handling,
            :subscription_identifier
        )
        ON DUPLICATE KEY UPDATE
            match_filter = VALUES(match_filter),
            shared_group = VALUES(shared_group),
            maximum_qos = VALUES(maximum_qos),
            no_local = VALUES(no_local),
            retain_as_published = VALUES(retain_as_published),
            retain_handling = VALUES(retain_handling),
            subscription_identifier = VALUES(subscription_identifier)
        "#,
        params! {
            "client_id" => &subscription.client_id,
            "topic_filter" => &subscription.filter,
            "match_filter" => &subscription.match_filter,
            "shared_group" => &subscription.shared_group,
            "maximum_qos" => qos_to_u8(subscription.maximum_qos),
            "no_local" => bool_to_u8(subscription.no_local),
            "retain_as_published" => bool_to_u8(subscription.retain_as_published),
            "retain_handling" => subscription.retain_handling,
            "subscription_identifier" => subscription.subscription_identifier,
        },
    )
}

fn delete_inflight(
    transaction: &mut Transaction<'_>,
    client_id: &str,
    qos: QoS,
    packet_ids: &std::collections::BTreeSet<u16>,
) -> mysql::Result<()> {
    for packet_id in packet_ids {
        transaction.exec_drop(
            "DELETE FROM outbound_inflight WHERE client_id = :client_id AND packet_id = :packet_id AND qos = :qos",
            params! {
                "client_id" => client_id,
                "packet_id" => packet_id,
                "qos" => qos_to_u8(qos),
            },
        )?;
    }
    Ok(())
}

fn upsert_inflight(
    transaction: &mut Transaction<'_>,
    client_id: &str,
    qos: QoS,
    entries: &std::collections::BTreeMap<u16, PendingSnapshot>,
) -> mysql::Result<()> {
    for (packet_id, pending) in entries {
        transaction.exec_drop(
            r#"
            INSERT INTO outbound_inflight (client_id, packet_id, qos, packet, expires_at_ms)
            VALUES (:client_id, :packet_id, :qos, :packet, :expires_at_ms)
            ON DUPLICATE KEY UPDATE
                packet = VALUES(packet),
                expires_at_ms = VALUES(expires_at_ms)
            "#,
            params! {
                "client_id" => client_id,
                "packet_id" => packet_id,
                "qos" => qos_to_u8(qos),
                "packet" => encode_publish(&pending.packet),
                "expires_at_ms" => pending.expires_at_ms,
            },
        )?;
    }
    Ok(())
}

fn persist_retained_patch(connection: &mut PooledConn, patch: &RetainedPatch) -> mysql::Result<()> {
    let mut transaction = connection.start_transaction(TxOpts::default())?;
    if let Some(message) = &patch.message {
        transaction.exec_drop(
            r#"
            INSERT INTO retained_messages (topic_name, packet, expires_at_ms)
            VALUES (:topic_name, :packet, :expires_at_ms)
            ON DUPLICATE KEY UPDATE
                packet = VALUES(packet),
                expires_at_ms = VALUES(expires_at_ms)
            "#,
            params! {
                "topic_name" => &patch.topic_name,
                "packet" => encode_retained(message),
                "expires_at_ms" => message.expires_at_ms,
            },
        )?;
    } else {
        transaction.exec_drop(
            "DELETE FROM retained_messages WHERE topic_name = :topic_name",
            params! { "topic_name" => &patch.topic_name },
        )?;
    }
    transaction.commit()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use rs_netty::codec::PublishPacket;

    use super::*;

    fn pending(qos: QoS, packet_id: u16, payload: &'static [u8]) -> PendingPublish {
        PendingPublish {
            packet: PublishPacket {
                dup: false,
                qos,
                retain: false,
                topic_name: "mysql/contract".to_string(),
                packet_id: Some(packet_id),
                properties: Vec::new(),
                payload: Bytes::from_static(payload),
            },
            expires_at_ms: None,
        }
    }

    #[test]
    #[ignore = "requires PULSE_TEST_MYSQL_URL"]
    fn mysql_patch_contract() {
        let Ok(url) = std::env::var("PULSE_TEST_MYSQL_URL") else {
            eprintln!("PULSE_TEST_MYSQL_URL is not set; skipping MySQL contract");
            return;
        };
        let suffix = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        );
        let client_id = format!("pulse-mysql-contract-{suffix}");
        let failure_filter = format!("pulse/mysql-contract/failure/{suffix}");
        let constraint_name = format!(
            "pulse_mysql_contract_fail_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        );
        let storage = MysqlStorage::open(&url).expect("open mysql storage");

        storage.with_state(&mut |state| {
            let mut session = SessionEntry::disconnected(60, None);
            session.next_packet_id = 7;
            session
                .outbound_qos2_publish
                .insert(10, pending(QoS::ExactlyOnce, 10, b"baseline-publish"));
            session.outbound_qos2_pubrel.insert(11);
            state
                .sessions_by_client_id
                .insert(client_id.clone(), session);
            state.mark_client_reset(client_id.clone());
        });

        let mut failing_snapshot = storage
            .projection
            .lock()
            .expect("mysql projection lock")
            .clients
            .get(&client_id)
            .expect("persisted client projection")
            .clone();
        failing_snapshot.session.next_packet_id = 99;
        failing_snapshot.subscriptions.insert(
            failure_filter.clone(),
            SubscriptionSnapshot {
                client_id: client_id.clone(),
                filter: failure_filter.clone(),
                match_filter: failure_filter.clone(),
                shared_group: None,
                maximum_qos: QoS::AtLeastOnce,
                no_local: false,
                retain_as_published: false,
                retain_handling: 0,
                subscription_identifier: None,
            },
        );
        let failing_patch = ClientPatch::reset(client_id.clone(), &failing_snapshot);
        let failure = {
            let mut connection = storage.pool.get_conn().expect("get mysql connection");
            let failure_filter_hex = failure_filter
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            connection
                .query_drop(format!(
                    "ALTER TABLE subscriptions ADD CONSTRAINT `{constraint_name}` CHECK (topic_filter <> X'{failure_filter_hex}')"
                ))
                .expect("create MySQL failure constraint");
            let failure = persist_client_patch(&mut connection, &failing_patch);
            connection
                .query_drop(format!(
                    "ALTER TABLE subscriptions DROP CHECK `{constraint_name}`"
                ))
                .expect("drop MySQL failure constraint");
            failure
        };
        assert!(
            failure.is_err(),
            "injected SQL failure should roll back reset"
        );

        {
            let mut connection = storage.pool.get_conn().expect("get mysql connection");
            let next_packet_id: Option<u16> = connection
                .exec_first(
                    "SELECT next_packet_id FROM sessions WHERE client_id = :client_id",
                    params! { "client_id" => &client_id },
                )
                .expect("load session after rollback");
            assert_eq!(next_packet_id, Some(7));
            let qos2_publish: Vec<(u16, u8)> = connection
                .exec(
                    "SELECT packet_id, qos FROM outbound_inflight WHERE client_id = :client_id",
                    params! { "client_id" => &client_id },
                )
                .expect("load qos2 publish after rollback");
            assert_eq!(qos2_publish, vec![(10, qos_to_u8(QoS::ExactlyOnce))]);
            let pubrel: Vec<u16> = connection
                .exec(
                    "SELECT packet_id FROM outbound_pubrel WHERE client_id = :client_id",
                    params! { "client_id" => &client_id },
                )
                .expect("load pubrel after rollback");
            assert_eq!(pubrel, vec![11]);
        }

        storage.with_state(&mut |state| {
            let session = state
                .sessions_by_client_id
                .get_mut(&client_id)
                .expect("mysql contract session");
            session.next_packet_id = 14;
            session.outbound_qos2_publish.remove(&10);
            session
                .outbound_qos2_publish
                .insert(12, pending(QoS::ExactlyOnce, 12, b"merged-publish"));
            session.outbound_qos2_pubrel.remove(&11);
            session.outbound_qos2_pubrel.insert(13);
            state.mark_client_changed(client_id.clone());
        });

        {
            let mut connection = storage.pool.get_conn().expect("get mysql connection");
            let next_packet_id: Option<u16> = connection
                .exec_first(
                    "SELECT next_packet_id FROM sessions WHERE client_id = :client_id",
                    params! { "client_id" => &client_id },
                )
                .expect("load merged session");
            assert_eq!(next_packet_id, Some(14));
            let qos2_publish: Vec<(u16, u8)> = connection
                .exec(
                    "SELECT packet_id, qos FROM outbound_inflight WHERE client_id = :client_id",
                    params! { "client_id" => &client_id },
                )
                .expect("load merged qos2 publish");
            assert_eq!(qos2_publish, vec![(12, qos_to_u8(QoS::ExactlyOnce))]);
            let pubrel: Vec<u16> = connection
                .exec(
                    "SELECT packet_id FROM outbound_pubrel WHERE client_id = :client_id",
                    params! { "client_id" => &client_id },
                )
                .expect("load merged pubrel");
            assert_eq!(pubrel, vec![13]);
        }

        storage.with_state(&mut |state| {
            state.sessions_by_client_id.remove(&client_id);
            state
                .subscriptions
                .retain(|subscription| subscription.client_id != client_id);
            state.mark_client_reset(client_id.clone());
        });

        let mut connection = storage.pool.get_conn().expect("get mysql connection");
        for table in [
            "sessions",
            "subscriptions",
            "offline_queue",
            "outbound_inflight",
            "outbound_pubrel",
        ] {
            let count: Option<u64> = connection
                .exec_first(
                    format!("SELECT COUNT(*) FROM {table} WHERE client_id = :client_id"),
                    params! { "client_id" => &client_id },
                )
                .expect("count deleted mysql rows");
            assert_eq!(count, Some(0), "{table} retained deleted client rows");
        }
    }
}
