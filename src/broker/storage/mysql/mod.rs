use std::sync::Mutex;

use mysql::{Pool, PooledConn, TxOpts, params, prelude::Queryable};
use rs_netty::codec::{QoS, SubscriptionOptions};

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
}

impl MysqlStorage {
    pub(crate) fn open(url: &str) -> mysql::Result<Self> {
        let pool = Pool::new(url)?;
        let mut connection = pool.get_conn()?;
        migrate(&mut connection)?;
        let state = load_state(&mut connection)?;

        Ok(Self {
            pool,
            state: Mutex::new(state),
        })
    }
}

impl BrokerStorage for MysqlStorage {
    fn with_state(&self, operation: &mut dyn FnMut(&mut BrokerState)) {
        let mut state = self.state.lock().expect("broker state lock poisoned");
        operation(&mut state);

        let mut connection = self.pool.get_conn().expect("get mysql connection");
        persist_state(&mut connection, &state).expect("persist broker state to mysql");
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
    let rows: Vec<(String, u32, Option<u64>, u16)> = connection.query(
        "SELECT client_id, session_expiry_interval, expires_at_ms, next_packet_id FROM sessions",
    )?;
    for (client_id, session_expiry_interval, expires_at_ms, next_packet_id) in rows {
        let mut session = SessionEntry::disconnected(session_expiry_interval, expires_at_ms);
        session.next_packet_id = next_packet_id;
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
    let rows: Vec<(String, Vec<u8>, Option<u64>)> = connection.query(
        "SELECT client_id, packet, expires_at_ms FROM offline_queue ORDER BY client_id, sequence",
    )?;
    for (client_id, packet, expires_at_ms) in rows {
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

fn persist_state(connection: &mut PooledConn, state: &BrokerState) -> mysql::Result<()> {
    let mut transaction = connection.start_transaction(TxOpts::default())?;
    transaction.query_drop("DELETE FROM offline_queue")?;
    transaction.query_drop("DELETE FROM outbound_pubrel")?;
    transaction.query_drop("DELETE FROM outbound_inflight")?;
    transaction.query_drop("DELETE FROM subscriptions")?;
    transaction.query_drop("DELETE FROM retained_messages")?;
    transaction.query_drop("DELETE FROM sessions")?;

    for (client_id, session) in &state.sessions_by_client_id {
        transaction.exec_drop(
            "INSERT INTO sessions (client_id, session_expiry_interval, expires_at_ms, next_packet_id) VALUES (:client_id, :session_expiry_interval, :expires_at_ms, :next_packet_id)",
            params! {
                "client_id" => client_id,
                "session_expiry_interval" => session.session_expiry_interval,
                "expires_at_ms" => session.expires_at_ms,
                "next_packet_id" => session.next_packet_id,
            },
        )?;
    }

    for subscription in &state.subscriptions {
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
            ) VALUES (:client_id, :topic_filter, :match_filter, :shared_group, :maximum_qos, :no_local, :retain_as_published, :retain_handling, :subscription_identifier)
            "#,
            params! {
                "client_id" => &subscription.client_id,
                "topic_filter" => &subscription.filter,
                "match_filter" => &subscription.match_filter,
                "shared_group" => &subscription.shared_group,
                "maximum_qos" => qos_to_u8(subscription.options.maximum_qos),
                "no_local" => bool_to_u8(subscription.options.no_local),
                "retain_as_published" => bool_to_u8(subscription.options.retain_as_published),
                "retain_handling" => subscription.options.retain_handling,
                "subscription_identifier" => subscription.subscription_identifier,
            },
        )?;
    }

    for (topic_name, message) in &state.retained {
        transaction.exec_drop(
            "INSERT INTO retained_messages (topic_name, packet, expires_at_ms) VALUES (:topic_name, :packet, :expires_at_ms)",
            params! {
                "topic_name" => topic_name,
                "packet" => encode_retained(message),
                "expires_at_ms" => message.expires_at_ms,
            },
        )?;
    }

    for (client_id, session) in &state.sessions_by_client_id {
        for (packet_id, pending) in &session.outbound_qos1 {
            transaction.exec_drop(
                "INSERT INTO outbound_inflight (client_id, packet_id, qos, packet, expires_at_ms) VALUES (:client_id, :packet_id, :qos, :packet, :expires_at_ms)",
                params! {
                    "client_id" => client_id,
                    "packet_id" => packet_id,
                    "qos" => qos_to_u8(QoS::AtLeastOnce),
                    "packet" => encode_publish(&pending.packet),
                    "expires_at_ms" => pending.expires_at_ms,
                },
            )?;
        }
        for (packet_id, pending) in &session.outbound_qos2_publish {
            transaction.exec_drop(
                "INSERT INTO outbound_inflight (client_id, packet_id, qos, packet, expires_at_ms) VALUES (:client_id, :packet_id, :qos, :packet, :expires_at_ms)",
                params! {
                    "client_id" => client_id,
                    "packet_id" => packet_id,
                    "qos" => qos_to_u8(QoS::ExactlyOnce),
                    "packet" => encode_publish(&pending.packet),
                    "expires_at_ms" => pending.expires_at_ms,
                },
            )?;
        }
        for packet_id in &session.outbound_qos2_pubrel {
            transaction.exec_drop(
                "INSERT INTO outbound_pubrel (client_id, packet_id) VALUES (:client_id, :packet_id)",
                params! {
                    "client_id" => client_id,
                    "packet_id" => packet_id,
                },
            )?;
        }
        for (sequence, pending) in session.offline_queue.iter().enumerate() {
            transaction.exec_drop(
                "INSERT INTO offline_queue (client_id, sequence, packet, expires_at_ms) VALUES (:client_id, :sequence, :packet, :expires_at_ms)",
                params! {
                    "client_id" => client_id,
                    "sequence" => sequence as u64,
                    "packet" => encode_publish(&pending.packet),
                    "expires_at_ms" => pending.expires_at_ms,
                },
            )?;
        }
    }

    transaction.commit()
}
