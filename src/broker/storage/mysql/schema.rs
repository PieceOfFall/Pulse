use mysql::{PooledConn, prelude::Queryable};

pub(super) fn migrate(connection: &mut PooledConn) -> mysql::Result<()> {
    connection.query_drop(
        r#"
        CREATE TABLE IF NOT EXISTS sessions (
            client_id VARBINARY(512) PRIMARY KEY,
            session_expiry_interval BIGINT UNSIGNED NOT NULL,
            expires_at_ms BIGINT UNSIGNED,
            next_packet_id INT UNSIGNED NOT NULL DEFAULT 1
        ) ENGINE=InnoDB
        "#,
    )?;
    connection.query_drop(
        r#"
        CREATE TABLE IF NOT EXISTS subscriptions (
            client_id VARBINARY(512) NOT NULL,
            topic_filter VARBINARY(2048) NOT NULL,
            match_filter VARBINARY(2048) NOT NULL DEFAULT '',
            shared_group VARBINARY(512),
            maximum_qos TINYINT UNSIGNED NOT NULL,
            no_local TINYINT UNSIGNED NOT NULL,
            retain_as_published TINYINT UNSIGNED NOT NULL,
            retain_handling TINYINT UNSIGNED NOT NULL,
            subscription_identifier BIGINT UNSIGNED,
            PRIMARY KEY (client_id, topic_filter),
            CONSTRAINT subscriptions_client_fk
                FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
        ) ENGINE=InnoDB
        "#,
    )?;
    connection.query_drop(
        r#"
        CREATE TABLE IF NOT EXISTS retained_messages (
            topic_name VARBINARY(2048) PRIMARY KEY,
            packet LONGBLOB NOT NULL,
            expires_at_ms BIGINT UNSIGNED
        ) ENGINE=InnoDB
        "#,
    )?;
    connection.query_drop(
        r#"
        CREATE TABLE IF NOT EXISTS outbound_inflight (
            client_id VARBINARY(512) NOT NULL,
            packet_id INT UNSIGNED NOT NULL,
            qos TINYINT UNSIGNED NOT NULL,
            packet LONGBLOB NOT NULL,
            expires_at_ms BIGINT UNSIGNED,
            PRIMARY KEY (client_id, packet_id, qos),
            CONSTRAINT outbound_inflight_client_fk
                FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
        ) ENGINE=InnoDB
        "#,
    )?;
    connection.query_drop(
        r#"
        CREATE TABLE IF NOT EXISTS outbound_pubrel (
            client_id VARBINARY(512) NOT NULL,
            packet_id INT UNSIGNED NOT NULL,
            PRIMARY KEY (client_id, packet_id),
            CONSTRAINT outbound_pubrel_client_fk
                FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
        ) ENGINE=InnoDB
        "#,
    )?;
    connection.query_drop(
        r#"
        CREATE TABLE IF NOT EXISTS offline_queue (
            client_id VARBINARY(512) NOT NULL,
            sequence BIGINT UNSIGNED NOT NULL,
            packet LONGBLOB NOT NULL,
            expires_at_ms BIGINT UNSIGNED,
            PRIMARY KEY (client_id, sequence),
            CONSTRAINT offline_queue_client_fk
                FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
        ) ENGINE=InnoDB
        "#,
    )
}
