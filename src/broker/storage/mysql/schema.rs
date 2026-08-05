use mysql::{PooledConn, TxOpts, params, prelude::Queryable};

pub(super) fn migrate(connection: &mut PooledConn) -> mysql::Result<()> {
    let now_ms = crate::broker::runtime::time::now_ms();
    connection.query_drop(
        r#"
        CREATE TABLE IF NOT EXISTS sessions (
            client_id VARBINARY(512) PRIMARY KEY,
            session_expiry_interval BIGINT UNSIGNED NOT NULL,
            expires_at_ms BIGINT UNSIGNED,
            next_packet_id INT UNSIGNED NOT NULL DEFAULT 1,
            next_offline_sequence BIGINT UNSIGNED NOT NULL DEFAULT 0
                CHECK (next_offline_sequence <= 9223372036854775807)
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
            sequence BIGINT UNSIGNED NOT NULL
                CHECK (sequence <= 9223372036854775807),
            packet LONGBLOB NOT NULL,
            expires_at_ms BIGINT UNSIGNED,
            PRIMARY KEY (client_id, sequence),
            CONSTRAINT offline_queue_client_fk
                FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
        ) ENGINE=InnoDB
        "#,
    )?;
    add_column_if_missing(
        connection,
        "sessions",
        "next_offline_sequence",
        "BIGINT UNSIGNED NOT NULL DEFAULT 0",
    )?;
    let mut transaction = connection.start_transaction(TxOpts::default())?;
    transaction.exec_drop(
        r#"
        DELETE FROM sessions
        WHERE session_expiry_interval = 0
           OR (expires_at_ms IS NOT NULL AND expires_at_ms <= :now_ms)
        "#,
        params! { "now_ms" => now_ms },
    )?;
    for table in [
        "subscriptions",
        "outbound_inflight",
        "outbound_pubrel",
        "offline_queue",
    ] {
        transaction.query_drop(format!(
            "DELETE FROM {table} WHERE NOT EXISTS (SELECT 1 FROM sessions WHERE sessions.client_id = {table}.client_id)"
        ))?;
    }
    transaction.query_drop(
        r#"
        UPDATE sessions AS session
        INNER JOIN (
            SELECT client_id, MAX(sequence) + 1 AS next_sequence
            FROM offline_queue
            GROUP BY client_id
        ) AS queued ON queued.client_id = session.client_id
        SET session.next_offline_sequence = queued.next_sequence
        WHERE session.next_offline_sequence = 0
        "#,
    )?;
    transaction.exec_drop(
        r#"
        UPDATE sessions
        SET expires_at_ms = :now_ms + session_expiry_interval * 1000
        WHERE expires_at_ms IS NULL
          AND session_expiry_interval > 0
          AND session_expiry_interval < 4294967295
        "#,
        params! { "now_ms" => now_ms },
    )?;
    transaction.commit()
}

fn add_column_if_missing(
    connection: &mut PooledConn,
    table: &str,
    column: &str,
    definition: &str,
) -> mysql::Result<()> {
    let exists: Option<u8> = connection.exec_first(
        r#"
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = :table
          AND column_name = :column
        LIMIT 1
        "#,
        params! {
            "table" => table,
            "column" => column,
        },
    )?;
    if exists.is_none() {
        connection.query_drop(format!(
            "ALTER TABLE {table} ADD COLUMN {column} {definition}"
        ))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use mysql::Pool;

    use super::*;

    #[test]
    #[ignore = "requires PULSE_TEST_MYSQL_ADMIN_URL"]
    fn mysql_legacy_schema_migration_contract() {
        let Ok(admin_url) = std::env::var("PULSE_TEST_MYSQL_ADMIN_URL") else {
            eprintln!("PULSE_TEST_MYSQL_ADMIN_URL is not set; skipping MySQL migration contract");
            return;
        };
        let database = format!(
            "pulse_migration_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        );
        let admin_pool = Pool::new(admin_url.as_str()).expect("connect to MySQL admin database");
        let mut admin = admin_pool.get_conn().expect("get MySQL admin connection");
        admin
            .query_drop(format!("CREATE DATABASE `{database}`"))
            .expect("create isolated MySQL migration database");
        let database_url = format!("{}/{database}", admin_url.trim_end_matches('/'));

        let result = (|| -> mysql::Result<_> {
            let pool = Pool::new(database_url.as_str())?;
            let mut connection = pool.get_conn()?;
            connection.query_drop(
                r#"
                CREATE TABLE sessions (
                    client_id VARBINARY(512) PRIMARY KEY,
                    session_expiry_interval BIGINT UNSIGNED NOT NULL,
                    expires_at_ms BIGINT UNSIGNED,
                    next_packet_id INT UNSIGNED NOT NULL DEFAULT 1
                ) ENGINE=InnoDB
                "#,
            )?;
            connection.query_drop(
                r#"
                CREATE TABLE offline_queue (
                    client_id VARBINARY(512) NOT NULL,
                    sequence BIGINT UNSIGNED NOT NULL,
                    packet LONGBLOB NOT NULL,
                    expires_at_ms BIGINT UNSIGNED,
                    PRIMARY KEY (client_id, sequence),
                    CONSTRAINT offline_queue_client_fk
                        FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
                ) ENGINE=InnoDB
                "#,
            )?;
            connection.query_drop(
                r#"
                INSERT INTO sessions (client_id, session_expiry_interval, expires_at_ms)
                VALUES
                    ('finite', 60, NULL),
                    ('transient', 0, NULL),
                    ('expired', 60, 1)
                "#,
            )?;
            connection.query_drop(
                r#"
                INSERT INTO offline_queue (client_id, sequence, packet)
                VALUES ('finite', 2, X'00'), ('finite', 7, X'00'), ('transient', 0, X'00')
                "#,
            )?;
            connection.query_drop("SET FOREIGN_KEY_CHECKS = 0")?;
            connection.query_drop(
                "INSERT INTO offline_queue (client_id, sequence, packet) VALUES ('orphan', 0, X'00')",
            )?;
            connection.query_drop("SET FOREIGN_KEY_CHECKS = 1")?;

            migrate(&mut connection)?;
            let next_sequence: Option<u64> = connection.query_first(
                "SELECT next_offline_sequence FROM sessions WHERE client_id = 'finite'",
            )?;
            let first_deadline: Option<u64> = connection
                .query_first("SELECT expires_at_ms FROM sessions WHERE client_id = 'finite'")?;
            let removed_sessions: Option<u64> = connection.query_first(
                "SELECT COUNT(*) FROM sessions WHERE client_id IN ('transient', 'expired')",
            )?;
            let removed_queues: Option<u64> = connection.query_first(
                "SELECT COUNT(*) FROM offline_queue WHERE client_id IN ('transient', 'orphan')",
            )?;

            std::thread::sleep(std::time::Duration::from_millis(2));
            migrate(&mut connection)?;
            let second_deadline: Option<u64> = connection
                .query_first("SELECT expires_at_ms FROM sessions WHERE client_id = 'finite'")?;
            Ok((
                next_sequence,
                first_deadline,
                second_deadline,
                removed_sessions,
                removed_queues,
            ))
        })();

        admin
            .query_drop(format!("DROP DATABASE `{database}`"))
            .expect("drop isolated MySQL migration database");

        let (next_sequence, first_deadline, second_deadline, removed_sessions, removed_queues) =
            result.expect("migrate legacy MySQL schema");
        assert_eq!(next_sequence, Some(8));
        assert!(first_deadline.is_some());
        assert_eq!(second_deadline, first_deadline);
        assert_eq!(removed_sessions, Some(0));
        assert_eq!(removed_queues, Some(0));
    }
}
