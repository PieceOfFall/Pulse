use rusqlite::Connection;

pub(super) fn configure_connection(connection: &Connection) -> rusqlite::Result<()> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(())
}

pub(super) fn migrate(connection: &mut Connection) -> rusqlite::Result<()> {
    let now_ms = crate::broker::runtime::time::now_ms() as i64;
    let transaction = connection.transaction()?;
    transaction.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS sessions (
            client_id TEXT PRIMARY KEY,
            session_expiry_interval INTEGER NOT NULL,
            expires_at_ms INTEGER,
            next_packet_id INTEGER NOT NULL DEFAULT 1,
            next_offline_sequence INTEGER NOT NULL DEFAULT 0
                CHECK (next_offline_sequence BETWEEN 0 AND 9223372036854775807)
        );

        CREATE TABLE IF NOT EXISTS subscriptions (
            client_id TEXT NOT NULL,
            topic_filter TEXT NOT NULL,
            match_filter TEXT NOT NULL DEFAULT '',
            shared_group TEXT,
            maximum_qos INTEGER NOT NULL,
            no_local INTEGER NOT NULL,
            retain_as_published INTEGER NOT NULL,
            retain_handling INTEGER NOT NULL,
            subscription_identifier INTEGER,
            PRIMARY KEY (client_id, topic_filter),
            FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS retained_messages (
            topic_name TEXT PRIMARY KEY,
            packet BLOB NOT NULL,
            expires_at_ms INTEGER
        );

        CREATE TABLE IF NOT EXISTS outbound_inflight (
            client_id TEXT NOT NULL,
            packet_id INTEGER NOT NULL,
            qos INTEGER NOT NULL,
            packet BLOB NOT NULL,
            expires_at_ms INTEGER,
            PRIMARY KEY (client_id, packet_id, qos),
            FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS outbound_pubrel (
            client_id TEXT NOT NULL,
            packet_id INTEGER NOT NULL,
            PRIMARY KEY (client_id, packet_id),
            FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS offline_queue (
            client_id TEXT NOT NULL,
            sequence INTEGER NOT NULL
                CHECK (sequence BETWEEN 0 AND 9223372036854775807),
            packet BLOB NOT NULL,
            expires_at_ms INTEGER,
            PRIMARY KEY (client_id, sequence),
            FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
        );
        "#,
    )?;
    add_column_if_missing(
        &transaction,
        "sessions",
        "next_packet_id",
        "INTEGER NOT NULL DEFAULT 1",
    )?;
    add_column_if_missing(
        &transaction,
        "sessions",
        "next_offline_sequence",
        "INTEGER NOT NULL DEFAULT 0",
    )?;
    add_column_if_missing(
        &transaction,
        "subscriptions",
        "subscription_identifier",
        "INTEGER",
    )?;
    add_column_if_missing(
        &transaction,
        "subscriptions",
        "match_filter",
        "TEXT NOT NULL DEFAULT ''",
    )?;
    add_column_if_missing(&transaction, "subscriptions", "shared_group", "TEXT")?;
    add_column_if_missing(
        &transaction,
        "retained_messages",
        "expires_at_ms",
        "INTEGER",
    )?;
    add_column_if_missing(
        &transaction,
        "outbound_inflight",
        "expires_at_ms",
        "INTEGER",
    )?;
    add_column_if_missing(&transaction, "offline_queue", "expires_at_ms", "INTEGER")?;
    transaction.execute(
        "DELETE FROM sessions WHERE session_expiry_interval = 0 OR (expires_at_ms IS NOT NULL AND expires_at_ms <= ?1)",
        [now_ms],
    )?;
    for table in [
        "subscriptions",
        "outbound_inflight",
        "outbound_pubrel",
        "offline_queue",
    ] {
        transaction.execute(
            &format!(
                "DELETE FROM {table} WHERE NOT EXISTS (SELECT 1 FROM sessions WHERE sessions.client_id = {table}.client_id)"
            ),
            [],
        )?;
    }
    transaction.execute(
        r#"
        UPDATE sessions
        SET next_offline_sequence = (
            SELECT COALESCE(MAX(offline_queue.sequence) + 1, 0)
            FROM offline_queue
            WHERE offline_queue.client_id = sessions.client_id
        )
        WHERE next_offline_sequence = 0
          AND EXISTS (
              SELECT 1
              FROM offline_queue
              WHERE offline_queue.client_id = sessions.client_id
          )
        "#,
        [],
    )?;
    transaction.execute(
        r#"
        UPDATE sessions
        SET expires_at_ms = ?1 + session_expiry_interval * 1000
        WHERE expires_at_ms IS NULL
          AND session_expiry_interval > 0
          AND session_expiry_interval < 4294967295
        "#,
        [now_ms],
    )?;
    transaction.commit()
}

fn add_column_if_missing(
    connection: &Connection,
    table: &str,
    column: &str,
    definition: &str,
) -> rusqlite::Result<()> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    for existing in columns {
        if existing? == column {
            return Ok(());
        }
    }

    connection.execute(
        &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
        [],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn migration_backfills_next_offline_sequence() {
        let mut connection = Connection::open_in_memory().expect("open sqlite");
        configure_connection(&connection).expect("configure sqlite");
        connection
            .execute_batch(
                r#"
                CREATE TABLE sessions (
                    client_id TEXT PRIMARY KEY,
                    session_expiry_interval INTEGER NOT NULL,
                    expires_at_ms INTEGER,
                    next_packet_id INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE offline_queue (
                    client_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    packet BLOB NOT NULL,
                    expires_at_ms INTEGER,
                    PRIMARY KEY (client_id, sequence),
                    FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
                );
                INSERT INTO sessions (client_id, session_expiry_interval, next_packet_id)
                VALUES ('client', 60, 1);
                INSERT INTO offline_queue (client_id, sequence, packet)
                VALUES ('client', 2, X'01'), ('client', 7, X'02');
                "#,
            )
            .expect("create legacy schema");

        migrate(&mut connection).expect("migrate sqlite");

        let next_sequence: i64 = connection
            .query_row(
                "SELECT next_offline_sequence FROM sessions WHERE client_id = 'client'",
                [],
                |row| row.get(0),
            )
            .expect("load next offline sequence");
        assert_eq!(next_sequence, 8);
    }

    #[test]
    fn migration_removes_transient_sessions_and_orphan_children() {
        let mut connection = Connection::open_in_memory().expect("open sqlite");
        configure_connection(&connection).expect("configure sqlite");
        migrate(&mut connection).expect("create sqlite schema");
        connection
            .pragma_update(None, "foreign_keys", "OFF")
            .expect("disable foreign keys for legacy fixture");
        connection
            .execute_batch(
                r#"
                INSERT INTO sessions (client_id, session_expiry_interval)
                VALUES ('durable', 60), ('transient', 0);
                INSERT INTO subscriptions (
                    client_id,
                    topic_filter,
                    maximum_qos,
                    no_local,
                    retain_as_published,
                    retain_handling
                ) VALUES
                    ('transient', 'transient/filter', 1, 0, 0, 0),
                    ('orphan', 'orphan/filter', 1, 0, 0, 0);
                INSERT INTO outbound_inflight (client_id, packet_id, qos, packet)
                VALUES ('transient', 1, 1, X'01'), ('orphan', 1, 1, X'01');
                INSERT INTO outbound_pubrel (client_id, packet_id)
                VALUES ('transient', 2), ('orphan', 2);
                INSERT INTO offline_queue (client_id, sequence, packet)
                VALUES ('transient', 0, X'01'), ('orphan', 0, X'01');
                "#,
            )
            .expect("create legacy transient and orphan rows");

        migrate(&mut connection).expect("clean sqlite persistence rows");

        let durable_sessions: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE client_id = 'durable'",
                [],
                |row| row.get(0),
            )
            .expect("count durable session");
        assert_eq!(durable_sessions, 1);
        let transient_sessions: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE client_id = 'transient'",
                [],
                |row| row.get(0),
            )
            .expect("count transient session");
        assert_eq!(transient_sessions, 0);
        for table in [
            "subscriptions",
            "outbound_inflight",
            "outbound_pubrel",
            "offline_queue",
        ] {
            let count: i64 = connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .expect("count orphan rows");
            assert_eq!(count, 0, "{table} retained non-durable rows");
        }
    }

    #[test]
    fn migration_persists_recovery_deadline_once_and_drops_expired_sessions() {
        let mut connection = Connection::open_in_memory().expect("open sqlite");
        configure_connection(&connection).expect("configure sqlite");
        migrate(&mut connection).expect("create sqlite schema");
        connection
            .execute_batch(
                r#"
                INSERT INTO sessions (client_id, session_expiry_interval, expires_at_ms)
                VALUES
                    ('finite', 60, NULL),
                    ('forever', 4294967295, NULL),
                    ('expired', 60, 1);
                "#,
            )
            .expect("seed recovered sessions");

        migrate(&mut connection).expect("canonicalize sqlite sessions");
        let first_deadline: i64 = connection
            .query_row(
                "SELECT expires_at_ms FROM sessions WHERE client_id = 'finite'",
                [],
                |row| row.get(0),
            )
            .expect("finite recovery deadline");
        assert!(first_deadline > crate::broker::runtime::time::now_ms() as i64);
        let forever_deadline: Option<i64> = connection
            .query_row(
                "SELECT expires_at_ms FROM sessions WHERE client_id = 'forever'",
                [],
                |row| row.get(0),
            )
            .expect("never-expire session");
        assert_eq!(forever_deadline, None);
        let expired_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE client_id = 'expired'",
                [],
                |row| row.get(0),
            )
            .expect("expired session count");
        assert_eq!(expired_count, 0);

        std::thread::sleep(std::time::Duration::from_millis(2));
        migrate(&mut connection).expect("repeat sqlite canonicalization");
        let second_deadline: i64 = connection
            .query_row(
                "SELECT expires_at_ms FROM sessions WHERE client_id = 'finite'",
                [],
                |row| row.get(0),
            )
            .expect("stable finite recovery deadline");
        assert_eq!(second_deadline, first_deadline);
    }

    #[test]
    fn legacy_negative_offline_identifiers_fail_closed_on_load() {
        let invalid_fixture = |session_sequence: i64, queued_sequence: Option<i64>| {
            let mut connection = Connection::open_in_memory().expect("open sqlite");
            configure_connection(&connection).expect("configure sqlite");
            connection
                .execute_batch(
                    r#"
                    CREATE TABLE sessions (
                        client_id TEXT PRIMARY KEY,
                        session_expiry_interval INTEGER NOT NULL,
                        expires_at_ms INTEGER,
                        next_packet_id INTEGER NOT NULL DEFAULT 1,
                        next_offline_sequence INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE TABLE offline_queue (
                        client_id TEXT NOT NULL,
                        sequence INTEGER NOT NULL,
                        packet BLOB NOT NULL,
                        expires_at_ms INTEGER,
                        PRIMARY KEY (client_id, sequence),
                        FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
                    );
                    "#,
                )
                .expect("create legacy schema");
            connection
                .execute(
                    "INSERT INTO sessions (client_id, session_expiry_interval, next_offline_sequence) VALUES ('client', 60, ?1)",
                    [session_sequence],
                )
                .expect("insert legacy session");
            if let Some(sequence) = queued_sequence {
                connection
                    .execute(
                        "INSERT INTO offline_queue (client_id, sequence, packet) VALUES ('client', ?1, X'00')",
                        [sequence],
                    )
                    .expect("insert legacy offline row");
            }
            migrate(&mut connection).expect("migrate legacy schema");
            super::super::load_state(&connection)
        };

        assert!(invalid_fixture(-1, None).is_err());
        assert!(invalid_fixture(1, Some(-1)).is_err());
    }
}
