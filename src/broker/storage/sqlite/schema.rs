use rusqlite::Connection;

pub(super) fn configure_connection(connection: &Connection) -> rusqlite::Result<()> {
    connection.pragma_update(None, "journal_mode", "WAL")?;
    connection.pragma_update(None, "synchronous", "NORMAL")?;
    connection.pragma_update(None, "foreign_keys", "ON")?;
    connection.busy_timeout(std::time::Duration::from_secs(5))?;
    Ok(())
}

pub(super) fn migrate(connection: &Connection) -> rusqlite::Result<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS sessions (
            client_id TEXT PRIMARY KEY,
            session_expiry_interval INTEGER NOT NULL,
            expires_at_ms INTEGER,
            next_packet_id INTEGER NOT NULL DEFAULT 1
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
            sequence INTEGER NOT NULL,
            packet BLOB NOT NULL,
            expires_at_ms INTEGER,
            PRIMARY KEY (client_id, sequence),
            FOREIGN KEY (client_id) REFERENCES sessions(client_id) ON DELETE CASCADE
        );
        "#,
    )?;
    add_column_if_missing(
        connection,
        "sessions",
        "next_packet_id",
        "INTEGER NOT NULL DEFAULT 1",
    )?;
    add_column_if_missing(
        connection,
        "subscriptions",
        "subscription_identifier",
        "INTEGER",
    )?;
    add_column_if_missing(
        connection,
        "subscriptions",
        "match_filter",
        "TEXT NOT NULL DEFAULT ''",
    )?;
    add_column_if_missing(connection, "subscriptions", "shared_group", "TEXT")?;
    add_column_if_missing(connection, "retained_messages", "expires_at_ms", "INTEGER")?;
    add_column_if_missing(connection, "outbound_inflight", "expires_at_ms", "INTEGER")?;
    add_column_if_missing(connection, "offline_queue", "expires_at_ms", "INTEGER")
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
