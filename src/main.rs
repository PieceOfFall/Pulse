mod broker;
mod observability;
mod protocol;
mod settings;
mod tls;

use std::{fs, path::Path, sync::Arc, time::Duration};

use broker::{Broker, BrokerLife, MqttHandler, runtime::auth::ConfiguredAuthenticator};
use rs_netty::{Error, Result, TcpServer, codec::MqttCodec, pipeline};
use settings::{AppConfig, StorageEngine};
use tls::build_server_tls_context;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load()
        .map_err(|error| Error::Pipeline(format!("load configuration: {error}")))?;
    observability::init(&config.observability)
        .map_err(|error| Error::Pipeline(format!("initialize observability: {error}")))?;
    let server_tls = build_server_tls_context(&config.server.tls)?;
    let authenticator = Arc::new(ConfiguredAuthenticator::new(config.auth.clone()));

    let broker = match config.storage.engine {
        StorageEngine::Mysql => {
            let url = config
                .storage
                .mysql
                .as_deref()
                .expect("validated mysql storage url");
            Broker::with_mysql_auth_and_config(url, config.limits, authenticator.clone())
                .map_err(|error| Error::Pipeline(format!("open mysql storage: {error}")))?
        }
        StorageEngine::Sqlite => {
            let path = config
                .storage
                .sqlite
                .as_deref()
                .expect("validated sqlite storage path");
            ensure_sqlite_parent_dir(path)?;
            Broker::with_sqlite_auth_and_config(path, config.limits, authenticator.clone())
                .map_err(|error| Error::Pipeline(format!("open sqlite storage: {error}")))?
        }
        StorageEngine::Wal => {
            let dir = config
                .storage
                .wal_dir
                .clone()
                .unwrap_or_else(|| "pulse-wal".into());
            Broker::with_binary_auth_and_config(
                dir,
                config.storage.commit_policy,
                config.limits,
                authenticator.clone(),
            )
            .map_err(|error| Error::Pipeline(format!("open binary wal storage: {error}")))?
        }
        StorageEngine::Memory => Broker::with_config_and_auth(config.limits, authenticator),
    };

    let server =
        TcpServer::bind(config.server.bind).outbound_queue_size(config.server.outbound_queue_size);
    let server = if let Some(server_tls) = server_tls {
        server.tls(server_tls)
    } else {
        server
    };
    let broker_for_pipeline = broker.clone();
    let server = server
        .track_connection_stats()
        .life(BrokerLife::new(broker.clone()))
        .pipeline(move || {
            pipeline()
                .codec(MqttCodec::with_max_packet_size(
                    broker_for_pipeline.config().server_maximum_packet_size as usize,
                ))
                .handler(MqttHandler::new(broker_for_pipeline.clone()))
        })
        .start()
        .await?;

    info!(bind_addr = %server.local_addr(), "Pulse listening");

    tokio::signal::ctrl_c()
        .await
        .map_err(|error| Error::Pipeline(format!("listen for shutdown signal: {error}")))?;
    info!("shutdown signal received");

    broker
        .shutdown_active_sessions(Duration::from_millis(
            config.server.shutdown_drain_timeout_ms,
        ))
        .await;
    server.shutdown();
    server.wait().await
}

fn ensure_sqlite_parent_dir(path: &str) -> Result<()> {
    let Some(parent) = Path::new(path)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    else {
        return Ok(());
    };

    fs::create_dir_all(parent)
        .map_err(|error| Error::Pipeline(format!("create sqlite directory: {error}")))
}
