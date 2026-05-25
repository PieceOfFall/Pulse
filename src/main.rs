mod broker;
mod observability;
mod protocol;
mod settings;

use broker::{Broker, BrokerLife, MqttHandler};
use rs_netty::{Error, Result, TcpServer, codec::MqttCodec, pipeline};
use settings::AppConfig;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = AppConfig::load()
        .map_err(|error| Error::Pipeline(format!("load configuration: {error}")))?;
    observability::init(&config.observability)
        .map_err(|error| Error::Pipeline(format!("initialize observability: {error}")))?;

    let broker = if let Some(url) = &config.storage.mysql {
        Broker::with_mysql_and_config(url, config.limits)
            .map_err(|error| Error::Pipeline(format!("open mysql storage: {error}")))?
    } else if let Some(path) = &config.storage.sqlite {
        Broker::with_sqlite_and_config(path, config.limits)
            .map_err(|error| Error::Pipeline(format!("open sqlite storage: {error}")))?
    } else {
        Broker::with_config(config.limits)
    };

    let server = TcpServer::bind(config.server.bind)
        .outbound_queue_size(config.server.outbound_queue_size)
        .track_connection_stats()
        .life(BrokerLife::new(broker.clone()))
        .pipeline(move || {
            pipeline()
                .codec(MqttCodec::with_max_packet_size(
                    broker.config().server_maximum_packet_size as usize,
                ))
                .handler(MqttHandler::new(broker.clone()))
        })
        .start()
        .await?;

    info!(bind_addr = %server.local_addr(), "mqtt-rs listening");

    tokio::signal::ctrl_c()
        .await
        .map_err(|error| Error::Pipeline(format!("listen for shutdown signal: {error}")))?;
    info!("shutdown signal received");

    server.shutdown();
    server.wait().await
}
