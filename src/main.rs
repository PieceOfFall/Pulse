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

    let broker = if let Some(path) = &config.storage.sqlite {
        Broker::with_sqlite_and_config(path, config.limits)
            .map_err(|error| Error::Pipeline(format!("open sqlite storage: {error}")))?
    } else {
        Broker::with_config(config.limits)
    };

    info!(bind_addr = config.server.bind, "mqtt-rs listening");

    TcpServer::bind(config.server.bind)
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
        .run()
        .await
}
