mod broker;
mod observability;
mod protocol;

use broker::{Broker, BrokerLife, MqttHandler};
use rs_netty::{Error, Result, TcpServer, codec::MqttCodec, pipeline};
use tracing::info;

const DEFAULT_BIND_ADDR: &str = "0.0.0.0:1883";

#[tokio::main]
async fn main() -> Result<()> {
    observability::init_from_env()
        .map_err(|error| Error::Pipeline(format!("initialize observability: {error}")))?;

    let bind_addr = std::env::var("MQTT_RS_BIND").unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_string());
    let broker = if let Ok(path) = std::env::var("MQTT_RS_SQLITE") {
        Broker::with_sqlite(path)
            .map_err(|error| Error::Pipeline(format!("open sqlite storage: {error}")))?
    } else {
        Broker::new()
    };

    info!(bind_addr, "mqtt-rs listening");

    TcpServer::bind(bind_addr)
        .outbound_queue_size(1024)
        .track_connection_stats()
        .life(BrokerLife::new(broker.clone()))
        .pipeline(move || {
            pipeline()
                .codec(MqttCodec::with_max_packet_size(
                    broker::runtime::config::SERVER_MAXIMUM_PACKET_SIZE as usize,
                ))
                .handler(MqttHandler::new(broker.clone()))
        })
        .run()
        .await
}
