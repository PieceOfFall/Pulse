use super::Delivery;
use crate::observability::metrics;
use rs_netty::codec::{MqttPacket, QoS};

pub(in crate::broker) async fn flush_deliveries(deliveries: Vec<Delivery>) {
    let mut flush_channels = Vec::new();

    for delivery in deliveries {
        if let MqttPacket::Publish(packet) = &delivery.packet {
            metrics::publish_sent(qos_name(packet.qos));
        }

        let channel = delivery.channel;
        let channel_id = channel.id();
        if channel.write(delivery.packet).await.is_err() {
            metrics::delivery_flush_failed();
            continue;
        }
        if !flush_channels
            .iter()
            .any(|queued: &rs_netty::Channel<MqttPacket>| queued.id() == channel_id)
        {
            flush_channels.push(channel);
        }
    }

    for channel in flush_channels {
        let _flush_task = tokio::spawn(async move {
            if channel.flush().await.is_err() {
                metrics::delivery_flush_failed();
            }
        });
    }
}

fn qos_name(qos: QoS) -> &'static str {
    match qos {
        QoS::AtMostOnce => "0",
        QoS::AtLeastOnce => "1",
        QoS::ExactlyOnce => "2",
    }
}
