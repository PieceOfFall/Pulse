use super::Delivery;
use crate::observability::metrics;
use rs_netty::codec::{MqttPacket, QoS};

pub(in crate::broker) async fn flush_deliveries(deliveries: Vec<Delivery>) {
    for delivery in deliveries {
        if let MqttPacket::Publish(packet) = &delivery.packet {
            metrics::publish_sent(qos_name(packet.qos));
        }
        if delivery.channel.write(delivery.packet).await.is_err() {
            metrics::delivery_flush_failed();
        }
    }
}

fn qos_name(qos: QoS) -> &'static str {
    match qos {
        QoS::AtMostOnce => "0",
        QoS::AtLeastOnce => "1",
        QoS::ExactlyOnce => "2",
    }
}
