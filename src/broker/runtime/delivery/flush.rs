use super::Delivery;
use crate::observability::metrics;
use rs_netty::{
    Context, Result,
    codec::{MqttPacket, QoS},
};
use std::collections::HashSet;

pub(in crate::broker) async fn flush_deliveries(deliveries: Vec<Delivery>) {
    if deliveries.len() == 1 {
        flush_single_delivery_via_channel(deliveries.into_iter().next().expect("single delivery"))
            .await;
        return;
    }

    let mut flush_channels = Vec::new();
    let mut flush_channel_ids = HashSet::new();

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
        if flush_channel_ids.insert(channel_id) {
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

pub(in crate::broker) async fn flush_deliveries_to_context(
    ctx: &mut Context<MqttPacket>,
    deliveries: Vec<Delivery>,
) -> Result<()> {
    if deliveries.is_empty() {
        return Ok(());
    }

    let mut external = Vec::new();
    for delivery in deliveries {
        if delivery.channel.id() == ctx.id() {
            if let MqttPacket::Publish(packet) = &delivery.packet {
                metrics::publish_sent(qos_name(packet.qos));
            }
            ctx.write(delivery.packet).await?;
        } else {
            external.push(delivery);
        }
    }

    let _flush = ctx.flush();
    if !external.is_empty() {
        flush_deliveries(external).await;
    }
    Ok(())
}

async fn flush_single_delivery_via_channel(delivery: Delivery) {
    if let MqttPacket::Publish(packet) = &delivery.packet {
        metrics::publish_sent(qos_name(packet.qos));
    }
    let channel = delivery.channel;
    if channel.write(delivery.packet).await.is_err() {
        metrics::delivery_flush_failed();
        return;
    }
    let _flush_task = tokio::spawn(async move {
        if channel.flush().await.is_err() {
            metrics::delivery_flush_failed();
        }
    });
}

fn qos_name(qos: QoS) -> &'static str {
    match qos {
        QoS::AtMostOnce => "0",
        QoS::AtLeastOnce => "1",
        QoS::ExactlyOnce => "2",
    }
}
