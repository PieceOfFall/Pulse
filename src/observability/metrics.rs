use std::net::SocketAddr;

use std::time::Duration;

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing::info;

const METRICS_BIND_ENV: &str = "MQTT_RS_METRICS_BIND";

pub(crate) fn init_from_env() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Ok(bind_addr) = std::env::var(METRICS_BIND_ENV) {
        let socket_addr: SocketAddr = bind_addr.parse()?;
        PrometheusBuilder::new()
            .with_http_listener(socket_addr)
            .install()?;
        info!(bind_addr, "prometheus metrics exporter listening");
    }

    describe();

    Ok(())
}

fn describe() {
    describe_gauge!(
        "mqtt_connections_current",
        "Current MQTT client connections."
    );
    describe_counter!(
        "mqtt_connections_total",
        "Total MQTT client connections accepted by the broker."
    );
    describe_counter!(
        "mqtt_publish_in_total",
        "Total inbound MQTT PUBLISH packets received by QoS."
    );
    describe_counter!(
        "mqtt_publish_out_total",
        "Total outbound MQTT PUBLISH packets written to client channels by QoS."
    );
    describe_gauge!("mqtt_subscriptions_current", "Current MQTT subscriptions.");
    describe_gauge!(
        "mqtt_session_queue_size",
        "Current total queued offline session messages."
    );
    describe_gauge!(
        "mqtt_retained_messages_current",
        "Current retained MQTT messages."
    );
    describe_counter!(
        "mqtt_auth_failures_total",
        "Total MQTT authentication failures by reason."
    );
    describe_gauge!(
        "mqtt_qos1_inflight_current",
        "Current outbound QoS 1 inflight messages."
    );
    describe_gauge!(
        "mqtt_qos2_inflight_current",
        "Current QoS 2 inflight messages."
    );
    describe_counter!(
        "mqtt_shared_subscription_dispatch_total",
        "Total dispatches to shared subscription group members."
    );
    describe_histogram!(
        "mqtt_publish_latency_seconds",
        "Latency from accepting an inbound PUBLISH to completing broker-side delivery flush."
    );
    describe_counter!(
        "mqtt_packet_parse_errors_total",
        "Total MQTT packet parse errors observed at the connection lifecycle boundary."
    );
    describe_counter!(
        "mqtt_delivery_flush_failures_total",
        "Total outbound MQTT delivery writes that failed."
    );
}

pub(crate) fn connection_opened() {
    counter!("mqtt_connections_total").increment(1);
    gauge!("mqtt_connections_current").increment(1.0);
}

pub(crate) fn connection_closed(reason: &'static str) {
    let _ = reason;
    gauge!("mqtt_connections_current").decrement(1.0);
}

pub(crate) fn publish_received(qos: &'static str) {
    counter!("mqtt_publish_in_total", "qos" => qos).increment(1);
}

pub(crate) fn publish_sent(qos: &'static str) {
    counter!("mqtt_publish_out_total", "qos" => qos).increment(1);
}

pub(crate) fn delivery_flush_failed() {
    counter!("mqtt_delivery_flush_failures_total").increment(1);
}

pub(crate) fn auth_failed(reason: &'static str) {
    counter!("mqtt_auth_failures_total", "reason" => reason).increment(1);
}

pub(crate) fn shared_subscription_dispatched() {
    counter!("mqtt_shared_subscription_dispatch_total").increment(1);
}

pub(crate) fn publish_latency(duration: Duration) {
    histogram!("mqtt_publish_latency_seconds").record(duration.as_secs_f64());
}

pub(crate) fn packet_parse_error(reason: &'static str) {
    counter!("mqtt_packet_parse_errors_total", "reason" => reason).increment(1);
}

pub(crate) fn set_subscriptions_current(value: usize) {
    gauge!("mqtt_subscriptions_current").set(value as f64);
}

pub(crate) fn set_session_queue_size(value: usize) {
    gauge!("mqtt_session_queue_size").set(value as f64);
}

pub(crate) fn set_retained_messages_current(value: usize) {
    gauge!("mqtt_retained_messages_current").set(value as f64);
}

pub(crate) fn set_qos1_inflight_current(value: usize) {
    gauge!("mqtt_qos1_inflight_current").set(value as f64);
}

pub(crate) fn set_qos2_inflight_current(value: usize) {
    gauge!("mqtt_qos2_inflight_current").set(value as f64);
}
