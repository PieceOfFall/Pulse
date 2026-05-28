use std::{sync::Arc, time::Duration};

use bytes::{Bytes, BytesMut};
use rs_netty::{
    TcpServer,
    codec::{
        ConnectPacket, Decoder, Encoder, MqttCodec, MqttPacket, MqttProperty, PublishPacket, QoS,
        SubscribePacket, Subscription, SubscriptionOptions, Will, mqtt::AckPacket,
    },
    pipeline,
    transport::tcp::server::TcpServerHandle,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use super::{
    Broker, BrokerLife, MqttHandler,
    runtime::{
        auth::{AuthAclConfig, AuthAction, AuthConfig, AuthUserConfig, ConfiguredAuthenticator},
        config::{
            BrokerConfig, MAX_OFFLINE_QUEUE_LEN, MAX_RETAINED_MESSAGES,
            MAX_SUBSCRIPTIONS_PER_CLIENT,
        },
        subscription_tree::{SubscriptionEntry, upsert_subscription},
    },
};
use crate::protocol;

#[tokio::test]
async fn duplicate_client_id_closes_previous_connection() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut first = broker.connect_raw("same-client").await?;
    let _second = broker.connect_raw("same-client").await?;

    let mut buf = [0; 1];
    let read = tokio::time::timeout(Duration::from_millis(200), first.read(&mut buf))
        .await
        .expect("previous connection should close")?;
    assert_eq!(read, 0);

    broker.shutdown().await
}

#[test]
fn upsert_subscription_returns_updated_subscription_index() {
    let mut subscriptions = vec![
        SubscriptionEntry {
            client_id: "client-one".to_string(),
            filter: "devices/one".to_string(),
            match_filter: "devices/one".to_string(),
            shared_group: None,
            options: SubscriptionOptions::default(),
            subscription_identifier: None,
        },
        SubscriptionEntry {
            client_id: "client-two".to_string(),
            filter: "devices/two".to_string(),
            match_filter: "devices/two".to_string(),
            shared_group: None,
            options: SubscriptionOptions::default(),
            subscription_identifier: None,
        },
    ];

    let upsert = upsert_subscription(
        &mut subscriptions,
        "client-one",
        Subscription {
            topic_filter: "devices/one".to_string(),
            options: SubscriptionOptions {
                maximum_qos: QoS::ExactlyOnce,
                ..SubscriptionOptions::default()
            },
        },
        Some(7),
    );

    assert_eq!(upsert.index, 0);
    assert!(!upsert.inserted);
    assert_eq!(subscriptions.len(), 2);
    assert_eq!(
        subscriptions[upsert.index].options.maximum_qos,
        QoS::ExactlyOnce
    );
    assert_eq!(subscriptions[upsert.index].subscription_identifier, Some(7));
}

#[tokio::test]
async fn connect_rejects_empty_client_id_without_clean_start() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut client = broker.open_client().await?;

    client.write(connect_with_clean_start("", false)).await?;
    client
        .expect_connack_reason(protocol::CLIENT_IDENTIFIER_NOT_VALID)
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn connect_rejects_invalid_will_topic() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut client = broker.open_client().await?;

    let mut packet = connect("publisher");
    let MqttPacket::Connect(connect) = &mut packet else {
        unreachable!();
    };
    connect.will = Some(Will {
        qos: QoS::AtMostOnce,
        retain: false,
        properties: Vec::new(),
        topic: "devices/+/state".to_string(),
        payload: Bytes::from_static(b"offline"),
    });

    client.write(packet).await?;
    client
        .expect_connack_reason(protocol::TOPIC_NAME_INVALID)
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn connect_rejects_unsupported_authentication_method() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut client = broker.open_client().await?;

    let mut packet = connect("auth-client");
    let MqttPacket::Connect(connect) = &mut packet else {
        unreachable!();
    };
    connect
        .properties
        .push(MqttProperty::AuthenticationMethod("token".to_string()));

    client.write(packet).await?;
    client
        .expect_connack_reason(protocol::BAD_AUTHENTICATION_METHOD)
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn connect_rejects_username_password_until_auth_is_supported() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut client = broker.open_client().await?;

    let mut packet = connect("auth-client");
    let MqttPacket::Connect(connect) = &mut packet else {
        unreachable!();
    };
    connect.username = Some("alice".to_string());
    connect.password = Some(Bytes::from_static(b"secret"));

    client.write(packet).await?;
    client
        .expect_connack_reason(protocol::BAD_USER_NAME_OR_PASSWORD)
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn auth_enabled_accepts_configured_username_password() -> rs_netty::Result<()> {
    let broker = TestBroker::start_with_broker(auth_broker(vec![], vec![])).await?;
    let mut client = broker.open_client().await?;

    client
        .write(connect_with_credentials("auth-client", "alice", "secret"))
        .await?;
    client.expect_connack().await?;

    broker.shutdown().await
}

#[tokio::test]
async fn auth_enabled_rejects_bad_password() -> rs_netty::Result<()> {
    let broker = TestBroker::start_with_broker(auth_broker(vec![], vec![])).await?;
    let mut client = broker.open_client().await?;

    client
        .write(connect_with_credentials("auth-client", "alice", "wrong"))
        .await?;
    client
        .expect_connack_reason(protocol::BAD_USER_NAME_OR_PASSWORD)
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn subscribe_acl_partially_denies_subscriptions() -> rs_netty::Result<()> {
    let broker = TestBroker::start_with_broker(auth_broker(
        vec![AuthAclConfig {
            username: "alice".to_string(),
            action: AuthAction::Subscribe,
            topic_filter: "devices/alice/#".to_string(),
        }],
        vec![],
    ))
    .await?;
    let mut client = broker.open_client().await?;
    client
        .write(connect_with_credentials("auth-client", "alice", "secret"))
        .await?;
    client.expect_connack().await?;

    client
        .write(MqttPacket::Subscribe(SubscribePacket {
            packet_id: 1,
            properties: Vec::new(),
            subscriptions: vec![
                Subscription {
                    topic_filter: "devices/alice/temp".to_string(),
                    options: SubscriptionOptions::default(),
                },
                Subscription {
                    topic_filter: "devices/bob/temp".to_string(),
                    options: SubscriptionOptions::default(),
                },
            ],
        }))
        .await?;

    assert!(matches!(
        client.read().await?,
        MqttPacket::SubAck(packet)
            if packet.reason_codes == vec![protocol::QOS_0_GRANTED, protocol::NOT_AUTHORIZED]
    ));

    broker.shutdown().await
}

#[tokio::test]
async fn publish_acl_denies_qos0_with_disconnect() -> rs_netty::Result<()> {
    let broker = TestBroker::start_with_broker(auth_broker(
        vec![AuthAclConfig {
            username: "alice".to_string(),
            action: AuthAction::Publish,
            topic_filter: "devices/alice/#".to_string(),
        }],
        vec![],
    ))
    .await?;
    let mut client = broker.open_client().await?;
    client
        .write(connect_with_credentials("auth-client", "alice", "secret"))
        .await?;
    client.expect_connack().await?;

    client
        .write(publish("devices/bob/temp", QoS::AtMostOnce, None, "denied"))
        .await?;
    client
        .expect_disconnect_reason(protocol::NOT_AUTHORIZED)
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn publish_acl_denies_qos1_and_qos2_without_routing() -> rs_netty::Result<()> {
    let broker = TestBroker::start_with_broker(auth_broker(
        vec![
            AuthAclConfig {
                username: "alice".to_string(),
                action: AuthAction::Publish,
                topic_filter: "devices/alice/#".to_string(),
            },
            AuthAclConfig {
                username: "bob".to_string(),
                action: AuthAction::Subscribe,
                topic_filter: "devices/bob/#".to_string(),
            },
        ],
        vec![AuthUserConfig {
            username: "bob".to_string(),
            password: "secret".to_string(),
        }],
    ))
    .await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_credentials("subscriber", "bob", "secret"))
        .await?;
    subscriber.expect_connack().await?;
    subscriber
        .subscribe(1, "devices/bob/#", QoS::ExactlyOnce)
        .await?;

    let mut publisher = broker.open_client().await?;
    publisher
        .write(connect_with_credentials("publisher", "alice", "secret"))
        .await?;
    publisher.expect_connack().await?;

    publisher
        .write(publish(
            "devices/bob/temp",
            QoS::AtLeastOnce,
            Some(1),
            "denied",
        ))
        .await?;
    publisher
        .expect_puback_reason(1, protocol::NOT_AUTHORIZED)
        .await?;
    subscriber
        .expect_no_packet(Duration::from_millis(100))
        .await?;

    publisher
        .write(publish(
            "devices/bob/temp",
            QoS::ExactlyOnce,
            Some(2),
            "denied",
        ))
        .await?;
    publisher
        .expect_pubrec_reason(2, protocol::NOT_AUTHORIZED)
        .await?;
    subscriber
        .expect_no_packet(Duration::from_millis(100))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn malformed_connect_protocol_name_closes_connection() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut client = broker.open_client().await?;

    client
        .write_raw(&connect_packet_with_protocol_name("MQIs"))
        .await?;
    client.expect_closed(Duration::from_millis(200)).await?;

    broker.shutdown().await
}

#[tokio::test]
async fn malformed_packet_after_connect_closes_connection() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut client = broker.connect("client").await?;

    client.write_raw(&[0x40, 0x02, 0x00, 0x00]).await?;
    client.expect_closed(Duration::from_millis(200)).await?;

    broker.shutdown().await
}

#[tokio::test]
async fn keep_alive_timeout_closes_idle_client() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut client = broker.open_client().await?;

    client
        .write(connect_with_keep_alive("idle-client", 1))
        .await?;
    client.expect_connack().await?;
    client.expect_closed(Duration::from_millis(2_500)).await?;

    broker.shutdown().await
}

#[tokio::test]
async fn keep_alive_activity_resets_timeout() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut client = broker.open_client().await?;

    client
        .write(connect_with_keep_alive("active-client", 1))
        .await?;
    client.expect_connack().await?;
    tokio::time::sleep(Duration::from_millis(1_000)).await;
    client.write(MqttPacket::PingReq).await?;
    assert!(matches!(client.read().await?, MqttPacket::PingResp));
    tokio::time::sleep(Duration::from_millis(800)).await;
    client.write(MqttPacket::PingReq).await?;
    assert!(matches!(client.read().await?, MqttPacket::PingResp));

    broker.shutdown().await
}

#[tokio::test]
async fn protocol_error_publishes_will_message() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "clients/publisher/status", QoS::AtMostOnce)
        .await?;

    let mut publisher = broker.open_client().await?;
    publisher
        .write(connect_with_will(
            "publisher",
            "clients/publisher/status",
            "offline",
        ))
        .await?;
    publisher.expect_connack().await?;
    publisher
        .write(MqttPacket::ConnAck(rs_netty::codec::ConnAckPacket {
            session_present: false,
            reason_code: protocol::SUCCESS,
            properties: Vec::new(),
        }))
        .await?;
    publisher
        .expect_disconnect_reason(protocol::PROTOCOL_ERROR)
        .await?;

    let packet = subscriber.expect_publish("expected will publish").await?;
    assert_eq!(packet.topic_name, "clients/publisher/status");
    assert_eq!(packet.payload, Bytes::from_static(b"offline"));

    broker.shutdown().await
}

#[tokio::test]
async fn normal_disconnect_does_not_publish_will_message() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "clients/publisher/status", QoS::AtMostOnce)
        .await?;

    let mut publisher = broker.open_client().await?;
    publisher
        .write(connect_with_will(
            "publisher",
            "clients/publisher/status",
            "offline",
        ))
        .await?;
    publisher.expect_connack().await?;
    publisher
        .write(MqttPacket::Disconnect(rs_netty::codec::DisconnectPacket {
            reason_code: protocol::SUCCESS,
            properties: Vec::new(),
        }))
        .await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn graceful_shutdown_sends_server_disconnect_and_suppresses_will() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "clients/publisher/status", QoS::AtMostOnce)
        .await?;

    let mut publisher = broker.open_client().await?;
    publisher
        .write(connect_with_will(
            "publisher",
            "clients/publisher/status",
            "offline",
        ))
        .await?;
    publisher.expect_connack().await?;

    broker.graceful_shutdown(Duration::from_millis(500)).await?;

    publisher
        .expect_disconnect_reason_string(protocol::SERVER_SHUTTING_DOWN, "server shutting down")
        .await?;
    assert!(matches!(
        subscriber.read().await?,
        MqttPacket::Disconnect(packet) if packet.reason_code == protocol::SERVER_SHUTTING_DOWN
    ));
    subscriber.expect_closed(Duration::from_millis(200)).await
}

#[tokio::test]
async fn connect_after_shutdown_started_is_rejected() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    broker.begin_shutdown();

    let mut client = broker.open_client().await?;
    client.write(connect("client")).await?;
    client
        .expect_connack_reason(protocol::SERVER_UNAVAILABLE)
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn persistent_session_preserves_subscriptions_across_reconnect() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/session", QoS::AtMostOnce)
        .await?;
    subscriber
        .write(MqttPacket::Disconnect(rs_netty::codec::DisconnectPacket {
            reason_code: protocol::SUCCESS,
            properties: Vec::new(),
        }))
        .await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish("devices/session", QoS::AtMostOnce, None, "resumed"))
        .await?;

    let packet = subscriber
        .expect_publish("expected resumed subscription publish")
        .await?;
    assert_eq!(packet.payload, Bytes::from_static(b"resumed"));

    broker.shutdown().await
}

#[tokio::test]
async fn clean_start_clears_persistent_session_subscriptions() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/session", QoS::AtMostOnce)
        .await?;
    subscriber
        .write(MqttPacket::Disconnect(rs_netty::codec::DisconnectPacket {
            reason_code: protocol::SUCCESS,
            properties: Vec::new(),
        }))
        .await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish("devices/session", QoS::AtMostOnce, None, "cleared"))
        .await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn expired_session_does_not_resume_subscriptions() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 1))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/session", QoS::AtMostOnce)
        .await?;
    subscriber
        .write(MqttPacket::Disconnect(rs_netty::codec::DisconnectPacket {
            reason_code: protocol::SUCCESS,
            properties: Vec::new(),
        }))
        .await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish("devices/session", QoS::AtMostOnce, None, "expired"))
        .await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn qos1_outbound_inflight_is_redelivered_after_reconnect() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/inflight", QoS::AtLeastOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish(
            "devices/inflight",
            QoS::AtLeastOnce,
            Some(7),
            "pending",
        ))
        .await?;
    publisher.expect_puback(7).await?;

    let packet = subscriber
        .expect_publish("expected first qos1 delivery")
        .await?;
    assert!(!packet.dup);
    assert_eq!(packet.packet_id, Some(1));
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    let packet = subscriber
        .expect_publish("expected redelivered qos1 publish")
        .await?;
    assert!(packet.dup);
    assert_eq!(packet.qos, QoS::AtLeastOnce);
    assert_eq!(packet.packet_id, Some(1));
    assert_eq!(packet.payload, Bytes::from_static(b"pending"));
    subscriber
        .write(MqttPacket::PubAck(AckPacket::new(1, protocol::SUCCESS)))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn qos2_outbound_inflight_is_redelivered_after_reconnect() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/inflight", QoS::ExactlyOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish(
            "devices/inflight",
            QoS::ExactlyOnce,
            Some(9),
            "pending",
        ))
        .await?;
    publisher.expect_pubrec(9).await?;
    publisher
        .write(MqttPacket::PubRel(AckPacket::new(9, protocol::SUCCESS)))
        .await?;
    publisher.expect_pubcomp(9).await?;

    let packet = subscriber
        .expect_publish("expected first qos2 delivery")
        .await?;
    assert!(!packet.dup);
    assert_eq!(packet.packet_id, Some(1));
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    let packet = subscriber
        .expect_publish("expected redelivered qos2 publish")
        .await?;
    assert!(packet.dup);
    assert_eq!(packet.qos, QoS::ExactlyOnce);
    assert_eq!(packet.packet_id, Some(1));
    assert_eq!(packet.payload, Bytes::from_static(b"pending"));
    subscriber
        .write(MqttPacket::PubRec(AckPacket::new(1, protocol::SUCCESS)))
        .await?;
    subscriber.expect_pubrel(1).await?;
    subscriber
        .write(MqttPacket::PubComp(AckPacket::new(1, protocol::SUCCESS)))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn clean_start_does_not_redeliver_outbound_inflight() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/inflight", QoS::AtLeastOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish(
            "devices/inflight",
            QoS::AtLeastOnce,
            Some(7),
            "pending",
        ))
        .await?;
    publisher.expect_puback(7).await?;
    let _ = subscriber
        .expect_publish("expected first qos1 delivery")
        .await?;
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn persistent_session_queues_qos1_messages_while_offline() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/offline", QoS::AtLeastOnce)
        .await?;
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish(
            "devices/offline",
            QoS::AtLeastOnce,
            Some(7),
            "queued",
        ))
        .await?;
    publisher.expect_puback(7).await?;

    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    let packet = subscriber
        .expect_publish("expected queued qos1 publish")
        .await?;
    assert!(!packet.dup);
    assert_eq!(packet.qos, QoS::AtLeastOnce);
    assert_eq!(packet.packet_id, Some(1));
    assert_eq!(packet.payload, Bytes::from_static(b"queued"));
    subscriber
        .write(MqttPacket::PubAck(AckPacket::new(1, protocol::SUCCESS)))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn persistent_session_queues_qos2_messages_while_offline() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/offline", QoS::ExactlyOnce)
        .await?;
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish(
            "devices/offline",
            QoS::ExactlyOnce,
            Some(9),
            "queued",
        ))
        .await?;
    publisher.expect_pubrec(9).await?;
    publisher
        .write(MqttPacket::PubRel(AckPacket::new(9, protocol::SUCCESS)))
        .await?;
    publisher.expect_pubcomp(9).await?;

    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    let packet = subscriber
        .expect_publish("expected queued qos2 publish")
        .await?;
    assert!(!packet.dup);
    assert_eq!(packet.qos, QoS::ExactlyOnce);
    assert_eq!(packet.packet_id, Some(1));
    assert_eq!(packet.payload, Bytes::from_static(b"queued"));
    subscriber
        .write(MqttPacket::PubRec(AckPacket::new(1, protocol::SUCCESS)))
        .await?;
    subscriber.expect_pubrel(1).await?;
    subscriber
        .write(MqttPacket::PubComp(AckPacket::new(1, protocol::SUCCESS)))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn persistent_session_does_not_queue_qos0_messages_while_offline() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/offline", QoS::AtMostOnce)
        .await?;
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish("devices/offline", QoS::AtMostOnce, None, "dropped"))
        .await?;

    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn receive_maximum_queues_online_qos1_until_ack() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_properties(
            "subscriber",
            true,
            vec![MqttProperty::ReceiveMaximum(1)],
        ))
        .await?;
    subscriber.expect_connack().await?;
    subscriber
        .subscribe(1, "devices/receive-maximum", QoS::AtLeastOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish(
            "devices/receive-maximum",
            QoS::AtLeastOnce,
            Some(7),
            "first",
        ))
        .await?;
    publisher.expect_puback(7).await?;
    publisher
        .write(publish(
            "devices/receive-maximum",
            QoS::AtLeastOnce,
            Some(8),
            "second",
        ))
        .await?;
    publisher.expect_puback(8).await?;

    let first = subscriber
        .expect_publish("expected first receive-maximum publish")
        .await?;
    assert_eq!(first.payload, Bytes::from_static(b"first"));
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    subscriber
        .write(MqttPacket::PubAck(AckPacket::new(
            first.packet_id.unwrap(),
            protocol::SUCCESS,
        )))
        .await?;
    let second = subscriber
        .expect_publish("expected queued publish after ack")
        .await?;
    assert_eq!(second.payload, Bytes::from_static(b"second"));

    broker.shutdown().await
}

#[tokio::test]
async fn qos1_outbound_inflight_retransmits_until_ack() -> rs_netty::Result<()> {
    let broker = TestBroker::start_with_broker(retransmit_broker(50)).await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/retransmit", QoS::AtLeastOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish(
            "devices/retransmit",
            QoS::AtLeastOnce,
            Some(7),
            "retry",
        ))
        .await?;
    publisher.expect_puback(7).await?;

    let first = subscriber
        .expect_publish("expected initial publish")
        .await?;
    assert!(!first.dup);
    let duplicate = subscriber.expect_publish("expected retransmit").await?;
    assert!(duplicate.dup);
    assert_eq!(duplicate.packet_id, first.packet_id);

    subscriber
        .write(MqttPacket::PubAck(AckPacket::new(
            first.packet_id.unwrap(),
            protocol::SUCCESS,
        )))
        .await?;
    subscriber
        .expect_no_packet(Duration::from_millis(120))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn qos2_outbound_publish_and_pubrel_retransmit() -> rs_netty::Result<()> {
    let broker = TestBroker::start_with_broker(retransmit_broker(50)).await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/retransmit-qos2", QoS::ExactlyOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish(
            "devices/retransmit-qos2",
            QoS::ExactlyOnce,
            Some(7),
            "retry",
        ))
        .await?;
    publisher.expect_pubrec(7).await?;
    publisher
        .write(MqttPacket::PubRel(AckPacket::new(7, protocol::SUCCESS)))
        .await?;
    publisher.expect_pubcomp(7).await?;

    let first = subscriber.expect_publish("expected initial qos2").await?;
    assert!(!first.dup);
    let duplicate = subscriber
        .expect_publish("expected qos2 publish retransmit")
        .await?;
    assert!(duplicate.dup);
    assert_eq!(duplicate.packet_id, first.packet_id);

    subscriber
        .write(MqttPacket::PubRec(AckPacket::new(
            first.packet_id.unwrap(),
            protocol::SUCCESS,
        )))
        .await?;
    subscriber.expect_pubrel(first.packet_id.unwrap()).await?;
    subscriber.expect_pubrel(first.packet_id.unwrap()).await?;

    subscriber
        .write(MqttPacket::PubComp(AckPacket::new(
            first.packet_id.unwrap(),
            protocol::SUCCESS,
        )))
        .await?;
    subscriber
        .expect_no_packet(Duration::from_millis(120))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn expired_outbound_inflight_is_not_retransmitted() -> rs_netty::Result<()> {
    let broker = TestBroker::start_with_broker(retransmit_broker(1_100)).await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/retransmit-expiry", QoS::AtLeastOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_message_expiry(
            "devices/retransmit-expiry",
            QoS::AtLeastOnce,
            Some(7),
            "expires",
            false,
            1,
        ))
        .await?;
    publisher.expect_puback(7).await?;
    let first = subscriber
        .expect_publish("expected initial expiring publish")
        .await?;
    assert!(!first.dup);
    subscriber
        .expect_no_packet(Duration::from_millis(1_300))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn maximum_packet_size_drops_oversized_outbound_publish() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_properties(
            "subscriber",
            true,
            vec![MqttProperty::MaximumPacketSize(40)],
        ))
        .await?;
    subscriber.expect_connack().await?;
    subscriber
        .subscribe(1, "devices/max-packet", QoS::AtMostOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish(
            "devices/max-packet",
            QoS::AtMostOnce,
            None,
            "this payload is intentionally too large for the subscriber",
        ))
        .await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn subscription_identifier_is_forwarded_on_matching_publish() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .write(subscribe_with_subscription_identifier(
            1,
            "devices/sub-id",
            QoS::AtMostOnce,
            42,
        ))
        .await?;
    assert!(matches!(subscriber.read().await?, MqttPacket::SubAck(_)));

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish("devices/sub-id", QoS::AtMostOnce, None, "tagged"))
        .await?;

    let packet = subscriber
        .expect_publish("expected subscription identifier publish")
        .await?;
    assert!(
        packet
            .properties
            .contains(&MqttProperty::SubscriptionIdentifier(42))
    );

    broker.shutdown().await
}

#[tokio::test]
async fn publish_properties_are_forwarded() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/properties", QoS::AtMostOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_properties(
            "devices/properties",
            QoS::AtMostOnce,
            None,
            "props",
            vec![
                MqttProperty::UserProperty("trace".to_string(), "abc".to_string()),
                MqttProperty::ResponseTopic("devices/reply".to_string()),
                MqttProperty::CorrelationData(Bytes::from_static(b"corr")),
            ],
        ))
        .await?;

    let packet = subscriber
        .expect_publish("expected forwarded properties")
        .await?;
    assert!(packet.properties.contains(&MqttProperty::UserProperty(
        "trace".to_string(),
        "abc".to_string()
    )));
    assert!(
        packet
            .properties
            .contains(&MqttProperty::ResponseTopic("devices/reply".to_string()))
    );
    assert!(
        packet
            .properties
            .contains(&MqttProperty::CorrelationData(Bytes::from_static(b"corr")))
    );

    broker.shutdown().await
}

#[tokio::test]
async fn topic_alias_resolves_subsequent_empty_topic_publish() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/alias", QoS::AtMostOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_properties(
            "devices/alias",
            QoS::AtMostOnce,
            None,
            "first",
            vec![MqttProperty::TopicAlias(1)],
        ))
        .await?;
    let first = subscriber.expect_publish("expected aliased seed").await?;
    assert_eq!(first.payload, Bytes::from_static(b"first"));

    publisher
        .write(publish_with_properties(
            "",
            QoS::AtMostOnce,
            None,
            "second",
            vec![MqttProperty::TopicAlias(1)],
        ))
        .await?;
    let second = subscriber
        .expect_publish("expected resolved topic alias")
        .await?;
    assert_eq!(second.topic_name, "devices/alias");
    assert_eq!(second.payload, Bytes::from_static(b"second"));

    broker.shutdown().await
}

#[tokio::test]
async fn invalid_topic_alias_disconnects_with_reason_string() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_properties(
            "",
            QoS::AtMostOnce,
            None,
            "invalid",
            vec![MqttProperty::TopicAlias(1)],
        ))
        .await?;
    publisher
        .expect_disconnect_reason_string(protocol::TOPIC_ALIAS_INVALID, "topic alias invalid")
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn shared_subscription_round_robins_online_group_members() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut first = broker.connect("shared-a").await?;
    let mut second = broker.connect("shared-b").await?;
    first
        .subscribe(1, "$share/group/devices/shared", QoS::AtMostOnce)
        .await?;
    second
        .subscribe(1, "$share/group/devices/shared", QoS::AtMostOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish("devices/shared", QoS::AtMostOnce, None, "one"))
        .await?;
    publisher
        .write(publish("devices/shared", QoS::AtMostOnce, None, "two"))
        .await?;

    let first_packet = first
        .expect_publish("expected first shared publish")
        .await?;
    let second_packet = second
        .expect_publish("expected second shared publish")
        .await?;
    assert_eq!(first_packet.payload, Bytes::from_static(b"one"));
    assert_eq!(second_packet.payload, Bytes::from_static(b"two"));

    broker.shutdown().await
}

#[tokio::test]
async fn subscription_quota_returns_quota_exceeded() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut client = broker.connect("subscriber").await?;

    for index in 0..MAX_SUBSCRIPTIONS_PER_CLIENT {
        client
            .write(subscribe(
                (index + 1) as u16,
                &format!("devices/quota/{index}"),
                QoS::AtMostOnce,
            ))
            .await?;
        assert!(matches!(
            client.read().await?,
            MqttPacket::SubAck(packet) if packet.reason_codes == vec![protocol::QOS_0_GRANTED]
        ));
    }

    client
        .write(subscribe(65_000, "devices/quota/overflow", QoS::AtMostOnce))
        .await?;
    assert!(matches!(
        client.read().await?,
        MqttPacket::SubAck(packet)
            if packet.reason_codes == vec![protocol::QUOTA_EXCEEDED]
                && packet.properties.contains(&MqttProperty::ReasonString("quota exceeded".to_string()))
    ));

    broker.shutdown().await
}

#[tokio::test]
async fn offline_queue_limit_drops_new_messages_after_capacity() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/offline-limit", QoS::AtLeastOnce)
        .await?;
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut publisher = broker.connect("publisher").await?;
    for index in 0..=MAX_OFFLINE_QUEUE_LEN {
        publisher
            .write(publish(
                "devices/offline-limit",
                QoS::AtLeastOnce,
                Some((index + 1) as u16),
                &format!("queued-{index}"),
            ))
            .await?;
        publisher.expect_puback((index + 1) as u16).await?;
    }

    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    for index in 0..MAX_OFFLINE_QUEUE_LEN {
        let packet = subscriber
            .expect_publish("expected capped offline publish")
            .await?;
        assert_eq!(
            packet.payload,
            Bytes::copy_from_slice(format!("queued-{index}").as_bytes())
        );
        subscriber
            .write(MqttPacket::PubAck(AckPacket::new(
                packet.packet_id.unwrap(),
                protocol::SUCCESS,
            )))
            .await?;
    }
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn retained_message_limit_drops_new_retained_messages_after_capacity() -> rs_netty::Result<()>
{
    let broker = TestBroker::start().await?;
    let mut publisher = broker.connect("publisher").await?;
    for index in 0..=MAX_RETAINED_MESSAGES {
        publisher
            .write(publish_with_retain(
                &format!("devices/retained-limit/{index}"),
                QoS::AtMostOnce,
                None,
                "retained",
                true,
            ))
            .await?;
    }

    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(
            1,
            &format!("devices/retained-limit/{MAX_RETAINED_MESSAGES}"),
            QoS::AtMostOnce,
        )
        .await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn expired_offline_message_is_not_delivered_after_reconnect() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/expiry/offline", QoS::AtLeastOnce)
        .await?;
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_message_expiry(
            "devices/expiry/offline",
            QoS::AtLeastOnce,
            Some(7),
            "expired",
            false,
            1,
        ))
        .await?;
    publisher.expect_puback(7).await?;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn offline_message_expiry_is_not_refreshed_when_delivered() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/expiry/offline-inflight", QoS::AtLeastOnce)
        .await?;
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_message_expiry(
            "devices/expiry/offline-inflight",
            QoS::AtLeastOnce,
            Some(7),
            "expires-soon",
            false,
            2,
        ))
        .await?;
    publisher.expect_puback(7).await?;

    tokio::time::sleep(Duration::from_millis(1_000)).await;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    let packet = subscriber
        .expect_publish("expected queued publish before expiry")
        .await?;
    assert_eq!(packet.payload, Bytes::from_static(b"expires-soon"));
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    tokio::time::sleep(Duration::from_millis(1_200)).await;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn expired_outbound_inflight_is_not_redelivered_after_reconnect() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/expiry/inflight", QoS::AtLeastOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_message_expiry(
            "devices/expiry/inflight",
            QoS::AtLeastOnce,
            Some(8),
            "expired",
            false,
            1,
        ))
        .await?;
    publisher.expect_puback(8).await?;

    let packet = subscriber
        .expect_publish("expected first expiring qos1 delivery")
        .await?;
    assert_eq!(packet.packet_id, Some(1));
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn expired_inbound_qos2_publish_is_not_delivered_on_pubrel() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/expiry/inbound-qos2", QoS::ExactlyOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_message_expiry(
            "devices/expiry/inbound-qos2",
            QoS::ExactlyOnce,
            Some(9),
            "expired",
            false,
            1,
        ))
        .await?;
    publisher.expect_pubrec(9).await?;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    publisher
        .write(MqttPacket::PubRel(AckPacket::new(9, protocol::SUCCESS)))
        .await?;
    publisher.expect_pubcomp(9).await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn sqlite_broker_recovers_retained_messages_after_restart() -> rs_netty::Result<()> {
    let path = temp_sqlite_path("retained-restart");
    let _ = std::fs::remove_file(&path);

    let broker =
        TestBroker::start_with_broker(Broker::with_sqlite(&path).expect("sqlite broker")).await?;
    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_retain(
            "devices/sqlite",
            QoS::AtLeastOnce,
            Some(3),
            "durable",
            true,
        ))
        .await?;
    publisher.expect_puback(3).await?;
    broker.shutdown().await?;

    let broker =
        TestBroker::start_with_broker(Broker::with_sqlite(&path).expect("sqlite broker")).await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/sqlite", QoS::AtMostOnce)
        .await?;

    let packet = subscriber
        .expect_publish("expected recovered retained publish")
        .await?;
    assert_eq!(packet.qos, QoS::AtMostOnce);
    assert!(packet.retain);
    assert_eq!(packet.payload, Bytes::from_static(b"durable"));

    broker.shutdown().await?;
    cleanup_sqlite_path(&path);
    Ok(())
}

#[tokio::test]
async fn sqlite_broker_recovers_offline_queue_after_restart() -> rs_netty::Result<()> {
    let path = temp_sqlite_path("offline-restart");
    cleanup_sqlite_path(&path);

    let broker =
        TestBroker::start_with_broker(Broker::with_sqlite(&path).expect("sqlite broker")).await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/sqlite-offline", QoS::AtLeastOnce)
        .await?;
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish(
            "devices/sqlite-offline",
            QoS::AtLeastOnce,
            Some(4),
            "durable-offline",
        ))
        .await?;
    publisher.expect_puback(4).await?;
    broker.shutdown().await?;

    let broker =
        TestBroker::start_with_broker(Broker::with_sqlite(&path).expect("sqlite broker")).await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    let packet = subscriber
        .expect_publish("expected recovered offline publish")
        .await?;
    assert!(!packet.dup);
    assert_eq!(packet.qos, QoS::AtLeastOnce);
    assert_eq!(packet.packet_id, Some(1));
    assert_eq!(packet.payload, Bytes::from_static(b"durable-offline"));

    broker.shutdown().await?;
    cleanup_sqlite_path(&path);
    Ok(())
}

#[tokio::test]
async fn sqlite_broker_drops_expired_offline_queue_after_restart() -> rs_netty::Result<()> {
    let path = temp_sqlite_path("offline-expiry-restart");
    cleanup_sqlite_path(&path);

    let broker =
        TestBroker::start_with_broker(Broker::with_sqlite(&path).expect("sqlite broker")).await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/sqlite-expiry", QoS::AtLeastOnce)
        .await?;
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_message_expiry(
            "devices/sqlite-expiry",
            QoS::AtLeastOnce,
            Some(6),
            "expired-durable",
            false,
            1,
        ))
        .await?;
    publisher.expect_puback(6).await?;
    broker.shutdown().await?;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let broker =
        TestBroker::start_with_broker(Broker::with_sqlite(&path).expect("sqlite broker")).await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await?;
    cleanup_sqlite_path(&path);
    Ok(())
}

#[tokio::test]
async fn sqlite_broker_recovers_outbound_inflight_after_restart() -> rs_netty::Result<()> {
    let path = temp_sqlite_path("inflight-restart");
    cleanup_sqlite_path(&path);

    let broker =
        TestBroker::start_with_broker(Broker::with_sqlite(&path).expect("sqlite broker")).await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", true, 60))
        .await?;
    subscriber.expect_connack_session_present(false).await?;
    subscriber
        .subscribe(1, "devices/sqlite-inflight", QoS::AtLeastOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish(
            "devices/sqlite-inflight",
            QoS::AtLeastOnce,
            Some(5),
            "durable-inflight",
        ))
        .await?;
    publisher.expect_puback(5).await?;
    let packet = subscriber
        .expect_publish("expected first sqlite inflight publish")
        .await?;
    assert_eq!(packet.packet_id, Some(1));
    subscriber.write(disconnect_success()).await?;
    subscriber.expect_closed(Duration::from_millis(200)).await?;
    broker.shutdown().await?;

    let broker =
        TestBroker::start_with_broker(Broker::with_sqlite(&path).expect("sqlite broker")).await?;
    let mut subscriber = broker.open_client().await?;
    subscriber
        .write(connect_with_session_expiry("subscriber", false, 60))
        .await?;
    subscriber.expect_connack_session_present(true).await?;
    let packet = subscriber
        .expect_publish("expected recovered inflight publish")
        .await?;
    assert!(packet.dup);
    assert_eq!(packet.qos, QoS::AtLeastOnce);
    assert_eq!(packet.packet_id, Some(1));
    assert_eq!(packet.payload, Bytes::from_static(b"durable-inflight"));

    broker.shutdown().await?;
    cleanup_sqlite_path(&path);
    Ok(())
}

#[tokio::test]
async fn qos1_publish_is_delivered_with_qos1_and_acknowledged() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/one", QoS::AtLeastOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish("devices/one", QoS::AtLeastOnce, Some(7), "hello"))
        .await?;
    publisher.expect_puback(7).await?;

    let packet = subscriber.expect_publish("expected publish").await?;
    assert_eq!(packet.qos, QoS::AtLeastOnce);
    assert_eq!(packet.packet_id, Some(1));
    assert_eq!(packet.payload, Bytes::from_static(b"hello"));
    subscriber
        .write(MqttPacket::PubAck(AckPacket::new(
            packet.packet_id.unwrap(),
            protocol::SUCCESS,
        )))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn qos2_publish_completes_both_handshakes() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/two", QoS::ExactlyOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish("devices/two", QoS::ExactlyOnce, Some(9), "hello"))
        .await?;
    publisher.expect_pubrec(9).await?;
    publisher
        .write(MqttPacket::PubRel(AckPacket::new(9, protocol::SUCCESS)))
        .await?;
    publisher.expect_pubcomp(9).await?;

    let packet = subscriber.expect_publish("expected publish").await?;
    assert_eq!(packet.qos, QoS::ExactlyOnce);
    assert_eq!(packet.packet_id, Some(1));
    assert_eq!(packet.payload, Bytes::from_static(b"hello"));
    subscriber
        .write(MqttPacket::PubRec(AckPacket::new(
            packet.packet_id.unwrap(),
            protocol::SUCCESS,
        )))
        .await?;
    subscriber.expect_pubrel(1).await?;
    subscriber
        .write(MqttPacket::PubComp(AckPacket::new(1, protocol::SUCCESS)))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn qos2_duplicate_packet_id_is_acknowledged_without_replacing_original()
-> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/dup", QoS::ExactlyOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish("devices/dup", QoS::ExactlyOnce, Some(9), "first"))
        .await?;
    publisher.expect_pubrec(9).await?;
    publisher
        .write(publish("devices/dup", QoS::ExactlyOnce, Some(9), "second"))
        .await?;
    publisher.expect_pubrec(9).await?;
    publisher
        .write(MqttPacket::PubRel(AckPacket::new(9, protocol::SUCCESS)))
        .await?;
    publisher.expect_pubcomp(9).await?;

    let packet = subscriber
        .expect_publish("expected original publish")
        .await?;
    assert_eq!(packet.payload, Bytes::from_static(b"first"));

    broker.shutdown().await
}

#[tokio::test]
async fn pubrel_for_missing_inbound_packet_id_returns_not_found() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut publisher = broker.connect("publisher").await?;

    publisher
        .write(MqttPacket::PubRel(AckPacket::new(42, protocol::SUCCESS)))
        .await?;
    publisher
        .expect_pubcomp_reason(42, protocol::PACKET_IDENTIFIER_NOT_FOUND)
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn unexpected_outbound_ack_disconnects_client() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut client = broker.connect("subscriber").await?;

    client
        .write(MqttPacket::PubAck(AckPacket::new(42, protocol::SUCCESS)))
        .await?;
    client
        .expect_disconnect_reason(protocol::PACKET_IDENTIFIER_NOT_FOUND)
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn retained_qos2_publish_replays_at_subscriber_qos() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_retain(
            "devices/retained",
            QoS::ExactlyOnce,
            Some(11),
            "sticky",
            true,
        ))
        .await?;
    publisher.expect_pubrec(11).await?;
    publisher
        .write(MqttPacket::PubRel(AckPacket::new(11, protocol::SUCCESS)))
        .await?;
    publisher.expect_pubcomp(11).await?;

    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/retained", QoS::ExactlyOnce)
        .await?;

    let packet = subscriber
        .expect_publish("expected retained publish")
        .await?;
    assert_eq!(packet.qos, QoS::ExactlyOnce);
    assert!(packet.retain);
    assert_eq!(packet.packet_id, Some(1));
    assert_eq!(packet.payload, Bytes::from_static(b"sticky"));

    broker.shutdown().await
}

#[tokio::test]
async fn retain_handling_one_replays_only_for_new_subscription() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_retain(
            "devices/retained-once",
            QoS::AtMostOnce,
            None,
            "sticky",
            true,
        ))
        .await?;

    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .write(subscribe_with_retain_handling(
            1,
            "devices/retained-once",
            QoS::AtMostOnce,
            1,
        ))
        .await?;
    assert!(matches!(subscriber.read().await?, MqttPacket::SubAck(_)));
    let packet = subscriber
        .expect_publish("expected retained publish for new subscription")
        .await?;
    assert_eq!(packet.payload, Bytes::from_static(b"sticky"));

    subscriber
        .write(subscribe_with_retain_handling(
            2,
            "devices/retained-once",
            QoS::AtMostOnce,
            1,
        ))
        .await?;
    assert!(matches!(subscriber.read().await?, MqttPacket::SubAck(_)));
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn expired_retained_message_is_not_replayed() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_message_expiry(
            "devices/expiry/retained",
            QoS::AtMostOnce,
            None,
            "expired",
            true,
            1,
        ))
        .await?;

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/expiry/retained", QoS::AtMostOnce)
        .await?;
    subscriber
        .expect_no_packet(Duration::from_millis(200))
        .await?;

    broker.shutdown().await
}

#[tokio::test]
async fn non_expired_message_expiry_is_delivered() -> rs_netty::Result<()> {
    let broker = TestBroker::start().await?;
    let mut subscriber = broker.connect("subscriber").await?;
    subscriber
        .subscribe(1, "devices/expiry/live", QoS::AtMostOnce)
        .await?;

    let mut publisher = broker.connect("publisher").await?;
    publisher
        .write(publish_with_message_expiry(
            "devices/expiry/live",
            QoS::AtMostOnce,
            None,
            "fresh",
            false,
            60,
        ))
        .await?;

    let packet = subscriber
        .expect_publish("expected non-expired publish")
        .await?;
    assert_eq!(packet.payload, Bytes::from_static(b"fresh"));

    broker.shutdown().await
}

struct TestBroker {
    server: TcpServerHandle,
    broker: Broker,
}

impl TestBroker {
    async fn start() -> rs_netty::Result<Self> {
        Self::start_with_broker(Broker::new()).await
    }

    async fn start_with_broker(broker: Broker) -> rs_netty::Result<Self> {
        let broker_for_pipeline = broker.clone();
        let server = TcpServer::bind("127.0.0.1:0")
            .life(BrokerLife::new(broker.clone()))
            .pipeline(move || {
                pipeline()
                    .codec(MqttCodec::with_max_packet_size(1024 * 1024))
                    .handler(MqttHandler::new(broker_for_pipeline.clone()))
            })
            .start()
            .await?;

        Ok(Self { server, broker })
    }

    async fn connect(&self, client_id: &str) -> rs_netty::Result<TestClient> {
        let mut client = self.open_client().await?;
        client.write(connect(client_id)).await?;
        client.expect_connack().await?;
        Ok(client)
    }

    async fn open_client(&self) -> rs_netty::Result<TestClient> {
        let stream = TcpStream::connect(self.server.local_addr()).await?;
        Ok(TestClient::new(stream))
    }

    async fn connect_raw(&self, client_id: &str) -> rs_netty::Result<TcpStream> {
        let mut stream = TcpStream::connect(self.server.local_addr()).await?;
        stream.write_all(&connect_packet(client_id)).await?;
        read_connack(&mut stream).await?;
        Ok(stream)
    }

    async fn shutdown(self) -> rs_netty::Result<()> {
        self.server.shutdown();
        self.server.wait().await
    }

    async fn graceful_shutdown(self, timeout: Duration) -> rs_netty::Result<()> {
        self.broker.shutdown_active_sessions(timeout).await;
        self.server.shutdown();
        self.server.wait().await
    }

    fn begin_shutdown(&self) {
        self.broker.begin_shutdown();
    }
}

fn temp_sqlite_path(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("pulse-{name}-{}.db", std::process::id()))
}

fn cleanup_sqlite_path(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}

fn auth_broker(mut acl: Vec<AuthAclConfig>, mut extra_users: Vec<AuthUserConfig>) -> Broker {
    let mut users = vec![AuthUserConfig {
        username: "alice".to_string(),
        password: "secret".to_string(),
    }];
    users.append(&mut extra_users);
    acl.shrink_to_fit();
    Broker::with_config_and_auth(
        BrokerConfig::default(),
        Arc::new(ConfiguredAuthenticator::new(AuthConfig {
            enabled: true,
            users,
            acl,
        })),
    )
}

fn retransmit_broker(interval_ms: u64) -> Broker {
    let mut config = BrokerConfig::default();
    config.inflight_retransmit_interval_ms = interval_ms;
    Broker::with_config(config)
}

struct TestClient {
    stream: TcpStream,
    buf: BytesMut,
}

impl TestClient {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buf: BytesMut::new(),
        }
    }

    async fn write(&mut self, packet: MqttPacket) -> rs_netty::Result<()> {
        write_packet(&mut self.stream, packet).await
    }

    async fn write_raw(&mut self, bytes: &[u8]) -> rs_netty::Result<()> {
        self.stream.write_all(bytes).await?;
        Ok(())
    }

    async fn read(&mut self) -> rs_netty::Result<MqttPacket> {
        read_packet_with_buf(&mut self.stream, &mut self.buf).await
    }

    async fn subscribe(
        &mut self,
        packet_id: u16,
        topic_filter: &str,
        maximum_qos: QoS,
    ) -> rs_netty::Result<()> {
        self.write(subscribe(packet_id, topic_filter, maximum_qos))
            .await?;
        assert!(matches!(self.read().await?, MqttPacket::SubAck(_)));
        Ok(())
    }

    async fn expect_connack(&mut self) -> rs_netty::Result<()> {
        assert!(matches!(self.read().await?, MqttPacket::ConnAck(_)));
        Ok(())
    }

    async fn expect_connack_session_present(
        &mut self,
        session_present: bool,
    ) -> rs_netty::Result<()> {
        assert!(matches!(
            self.read().await?,
            MqttPacket::ConnAck(packet) if packet.session_present == session_present
        ));
        Ok(())
    }

    async fn expect_connack_reason(&mut self, reason_code: u8) -> rs_netty::Result<()> {
        assert!(matches!(
            self.read().await?,
            MqttPacket::ConnAck(packet) if packet.reason_code == reason_code
        ));
        Ok(())
    }

    async fn expect_closed(&mut self, timeout: Duration) -> rs_netty::Result<()> {
        let mut byte = [0; 1];
        let read = tokio::time::timeout(timeout, self.stream.read(&mut byte))
            .await
            .expect("connection should close before timeout")?;
        assert_eq!(read, 0);
        Ok(())
    }

    async fn expect_no_packet(&mut self, timeout: Duration) -> rs_netty::Result<()> {
        assert!(
            tokio::time::timeout(timeout, self.read()).await.is_err(),
            "unexpected MQTT packet before timeout"
        );
        Ok(())
    }

    async fn expect_disconnect_reason(&mut self, reason_code: u8) -> rs_netty::Result<()> {
        assert!(matches!(
            self.read().await?,
            MqttPacket::Disconnect(packet) if packet.reason_code == reason_code
        ));
        Ok(())
    }

    async fn expect_disconnect_reason_string(
        &mut self,
        reason_code: u8,
        reason_string: &str,
    ) -> rs_netty::Result<()> {
        assert!(matches!(
            self.read().await?,
            MqttPacket::Disconnect(packet)
                if packet.reason_code == reason_code
                    && packet.properties.contains(&MqttProperty::ReasonString(reason_string.to_string()))
        ));
        Ok(())
    }

    async fn expect_publish(&mut self, message: &str) -> rs_netty::Result<PublishPacket> {
        let delivered = self.read().await?;
        let MqttPacket::Publish(packet) = delivered else {
            panic!("{message}, got {delivered:?}");
        };
        Ok(packet)
    }

    async fn expect_puback(&mut self, packet_id: u16) -> rs_netty::Result<()> {
        self.expect_puback_reason(packet_id, protocol::SUCCESS)
            .await
    }

    async fn expect_puback_reason(
        &mut self,
        packet_id: u16,
        reason_code: u8,
    ) -> rs_netty::Result<()> {
        assert!(matches!(
            self.read().await?,
            MqttPacket::PubAck(packet)
                if packet.packet_id == packet_id && packet.reason_code == reason_code
        ));
        Ok(())
    }

    async fn expect_pubrec(&mut self, packet_id: u16) -> rs_netty::Result<()> {
        self.expect_pubrec_reason(packet_id, protocol::SUCCESS)
            .await
    }

    async fn expect_pubrec_reason(
        &mut self,
        packet_id: u16,
        reason_code: u8,
    ) -> rs_netty::Result<()> {
        assert!(matches!(
            self.read().await?,
            MqttPacket::PubRec(packet)
                if packet.packet_id == packet_id && packet.reason_code == reason_code
        ));
        Ok(())
    }

    async fn expect_pubrel(&mut self, packet_id: u16) -> rs_netty::Result<()> {
        assert!(matches!(
            self.read().await?,
            MqttPacket::PubRel(packet) if packet.packet_id == packet_id
        ));
        Ok(())
    }

    async fn expect_pubcomp(&mut self, packet_id: u16) -> rs_netty::Result<()> {
        self.expect_pubcomp_reason(packet_id, protocol::SUCCESS)
            .await
    }

    async fn expect_pubcomp_reason(
        &mut self,
        packet_id: u16,
        reason_code: u8,
    ) -> rs_netty::Result<()> {
        assert!(matches!(
            self.read().await?,
            MqttPacket::PubComp(packet)
                if packet.packet_id == packet_id && packet.reason_code == reason_code
        ));
        Ok(())
    }
}

fn connect_packet(client_id: &str) -> Vec<u8> {
    connect_packet_with_protocol_name_and_level(client_id, "MQTT", 5)
}

fn connect_packet_with_protocol_name(protocol_name: &str) -> Vec<u8> {
    connect_packet_with_protocol_name_and_level("client", protocol_name, 5)
}

fn connect_packet_with_protocol_name_and_level(
    client_id: &str,
    protocol_name: &str,
    protocol_level: u8,
) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.push(0x10);
    let remaining_len = 2 + protocol_name.len() + 1 + 1 + 2 + 1 + 2 + client_id.len();
    encode_remaining_len(remaining_len, &mut packet);
    packet.extend_from_slice(&(protocol_name.len() as u16).to_be_bytes());
    packet.extend_from_slice(protocol_name.as_bytes());
    packet.extend_from_slice(&[protocol_level, 0x02, 0x00, 0x3c, 0x00]);
    packet.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
    packet.extend_from_slice(client_id.as_bytes());
    packet
}

fn encode_remaining_len(mut len: usize, dst: &mut Vec<u8>) {
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        dst.push(byte);
        if len == 0 {
            break;
        }
    }
}

async fn read_connack(stream: &mut TcpStream) -> rs_netty::Result<()> {
    let mut fixed = [0; 2];
    stream.read_exact(&mut fixed).await?;
    assert_eq!(fixed[0], 0x20);
    let mut rest = vec![0; fixed[1] as usize];
    stream.read_exact(&mut rest).await?;
    assert_eq!(rest[0], 0);
    assert_eq!(rest[1], 0);
    Ok(())
}

fn connect(client_id: &str) -> MqttPacket {
    connect_with_clean_start(client_id, true)
}

fn connect_with_clean_start(client_id: &str, clean_start: bool) -> MqttPacket {
    MqttPacket::Connect(ConnectPacket {
        clean_start,
        keep_alive: 60,
        properties: Vec::new(),
        client_id: client_id.to_string(),
        will: None,
        username: None,
        password: None,
    })
}

fn connect_with_session_expiry(
    client_id: &str,
    clean_start: bool,
    session_expiry_interval: u32,
) -> MqttPacket {
    connect_with_properties(
        client_id,
        clean_start,
        vec![MqttProperty::SessionExpiryInterval(session_expiry_interval)],
    )
}

fn connect_with_properties(
    client_id: &str,
    clean_start: bool,
    properties: Vec<MqttProperty>,
) -> MqttPacket {
    MqttPacket::Connect(ConnectPacket {
        clean_start,
        keep_alive: 60,
        properties,
        client_id: client_id.to_string(),
        will: None,
        username: None,
        password: None,
    })
}

fn connect_with_keep_alive(client_id: &str, keep_alive: u16) -> MqttPacket {
    MqttPacket::Connect(ConnectPacket {
        clean_start: true,
        keep_alive,
        properties: Vec::new(),
        client_id: client_id.to_string(),
        will: None,
        username: None,
        password: None,
    })
}

fn connect_with_credentials(client_id: &str, username: &str, password: &str) -> MqttPacket {
    MqttPacket::Connect(ConnectPacket {
        clean_start: true,
        keep_alive: 60,
        properties: Vec::new(),
        client_id: client_id.to_string(),
        will: None,
        username: Some(username.to_string()),
        password: Some(Bytes::copy_from_slice(password.as_bytes())),
    })
}

fn connect_with_will(client_id: &str, topic: &str, payload: &str) -> MqttPacket {
    MqttPacket::Connect(ConnectPacket {
        clean_start: true,
        keep_alive: 60,
        properties: Vec::new(),
        client_id: client_id.to_string(),
        will: Some(Will {
            qos: QoS::AtMostOnce,
            retain: false,
            properties: Vec::new(),
            topic: topic.to_string(),
            payload: Bytes::copy_from_slice(payload.as_bytes()),
        }),
        username: None,
        password: None,
    })
}

fn disconnect_success() -> MqttPacket {
    MqttPacket::Disconnect(rs_netty::codec::DisconnectPacket {
        reason_code: protocol::SUCCESS,
        properties: Vec::new(),
    })
}

fn subscribe(packet_id: u16, topic_filter: &str, maximum_qos: QoS) -> MqttPacket {
    subscribe_with_retain_handling(packet_id, topic_filter, maximum_qos, 0)
}

fn subscribe_with_retain_handling(
    packet_id: u16,
    topic_filter: &str,
    maximum_qos: QoS,
    retain_handling: u8,
) -> MqttPacket {
    MqttPacket::Subscribe(SubscribePacket {
        packet_id,
        properties: Vec::new(),
        subscriptions: vec![Subscription {
            topic_filter: topic_filter.to_string(),
            options: SubscriptionOptions {
                maximum_qos,
                retain_handling,
                ..SubscriptionOptions::default()
            },
        }],
    })
}

fn subscribe_with_subscription_identifier(
    packet_id: u16,
    topic_filter: &str,
    maximum_qos: QoS,
    subscription_identifier: u32,
) -> MqttPacket {
    MqttPacket::Subscribe(SubscribePacket {
        packet_id,
        properties: vec![MqttProperty::SubscriptionIdentifier(
            subscription_identifier,
        )],
        subscriptions: vec![Subscription {
            topic_filter: topic_filter.to_string(),
            options: SubscriptionOptions {
                maximum_qos,
                ..SubscriptionOptions::default()
            },
        }],
    })
}

fn publish(topic_name: &str, qos: QoS, packet_id: Option<u16>, payload: &str) -> MqttPacket {
    publish_with_retain(topic_name, qos, packet_id, payload, false)
}

fn publish_with_retain(
    topic_name: &str,
    qos: QoS,
    packet_id: Option<u16>,
    payload: &str,
    retain: bool,
) -> MqttPacket {
    MqttPacket::Publish(PublishPacket {
        dup: false,
        qos,
        retain,
        topic_name: topic_name.to_string(),
        packet_id,
        properties: Vec::new(),
        payload: Bytes::copy_from_slice(payload.as_bytes()),
    })
}

fn publish_with_message_expiry(
    topic_name: &str,
    qos: QoS,
    packet_id: Option<u16>,
    payload: &str,
    retain: bool,
    expiry_interval: u32,
) -> MqttPacket {
    MqttPacket::Publish(PublishPacket {
        dup: false,
        qos,
        retain,
        topic_name: topic_name.to_string(),
        packet_id,
        properties: vec![MqttProperty::MessageExpiryInterval(expiry_interval)],
        payload: Bytes::copy_from_slice(payload.as_bytes()),
    })
}

fn publish_with_properties(
    topic_name: &str,
    qos: QoS,
    packet_id: Option<u16>,
    payload: &str,
    properties: Vec<MqttProperty>,
) -> MqttPacket {
    MqttPacket::Publish(PublishPacket {
        dup: false,
        qos,
        retain: false,
        topic_name: topic_name.to_string(),
        packet_id,
        properties,
        payload: Bytes::copy_from_slice(payload.as_bytes()),
    })
}

async fn write_packet(stream: &mut TcpStream, packet: MqttPacket) -> rs_netty::Result<()> {
    let mut codec = MqttCodec::new();
    let mut buf = BytesMut::new();
    codec.encode(packet, &mut buf)?;
    stream.write_all(&buf).await?;
    Ok(())
}

async fn read_packet_with_buf(
    stream: &mut TcpStream,
    buf: &mut BytesMut,
) -> rs_netty::Result<MqttPacket> {
    let mut codec = MqttCodec::new();
    loop {
        if let Some(packet) = codec.decode(buf)? {
            return Ok(packet);
        }

        let mut chunk = [0; 1024];
        let read = stream.read(&mut chunk).await?;
        assert_ne!(read, 0, "connection closed before next MQTT packet");
        buf.extend_from_slice(&chunk[..read]);
    }
}
