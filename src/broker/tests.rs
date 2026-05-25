use std::time::Duration;

use bytes::{Bytes, BytesMut};
use rs_netty::{
    TcpServer,
    codec::{
        ConnectPacket, Decoder, Encoder, MqttCodec, MqttPacket, PublishPacket, QoS,
        SubscribePacket, Subscription, SubscriptionOptions, mqtt::AckPacket,
    },
    pipeline,
    transport::tcp::server::TcpServerHandle,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use super::{Broker, BrokerLife, MqttHandler, SubscriptionEntry, protocol, upsert_subscription};

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
            connection_id: 1,
            filter: "devices/one".to_string(),
            options: SubscriptionOptions::default(),
        },
        SubscriptionEntry {
            connection_id: 2,
            filter: "devices/two".to_string(),
            options: SubscriptionOptions::default(),
        },
    ];

    let index = upsert_subscription(
        &mut subscriptions,
        1,
        Subscription {
            topic_filter: "devices/one".to_string(),
            options: SubscriptionOptions {
                maximum_qos: QoS::ExactlyOnce,
                ..SubscriptionOptions::default()
            },
        },
    );

    assert_eq!(index, 0);
    assert_eq!(subscriptions.len(), 2);
    assert_eq!(subscriptions[index].options.maximum_qos, QoS::ExactlyOnce);
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

struct TestBroker {
    server: TcpServerHandle,
}

impl TestBroker {
    async fn start() -> rs_netty::Result<Self> {
        let broker = Broker::new();
        let server = TcpServer::bind("127.0.0.1:0")
            .life(BrokerLife::new(broker.clone()))
            .pipeline(move || {
                pipeline()
                    .codec(MqttCodec::with_max_packet_size(1024 * 1024))
                    .handler(MqttHandler::new(broker.clone()))
            })
            .start()
            .await?;

        Ok(Self { server })
    }

    async fn connect(&self, client_id: &str) -> rs_netty::Result<TestClient> {
        let stream = TcpStream::connect(self.server.local_addr()).await?;
        let mut client = TestClient::new(stream);
        client.write(connect(client_id)).await?;
        client.expect_connack().await?;
        Ok(client)
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

    async fn expect_publish(&mut self, message: &str) -> rs_netty::Result<PublishPacket> {
        let delivered = self.read().await?;
        let MqttPacket::Publish(packet) = delivered else {
            panic!("{message}, got {delivered:?}");
        };
        Ok(packet)
    }

    async fn expect_puback(&mut self, packet_id: u16) -> rs_netty::Result<()> {
        assert!(matches!(
            self.read().await?,
            MqttPacket::PubAck(packet) if packet.packet_id == packet_id
        ));
        Ok(())
    }

    async fn expect_pubrec(&mut self, packet_id: u16) -> rs_netty::Result<()> {
        assert!(matches!(
            self.read().await?,
            MqttPacket::PubRec(packet) if packet.packet_id == packet_id
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
        assert!(matches!(
            self.read().await?,
            MqttPacket::PubComp(packet) if packet.packet_id == packet_id
        ));
        Ok(())
    }
}

fn connect_packet(client_id: &str) -> Vec<u8> {
    let mut packet = Vec::new();
    packet.push(0x10);
    let remaining_len = 13 + client_id.len();
    encode_remaining_len(remaining_len, &mut packet);
    packet.extend_from_slice(&[0x00, 0x04]);
    packet.extend_from_slice(b"MQTT");
    packet.extend_from_slice(&[0x05, 0x02, 0x00, 0x3c, 0x00]);
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
    MqttPacket::Connect(ConnectPacket {
        clean_start: true,
        keep_alive: 60,
        properties: Vec::new(),
        client_id: client_id.to_string(),
        will: None,
        username: None,
        password: None,
    })
}

fn subscribe(packet_id: u16, topic_filter: &str, maximum_qos: QoS) -> MqttPacket {
    MqttPacket::Subscribe(SubscribePacket {
        packet_id,
        properties: Vec::new(),
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
