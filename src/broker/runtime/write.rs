use bytes::{Bytes, BytesMut};
use rs_netty::{
    Error, Result,
    codec::{Decoder, Encoder, MqttCodec, MqttPacket, QoS},
};

#[derive(Clone, Debug, PartialEq)]
pub enum BrokerWrite {
    Packet(Box<MqttPacket>),
    Preencoded {
        bytes: Bytes,
        publish_qos: Option<QoS>,
    },
}

impl BrokerWrite {
    pub(crate) fn publish_qos(&self) -> Option<QoS> {
        match self {
            Self::Packet(packet) => match packet.as_ref() {
                MqttPacket::Publish(packet) => Some(packet.qos),
                _ => None,
            },
            Self::Preencoded { publish_qos, .. } => *publish_qos,
        }
    }
}

impl From<MqttPacket> for BrokerWrite {
    fn from(packet: MqttPacket) -> Self {
        Self::Packet(Box::new(packet))
    }
}

pub(crate) struct PulseMqttCodec {
    inner: MqttCodec,
    max_packet_size: usize,
}

impl PulseMqttCodec {
    pub(crate) fn with_max_packet_size(max_packet_size: usize) -> Self {
        Self {
            inner: MqttCodec::with_max_packet_size(max_packet_size),
            max_packet_size,
        }
    }
}

impl Decoder for PulseMqttCodec {
    type Item = MqttPacket;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        self.inner.decode(src)
    }
}

impl Encoder<BrokerWrite> for PulseMqttCodec {
    fn encode(&mut self, item: BrokerWrite, dst: &mut BytesMut) -> Result<()> {
        match item {
            BrokerWrite::Packet(packet) => self.inner.encode(*packet, dst),
            BrokerWrite::Preencoded { bytes, .. } => {
                if bytes.len() > self.max_packet_size {
                    return Err(Error::FrameTooLarge {
                        current: bytes.len(),
                        max: self.max_packet_size,
                    });
                }
                dst.reserve(bytes.len());
                dst.extend_from_slice(&bytes);
                Ok(())
            }
        }
    }
}

pub(crate) fn preencoded_single_suback(packet_id: u16, reason_code: u8) -> BrokerWrite {
    let bytes = Bytes::copy_from_slice(&[
        0x90,
        0x04,
        (packet_id >> 8) as u8,
        packet_id as u8,
        0x00,
        reason_code,
    ]);
    BrokerWrite::Preencoded {
        bytes,
        publish_qos: None,
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Bytes, BytesMut};
    use rs_netty::codec::{Encoder, MqttCodec, MqttPacket, PublishPacket, QoS};

    use super::{BrokerWrite, PulseMqttCodec, preencoded_single_suback};

    #[test]
    fn packet_encoding_matches_mqtt_codec() {
        let packet = MqttPacket::Publish(PublishPacket {
            dup: false,
            qos: QoS::AtMostOnce,
            retain: true,
            topic_name: "devices/a".to_string(),
            packet_id: None,
            properties: Vec::new(),
            payload: Bytes::from_static(b"hello"),
        });

        let mut expected = BytesMut::new();
        MqttCodec::new()
            .encode(packet.clone(), &mut expected)
            .expect("encode mqtt packet");

        let mut actual = BytesMut::new();
        PulseMqttCodec::with_max_packet_size(1024)
            .encode(BrokerWrite::from(packet), &mut actual)
            .expect("encode broker write");

        assert_eq!(actual, expected);
    }

    #[test]
    fn preencoded_publish_is_appended_unchanged() {
        let bytes = Bytes::from_static(b"\x31\x04\x00\x01a\x00");
        let mut actual = BytesMut::new();

        PulseMqttCodec::with_max_packet_size(1024)
            .encode(
                BrokerWrite::Preencoded {
                    bytes: bytes.clone(),
                    publish_qos: Some(QoS::AtMostOnce),
                },
                &mut actual,
            )
            .expect("encode preencoded publish");

        assert_eq!(actual.freeze(), bytes);
    }

    #[test]
    fn preencoded_single_suback_uses_mqtt_wire_shape() {
        let mut actual = BytesMut::new();
        PulseMqttCodec::with_max_packet_size(1024)
            .encode(preencoded_single_suback(7, 0), &mut actual)
            .expect("encode preencoded suback");

        assert_eq!(
            actual.freeze(),
            Bytes::from_static(b"\x90\x04\x00\x07\x00\x00")
        );
    }
}
