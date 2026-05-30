use bytes::{Bytes, BytesMut};
use rs_netty::{
    Context, Error, Handler, Result,
    codec::{
        Decoder, HttpResponse, HttpWsInbound, MqttCodec, MqttPacket, WebSocketClose,
        WebSocketHandshake, WebSocketMessage,
    },
};

use super::{ConnectionIdMap, MqttHandler};
use crate::broker::{Broker, runtime::write::BrokerWrite};

const WEBSOCKET_PROTOCOL_ERROR: u16 = 1002;
const WEBSOCKET_UNSUPPORTED_DATA: u16 = 1003;

pub(crate) struct WebSocketMqttHandler {
    path: String,
    accepted: bool,
    mqtt: MqttHandler,
    decoder: MqttCodec,
}

impl WebSocketMqttHandler {
    #[allow(dead_code)]
    pub(crate) fn new(broker: Broker, path: String) -> Self {
        Self::with_connection_ids(broker, path, ConnectionIdMap::default())
    }

    pub(crate) fn with_connection_ids(
        broker: Broker,
        path: String,
        connection_ids: ConnectionIdMap,
    ) -> Self {
        let max_packet_size = broker.config().server_maximum_packet_size as usize;
        Self {
            path,
            accepted: false,
            mqtt: MqttHandler::with_connection_ids(broker, connection_ids),
            decoder: MqttCodec::with_max_packet_size(max_packet_size),
        }
    }

    async fn accept_handshake(
        &mut self,
        ctx: &mut Context<BrokerWrite>,
        handshake: WebSocketHandshake,
    ) -> Result<()> {
        if handshake.path() != self.path {
            return reject_http(ctx, 404, "websocket path not found").await;
        }

        let Some(protocol) = select_mqtt_subprotocol(handshake.header("Sec-WebSocket-Protocol"))
        else {
            return reject_http(ctx, 400, "missing MQTT websocket subprotocol").await;
        };

        self.accepted = true;
        ctx.write_and_flush(BrokerWrite::WebSocketHandshake(
            handshake
                .accept_response()
                .header("Sec-WebSocket-Protocol", protocol),
        ))
        .await
    }

    async fn close_websocket(
        &mut self,
        ctx: &mut Context<BrokerWrite>,
        code: u16,
        reason: &'static str,
    ) -> Result<()> {
        ctx.write_and_flush(BrokerWrite::WebSocketClose(Some(WebSocketClose {
            code,
            reason: reason.to_string(),
        })))
        .await?;
        ctx.close().await
    }

    fn decode_mqtt_packet(&mut self, bytes: Bytes) -> Result<MqttPacket> {
        let mut frame = BytesMut::from(bytes.as_ref());
        let Some(packet) = self.decoder.decode(&mut frame)? else {
            return Err(Error::Decode(
                "websocket binary frame did not contain a complete MQTT packet".to_string(),
            ));
        };
        if !frame.is_empty() {
            return Err(Error::Decode(
                "websocket binary frame must contain exactly one MQTT packet".to_string(),
            ));
        }
        Ok(packet)
    }
}

impl Handler<HttpWsInbound> for WebSocketMqttHandler {
    type Write = BrokerWrite;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: HttpWsInbound) -> Result<()> {
        match msg {
            HttpWsInbound::Http(_) => reject_http(ctx, 404, "not found").await,
            HttpWsInbound::WebSocketHandshake(handshake) => {
                self.accept_handshake(ctx, handshake).await
            }
            HttpWsInbound::WebSocket(message) if !self.accepted => {
                self.close_websocket(ctx, WEBSOCKET_PROTOCOL_ERROR, "websocket not accepted")
                    .await
            }
            HttpWsInbound::WebSocket(WebSocketMessage::Binary(bytes)) => {
                let packet = self.decode_mqtt_packet(bytes)?;
                self.mqtt.read(ctx, packet).await
            }
            HttpWsInbound::WebSocket(WebSocketMessage::Text(_)) => {
                self.close_websocket(
                    ctx,
                    WEBSOCKET_UNSUPPORTED_DATA,
                    "MQTT websocket payload must be binary",
                )
                .await
            }
            HttpWsInbound::WebSocket(WebSocketMessage::Ping(bytes)) => {
                ctx.write_and_flush(BrokerWrite::WebSocketPong(bytes)).await
            }
            HttpWsInbound::WebSocket(WebSocketMessage::Pong(_)) => Ok(()),
            HttpWsInbound::WebSocket(WebSocketMessage::Close(close)) => {
                ctx.write_and_flush(BrokerWrite::WebSocketClose(close))
                    .await?;
                ctx.close().await
            }
        }
    }
}

async fn reject_http(
    ctx: &mut Context<BrokerWrite>,
    status: u16,
    body: &'static str,
) -> Result<()> {
    ctx.write_and_flush(BrokerWrite::HttpResponse(
        HttpResponse::new(status)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(Bytes::from_static(body.as_bytes())),
    ))
    .await?;
    ctx.close().await
}

fn select_mqtt_subprotocol(value: Option<&str>) -> Option<&'static str> {
    let mut legacy = false;
    for protocol in value?.split(',').map(str::trim) {
        if protocol.eq_ignore_ascii_case("mqtt") {
            return Some("mqtt");
        }
        if protocol.eq_ignore_ascii_case("mqttv3.1") {
            legacy = true;
        }
    }
    legacy.then_some("mqttv3.1")
}

#[cfg(test)]
mod tests {
    use super::select_mqtt_subprotocol;

    #[test]
    fn selects_mqtt_subprotocol_prefering_modern_name() {
        assert_eq!(select_mqtt_subprotocol(Some("mqtt")), Some("mqtt"));
        assert_eq!(select_mqtt_subprotocol(Some("mqttv3.1")), Some("mqttv3.1"));
        assert_eq!(
            select_mqtt_subprotocol(Some("mqttv3.1, mqtt")),
            Some("mqtt")
        );
        assert_eq!(select_mqtt_subprotocol(Some("chat")), None);
        assert_eq!(select_mqtt_subprotocol(None), None);
    }
}
