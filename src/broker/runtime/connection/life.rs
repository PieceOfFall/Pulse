use rs_netty::{CloseReason, ConnInfo, Life, Result};
use tracing::debug;

use crate::{broker::Broker, observability::metrics};

use super::ConnectionIdMap;

#[derive(Clone)]
pub struct BrokerLife {
    broker: Broker,
    connection_ids: ConnectionIdMap,
}

impl BrokerLife {
    #[allow(dead_code)]
    pub fn new(broker: Broker) -> Self {
        Self::with_connection_ids(broker, ConnectionIdMap::default())
    }

    pub(crate) fn with_connection_ids(broker: Broker, connection_ids: ConnectionIdMap) -> Self {
        Self {
            broker,
            connection_ids,
        }
    }
}

impl Life for BrokerLife {
    async fn tcp_connection_closed(&self, info: ConnInfo, reason: CloseReason) -> Result<()> {
        let Some(connection_id) = self.connection_ids.remove(info.id()) else {
            debug!(
                local_connection_id = info.id(),
                ?reason,
                "tcp connection closed"
            );
            if is_packet_parse_error(reason) {
                metrics::packet_parse_error(close_reason_label(reason));
            }
            return Ok(());
        };
        debug!(connection_id, ?reason, "tcp connection closed");
        if is_packet_parse_error(reason) {
            metrics::packet_parse_error(close_reason_label(reason));
        }
        if let Some(outcome) = self.broker.remove_connection(connection_id) {
            metrics::connection_closed(close_reason_label(reason));
            if let Some(will) = outcome.will
                && should_publish_will(reason)
            {
                self.broker.publish_will(connection_id, will).await;
            }
        }
        Ok(())
    }
}

fn should_publish_will(reason: CloseReason) -> bool {
    !matches!(
        reason,
        CloseReason::HandlerClosed | CloseReason::LocalClosed | CloseReason::ServerShutdown
    )
}

fn is_packet_parse_error(reason: CloseReason) -> bool {
    matches!(
        reason,
        CloseReason::DecodeError | CloseReason::FrameTooLarge
    )
}

fn close_reason_label(reason: CloseReason) -> &'static str {
    match reason {
        CloseReason::HandlerClosed => "handler_closed",
        CloseReason::LocalClosed => "local_closed",
        CloseReason::PeerClosed => "peer_closed",
        CloseReason::ChannelClosed => "channel_closed",
        CloseReason::ServerShutdown => "server_shutdown",
        CloseReason::IdleTimeout => "idle_timeout",
        CloseReason::IoError => "io_error",
        CloseReason::DecodeError => "decode_error",
        CloseReason::EncodeError => "encode_error",
        CloseReason::FrameTooLarge => "frame_too_large",
        CloseReason::HandlerError => "handler_error",
    }
}
