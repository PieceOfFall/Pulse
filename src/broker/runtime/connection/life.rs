use rs_netty::{CloseReason, ConnInfo, Life, Result};

use crate::broker::Broker;

#[derive(Clone)]
pub struct BrokerLife {
    broker: Broker,
}

impl BrokerLife {
    pub fn new(broker: Broker) -> Self {
        Self { broker }
    }
}

impl Life for BrokerLife {
    async fn tcp_connection_closed(&self, info: ConnInfo, reason: CloseReason) -> Result<()> {
        let will = self.broker.remove_connection(info.id());
        if let Some(will) = will
            && should_publish_will(reason)
        {
            self.broker.publish_will(info.id(), will).await;
        }
        Ok(())
    }
}

fn should_publish_will(reason: CloseReason) -> bool {
    !matches!(
        reason,
        CloseReason::HandlerClosed | CloseReason::LocalClosed
    )
}
