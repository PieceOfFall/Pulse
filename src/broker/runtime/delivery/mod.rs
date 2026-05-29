mod flush;
mod inflight;
mod offline_queue;
mod packet;
mod retained;
mod router;
mod service;

use rs_netty::Channel;

use super::write::BrokerWrite;

pub(in crate::broker) use flush::{flush_deliveries, flush_deliveries_to_context};
pub(in crate::broker) use inflight::{
    queued_deliveries_for_client, redeliveries_for_client, retransmissions_for_connection,
};
pub(in crate::broker) use packet::packet_size;
pub(in crate::broker) use retained::retained_for_subscription;
pub(in crate::broker) use router::{deliveries_for_publish, qos0_deliveries_for_publish_readonly};

#[derive(Clone)]
pub(in crate::broker) struct Delivery {
    pub(super) channel: Channel<BrokerWrite>,
    pub(super) packet: BrokerWrite,
}

#[derive(Clone)]
pub(super) struct DeliveryTarget {
    pub(super) channel: Channel<BrokerWrite>,
    pub(super) receive_maximum: u16,
    pub(super) maximum_packet_size: u32,
}
