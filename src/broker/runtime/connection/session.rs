use rs_netty::{
    Channel,
    codec::{MqttPacket, Will},
};

use super::ConnectOptions;
use crate::broker::{
    Broker,
    runtime::{
        delivery::{Delivery, redeliveries_for_client},
        session_registry::{ClientEntry, SessionEntry},
        time::now_ms,
    },
};

pub(in crate::broker) struct ConnectOutcome {
    pub(in crate::broker) client_id: String,
    pub(in crate::broker) session_present: bool,
    pub(in crate::broker) replaced_channel: Option<Channel<MqttPacket>>,
    pub(in crate::broker) redeliveries: Vec<Delivery>,
}

pub(in crate::broker) struct RemoveConnectionOutcome {
    pub(in crate::broker) client_id: String,
    pub(in crate::broker) will: Option<Will>,
}

impl Broker {
    pub(in crate::broker) fn connect(
        &self,
        connection_id: u64,
        requested_client_id: String,
        channel: Channel<MqttPacket>,
        will: Option<Will>,
        principal: Option<String>,
        options: ConnectOptions,
    ) -> ConnectOutcome {
        let client_id = if requested_client_id.is_empty() {
            self.generated_client_id()
        } else {
            requested_client_id
        };

        self.with_state(|state| {
            state.expire_sessions(now_ms());
            let had_session = state.sessions_by_client_id.contains_key(&client_id);
            if options.clean_start {
                state.sessions_by_client_id.remove(&client_id);
                state.subscriptions.retain(|sub| sub.client_id != client_id);
            }

            let replaced_channel = if let Some(previous_connection_id) = state
                .connection_by_client_id
                .insert(client_id.clone(), connection_id)
            {
                if previous_connection_id != connection_id {
                    let previous = state.remove_connection_state(previous_connection_id, true);
                    previous.map(|previous| previous.channel)
                } else {
                    None
                }
            } else {
                None
            };

            state
                .sessions_by_client_id
                .entry(client_id.clone())
                .and_modify(|session| {
                    session.expires_at_ms = None;
                    session.session_expiry_interval = options.session_expiry_interval;
                })
                .or_insert_with(|| SessionEntry::connected(options.session_expiry_interval));
            state.clients_by_connection.insert(
                connection_id,
                ClientEntry::new(
                    client_id.clone(),
                    channel,
                    will,
                    principal,
                    options.session_expiry_interval,
                    options.receive_maximum,
                    options.maximum_packet_size,
                ),
            );
            let redeliveries = redeliveries_for_client(state, &client_id);

            ConnectOutcome {
                client_id,
                session_present: !options.clean_start && had_session,
                replaced_channel,
                redeliveries,
            }
        })
    }

    pub(in crate::broker) fn remove_connection(
        &self,
        connection_id: u64,
    ) -> Option<RemoveConnectionOutcome> {
        self.with_state(|state| {
            let client = state.remove_connection_state(connection_id, false)?;
            state.connection_by_client_id.remove(&client.client_id);
            Some(RemoveConnectionOutcome {
                client_id: client.client_id,
                will: client.will,
            })
        })
    }
}
