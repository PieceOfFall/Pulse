use std::collections::HashMap;

use rs_netty::codec::{MqttProperty, PublishPacket};

use crate::broker::runtime::config::SERVER_TOPIC_ALIAS_MAXIMUM;

#[derive(Default)]
pub(in crate::broker) struct TopicAliases {
    aliases: HashMap<u16, String>,
}

impl TopicAliases {
    pub(in crate::broker) fn resolve_publish(&mut self, packet: &mut PublishPacket) -> bool {
        let alias = packet
            .properties
            .iter()
            .find_map(|property| match property {
                MqttProperty::TopicAlias(alias) => Some(*alias),
                _ => None,
            });

        let Some(alias) = alias else {
            return true;
        };
        if alias == 0 || alias > SERVER_TOPIC_ALIAS_MAXIMUM {
            return false;
        }

        if packet.topic_name.is_empty() {
            let Some(topic_name) = self.aliases.get(&alias) else {
                return false;
            };
            packet.topic_name = topic_name.clone();
        } else {
            self.aliases.insert(alias, packet.topic_name.clone());
        }
        true
    }
}
