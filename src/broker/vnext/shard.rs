use std::collections::VecDeque;

use bytes::Bytes;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ShardCommand {
    Deliver {
        client_id: String,
        topic_name: String,
        payload: Bytes,
    },
    DisconnectSlowConsumer {
        client_id: String,
    },
}

#[derive(Debug)]
pub(crate) struct ShardRuntime {
    id: usize,
    commands: VecDeque<ShardCommand>,
}

impl ShardRuntime {
    pub(crate) fn new(id: usize) -> Self {
        Self {
            id,
            commands: VecDeque::new(),
        }
    }

    pub(crate) fn id(&self) -> usize {
        self.id
    }

    pub(crate) fn enqueue_delivery(
        &mut self,
        client_id: impl Into<String>,
        topic_name: impl Into<String>,
        payload: Bytes,
    ) {
        self.commands.push_back(ShardCommand::Deliver {
            client_id: client_id.into(),
            topic_name: topic_name.into(),
            payload,
        });
    }

    pub(crate) fn queue_len(&self) -> usize {
        self.commands.len()
    }

    pub(crate) fn drain_commands(&mut self) -> impl Iterator<Item = ShardCommand> + '_ {
        self.commands.drain(..)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::{ShardCommand, ShardRuntime};

    #[test]
    fn shard_keeps_payload_as_shared_bytes() {
        let payload = Bytes::from_static(b"hello");
        let mut shard = ShardRuntime::new(3);
        shard.enqueue_delivery("client-a", "devices/a", payload.clone());

        assert_eq!(shard.id(), 3);
        assert_eq!(shard.queue_len(), 1);
        let command = shard.drain_commands().next().expect("queued command");
        match command {
            ShardCommand::Deliver {
                client_id,
                topic_name,
                payload: delivered,
            } => {
                assert_eq!(client_id, "client-a");
                assert_eq!(topic_name, "devices/a");
                assert_eq!(delivered, payload);
            }
            ShardCommand::DisconnectSlowConsumer { .. } => {
                panic!("unexpected disconnect command")
            }
        }
    }
}
