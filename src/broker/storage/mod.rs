mod memory;

pub(super) use memory::InMemoryStorage;

use super::state::BrokerState;

pub(super) trait BrokerStorage: Send + Sync {
    fn with_state(&self, operation: &mut dyn FnMut(&mut BrokerState));
}
