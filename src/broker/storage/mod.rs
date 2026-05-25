mod memory;
mod sqlite;

pub(super) use memory::InMemoryStorage;
pub(super) use sqlite::SqliteStorage;

use super::state::BrokerState;

pub(super) trait BrokerStorage: Send + Sync {
    fn with_state(&self, operation: &mut dyn FnMut(&mut BrokerState));
}
