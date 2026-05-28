mod memory;
mod mysql;
mod sqlite;

pub(super) use memory::InMemoryStorage;
pub(super) use mysql::MysqlStorage;
pub(super) use sqlite::SqliteStorage;

use super::runtime::session_registry::BrokerState;

pub(super) trait BrokerStorage: Send + Sync {
    fn with_state(&self, operation: &mut dyn FnMut(&mut BrokerState));
    fn with_transient_state(&self, operation: &mut dyn FnMut(&mut BrokerState));
    fn read_state(&self, operation: &mut dyn FnMut(&BrokerState));
}
