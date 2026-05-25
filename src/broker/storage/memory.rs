use std::sync::Mutex;

use super::BrokerStorage;
use crate::broker::state::BrokerState;

#[derive(Default)]
pub(crate) struct InMemoryStorage {
    state: Mutex<BrokerState>,
}

impl BrokerStorage for InMemoryStorage {
    fn with_state(&self, operation: &mut dyn FnMut(&mut BrokerState)) {
        let mut state = self.state.lock().expect("broker state lock poisoned");
        operation(&mut state);
    }
}
