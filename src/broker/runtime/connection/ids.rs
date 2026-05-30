use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

#[derive(Clone, Default)]
pub(crate) struct ConnectionIdAllocator {
    next: Arc<AtomicU64>,
}

impl ConnectionIdAllocator {
    pub(crate) fn listener(&self) -> ConnectionIdMap {
        ConnectionIdMap::allocated(self.clone())
    }

    fn allocate(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed) + 1
    }
}

#[derive(Clone, Default)]
pub(crate) struct ConnectionIdMap {
    inner: ConnectionIdMapInner,
}

#[derive(Clone, Default)]
enum ConnectionIdMapInner {
    #[default]
    Identity,
    Allocated {
        allocator: ConnectionIdAllocator,
        ids: Arc<Mutex<HashMap<u64, u64>>>,
    },
}

impl ConnectionIdMap {
    fn allocated(allocator: ConnectionIdAllocator) -> Self {
        Self {
            inner: ConnectionIdMapInner::Allocated {
                allocator,
                ids: Arc::new(Mutex::new(HashMap::new())),
            },
        }
    }

    pub(crate) fn broker_id(&self, local_id: u64) -> u64 {
        match &self.inner {
            ConnectionIdMapInner::Identity => local_id,
            ConnectionIdMapInner::Allocated { allocator, ids } => {
                let mut ids = ids.lock().expect("connection id map poisoned");
                *ids.entry(local_id).or_insert_with(|| allocator.allocate())
            }
        }
    }

    pub(crate) fn remove(&self, local_id: u64) -> Option<u64> {
        match &self.inner {
            ConnectionIdMapInner::Identity => Some(local_id),
            ConnectionIdMapInner::Allocated { ids, .. } => ids
                .lock()
                .expect("connection id map poisoned")
                .remove(&local_id),
        }
    }
}
