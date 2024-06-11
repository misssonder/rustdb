use crate::sql::types::Value;
use crate::storage::mvcc::manager::Manager;
use crate::storage::mvcc::TransactionId;
use crate::storage::page::table::Tuple;
use crate::storage::{Storage, StorageResult, TimeStamp};
use std::collections::HashSet;
use std::sync::Arc;

pub struct Transaction<S: Storage> {
    engine: S,
    manager: Arc<Manager>,
    state: TransactionState,
}

pub struct TransactionState {
    id: TransactionId,
    timestamp: TimeStamp,
    active: HashSet<TransactionId>,
}

impl<S: Storage> Transaction<S> {
    pub async fn begin(engine: S, manager: Arc<Manager>) -> Self {
        let state = TransactionState {
            id: manager.next_transaction_id(),
            timestamp: manager.current_ts(),
            active: manager.scan_active().await,
        };
        Self {
            engine,
            manager,
            state,
        }
    }

    fn is_visible(&self, timestamp: TimeStamp) -> bool {
        if self.state.active.get(&timestamp).is_some() {
            false
        } else {
            timestamp <= self.state.timestamp
        }
    }

    async fn read(&self, name: &str, key: &Value) -> StorageResult<Option<Tuple>> {
        let versions = self.manager.versions.lock().await;
        todo!()
    }
}
