use crate::sql::types::Value;
use crate::storage::mvcc::version::Versions;
use crate::storage::mvcc::{AtomicTransactionId, TransactionId};
use crate::storage::AtomicTimeStamp;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct Manager {
    commit_ts: AtomicTimeStamp,
    transaction_id: AtomicTransactionId,
    /// table name -> key -> versions
    pub(super) versions: Mutex<HashMap<String, HashMap<Value, Versions>>>,
    active: Mutex<HashSet<TransactionId>>,
}

impl Manager {
    pub async fn add_transaction(&self, transaction_id: TransactionId) -> bool {
        self.active.lock().await.insert(transaction_id)
    }

    pub async fn remove_transaction(&self, transaction_id: TransactionId) -> bool {
        self.active.lock().await.remove(&transaction_id)
    }

    pub async fn scan_active(&self) -> HashSet<TransactionId> {
        self.active.lock().await.clone()
    }

    pub fn next_transaction_id(&self) -> TransactionId {
        self.transaction_id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn next_commit_ts(&self) -> TransactionId {
        self.commit_ts.fetch_add(1, Ordering::Relaxed)
    }

    pub fn current_ts(&self) -> TransactionId {
        self.commit_ts.load(Ordering::Relaxed)
    }
}
