use std::sync::atomic::AtomicU64;

mod manager;
mod transaction;
mod version;

pub type AtomicTransactionId = AtomicU64;
pub type TransactionId = u64;
