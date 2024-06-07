use crate::sql::transaction::Transaction;
use crate::sql::SqlResult;
use std::future::Future;

/// Executor execute the physical plan
pub trait Executor<T: Transaction> {
    fn execute(self, txn: &T) -> impl Future<Output = SqlResult<ResultSet>>;
}

#[derive(Debug, Clone)]
pub enum ResultSet {}
