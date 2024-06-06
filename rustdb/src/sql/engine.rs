use crate::sql::transaction::Transaction;
use crate::sql::SqlResult;
use std::future::Future;

/// A sql engine trait
pub trait Engine {
    type Transaction: Transaction;

    fn begin(&self) -> impl Future<Output = SqlResult<Self::Transaction>>;
}
