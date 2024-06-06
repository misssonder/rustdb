use crate::sql::catalog::Catalog;
use crate::sql::types::Row;
use crate::sql::SqlResult;
use std::future::Future;

/// Transaction trait
pub trait Transaction: Catalog {
    fn commit(self) -> impl Future<Output = SqlResult<()>>;

    fn rollback(self) -> impl Future<Output = SqlResult<()>>;

    fn insert(&self, table: &str, row: Row) -> impl Future<Output = SqlResult<()>>;

    fn read(&self, table: &str, key: &Row) -> impl Future<Output = SqlResult<Option<Row>>>;

    fn delete(&self, table: &str, key: &Row) -> impl Future<Output = SqlResult<Option<Row>>>;

    fn update(&self, table: &str, row: Row) -> impl Future<Output = SqlResult<Option<Row>>>;
}
