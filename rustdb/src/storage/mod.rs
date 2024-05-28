use crate::error::{RustDBError, RustDBResult};
use crate::sql::catalog::{TableId, TableRefId};
use crate::storage::page::column::Column;
use std::future::Future;
use std::sync::atomic::AtomicUsize;

pub mod disk;
mod index;
pub mod page;
pub mod table;

pub const PAGE_SIZE: usize = 4096;
pub type PageId = usize;

pub type AtomicPageId = AtomicUsize;
pub const NULL_PAGE: PageId = PageId::MAX;

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_num: u32,
}

pub trait Engine {
    type Table;
    fn create_table(
        &self,
        id: TableRefId,
        name: &str,
        columns: &[Column],
    ) -> impl Future<Output = RustDBResult<()>> + Send;

    fn read_table(&self, id: TableRefId) -> impl Future<Output = RustDBResult<Self::Table>>;

    fn delete_tale(&self, id: TableRefId) -> impl Future<Output = RustDBResult<()>>;
}
