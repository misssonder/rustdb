use crate::catalog::TableRefId;
use crate::storage::page::column::Column;
use crate::{buffer, encoding};
use std::future::Future;
use std::sync::atomic::AtomicUsize;
use thiserror::Error;

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
    ) -> impl Future<Output = StorageResult<()>> + Send;

    fn read_table(&self, id: TableRefId) -> impl Future<Output = StorageResult<Self::Table>>;

    fn delete_tale(&self, id: TableRefId) -> impl Future<Output = StorageResult<()>>;
}

pub type StorageResult<T> = Result<T, Error>;
#[derive(Error, Debug)]
pub enum Error {
    #[error("buffer error {0}")]
    Buffer(#[from] buffer::Error),
    #[error("encoding error {0}")]
    Encoding(#[from] encoding::error::Error),
    #[error("io error: {0}")]
    IO(#[from] std::io::Error),
}
