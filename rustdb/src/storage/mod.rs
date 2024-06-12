use crate::sql::types::Value;
use crate::storage::page::column::Column;
use crate::storage::page::table::{Tuple, Tuples};
use crate::storage::table::Table;
use crate::{buffer, encoding};
use futures::Stream;
use std::future::Future;
use std::ops::RangeBounds;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use thiserror::Error;

pub mod disk;
mod engine;
mod index;
pub mod page;
pub mod table;

pub const PAGE_SIZE: usize = 4096;
pub type PageId = usize;

pub type AtomicPageId = AtomicUsize;
pub const NULL_PAGE: PageId = PageId::MAX;
pub type TimeStamp = u64;
pub type AtomicTimeStamp = AtomicU64;

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_num: u32,
}

impl RecordId {
    pub fn new(page_id: PageId, slot_num: u32) -> Self {
        Self { page_id, slot_num }
    }
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
    #[error("{0} {1} not found")]
    NotFound(&'static str, String),
    #[error("{0}")]
    Value(String),
}

pub trait Storage {
    fn create_table<T: Into<String> + Clone>(
        &self,
        name: T,
        columns: Vec<Column>,
    ) -> impl Future<Output = StorageResult<Table>>;

    fn read_table(&self, name: &str) -> impl Future<Output = StorageResult<Option<Table>>>;

    fn drop_table(&self, name: &str) -> impl Future<Output = StorageResult<Option<Table>>>;

    fn insert(&self, name: &str, tuples: Tuples) -> impl Future<Output = StorageResult<usize>>;

    fn read(&self, name: &str, key: &Value) -> impl Future<Output = StorageResult<Option<Tuple>>>;

    fn delete(&self, name: &str, key: &Value)
        -> impl Future<Output = StorageResult<Option<Tuple>>>;

    fn update(&self, name: &str, tuple: Tuple) -> impl Future<Output = StorageResult<Option<()>>>;

    fn scan<'a, R>(
        &self,
        name: &str,
        range: R,
    ) -> impl Future<Output = StorageResult<impl Stream<Item = StorageResult<Tuple>>>>
    where
        R: RangeBounds<&'a Value>,
        Value: 'a;
}
