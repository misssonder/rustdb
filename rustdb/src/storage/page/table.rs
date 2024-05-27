use crate::sql::types::Value;
use crate::storage::page::column::Column;
use crate::storage::{PageId, RecordId};

/// Table is List, it contains a bunch of pages which can be decoded into TableNode
#[derive(Debug, PartialEq)]
pub struct Table {
    pub(crate) start: PageId,
    pub(crate) end: PageId,
    pub(crate) columns: Vec<Column>,
}
#[derive(Debug, PartialEq)]
pub struct TableNode {
    pub(crate) page_id: PageId,
    pub(crate) next: Option<PageId>,
    pub(crate) tuples: Vec<Tuple>,
}
#[derive(Debug, PartialEq)]
pub struct Tuple {
    pub(crate) record_id: RecordId,
    pub(crate) values: Vec<Value>,
}
