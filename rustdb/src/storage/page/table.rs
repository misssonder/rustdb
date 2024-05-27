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

impl Table {
    pub fn new(columns: Vec<Column>, page_id: PageId) -> Self {
        Self {
            start: page_id,
            end: page_id,
            columns,
        }
    }
}
#[derive(Debug, PartialEq)]
pub struct TableNode {
    pub(crate) page_id: PageId,
    pub(crate) next: Option<PageId>,
    pub(crate) tuples: Vec<Tuple>,
}

impl TableNode {
    pub fn new(page_id: PageId, tuples: Vec<Tuple>) -> Self {
        Self {
            page_id,
            next: None,
            tuples,
        }
    }

    pub fn set_next(&mut self, page_id: PageId) {
        self.next = Some(page_id)
    }

    pub fn next(&self) -> Option<PageId> {
        self.next
    }

    pub fn insert_tuple(&mut self, tuple: Tuple) {
        self.tuples.push(tuple)
    }
}
#[derive(Debug, PartialEq)]
pub struct Tuple {
    pub(crate) record_id: RecordId,
    pub(crate) values: Vec<Value>,
}

impl Tuple {
    pub fn new(record_id: RecordId, values: Vec<Value>) -> Self {
        Self { record_id, values }
    }
}
