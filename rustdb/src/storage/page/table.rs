use crate::sql::types::Value;
use crate::storage::page::column::Column;
use crate::storage::{PageId, RecordId};

/// Table is List, it contains a bunch of pages which can be decoded into TableNode
#[derive(Debug, PartialEq)]
pub struct Table {
    /// Table name
    pub(crate) name: String,
    /// This Table's page_id
    pub(crate) page_id: PageId,
    /// First TableNode's page_id
    pub(crate) start: PageId,
    /// Last TableNode's page_id
    pub(crate) end: PageId,
    /// Columns
    pub(crate) columns: Vec<Column>,
}

impl Table {
    pub fn new(
        name: impl Into<String>,
        page_id: PageId,
        node_page_id: PageId,
        columns: Vec<Column>,
    ) -> Self {
        Self {
            name: name.into(),
            page_id,
            start: node_page_id,
            end: node_page_id,
            columns,
        }
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn set_start(&mut self, page_id: PageId) {
        self.start = page_id
    }

    pub fn set_end(&mut self, page_id: PageId) {
        self.end = page_id
    }

    pub fn push_column(&mut self, column: Column) {
        self.columns.push(column);
    }

    pub fn insert_column(&mut self, index: usize, column: Column) {
        self.columns.insert(index, column);
    }

    pub fn columns(&self) -> &[Column] {
        self.columns.as_slice()
    }
}
#[derive(Debug, PartialEq)]
pub struct TableNode {
    pub(crate) page_id: PageId,
    pub(crate) next: Option<PageId>,
    pub(crate) tuples: Tuples,
}

impl TableNode {
    pub fn new(page_id: PageId, tuples: Vec<Tuple>) -> Self {
        Self {
            page_id,
            next: None,
            tuples,
        }
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id
    }
    pub fn set_next(&mut self, page_id: PageId) {
        self.next = Some(page_id)
    }

    pub fn next(&self) -> Option<PageId> {
        self.next
    }

    pub fn insert(&mut self, tuple: Tuple) -> RecordId {
        let slot_num = self.tuples.len() as u32;
        self.tuples.push(tuple);
        RecordId::new(self.page_id, slot_num)
    }
}
#[derive(Debug, PartialEq)]
pub struct Tuple {
    pub(crate) values: Vec<Value>,
}

pub type Tuples = Vec<Tuple>;

impl Tuple {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn field(&self, position: usize) -> Value {
        self.values.get(position).cloned().unwrap_or(Value::Null)
    }
}
