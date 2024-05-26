use crate::storage::page::column::Column;
use crate::storage::PageId;

#[derive(Debug, PartialEq)]
pub struct Table {
    pub header: TableHeader,
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
}

#[derive(Debug, PartialEq)]
pub struct TableHeader {
    pub(crate) start: PageId,
    pub(crate) end: PageId,
    pub(crate) next_table: Option<PageId>,
    pub(crate) tuple_references: Vec<TupleReference>,
}
#[derive(Debug, PartialEq)]
pub struct TupleReference {
    pub(crate) offset: u16,
    pub(crate) size: u16,
}
