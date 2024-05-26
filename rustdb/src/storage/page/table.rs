use crate::storage::page::column::Column;
use crate::storage::PageId;

pub struct Table {
    pub header: TableHeader,
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
}

pub struct TableHeader {
    pub(crate) start: PageId,
    pub(crate) end: PageId,
    pub(crate) next_table: Option<PageId>,
    pub(crate) tuple_references: Vec<TupleReference>,
}

pub struct TupleReference {
    pub(crate) offset: u16,
    pub(crate) size: u16,
}
