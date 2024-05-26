use crate::sql::scheme::Column;
use crate::storage::PageId;

pub struct Table {
    name: String,
    columns: Vec<Column>,
    pub header: TableHeader,
}

pub struct TableHeader {
    start: PageId,
    end: PageId,
    next_table: Option<PageId>,
    tuple_references: Vec<TupleReference>,
}

pub struct TupleReference {
    offset: u16,
    size: u16,
}
