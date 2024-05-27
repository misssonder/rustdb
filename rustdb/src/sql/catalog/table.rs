use crate::sql::catalog::column::ColumnCatalog;
use crate::sql::catalog::{ColumnId, TableId};
use std::collections::{BTreeMap, HashMap};

pub struct TableCatalog {
    id: TableId,
    name: String,
    /// Mapping from column names to column ids
    column_idxs: HashMap<String, ColumnId>,
    columns: BTreeMap<ColumnId, ColumnCatalog>,
    next_column_id: ColumnId,
}
