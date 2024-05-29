use crate::catalog::table::TableCatalog;
use crate::catalog::{SchemaId, TableId};
use std::collections::HashMap;
use std::sync::Arc;

pub struct SchemaCatalog {
    id: SchemaId,
    name: String,
    table_idxs: HashMap<String, TableId>,
    tables: HashMap<TableId, Arc<TableCatalog>>,
    next_table_id: TableId,
}
