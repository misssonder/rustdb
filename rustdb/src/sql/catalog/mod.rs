use crate::sql::catalog::scheme::SchemaCatalog;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub mod error;
pub mod scheme;
pub mod table;

pub type SchemaId = u32;
pub type TableId = u32;
pub type ColumnId = u32;
#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Copy, Clone, Serialize, Deserialize)]
pub struct TableRefId {
    pub schema_id: SchemaId,
    pub table_id: TableId,
}

pub struct Catalog {
    schema_idxs: HashMap<String, SchemaId>,
    schemas: HashMap<SchemaId, SchemaCatalog>,
    next_schema_id: SchemaId,
}
