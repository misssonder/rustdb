use crate::catalog::error::Error;
use crate::catalog::scheme::SchemaCatalog;
use crate::catalog::table::TableCatalog;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

mod column;
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

#[derive(Debug, Default)]
pub struct Catalog {
    schema_idxs: HashMap<String, SchemaId>,
    schemas: HashMap<SchemaId, SchemaCatalog>,
    next_schema_id: SchemaId,
}

impl Catalog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn read_schema(&self, name: &str) -> Option<&SchemaCatalog> {
        self.read_id_name_by_name(name)
            .and_then(|id| self.read_schema_by_id(id))
    }

    pub fn read_schema_mut(&mut self, name: &str) -> Option<&mut SchemaCatalog> {
        self.read_id_name_by_name(name)
            .and_then(|id| self.read_schema_mut_by_id(id))
    }

    pub fn read_schema_by_id(&self, id: SchemaId) -> Option<&SchemaCatalog> {
        self.schemas.get(&id)
    }

    pub fn read_schema_mut_by_id(&mut self, id: SchemaId) -> Option<&mut SchemaCatalog> {
        self.schemas.get_mut(&id)
    }

    pub fn read_id_name_by_name(&self, schema_name: &str) -> Option<SchemaId> {
        self.schema_idxs.get(schema_name).copied()
    }

    pub fn create_table(&mut self, schema_name: &str, table: TableCatalog) -> Result<(), Error> {
        if let Some(schema) = self.read_schema_mut(schema_name) {
            schema.create_table(table)
        } else {
            let schema_id = self.next_schema_id();
            let mut schema = SchemaCatalog::new(schema_id, schema_name);
            schema.create_table(table)?;

            self.schema_idxs.insert(schema_name.to_string(), schema_id);
            self.schemas.insert(schema_id, schema);
            Ok(())
        }
    }

    pub fn drop_table(&mut self, schema_name: &str, table_name: &str) -> Option<TableCatalog> {
        self.read_schema_mut(schema_name)
            .and_then(|schema| schema.drop_table(table_name))
    }

    fn next_schema_id(&mut self) -> SchemaId {
        let id = self.next_schema_id;
        self.next_schema_id += 1;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog() -> Result<(), Error> {
        let mut catalog = Catalog::new();
        let table_user = TableCatalog::new(0, "user", vec![]).unwrap();
        catalog.create_table("default", table_user.clone())?;
        assert_eq!(
            catalog.read_schema("default").unwrap().read_table("user"),
            Some(&table_user)
        );
        assert_eq!(catalog.drop_table("default", "user"), Some(table_user));
        assert!(catalog
            .read_schema("default")
            .unwrap()
            .read_table("user")
            .is_none());
        Ok(())
    }
}
