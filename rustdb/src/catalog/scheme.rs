use crate::catalog::error::Error;
use crate::catalog::table::TableCatalog;
use crate::catalog::{SchemaId, TableId};
use std::collections::HashMap;

#[derive(Debug)]
pub struct SchemaCatalog {
    id: SchemaId,
    name: String,
    table_idxs: HashMap<String, TableId>,
    tables: HashMap<TableId, TableCatalog>,
    next_table_id: TableId,
}

impl SchemaCatalog {
    pub fn new<T: Into<String>>(id: SchemaId, name: T) -> Self {
        Self {
            id,
            name: name.into(),
            table_idxs: Default::default(),
            tables: Default::default(),
            next_table_id: 0,
        }
    }

    pub fn read_table(&self, name: &str) -> Option<&TableCatalog> {
        self.read_id_by_name(name)
            .and_then(|id| self.read_table_by_id(id))
    }

    pub fn read_table_by_id(&self, id: TableId) -> Option<&TableCatalog> {
        self.tables.get(&id)
    }

    pub fn read_id_by_name(&self, name: &str) -> Option<TableId> {
        self.table_idxs.get(name).copied()
    }

    pub fn create_table(&mut self, mut table: TableCatalog) -> Result<(), Error> {
        if self.read_table(table.name()).is_some() {
            return Err(Error::Duplicated("table", table.name().to_string()));
        }
        let table_id = self.next_table_id();
        table.set_id(table_id);

        self.table_idxs.insert(table.name().to_string(), table_id);
        self.tables.insert(table_id, table);
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Option<TableCatalog> {
        self.table_idxs
            .remove(name)
            .and_then(|id| self.tables.remove(&id))
    }

    fn next_table_id(&mut self) -> TableId {
        let id = self.next_table_id;
        self.next_table_id += 1;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema() {
        let mut schema = SchemaCatalog::new(0, "default");
        let mut table_user = TableCatalog::new(0, "user", vec![]).unwrap();
        schema.create_table(table_user.clone()).unwrap();
        assert_eq!(schema.read_table("user"), Some(&table_user));
        assert!(schema.create_table(table_user.clone()).is_err());
        schema.drop_table("user");
        assert!(schema.create_table(table_user.clone()).is_ok());
        table_user.set_id(1);
        assert_eq!(schema.read_table("user"), Some(&table_user));
        assert_eq!(schema.read_id_by_name("user"), Some(1));
    }
}
