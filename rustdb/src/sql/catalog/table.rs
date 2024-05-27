use crate::error::RustDBResult;
use crate::sql::catalog::column::ColumnCatalog;
use crate::sql::catalog::error::Error;
use crate::sql::catalog::{Catalog, ColumnId, TableId};
use std::collections::{BTreeMap, HashMap};

pub struct TableCatalog {
    id: TableId,
    name: String,
    /// Mapping from column names to column ids
    column_idxs: HashMap<String, ColumnId>,
    columns: BTreeMap<ColumnId, ColumnCatalog>,
    next_column_id: ColumnId,
    primary_key: Vec<ColumnId>,
}

impl TableCatalog {
    pub fn new(
        table_id: TableId,
        name: impl Into<String>,
        columns: Vec<ColumnCatalog>,
    ) -> RustDBResult<Self> {
        let mut table_catalog = Self {
            id: table_id,
            name: name.into(),
            column_idxs: Default::default(),
            columns: Default::default(),
            next_column_id: 0,
            primary_key: Default::default(),
        };
        for column in columns {
            table_catalog.add_column(column)?;
        }
        Ok(table_catalog)
    }

    pub fn read_column(&self, column_id: ColumnId) -> Option<&ColumnCatalog> {
        self.columns.get(&column_id)
    }

    pub fn read_column_name(&self, column_name: &str) -> Option<&ColumnCatalog> {
        self.column_idxs
            .get(column_name)
            .and_then(|column_id| self.read_column(*column_id))
    }

    pub fn read_column_id(&self, column_name: &str) -> Option<ColumnId> {
        self.column_idxs.get(column_name).copied()
    }
    pub fn columns(&self) -> BTreeMap<ColumnId, ColumnCatalog> {
        self.columns.clone()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn primary_keys(&self) -> Vec<ColumnId> {
        self.primary_key.clone()
    }

    fn add_primary_key(&mut self, column: &ColumnCatalog) {
        if column.primary() {
            self.primary_key.push(column.id())
        }
    }
    fn add_column(&mut self, mut column: ColumnCatalog) -> RustDBResult<()> {
        let column_id = self.add_next_column_id();
        if let Some(column) = self.read_column_name(column.name()) {
            return Err(Error::Duplicated(format!("column {}", column.name())).into());
        };
        column.set_id(column_id);
        self.add_primary_key(&column);
        self.column_idxs
            .insert(column.name().to_string(), column.id());
        self.columns.insert(column.id(), column);
        Ok(())
    }
    fn add_next_column_id(&mut self) -> ColumnId {
        let id = self.next_column_id;
        self.next_column_id += 1;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::types::{DataType, Value};
    use crate::storage::page::column::ColumnDesc;

    #[test]
    fn table_catalog() {
        let catalog = TableCatalog::new(
            0,
            "store",
            vec![
                ColumnCatalog::new(
                    0,
                    ColumnDesc::new("id", DataType::Bigint).with_primary(true),
                ),
                ColumnCatalog::new(
                    1,
                    ColumnDesc::new("name", DataType::String)
                        .with_default(Value::String("hello".to_string())),
                ),
            ],
        )
        .unwrap();
        assert_eq!(catalog.primary_keys(), vec![0]);
        assert_eq!(catalog.name(), "store");
    }
}
