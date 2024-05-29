use crate::catalog::column::ColumnCatalog;
use crate::catalog::error::Error;
use crate::catalog::{ColumnId, TableId};
use std::collections::{BTreeMap, HashMap};

pub struct TableCatalog {
    id: TableId,
    name: String,
    column_idxs: HashMap<String, ColumnId>,
    columns: BTreeMap<ColumnId, ColumnCatalog>,
    /// Primary keys
    primary_keys: Vec<ColumnId>,
}

impl TableCatalog {
    pub fn new<T: Into<String>>(
        id: TableId,
        name: T,
        columns: Vec<ColumnCatalog>,
    ) -> Result<Self, Error> {
        let mut table_catalog = Self {
            id,
            name: name.into(),
            column_idxs: Default::default(),
            columns: Default::default(),
            primary_keys: Vec::new(),
        };
        for column in columns {
            table_catalog.add_column(column)?;
        }
        Ok(table_catalog)
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn primary_keys(&self) -> &[ColumnId] {
        self.primary_keys.as_slice()
    }

    pub fn read_column(&self, name: &str) -> Option<&ColumnCatalog> {
        self.read_id_by_name(name)
            .and_then(|id| self.read_column_by_id(id))
    }

    pub fn read_column_by_id(&self, column_id: ColumnId) -> Option<&ColumnCatalog> {
        self.columns.get(&column_id)
    }

    pub fn read_id_by_name(&self, name: &str) -> Option<ColumnId> {
        self.column_idxs.get(name).copied()
    }

    fn add_column(&mut self, mut column: ColumnCatalog) -> Result<(), Error> {
        if self.contain_column(column.name()) {
            return Err(Error::Duplicated("column", column.name().to_string()));
        }
        let column_id = self.next_column_id();
        column.set_id(column_id);

        self.column_idxs
            .insert(column.name().to_string(), column_id);
        if column.primary() {
            self.primary_keys.push(column_id);
            self.primary_keys.sort();
        }
        self.columns.insert(column_id, column);
        Ok(())
    }

    fn next_column_id(&mut self) -> ColumnId {
        let id = self.id;
        self.id += 1;
        id
    }

    fn contain_column(&self, name: &str) -> bool {
        self.read_id_by_name(name).is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::types::{DataType, Value};

    #[tokio::test]
    async fn table_catalog() -> Result<(), Error> {
        let column_id = ColumnCatalog::new(0, "id", DataType::Bigint).with_primary(true);
        let column_name = ColumnCatalog::new(1, "name", DataType::String)
            .with_default(Value::String("hello".to_string()));
        let column_gender = ColumnCatalog::new(2, "gender", DataType::Boolean).with_primary(true);
        let mut catalog =
            TableCatalog::new(0, "store", vec![column_id.clone(), column_name.clone()])?;
        assert_eq!(catalog.primary_keys(), vec![0]);
        assert_eq!(catalog.name(), "store");
        assert_eq!(catalog.read_column("id"), Some(&column_id));
        assert_eq!(catalog.read_column("name"), Some(&column_name));
        assert_eq!(catalog.read_column("name_id"), None);
        assert_eq!(catalog.read_column("gender"), None);
        assert_eq!(catalog.read_column_by_id(2), None);
        catalog.add_column(column_gender.clone())?;
        assert!(catalog.add_column(column_gender.clone()).is_err());
        assert_eq!(catalog.read_column("gender"), Some(&column_gender));
        assert_eq!(catalog.read_column_by_id(2), Some(&column_gender));
        assert_eq!(catalog.read_column_by_id(3), None);
        assert_eq!(catalog.primary_keys(), vec![0, 2].as_slice());
        Ok(())
    }
}
