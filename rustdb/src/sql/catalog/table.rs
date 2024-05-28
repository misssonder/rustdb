use crate::buffer::buffer_poll_manager::BufferPoolManager;
use crate::error::RustDBResult;
use crate::sql::catalog::{ColumnId, TableId};
use crate::storage::page::column::Column;
use crate::storage::table::Table;
use std::collections::BTreeMap;
use std::sync::Arc;

pub struct TableCatalog {
    table: Table,
}

impl TableCatalog {
    pub async fn new(
        table_id: TableId,
        name: impl Into<String> + Clone,
        columns: Vec<Column>,
        buffer_pool: Arc<BufferPoolManager>,
    ) -> RustDBResult<Self> {
        let table_catalog = Self {
            table: Table::new(table_id, name, columns.clone(), buffer_pool.clone()).await?,
        };
        Ok(table_catalog)
    }

    pub fn read_column(&self, column_id: ColumnId) -> Option<&Column> {
        self.table.read_column(column_id)
    }

    pub fn read_column_name(&self, column_name: &str) -> Option<&Column> {
        self.table.read_column_name(column_name)
    }

    pub fn read_column_id(&self, column_name: &str) -> Option<ColumnId> {
        self.table.read_column_id(column_name)
    }

    pub fn columns(&self) -> BTreeMap<ColumnId, Column> {
        self.table
            .columns()
            .iter()
            .enumerate()
            .map(|(id, column)| (id as ColumnId, column.clone()))
            .collect()
    }

    pub fn name(&self) -> &str {
        self.table.name()
    }

    pub fn primary_keys(&self) -> &[ColumnId] {
        self.table.primary_keys()
    }

    pub async fn add_column(&mut self, column_id: ColumnId, column: Column) -> RustDBResult<()> {
        self.table.add_column(column_id, column).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::types::{DataType, Value};
    use crate::storage::disk::disk_manager::DiskManager;
    use crate::storage::page::column::Column;

    #[tokio::test]
    async fn table_catalog() -> RustDBResult<()> {
        let f = tempfile::NamedTempFile::new()?;
        let disk_manager = DiskManager::new(f.path()).await?;
        let buffer_manager = BufferPoolManager::new(100, 2, disk_manager).await?;
        let column_id = Column::new("id", DataType::Bigint).with_primary(true);
        let column_name =
            Column::new("name", DataType::String).with_default(Value::String("hello".to_string()));
        let column_gender = Column::new("gender", DataType::Boolean).with_primary(true);
        let mut catalog = TableCatalog::new(
            0,
            "store",
            vec![column_id.clone(), column_name.clone()],
            Arc::new(buffer_manager),
        )
        .await?;
        assert_eq!(catalog.primary_keys(), vec![0]);
        assert_eq!(catalog.name(), "store");
        assert_eq!(catalog.read_column_name("id"), Some(&column_id));
        assert_eq!(catalog.read_column_name("name"), Some(&column_name));
        assert_eq!(catalog.read_column_name("name_id"), None);
        assert_eq!(catalog.read_column_name("gender"), None);
        assert_eq!(catalog.read_column(2), None);
        catalog.add_column(2, column_gender.clone()).await?;
        assert_eq!(catalog.read_column_name("gender"), Some(&column_gender));
        assert_eq!(catalog.read_column(2), Some(&column_gender));
        assert_eq!(catalog.read_column(3), None);
        Ok(())
    }
}
