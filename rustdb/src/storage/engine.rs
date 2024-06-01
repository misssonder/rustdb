use crate::buffer::buffer_poll_manager::BufferPoolManager;
use crate::sql::types::Value;
use crate::storage::index::Index;
use crate::storage::page::column::Column;
use crate::storage::page::table::Tuples;
use crate::storage::table::Table;
use crate::storage::{Error, PageId, Storage, StorageResult};
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

type TableKey = String;
type TableValue = (PageId, Arc<Index>); // table page id , index
pub struct Engine {
    tables: RwLock<BTreeMap<TableKey, TableValue>>,
    buffer_pool: Arc<BufferPoolManager>,
}

impl Storage for Engine {
    async fn create_table<T: Into<String> + Clone>(
        &self,
        name: T,
        columns: Vec<Column>,
    ) -> StorageResult<Table> {
        for column in columns.iter() {
            column.validate()?;
        }
        let index =
            Index::new::<Vec<Value>>(self.buffer_pool.clone(), Self::evaluate_tree_size(&columns))
                .await?;
        let table = Table::new(name, columns, self.buffer_pool.clone()).await?;
        self.tables
            .write()
            .await
            .insert(table.name().to_string(), (table.page_id(), Arc::new(index)));
        Ok(table)
    }

    async fn read_table(&self, name: &str) -> StorageResult<Option<Table>> {
        Ok(match self.tables.read().await.get(name).map(|val| val.0) {
            Some(page_id) => Some(Table::try_from(page_id, self.buffer_pool.clone()).await?),
            None => None,
        })
    }

    async fn drop_table(&self, name: &str) -> StorageResult<Option<Table>> {
        todo!()
    }

    async fn insert_tuples(&self, name: &str, tuples: Tuples) -> StorageResult<usize> {
        let primary = self
            .read_primary(name)
            .await
            .ok_or(Error::NotFound("table", name.to_string()))?;
        let table = self
            .read_table(name)
            .await?
            .ok_or(Error::NotFound("table", name.to_string()))?;
        let primary_positions = table.primary_positions().await?;
        assert!(!primary_positions.is_empty());
        let mut count = 0;
        for tuple in tuples {
            let key = primary_positions
                .iter()
                .map(|position| {
                    let value = tuple.field(*position);
                    if let Value::Null = value {
                        Err(Error::Value("Primary value must not be null".to_string()))
                    } else {
                        Ok(value)
                    }
                })
                .collect::<StorageResult<Vec<_>>>()?;
            let record_id = table.insert(tuple).await?;
            primary.insert(key, record_id).await?;
            count += 1
        }
        Ok(count)
    }
}

impl Engine {
    pub fn evaluate_tree_size(_columns: &[Column]) -> usize {
        1024
    }

    pub async fn read_primary(&self, name: &str) -> Option<Arc<Index>> {
        self.tables
            .read()
            .await
            .get(name)
            .map(|(_, index)| index.clone())
    }
}
