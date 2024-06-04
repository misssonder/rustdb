use crate::buffer::buffer_poll_manager::BufferPoolManager;
use crate::sql::types::Value;
use crate::storage::index::Index;
use crate::storage::page::column::Column;
use crate::storage::page::table::{Tuple, Tuples};
use crate::storage::table::Table;
use crate::storage::{Error, PageId, Storage, StorageResult};
use async_stream::try_stream;
use futures::{Stream, StreamExt};
use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::RwLock;

type TableKey = String;
type TableValue = (PageId, Arc<Index<Vec<Value>>>); // table page id , index
pub struct Engine {
    tables: RwLock<BTreeMap<TableKey, TableValue>>,
    buffer_pool: Arc<BufferPoolManager>,
}

impl Storage for Engine {
    type Key = Vec<Value>;

    async fn create_table<T: Into<String> + Clone>(
        &self,
        name: T,
        columns: Vec<Column>,
    ) -> StorageResult<Table> {
        for column in columns.iter() {
            column.validate()?;
        }
        let index =
            Index::new(self.buffer_pool.clone(), Self::evaluate_tree_size(&columns)).await?;
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
        // todo delete table and index actually
        Ok(match self.tables.write().await.remove(name) {
            None => None,
            Some((table_page_id, _)) => {
                Some(Table::try_from(table_page_id, self.buffer_pool.clone()).await?)
            }
        })
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

    async fn read_tuple(&self, name: &str, key: &Self::Key) -> StorageResult<Option<Tuple>> {
        let primary = self
            .read_primary(name)
            .await
            .ok_or(Error::NotFound("table", name.to_string()))?;
        let table = self
            .read_table(name)
            .await?
            .ok_or(Error::NotFound("table", name.to_string()))?;
        Ok(match primary.search(key).await? {
            None => None,
            Some(record_id) => table.read_tuple(record_id).await?,
        })
    }

    async fn delete_tuple(&self, name: &str, key: &Self::Key) -> StorageResult<Option<Tuple>> {
        let primary = self
            .read_primary(name)
            .await
            .ok_or(Error::NotFound("table", name.to_string()))?;
        let table = self
            .read_table(name)
            .await?
            .ok_or(Error::NotFound("table", name.to_string()))?;
        Ok(match primary.delete(key).await? {
            None => None,
            Some((_, record_id)) => Some(table.delete(record_id).await?),
        })
    }

    async fn update_tuple(&self, name: &str, tuple: Tuple) -> StorageResult<Option<()>> {
        let primary = self
            .read_primary(name)
            .await
            .ok_or(Error::NotFound("table", name.to_string()))?;
        let table = self
            .read_table(name)
            .await?
            .ok_or(Error::NotFound("table", name.to_string()))?;
        let key = table.primary_keys(&tuple).await?;
        assert!(!key.is_empty());
        Ok(match primary.search(&key).await? {
            None => None,
            Some(record_id) => table.update_tuple(record_id, tuple).await?,
        })
    }

    async fn scan<R>(
        &self,
        name: &str,
        range: R,
    ) -> StorageResult<impl Stream<Item = StorageResult<Tuple>>>
    where
        R: RangeBounds<Self::Key>,
    {
        let primary = self
            .read_primary(name)
            .await
            .ok_or(Error::NotFound("table", name.to_string()))?;
        let table = self
            .read_table(name)
            .await?
            .ok_or(Error::NotFound("table", name.to_string()))?;
        let record_ids = primary.search_range(range).await?;
        let stream = try_stream! {
            for record_id in record_ids{
                yield table.read_tuple(record_id).await?
                .ok_or(Error::NotFound("tuple",format!("page: {} slot: {}",record_id.page_id,record_id.slot_num)))?;
            }
        };
        Ok(stream)
    }
}

impl Engine {
    pub fn new(buffer_pool: Arc<BufferPoolManager>) -> Self {
        Self {
            tables: Default::default(),
            buffer_pool,
        }
    }
    pub fn evaluate_tree_size(_columns: &[Column]) -> usize {
        64
    }

    pub async fn read_primary(&self, name: &str) -> Option<Arc<Index<Vec<Value>>>> {
        self.tables
            .read()
            .await
            .get(name)
            .map(|(_, index)| index.clone())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::sql::types::DataType;
    use crate::storage::disk::disk_manager::DiskManager;
    use crate::storage::page::table::Tuple;

    #[tokio::test]
    async fn engine() -> StorageResult<()> {
        let f = tempfile::NamedTempFile::new()?;
        let disk_manager = DiskManager::new(f.path()).await?;
        let buffer_pool = BufferPoolManager::new(100, 2, disk_manager).await?;
        let engine = Engine::new(Arc::new(buffer_pool));
        let column_id = Column::new("id", DataType::Bigint)
            .with_primary(true)
            .with_unique(true);
        let column_name =
            Column::new("name", DataType::String).with_default(Value::String("hello".to_string()));
        let len = 10240;
        let tuples = (0..len)
            .map(|id| Tuple::new(vec![Value::Bigint(id), Value::String("Mike".to_string())]))
            .collect::<Vec<_>>();
        engine
            .create_table("user", vec![column_id.clone(), column_name.clone()])
            .await?;
        engine.insert_tuples("user", tuples.clone()).await?;
        let table = engine.read_table("user").await?.unwrap();
        let tuples = table.tuples().await?.collect::<Vec<_>>();
        assert_eq!(tuples.len(), len as usize);
        for id in 0..len {
            assert_eq!(
                engine.read_tuple("user", &vec![Value::Bigint(id)]).await?,
                Some(Tuple::new(vec![
                    Value::Bigint(id),
                    Value::String("Mike".to_string())
                ]))
            );
        }
        let scan = engine
            .scan(
                "user",
                (
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Included(vec![Value::Bigint(len + 1)]),
                ),
            )
            .await?
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<StorageResult<Vec<_>>>()?;
        assert_eq!(scan.len(), len as usize);
        for id in 0..len {
            assert_eq!(
                engine
                    .delete_tuple("user", &vec![Value::Bigint(id)])
                    .await?
                    .map(|tuple| tuple.values),
                Some(Tuple::new(vec![Value::Bigint(id), Value::String("Mike".to_string())]).values)
            );
            assert!(engine
                .delete_tuple("user", &vec![Value::Bigint(id)])
                .await?
                .is_none())
        }

        Ok(())
    }
}
