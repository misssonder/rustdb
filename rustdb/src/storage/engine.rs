use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::sql::types::Value;
use crate::storage::index::Index;
use crate::storage::page::column::Column;
use crate::storage::page::table::{Tuple, Tuples};
use crate::storage::table::Table;
use crate::storage::{Error, PageId, Storage, StorageResult};
use async_stream::try_stream;
use futures::Stream;
use std::collections::BTreeMap;
use std::ops::RangeBounds;
use std::sync::Arc;
use tokio::sync::RwLock;

type TableKey = String;
type TableValue = (PageId, Arc<Index<Value>>); // table page id , index
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

    async fn insert(&self, name: &str, tuples: Tuples) -> StorageResult<usize> {
        let primary = self
            .read_primary(name)
            .await
            .ok_or(Error::NotFound("table", name.to_string()))?;
        let table = self
            .read_table(name)
            .await?
            .ok_or(Error::NotFound("table", name.to_string()))?;
        let primary_position = table.primary_position().await?;
        let mut count = 0;
        for tuple in tuples {
            let key = tuple
                .field(primary_position)
                .ok_or(Error::NotFound("column", String::from("primary key")))?;
            let record_id = table.insert(tuple).await?;
            primary.insert(key, record_id).await?;
            count += 1
        }
        Ok(count)
    }

    async fn read(&self, name: &str, key: &Value) -> StorageResult<Option<Tuple>> {
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

    async fn delete(&self, name: &str, key: &Value) -> StorageResult<Option<Tuple>> {
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

    async fn update(&self, name: &str, tuple: Tuple) -> StorageResult<Option<()>> {
        let primary = self
            .read_primary(name)
            .await
            .ok_or(Error::NotFound("table", name.to_string()))?;
        let table = self
            .read_table(name)
            .await?
            .ok_or(Error::NotFound("table", name.to_string()))?;
        let key = table.primary_key(&tuple).await?;
        Ok(match primary.search(&key).await? {
            None => None,
            Some(record_id) => table.update_tuple(record_id, tuple).await?,
        })
    }

    async fn scan<'a, R>(
        &self,
        name: &str,
        range: R,
    ) -> StorageResult<impl Stream<Item = StorageResult<Tuple>>>
    where
        R: RangeBounds<&'a Value>,
        Value: 'a,
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

    pub async fn read_primary(&self, name: &str) -> Option<Arc<Index<Value>>> {
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
    use futures::stream::StreamExt;

    async fn new_engine() -> StorageResult<Engine> {
        let f = tempfile::NamedTempFile::new()?;
        let disk_manager = DiskManager::new(f.path()).await?;
        let buffer_pool = BufferPoolManager::new(128, 2, disk_manager).await?;
        let engine = Engine::new(Arc::new(buffer_pool));
        let column_id = Column::new("id", DataType::Bigint)
            .with_primary(true)
            .with_unique(true);
        let column_name =
            Column::new("name", DataType::String).with_default(Value::String("hello".to_string()));
        engine
            .create_table("user", vec![column_id.clone(), column_name.clone()])
            .await?;
        Ok(engine)
    }
    #[tokio::test]
    async fn engine() -> StorageResult<()> {
        let engine = new_engine().await?;
        let len = 10240;
        let tuples = (0..len)
            .map(|id| {
                Tuple::new(
                    vec![Value::Bigint(id), Value::String("Mike".to_string())],
                    0,
                )
            })
            .collect::<Vec<_>>();
        engine.insert("user", tuples.clone()).await?;
        let table = engine.read_table("user").await?.unwrap();
        let tuples = table.tuples().await?.collect::<Vec<_>>();
        assert_eq!(tuples.len(), len as usize);
        for id in 0..len {
            assert_eq!(
                engine.read("user", &Value::Bigint(id)).await?,
                Some(Tuple::new(
                    vec![Value::Bigint(id), Value::String("Mike".to_string())],
                    0
                ))
            );
        }
        let scan = engine
            .scan(
                "user",
                (
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Included(&Value::Bigint(len + 1)),
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
                    .delete("user", &Value::Bigint(id))
                    .await?
                    .map(|tuple| tuple.values),
                Some(
                    Tuple::new(
                        vec![Value::Bigint(id), Value::String("Mike".to_string())],
                        0
                    )
                    .values
                )
            );
            assert!(engine.delete("user", &Value::Bigint(id)).await?.is_none())
        }

        Ok(())
    }

    #[tokio::test]
    async fn concurrency() -> StorageResult<()> {
        let engine = Arc::new(new_engine().await?);
        let concurrency = 2;
        let limit = 1000;
        let mut tasks = Vec::new();
        for i in 0..concurrency {
            let start = i * limit;
            let end = (i + 1) * limit;
            let engine_clone = engine.clone();
            let task = tokio::spawn(async move {
                let tuples = (start..end)
                    .map(|id| {
                        Tuple::new(
                            vec![Value::Bigint(id), Value::String("Mike".to_string())],
                            0,
                        )
                    })
                    .collect::<Vec<_>>();
                engine_clone.insert("user", tuples.clone()).await?;
                Ok::<_, Error>(())
            });
            tasks.push(task);
            let engine_clone = engine.clone();
            let task = tokio::spawn(async move {
                for id in start..end {
                    engine_clone.read("user", &Value::Bigint(id)).await?;
                }
                Ok::<_, Error>(())
            });
            tasks.push(task);
        }
        for task in tasks {
            task.await.unwrap()?;
        }
        let mut tasks = Vec::new();
        for i in 0..concurrency {
            let start = i * 1000;
            let end = (i + 1) * 1000;
            let engine_clone = engine.clone();
            let task = tokio::spawn(async move {
                for id in start..end {
                    let res = engine_clone.read("user", &Value::Bigint(id)).await?;
                    assert!(res.is_some());
                }
                Ok::<_, Error>(())
            });
            tasks.push(task);
        }
        for task in tasks {
            task.await.unwrap()?;
        }
        Ok(())
    }
}
