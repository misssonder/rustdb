use crate::buffer::buffer_pool_manager::{
    BufferPoolManager, OwnedPageDataReadGuard, OwnedPageDataWriteGuard, PageRef,
};
use crate::encoding::encoded_size::EncodedSize;
use crate::sql::types::Value;
use crate::storage::page::column::Column;
use crate::storage::page::table::{TableNode, Tuple};
use crate::storage::page::{PageEncoding, PageTrait};
use crate::storage::{page, Error, PageId, RecordId, StorageResult};
use std::sync::Arc;

/// The wrapper of physical table in [`page::table::Table`]
pub struct Table {
    name: String,
    buffer_pool: Arc<BufferPoolManager>,
    root: PageId,
}

impl Table {
    pub async fn new<T: Into<String> + Clone>(
        name: T,
        columns: Vec<Column>,
        buffer_pool: Arc<BufferPoolManager>,
    ) -> StorageResult<Self> {
        let mut table_node = TableNode::new(0, vec![]);
        let mut table_heap = page::table::Table::new(name.clone(), 0, 0, columns.clone());

        buffer_pool.new_page_table_node(&mut table_node).await?;

        table_heap.set_start(table_node.page_id());
        table_heap.set_end(table_node.page_id());
        buffer_pool.new_page_table(&mut table_heap).await?;

        Ok(Self {
            name: name.into(),
            buffer_pool,
            root: table_heap.page_id(),
        })
    }

    pub async fn try_from(
        page_id: PageId,
        buffer_pool: Arc<BufferPoolManager>,
    ) -> StorageResult<Self> {
        let table_heap = buffer_pool.fetch_page_table(page_id).await?.1;
        Ok(Self {
            name: table_heap.name.clone(),
            buffer_pool,
            root: table_heap.page_id(),
        })
    }

    pub fn page_id(&self) -> PageId {
        self.root
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn columns(&self) -> StorageResult<Vec<Column>> {
        Ok(self.table_read().await?.1.columns)
    }

    pub async fn push_column(&self, column: Column) -> StorageResult<()> {
        let (mut page, mut table) = self.table_write().await?;
        table.push_column(column);
        page.write_table_back(&table)?;
        Ok(())
    }

    pub async fn insert_column(&self, index: usize, column: Column) -> StorageResult<()> {
        let (mut page, mut table) = self.table_write().await?;
        table.insert_column(index, column);
        page.write_table_back(&table)?;
        Ok(())
    }

    pub async fn table_read(&self) -> StorageResult<(OwnedPageDataReadGuard, page::table::Table)> {
        let page = self.buffer_pool.fetch_page_read_owned(self.root).await?;
        let table = page.table()?;
        Ok((page, table))
    }

    pub async fn table_write(
        &self,
    ) -> StorageResult<(OwnedPageDataWriteGuard, page::table::Table)> {
        let page = self.buffer_pool.fetch_page_write_owned(self.root).await?;
        let table = page.table()?;
        Ok((page, table))
    }

    pub async fn primary_position(&self) -> StorageResult<usize> {
        self.table_read()
            .await?
            .1
            .columns()
            .iter()
            .enumerate()
            .find_map(|(position, column)| {
                if column.primary() {
                    Some(position)
                } else {
                    None
                }
            })
            .ok_or(Error::NotFound("column", String::from("primary key")))
    }

    pub async fn primary_key(&self, tuple: &Tuple) -> StorageResult<Value> {
        tuple
            .field(self.primary_position().await?)
            .ok_or(Error::NotFound("column", String::from("primary key")))
    }

    pub async fn insert(&self, tuple: Tuple) -> StorageResult<RecordId> {
        let (mut page, mut node) = if !self.has_remaining(&tuple).await? {
            self.add_node().await?
        } else {
            self.last_node_write().await?
        };
        let record_id = node.insert(tuple);
        page.write_table_node_back(&node)?;
        Ok(record_id)
    }

    pub async fn delete(&self, record_id: RecordId) -> StorageResult<Tuple> {
        let RecordId { page_id, slot_num } = record_id;
        let mut page = self.buffer_pool.fetch_page_write_owned(page_id).await?;
        let mut node = page.table_node()?;
        let tuple = match node
            .tuples
            .get_mut(slot_num as usize)
            .filter(|tuple| !tuple.deleted)
        {
            None => {
                return Err(Error::NotFound(
                    "tuple",
                    format!("page: {} slot: {}", page_id, slot_num),
                ))
            }
            Some(tuple) => {
                tuple.deleted = true;
                tuple.clone()
            }
        };
        page.write_table_node_back(&node)?;
        Ok(tuple)
    }

    pub async fn read_tuple(&self, record_id: RecordId) -> StorageResult<Option<Tuple>> {
        let RecordId { page_id, slot_num } = record_id;
        let page = self.buffer_pool.fetch_page_read_owned(page_id).await?;
        let node = page.table_node()?;
        Ok(node
            .tuples
            .get(slot_num as usize)
            .filter(|tuple| !tuple.deleted)
            .cloned())
    }

    pub async fn update_tuple(
        &self,
        record_id: RecordId,
        tuple: Tuple,
    ) -> StorageResult<Option<()>> {
        let RecordId { page_id, slot_num } = record_id;
        let mut page = self.buffer_pool.fetch_page_write_owned(page_id).await?;
        let mut node = page.table_node()?;
        let t = match node
            .tuples
            .get_mut(slot_num as usize)
            .filter(|tuple| !tuple.deleted)
        {
            None => return Ok(None),
            Some(tuple) => tuple,
        };
        t.values = tuple.values;
        page.write_table_node_back(&node)?;
        Ok(Some(()))
    }

    pub async fn tuples(&self) -> StorageResult<impl DoubleEndedIterator<Item = Tuple>> {
        let mut page_id = self.table_read().await?.1.start;
        let mut output = Vec::new();
        let mut latches = Vec::new();
        loop {
            let page = self.buffer_pool.fetch_page_read_owned(page_id).await?;
            let node = page.table_node()?;
            let next = node.next();
            output.extend(node.tuples.into_iter().filter(|tuple| !tuple.deleted));
            latches.push(page);
            match next {
                None => break,
                Some(next) => page_id = next,
            }
        }
        Ok(output.into_iter())
    }

    async fn add_node(&self) -> StorageResult<(OwnedPageDataWriteGuard, TableNode)> {
        let mut heap_page = self.buffer_pool.fetch_page_write_owned(self.root).await?;
        let mut table_heap = heap_page.table()?;
        let mut last_node_page = self
            .buffer_pool
            .fetch_page_write_owned(table_heap.end)
            .await?;
        let mut last_node = last_node_page.table_node()?;
        let mut node = TableNode::new(0, vec![]);
        let page = self.buffer_pool.new_page_write_owned(&mut node).await?;
        last_node.set_next(node.page_id());
        table_heap.set_end(node.page_id());
        heap_page.write_table_back(&table_heap)?;
        last_node_page.write_table_node_back(&last_node)?;
        Ok((page, node))
    }

    async fn first_node(&self) -> StorageResult<(PageRef, TableNode)> {
        self.buffer_pool
            .fetch_page_table_node(self.table_read().await?.1.start)
            .await
            .map_err(Into::into)
    }

    async fn last_node_read(&self) -> StorageResult<(OwnedPageDataReadGuard, TableNode)> {
        let page = self
            .buffer_pool
            .fetch_page_read_owned(self.table_read().await?.1.end)
            .await?;
        let node = page.table_node()?;
        Ok((page, node))
    }

    async fn last_node_write(&self) -> StorageResult<(OwnedPageDataWriteGuard, TableNode)> {
        let page = self
            .buffer_pool
            .fetch_page_write_owned(self.table_read().await?.1.end)
            .await?;
        let node = page.table_node()?;
        Ok((page, node))
    }

    async fn last_node(&self) -> StorageResult<(PageRef, TableNode)> {
        self.buffer_pool
            .fetch_page_table_node(self.table_read().await?.1.end)
            .await
            .map_err(Into::into)
    }

    async fn has_remaining(&self, tuple: &Tuple) -> StorageResult<bool> {
        let (_, node) = self.last_node().await?;
        Ok(node.total_size() > node.encoded_size() + tuple.encoded_size())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::types::DataType;
    use crate::storage::disk::disk_manager::DiskManager;
    use crate::storage::index::Index;

    async fn new_buffer_pool() -> StorageResult<BufferPoolManager> {
        let f = tempfile::NamedTempFile::new()?;
        let disk_manager = DiskManager::new(f.path()).await?;
        Ok(BufferPoolManager::new(1024, 2, disk_manager).await?)
    }

    #[tokio::test]
    async fn table() -> StorageResult<()> {
        let buffer_manager = new_buffer_pool().await?;

        let column_id = Column::new("id", DataType::Bigint).with_primary(true);
        let column_name =
            Column::new("name", DataType::String).with_default(Value::String("hello".to_string()));
        let column_gender = Column::new("gender", DataType::Boolean).with_primary(true);
        let table = Table::new(
            "user",
            vec![column_id.clone(), column_name.clone()],
            Arc::new(buffer_manager),
        )
        .await?;
        assert_eq!(table.name(), "user");
        assert_eq!(table.columns().await?.len(), 2);
        assert_eq!(
            table.columns().await?,
            vec![column_id.clone(), column_name.clone()]
        );
        for _ in 0..10 {
            table.push_column(column_gender.clone()).await?;
        }
        assert_eq!(table.columns().await?.len(), 12);
        Ok(())
    }

    #[tokio::test]
    async fn delete_tuple() -> StorageResult<()> {
        let buffer_manager = Arc::new(new_buffer_pool().await?);
        let column_id = Column::new("id", DataType::Bigint).with_primary(true);
        let column_name =
            Column::new("name", DataType::String).with_default(Value::String("hello".to_string()));
        let table = Table::new(
            "user",
            vec![column_id.clone(), column_name.clone()],
            buffer_manager.clone(),
        )
        .await?;
        let prediction = |id: i128| id % 2 == 0;
        let len = 4096;
        let index = Index::new(buffer_manager.clone(), 128).await?;
        for id in 0..len {
            let tuple = Tuple::new(
                vec![Value::Bigint(id), Value::String("Mike".to_string())],
                0,
            );
            let record_id = table.insert(tuple.clone()).await?;
            assert_eq!(table.read_tuple(record_id).await?, Some(tuple));
            index.insert(id, record_id).await?;
            if prediction(id) {
                table.delete(record_id).await?;
            }
        }
        assert_eq!(
            table.tuples().await?.collect::<Vec<_>>().len(),
            (0..len)
                .filter(|id| prediction(*id))
                .map(|id| Tuple::new(
                    vec![Value::Bigint(id), Value::String("Mike".to_string())],
                    0
                ))
                .collect::<Vec<_>>()
                .len()
        );
        Ok(())
    }
}
