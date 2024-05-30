use crate::buffer::buffer_poll_manager::{BufferPoolManager, PageRef};
use crate::encoding::encoded_size::EncodedSize;
use crate::storage::page::column::Column;
use crate::storage::page::table::{TableNode, Tuple};
use crate::storage::{page, PageId, StorageResult};
use std::sync::Arc;

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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn columns(&self) -> StorageResult<Vec<Column>> {
        Ok(self.table().await?.1.columns)
    }

    pub async fn push_column(&self, column: Column) -> StorageResult<()> {
        let (page, mut table) = self.table().await?;
        table.push_column(column);
        page.page().write_table_back(&table).await?;
        Ok(())
    }

    pub async fn insert_column(&self, index: usize, column: Column) -> StorageResult<()> {
        let (page, mut table) = self.table().await?;
        table.insert_column(index, column);
        page.page().write_table_back(&table).await?;
        Ok(())
    }

    pub async fn table(&self) -> StorageResult<(PageRef, page::table::Table)> {
        self.buffer_pool
            .fetch_page_table(self.root)
            .await
            .map_err(Into::into)
    }

    pub async fn insert(&self, tuple: Tuple) -> StorageResult<()> {
        let (page, mut node) = if !self.has_remaining(&tuple).await? {
            self.add_node().await?
        } else {
            self.last_node().await?
        };
        node.insert(tuple);
        page.page()
            .write_table_node_back(&node)
            .await
            .map_err(Into::into)
    }

    pub async fn tuples(&self) -> StorageResult<Vec<Tuple>> {
        let mut page_id = self.table().await?.1.start;
        let mut output = Vec::new();
        loop {
            let node = self.buffer_pool.fetch_page_table_node(page_id).await?.1;
            let next = node.next();
            output.extend(node.tuples);
            match next {
                None => break,
                Some(next) => page_id = next,
            }
        }
        Ok(output)
    }

    async fn add_node(&self) -> StorageResult<(PageRef, TableNode)> {
        let (heap_page, mut table_heap) = self.table().await?;
        let (last_node_page, mut last_node) = self
            .buffer_pool
            .fetch_page_table_node(table_heap.end)
            .await?;
        let mut node = TableNode::new(0, vec![]);
        let page_ref = self.buffer_pool.new_page_table_node(&mut node).await?;
        last_node.set_next(node.page_id());
        table_heap.set_end(node.page_id());
        heap_page.page().write_table_back(&table_heap).await?;
        last_node_page
            .page()
            .write_table_node_back(&last_node)
            .await?;
        Ok((page_ref, node))
    }

    async fn first_node(&self) -> StorageResult<(PageRef, TableNode)> {
        self.buffer_pool
            .fetch_page_table_node(self.table().await?.1.start)
            .await
            .map_err(Into::into)
    }

    async fn last_node(&self) -> StorageResult<(PageRef, TableNode)> {
        self.buffer_pool
            .fetch_page_table_node(self.table().await?.1.end)
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
    use crate::sql::types::{DataType, Value};
    use crate::storage::disk::disk_manager::DiskManager;

    async fn new_buffer_pool() -> StorageResult<BufferPoolManager> {
        let f = tempfile::NamedTempFile::new()?;
        let disk_manager = DiskManager::new(f.path()).await?;
        Ok(BufferPoolManager::new(100, 2, disk_manager).await?)
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
    async fn insert_tuple() -> StorageResult<()> {
        let buffer_manager = new_buffer_pool().await?;
        let column_id = Column::new("id", DataType::Bigint).with_primary(true);
        let column_name =
            Column::new("name", DataType::String).with_default(Value::String("hello".to_string()));
        let table = Table::new(
            "user",
            vec![column_id.clone(), column_name.clone()],
            Arc::new(buffer_manager),
        )
        .await?;
        for id in 0..4096 {
            table
                .insert(Tuple::new(vec![
                    Value::Bigint(id),
                    Value::String("world".to_string()),
                ]))
                .await?;
        }
        assert_eq!(table.tuples().await?.len(), 4096);
        Ok(())
    }
}
