use crate::buffer::buffer_poll_manager::{BufferPoolManager, PageRef};
use crate::catalog::{ColumnId, TableId};
use crate::encoding::encoded_size::EncodedSize;
use crate::error::RustDBResult;
use crate::storage::page::column::Column;
use crate::storage::page::table::{TableNode, Tuple};
use crate::storage::{page, PageId};
use std::collections::HashMap;
use std::sync::Arc;

pub struct Table {
    name: String,
    buffer_pool: Arc<BufferPoolManager>,
    root: PageId,
    column_idxs: HashMap<String, ColumnId>,
    columns: Vec<Column>,
    /// Primary keys
    primary_keys: Vec<ColumnId>,
}

impl Table {
    pub async fn new<T: Into<String> + Clone>(
        id: TableId,
        name: T,
        columns: Vec<Column>,
        buffer_pool: Arc<BufferPoolManager>,
    ) -> RustDBResult<Self> {
        let mut table_node = TableNode::new(0, vec![]);
        let mut table_heap = page::table::Table::new(id, name.clone(), 0, 0, columns.clone());

        buffer_pool.new_page_table_node(&mut table_node).await?;
        table_heap.set_start(table_node.page_id());
        table_heap.set_end(table_node.page_id());
        buffer_pool.new_page_table(&mut table_heap).await?;

        let column_idxs = columns
            .iter()
            .enumerate()
            .map(|(id, column)| (column.name().to_string(), id as ColumnId))
            .collect();
        let primary_keys = columns
            .iter()
            .enumerate()
            .filter_map(|(id, column)| {
                if column.primary() {
                    Some(id as ColumnId)
                } else {
                    None
                }
            })
            .collect();

        Ok(Self {
            name: name.into(),
            buffer_pool,
            root: table_heap.page_id(),
            column_idxs,
            columns,
            primary_keys,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn read_column(&self, column_id: ColumnId) -> Option<&Column> {
        self.columns.get(column_id as usize)
    }

    pub fn read_column_name(&self, column_name: &str) -> Option<&Column> {
        self.column_idxs
            .get(column_name)
            .and_then(|column_id| self.read_column(*column_id))
    }

    pub fn read_column_id(&self, column_name: &str) -> Option<ColumnId> {
        self.column_idxs.get(column_name).copied()
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub async fn add_column(&mut self, column_id: ColumnId, column: Column) -> RustDBResult<()> {
        let (page, mut table) = self.table().await?;
        table.add_column(column_id, column.clone());
        page.page().write_table_back(&table).await?;
        if column.primary_key {
            self.primary_keys.push(column_id);
            self.primary_keys.sort();
        }
        self.column_idxs
            .insert(column.name().to_string(), column_id);
        self.columns.insert(column_id as usize, column);
        Ok(())
    }

    pub async fn table(&self) -> RustDBResult<(PageRef, page::table::Table)> {
        self.buffer_pool
            .fetch_page_table(self.root)
            .await
            .map_err(Into::into)
    }

    pub async fn insert(&self, tuple: Tuple) -> RustDBResult<()> {
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

    pub fn primary_keys(&self) -> &[ColumnId] {
        self.primary_keys.as_slice()
    }

    async fn add_node(&self) -> RustDBResult<(PageRef, TableNode)> {
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

    async fn first_node(&self) -> RustDBResult<(PageRef, TableNode)> {
        self.buffer_pool
            .fetch_page_table_node(self.table().await?.1.start)
            .await
            .map_err(Into::into)
    }

    async fn last_node(&self) -> RustDBResult<(PageRef, TableNode)> {
        self.buffer_pool
            .fetch_page_table_node(self.table().await?.1.end)
            .await
            .map_err(Into::into)
    }

    async fn remaining_size(&self) -> RustDBResult<Option<usize>> {
        let (_, node) = self.last_node().await?;
        Ok(node.total_size().checked_sub(node.encoded_size()))
    }

    async fn has_remaining(&self, tuple: &Tuple) -> RustDBResult<bool> {
        let (_, node) = self.last_node().await?;
        Ok(node.encoded_size() + tuple.encoded_size() > node.total_size())
    }
}
