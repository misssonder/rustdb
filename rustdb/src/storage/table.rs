use crate::buffer::buffer_poll_manager::{BufferPoolManager, PageRef};
use crate::encoding::encoded_size::EncodedSize;
use crate::error::RustDBResult;
use crate::storage::page::column::ColumnDesc;
use crate::storage::page::table::{TableNode, Tuple};
use crate::storage::{page, PageId};

pub struct Table {
    name: String,
    buffer_pool: BufferPoolManager,
    root: PageId,
    columns: Vec<ColumnDesc>,
}

impl Table {
    pub async fn new<T: Into<String>>(
        name: T,
        columns: Vec<ColumnDesc>,
        buffer_pool: BufferPoolManager,
    ) -> RustDBResult<Self> {
        let mut table_node = TableNode::new(0, vec![]);
        let mut table_heap = page::table::Table::new(0, columns.clone(), 0);

        buffer_pool.new_page_table_node(&mut table_node).await?;
        table_heap.set_start(table_node.page_id());
        table_heap.set_end(table_node.page_id());
        buffer_pool.new_page_table(&mut table_heap).await?;
        Ok(Self {
            name: name.into(),
            buffer_pool,
            root: table_heap.page_id(),
            columns,
        })
    }

    pub fn column(&self) -> &[ColumnDesc] {
        &self.columns
    }

    pub async fn table(&self) -> RustDBResult<(PageRef, page::table::Table)> {
        self.buffer_pool.fetch_page_table(self.root).await
    }

    pub async fn insert(&self, tuple: Tuple) -> RustDBResult<()> {
        let (page, mut node) = if !self.has_remaining(&tuple).await? {
            self.add_node().await?
        } else {
            self.last_node().await?
        };
        node.insert(tuple);
        page.page().write_table_node_back(&node).await
    }

    async fn add_node(&self) -> RustDBResult<(PageRef, TableNode)> {
        let (heap_page, table_heap) = self.table().await?;
        let (last_node_page, mut last_node) = self
            .buffer_pool
            .fetch_page_table_node(table_heap.end)
            .await?;
        let mut node = TableNode::new(0, vec![]);
        let page_ref = self.buffer_pool.new_page_table_node(&mut node).await?;
        last_node.set_next(node.page_id());
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
    }

    async fn last_node(&self) -> RustDBResult<(PageRef, TableNode)> {
        self.buffer_pool
            .fetch_page_table_node(self.table().await?.1.end)
            .await
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
