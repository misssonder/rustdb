pub mod column;
/// This mod contain a bunch of structures which represent the page's layout
pub mod index;
pub mod table;

use crate::encoding::{Decoder, Encoder};
use crate::error::{RustDBError, RustDBResult};
use crate::storage::page::index::Node;
use crate::storage::page::table::{Table, TableNode};
use crate::storage::{AtomicPageId, PageId, PAGE_SIZE};
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

pub type PageData = Arc<RwLock<[u8; PAGE_SIZE]>>;
pub struct Page {
    page_id: AtomicPageId,
    data: PageData,
    pub pin_count: AtomicU32,
    pub is_dirty: AtomicBool,
}

impl Page {
    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id: AtomicPageId::new(page_id),
            data: Arc::new(RwLock::new([0; PAGE_SIZE])),
            pin_count: AtomicU32::new(0),
            is_dirty: AtomicBool::new(false),
        }
    }
    pub fn data(&self) -> PageData {
        self.data.clone()
    }

    pub fn data_ref(&self) -> &PageData {
        &self.data
    }

    pub async fn reset(&self) {
        self.page_id.store(0, Ordering::Relaxed);
        {
            let mut data = self.data.write().await;
            *data = [0; PAGE_SIZE];
        }
        self.pin_count.store(0, Ordering::Relaxed);
        self.is_dirty.store(false, Ordering::Relaxed);
    }

    pub fn page_id(&self) -> PageId {
        self.page_id.load(Ordering::Relaxed)
    }
    pub fn set_page_id(&self, page_id: PageId) {
        self.page_id.store(page_id, Ordering::Relaxed)
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty.load(Ordering::Relaxed)
    }

    pub fn set_dirty(&self, is_dirty: bool) {
        self.is_dirty.store(is_dirty, Ordering::Relaxed);
    }
    pub async fn node<K>(&self) -> RustDBResult<Node<K>>
    where
        K: Decoder<Error = RustDBError>,
    {
        self.decode().await
    }

    pub async fn write_node_back<K>(&self, node: &Node<K>) -> RustDBResult<()>
    where
        K: Encoder<Error = RustDBError>,
    {
        self.encode(node).await
    }

    pub async fn table(&self) -> RustDBResult<Table> {
        self.decode().await
    }

    pub async fn write_table_back(&self, table: &Table) -> RustDBResult<()> {
        self.encode(table).await
    }

    pub async fn table_node(&self) -> RustDBResult<TableNode> {
        self.decode().await
    }

    pub async fn write_table_node_back(&self, table_node: &TableNode) -> RustDBResult<()> {
        self.encode(table_node).await
    }

    async fn encode<T>(&self, t: &T) -> RustDBResult<()>
    where
        T: Encoder<Error = RustDBError>,
    {
        let mut data = self.data_ref().write().await;
        t.encode(&mut data.as_mut())
    }

    async fn decode<T>(&self) -> RustDBResult<T>
    where
        T: Decoder<Error = RustDBError>,
    {
        let data = self.data_ref().read().await;
        T::decode(&mut data.as_ref())
    }
}
