pub mod b_plus_tree;

use crate::error::{RustDBError, RustDBResult};
use crate::storage::codec::{Decoder, Encoder};
use crate::storage::page::b_plus_tree::Node;
use crate::storage::{PageId, PAGE_SIZE};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone)]
pub struct Page {
    page_id: PageId,
    data: [u8; PAGE_SIZE],
    pub pin_count: u32,
    is_dirty: bool,
}

impl Page {
    pub fn new(page_id: PageId) -> Self {
        Self {
            page_id,
            data: [0; PAGE_SIZE],
            pin_count: 0,
            is_dirty: false,
        }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
    pub fn mut_data(&mut self) -> &mut [u8] {
        &mut self.data
    }

    pub fn reset(&mut self) {
        self.page_id = 0;
        self.data = [0; PAGE_SIZE];
        self.pin_count = 0;
        self.is_dirty = false;
    }

    pub fn page_id(&self) -> PageId {
        self.page_id
    }
    pub fn set_page_id(&mut self, page_id: PageId) {
        self.page_id = page_id
    }

    pub fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    pub fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty
    }

    pub fn node<K>(&self) -> RustDBResult<Node<K>>
    where
        K: Decoder<Error = RustDBError>,
    {
        Node::decode(&mut self.data())
    }

    pub fn write_back<K>(&mut self, node: &Node<K>) -> RustDBResult<()>
    where
        K: Encoder<Error = RustDBError>,
    {
        node.encode(&mut self.mut_data())
    }
}

pub struct PageRef {
    page: Arc<RwLock<Page>>,
}

pub struct PageWriteGuard<'a> {
    guard: RwLockWriteGuard<'a, Page>,
}

pub struct PageReadGuard<'a> {
    guard: RwLockReadGuard<'a, Page>,
}

impl Drop for PageRef {
    fn drop(&mut self) {
        let page = self.page.clone();
        tokio::spawn(async move { page.write().await.pin_count -= 1 });
    }
}

impl Deref for PageWriteGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl DerefMut for PageWriteGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.guard.deref_mut()
    }
}

impl Deref for PageReadGuard<'_> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        self.guard.deref()
    }
}

impl PageRef {
    pub fn new(page: Arc<RwLock<Page>>) -> Self {
        Self { page }
    }
    pub async fn write(&self) -> PageWriteGuard<'_> {
        let mut guard = self.page.write().await;
        guard.is_dirty = true;
        PageWriteGuard { guard }
    }

    pub async fn read(&self) -> PageReadGuard<'_> {
        PageReadGuard {
            guard: self.page.read().await,
        }
    }
}
