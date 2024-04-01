pub mod b_plus_tree;

use crate::error::{RustDBError, RustDBResult};
use crate::storage::codec::{Decoder, Encoder};
use crate::storage::page::b_plus_tree::Node;
use crate::storage::{PageId, PAGE_SIZE};

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