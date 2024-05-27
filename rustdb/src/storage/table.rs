use crate::buffer::buffer_poll_manager::BufferPoolManager;
use crate::storage::PageId;
use tokio::sync::RwLock;

pub struct Table {
    name: String,
    buffer_pool: BufferPoolManager,
    root: RwLock<PageId>,
}
