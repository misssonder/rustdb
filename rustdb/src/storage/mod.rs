use std::sync::atomic::AtomicUsize;

pub mod disk;
mod index;
pub mod page;
mod table;

pub const PAGE_SIZE: usize = 4096;
pub type PageId = usize;

pub type AtomicPageId = AtomicUsize;
pub const NULL_PAGE: PageId = PageId::MAX;
pub type TableId = u16;

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_num: u32,
}
