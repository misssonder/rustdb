use std::sync::atomic::AtomicUsize;
use crate::error::RustDBError;
use crate::storage::codec::{Decoder, Encoder};
use bytes::{Buf, BufMut};

pub mod codec;
pub mod disk;
mod index;
pub mod page;

pub const PAGE_SIZE: usize = 4096;
pub type PageId = usize;

pub type AtomicPageId= AtomicUsize;
pub const NULL_PAGE: PageId = PageId::MAX;

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
pub struct RecordId {
    page_id: PageId,
    slot_num: u32,
}

impl Decoder for RecordId {
    type Error = RustDBError;

    fn decode<B>(buffer: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        Ok(Self {
            page_id: buffer.get_u64() as _,
            slot_num: buffer.get_u32(),
        })
    }
}

impl Encoder for RecordId {
    type Error = RustDBError;

    fn encode<B>(&self, buffer: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        buffer.put_u64(self.page_id as _);
        buffer.put_u32(self.slot_num);
        Ok(())
    }
}
