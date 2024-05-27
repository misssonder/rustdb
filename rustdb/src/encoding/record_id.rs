use crate::encoding::encoded_size::EncodedSize;
use crate::encoding::{Decoder, Encoder};
use crate::error::RustDBError;
use crate::storage::{PageId, RecordId};
use bytes::{Buf, BufMut};

impl Decoder for RecordId {
    type Error = RustDBError;

    fn decode<B>(buffer: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        Ok(Self {
            page_id: PageId::decode(buffer)?,
            slot_num: u32::decode(buffer)?,
        })
    }
}

impl Encoder for RecordId {
    type Error = RustDBError;

    fn encode<B>(&self, buffer: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        self.page_id.encode(buffer)?;
        self.slot_num.encode(buffer)?;
        Ok(())
    }
}

impl EncodedSize for RecordId {
    fn encoded_size(&self) -> usize {
        self.page_id.encoded_size() + self.slot_num.encoded_size()
    }
}
