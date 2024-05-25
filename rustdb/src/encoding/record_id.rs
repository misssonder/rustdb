use crate::encoding::{Decoder, Encoder};
use crate::error::RustDBError;
use crate::storage::RecordId;
use bytes::{Buf, BufMut};

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
