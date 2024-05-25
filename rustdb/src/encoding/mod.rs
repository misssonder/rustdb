use crate::error::RustDBError;

use bytes::{Buf, BufMut};
pub mod index;

mod record_id;

pub trait Encoder: Sized {
    type Error;
    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut;
}

pub trait Decoder: Sized {
    type Error;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf;
}

impl Encoder for u32 {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        buf.put_u32(*self);
        Ok(())
    }
}

impl Decoder for u32 {
    type Error = RustDBError;
    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        Ok(buf.get_u32())
    }
}
