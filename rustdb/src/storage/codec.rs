use bytes::{Buf, BufMut};

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
