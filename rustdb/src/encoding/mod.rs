use crate::error::RustDBError;

use bytes::{Buf, BufMut};
pub mod index;

mod datatype;
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

macro_rules! impl_decoder {
    ($($ty:ty,$fn:ident);+$(;)?) => {
        $(impl Decoder for $ty {
            type Error = RustDBError;

            fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
            where
                B: Buf,
            {
                Ok(buf.$fn())
            }
        })+
    };
}

macro_rules! impl_encoder {
    ($($ty:ty,$fn:ident);+$(;)?) => {
        $(impl Encoder for $ty{
            type Error = RustDBError;

            fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
            where
                B: BufMut,
            {
                buf.$fn(*self);
                Ok(())
            }
        })+
    };
}

impl_decoder! {
    u8, get_u8;
    u16, get_u16;
    u32, get_u32;
    u64, get_u64;
    u128, get_u128;
    i8, get_i8;
    i16, get_i16;
    i32, get_i32;
    i64, get_i64;
    i128, get_i128;
    f32, get_f32;
    f64, get_f64;
}

impl_encoder! {
    u8, put_u8;
    u16, put_u16;
    u32, put_u32;
    u64, put_u64;
    u128, put_u128;
    i8, put_i8;
    i16, put_i16;
    i32, put_i32;
    i64, put_i64;
    i128, put_i128;
    f32, put_f32;
    f64, put_f64;
}

impl Decoder for bool {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        let val = buf.get_u8();
        Ok(match val {
            0 => false,
            1 => true,
            other => return Err(RustDBError::Decode(format!("Can't decode {other} as bool"))),
        })
    }
}

impl Encoder for bool {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        match *self {
            true => buf.put_u8(1),
            false => buf.put_u8(0),
        }
        Ok(())
    }
}

impl Decoder for usize {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        u64::decode(buf).map(|n| n as usize)
    }
}
impl Encoder for usize {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        (*self as u64).encode(buf)
    }
}

impl Decoder for isize {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        i64::decode(buf).map(|n| n as isize)
    }
}

impl Encoder for isize {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        (*self as i64).encode(buf)
    }
}

impl Encoder for String {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        self.as_bytes().len().encode(buf)?;
        buf.put_slice(self.as_bytes());
        Ok(())
    }
}

impl Decoder for String {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        let len = usize::decode(buf)?;
        let mut bytes = Vec::with_capacity(len);
        for _ in 0..len {
            bytes.push(u8::decode(buf)?)
        }
        String::from_utf8(bytes)
            .map_err(|err| RustDBError::Decode("Can't read bytes in utf-8".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::PAGE_SIZE;

    #[test]
    fn encode_decode() {
        let mut buffer = [0; PAGE_SIZE];
        let str = String::from("Hello world");
        str.encode(&mut buffer.as_mut()).unwrap();
        assert_eq!(String::decode(&mut buffer.as_ref()).unwrap(), str);
    }
}
