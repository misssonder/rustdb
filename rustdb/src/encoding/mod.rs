use crate::encoding::error::Error;
use bytes::{Buf, BufMut};
use ordered_float::OrderedFloat;

pub mod index;

mod column;
mod datatype;
pub mod encoded_size;
pub mod error;
mod record_id;
mod table;

pub type EncoderVecLen = u32;

pub trait Encoder: Sized {
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut;
}

pub trait Decoder: Sized {
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf;
}

pub trait Nullable: Sized {
    fn null_value() -> Self;
}
macro_rules! impl_decoder {
    ($($ty:ty,$fn:ident);+$(;)?) => {
        $(impl Decoder for $ty {

            fn decode<B>(buf: &mut B) -> Result<Self, Error>
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

            fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
            where
                B: BufMut,
            {
                buf.$fn(*self as $ty);
                Ok(())
            }
        })+
    };
}

macro_rules! impl_nullable_as_max {
    ($($ty:ty);+$(;)?) => {
        $(impl Nullable for $ty  {
             fn null_value() -> Self{
                 <$ty>::MAX
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

impl_nullable_as_max! {
    u8;
    u16;
    u32;
    u64;
    u128;
    i8;
    i16;
    i32;
    i64;
    i128;
    f32;
    f64;
    isize;
    usize;
}

impl Decoder for bool {
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        let val = buf.get_u8();
        Ok(match val {
            0 => false,
            1 => true,
            other => return Err(Error::Decode(format!("Can't decode {other} as bool"))),
        })
    }
}

impl Encoder for bool {
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
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
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        u64::decode(buf).map(|n| n as usize)
    }
}
impl Encoder for usize {
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut,
    {
        (*self as u64).encode(buf)
    }
}

impl Decoder for isize {
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        i64::decode(buf).map(|n| n as isize)
    }
}

impl Encoder for isize {
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut,
    {
        (*self as i64).encode(buf)
    }
}

impl Encoder for String {
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut,
    {
        (self.as_bytes().len() as u32).encode(buf)?;
        buf.put_slice(self.as_bytes());
        Ok(())
    }
}

impl Decoder for String {
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        let len = u32::decode(buf)?;
        let mut bytes = Vec::with_capacity(len as usize);
        for _ in 0..len {
            bytes.push(u8::decode(buf)?)
        }
        String::from_utf8(bytes).map_err(|err| Error::Decode("Can't read bytes in utf-8".into()))
    }
}

impl Decoder for Option<bool> {
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        let val = buf.get_u8();
        Ok(match val {
            0 => Some(false),
            1 => Some(true),
            u8::MAX => None,
            other => return Err(Error::Decode(format!("Can't decode {other} as bool"))),
        })
    }
}

impl Encoder for Option<bool> {
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut,
    {
        match *self {
            None => buf.put_u8(u8::null_value()),
            Some(true) => buf.put_u8(1),
            Some(false) => buf.put_u8(0),
        }
        Ok(())
    }
}

impl Decoder for Option<String> {
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        let null_value = u32::null_value();
        let len = u32::decode(buf)?;
        if len == null_value {
            return Ok(None);
        }
        let mut bytes = Vec::with_capacity(len as usize);
        for _ in 0..len {
            bytes.push(u8::decode(buf)?)
        }
        String::from_utf8(bytes)
            .map(Some)
            .map_err(|_err| Error::Decode("Can't read bytes in utf-8".into()))
    }
}

impl Encoder for Option<String> {
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut,
    {
        match self {
            None => u32::null_value().encode(buf),
            Some(str) => str.encode(buf),
        }
    }
}

impl<T> Decoder for OrderedFloat<T>
where
    T: Decoder,
{
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        Ok(Self(T::decode(buf)?))
    }
}

impl<T> Encoder for OrderedFloat<T>
where
    T: Encoder,
{
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut,
    {
        self.0.encode(buf)
    }
}

impl<T> Decoder for Option<T>
where
    T: Decoder + Nullable + PartialEq,
{
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        let max_value = T::null_value();
        let decoded = T::decode(buf)?;
        if decoded == max_value {
            Ok(None)
        } else {
            Ok(Some(decoded))
        }
    }
}

impl<T> Encoder for Option<T>
where
    T: Encoder + Nullable,
{
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut,
    {
        let max_value = T::null_value();
        match self {
            None => max_value.encode(buf),
            Some(t) => t.encode(buf),
        }
    }
}

impl<T> Decoder for Vec<T>
where
    T: Decoder,
{
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        let len = EncoderVecLen::decode(buf)?;
        let mut output = Vec::with_capacity(len as usize);
        for _ in 0..len {
            output.push(T::decode(buf)?);
        }
        Ok(output)
    }
}

impl<T> Encoder for Vec<T>
where
    T: Encoder,
{
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut,
    {
        (self.len() as EncoderVecLen).encode(buf)?;
        for data in self {
            data.encode(buf)?;
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::PAGE_SIZE;

    #[test]
    fn encode_decode() {
        {
            let mut buffer = [0; PAGE_SIZE];
            let str = String::from("Hello world");
            str.encode(&mut buffer.as_mut()).unwrap();
            assert_eq!(String::decode(&mut buffer.as_ref()).unwrap(), str);
        }
        {
            let mut buffer = [0; PAGE_SIZE];
            let val: u32 = 256;
            val.encode(&mut buffer.as_mut()).unwrap();
            assert_eq!(u32::decode(&mut buffer.as_ref()).unwrap(), val);
        }
        {
            let mut buffer = [0; PAGE_SIZE];
            let val: Option<u32> = Some(256);
            val.encode(&mut buffer.as_mut()).unwrap();
            assert_eq!(Option::<u32>::decode(&mut buffer.as_ref()).unwrap(), val);
        }
        {
            let mut buffer = [0; PAGE_SIZE];
            let val: Option<u32> = None;
            val.encode(&mut buffer.as_mut()).unwrap();
            assert_eq!(Option::<u32>::decode(&mut buffer.as_ref()).unwrap(), val);
        }
        {
            let mut buffer = [0; PAGE_SIZE];
            let val: Option<bool> = None;
            u8::MAX.encode(&mut buffer.as_mut()).unwrap();
            assert_eq!(Option::<bool>::decode(&mut buffer.as_ref()).unwrap(), val);
        }
    }
}
