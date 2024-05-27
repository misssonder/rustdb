use crate::encoding::encoded_size::EncodedSize;
use crate::encoding::{Decoder, Encoder};
use crate::error::{RustDBError, RustDBResult};
use crate::sql::types::{DataType, Value};
use bytes::{Buf, BufMut};

mod basetype {
    use std::mem;

    pub const BOOLEAN: u8 = 0;
    pub const TINYINT: u8 = 1;
    pub const SMALLINT: u8 = 2;
    pub const INTEGER: u8 = 3;
    pub const BIGINT: u8 = 4;
    pub const FLOAT: u8 = 5;
    pub const DOUBLE: u8 = 6;
    pub const STRING: u8 = 7;
    pub const ENCODED_SIZE: usize = mem::size_of::<u8>();
}

mod basevalue {
    use std::mem;

    pub const NULL: u8 = u8::MAX;
    pub const BOOLEAN: u8 = 0;
    pub const TINYINT: u8 = 1;
    pub const SMALLINT: u8 = 2;
    pub const INTEGER: u8 = 3;
    pub const BIGINT: u8 = 4;
    pub const FLOAT: u8 = 5;
    pub const DOUBLE: u8 = 6;
    pub const STRING: u8 = 7;

    pub const NONE_VALUE: u8 = u8::MAX;
    pub const SOME_VALUE: u8 = 1;

    pub const ENCODED_SIZE: usize = mem::size_of::<u8>();
}

impl DataType {
    pub fn as_byte(&self) -> u8 {
        match self {
            DataType::Boolean => basetype::BOOLEAN,
            DataType::Tinyint => basetype::TINYINT,
            DataType::Smallint => basetype::SMALLINT,
            DataType::Integer => basetype::INTEGER,
            DataType::Bigint => basetype::BIGINT,
            DataType::Float => basetype::FLOAT,
            DataType::Double => basetype::DOUBLE,
            DataType::String => basetype::STRING,
        }
    }

    pub fn from_byte(byte: u8) -> RustDBResult<Self> {
        Ok(match byte {
            basetype::BOOLEAN => DataType::Boolean,
            basetype::TINYINT => DataType::Tinyint,
            basetype::SMALLINT => DataType::Smallint,
            basetype::INTEGER => DataType::Integer,
            basetype::BIGINT => DataType::Bigint,
            basetype::FLOAT => DataType::Float,
            basetype::DOUBLE => DataType::Double,
            basetype::STRING => DataType::String,
            other => {
                return Err(RustDBError::Decode(format!(
                    "Can't decode {} as datatype",
                    other
                )))
            }
        })
    }
}

impl Decoder for DataType {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        DataType::from_byte(u8::decode(buf)?)
    }
}

impl Encoder for DataType {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        self.as_byte().encode(buf)
    }
}

impl EncodedSize for DataType {
    fn encoded_size(&self) -> usize {
        self.as_byte().encoded_size()
    }
}

impl Decoder for Value {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        let value_type = u8::decode(buf)?;
        Ok(match value_type {
            basevalue::NULL => Value::Null,
            basevalue::BOOLEAN => Value::Boolean(bool::decode(buf)?),
            basevalue::TINYINT => Value::Tinyint(i16::decode(buf)?),
            basevalue::SMALLINT => Value::Smallint(i32::decode(buf)?),
            basevalue::INTEGER => Value::Integer(i64::decode(buf)?),
            basevalue::BIGINT => Value::Bigint(i128::decode(buf)?),
            basevalue::FLOAT => Value::Float(f32::decode(buf)?),
            basevalue::DOUBLE => Value::Double(f64::decode(buf)?),
            basevalue::STRING => Value::String(String::decode(buf)?),
            other => {
                return Err(RustDBError::Decode(format!(
                    "Can't decode {} as value",
                    other
                )))
            }
        })
    }
}

impl Encoder for Value {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        match self {
            Value::Null => basevalue::NULL.encode(buf),
            Value::Boolean(boolean) => {
                basevalue::BOOLEAN.encode(buf)?;
                boolean.encode(buf)
            }
            Value::Tinyint(tinyint) => {
                basevalue::TINYINT.encode(buf)?;
                tinyint.encode(buf)
            }
            Value::Smallint(smallint) => {
                basevalue::SMALLINT.encode(buf)?;
                smallint.encode(buf)
            }
            Value::Integer(integer) => {
                basevalue::INTEGER.encode(buf)?;
                integer.encode(buf)
            }
            Value::Bigint(bigint) => {
                basevalue::BIGINT.encode(buf)?;
                bigint.encode(buf)
            }
            Value::Float(float) => {
                basevalue::FLOAT.encode(buf)?;
                float.encode(buf)
            }
            Value::Double(double) => {
                basevalue::DOUBLE.encode(buf)?;
                double.encode(buf)
            }
            Value::String(str) => {
                basevalue::STRING.encode(buf)?;
                str.encode(buf)
            }
        }
    }
}

impl EncodedSize for Value {
    fn encoded_size(&self) -> usize {
        basevalue::ENCODED_SIZE
            + match self {
                Value::Null => 0,
                Value::Boolean(boolean) => boolean.encoded_size(),
                Value::Tinyint(tinyint) => tinyint.encoded_size(),
                Value::Smallint(smallint) => smallint.encoded_size(),
                Value::Integer(integer) => integer.encoded_size(),
                Value::Bigint(bigint) => bigint.encoded_size(),
                Value::Float(float) => float.encoded_size(),
                Value::Double(double) => double.encoded_size(),
                Value::String(str) => str.encoded_size(),
            }
    }
}

impl Encoder for Option<Value> {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        match self {
            None => basevalue::NONE_VALUE.encode(buf),
            Some(val) => {
                basevalue::SOME_VALUE.encode(buf)?;
                val.encode(buf)
            }
        }
    }
}

impl Decoder for Option<Value> {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        Ok(match u8::decode(buf)? {
            basevalue::NONE_VALUE => None,
            _ => Some(Value::decode(buf)?),
        })
    }
}

impl EncodedSize for Option<Value> {
    fn encoded_size(&self) -> usize {
        basevalue::ENCODED_SIZE
            + match self {
                None => 0,
                Some(value) => value.encoded_size(),
            }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::PAGE_SIZE;

    #[test]
    fn datatype_encode_decode() {
        let mut buffer = [0; PAGE_SIZE];
        let ty = DataType::Tinyint;
        ty.encode(&mut buffer.as_mut()).unwrap();
        let decoded = DataType::decode(&mut buffer[..ty.encoded_size()].as_ref()).unwrap();
        assert_eq!(decoded, ty)
    }

    #[test]
    fn value_encode_decode() {
        {
            let mut buffer = [0; PAGE_SIZE];
            let ty = Value::Bigint(128);
            ty.encode(&mut buffer.as_mut()).unwrap();
            let decoded = Value::decode(&mut buffer[..ty.encoded_size()].as_ref()).unwrap();
            assert_eq!(decoded, ty)
        }
        {
            let mut buffer = [0; PAGE_SIZE];
            let ty = Value::Double(2.0);
            ty.encode(&mut buffer.as_mut()).unwrap();
            let decoded = Value::decode(&mut buffer[..ty.encoded_size()].as_ref()).unwrap();
            assert_eq!(decoded, ty)
        }
        {
            let mut buffer = [0; PAGE_SIZE];
            let ty = Some(Value::Double(2.0));
            ty.encode(&mut buffer.as_mut()).unwrap();
            let decoded =
                Option::<Value>::decode(&mut buffer[..ty.encoded_size()].as_ref()).unwrap();
            assert_eq!(decoded, ty)
        }
        {
            let mut buffer = [0; PAGE_SIZE];
            let ty = Value::String("Hello world".into());
            ty.encode(&mut buffer.as_mut()).unwrap();
            let decoded = Value::decode(&mut buffer[..ty.encoded_size()].as_ref()).unwrap();
            assert_eq!(decoded, ty)
        }
    }
}
