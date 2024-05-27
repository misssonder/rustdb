use crate::encoding::encoded_size::EncodedSize;
use crate::encoding::{Decoder, Encoder};
use crate::error::RustDBError;
use crate::sql::types::{DataType, Value};
use crate::storage::page::column::ColumnDesc;
use bytes::{Buf, BufMut};

impl Decoder for ColumnDesc {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        let name = String::decode(buf)?;
        let datatype = DataType::decode(buf)?;
        let primary_key = bool::decode(buf)?;
        let nullable = Option::<bool>::decode(buf)?;
        let default = Option::<Value>::decode(buf)?;
        let unique = bool::decode(buf)?;
        let index = bool::decode(buf)?;
        let references = Option::<String>::decode(buf)?;
        Ok(Self {
            name,
            datatype,
            primary_key,
            nullable,
            default,
            unique,
            index,
            references,
        })
    }
}
impl Encoder for ColumnDesc {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        self.name.encode(buf)?;
        self.datatype.encode(buf)?;
        self.primary_key.encode(buf)?;
        self.nullable.encode(buf)?;
        self.default.encode(buf)?;
        self.unique.encode(buf)?;
        self.index.encode(buf)?;
        self.references.encode(buf)?;
        Ok(())
    }
}

impl EncodedSize for ColumnDesc {
    fn encoded_size(&self) -> usize {
        self.name.encoded_size()
            + self.datatype.encoded_size()
            + self.primary_key.encoded_size()
            + self.nullable.encoded_size()
            + self.default.encoded_size()
            + self.unique.encoded_size()
            + self.index.encoded_size()
            + self.references.encoded_size()
    }
}
