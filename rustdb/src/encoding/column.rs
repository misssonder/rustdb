use crate::encoding::encoded_size::EncodedSize;
use crate::encoding::error::Error;
use crate::encoding::{Decoder, Encoder};
use crate::sql::types::{DataType, Value};
use crate::storage::page::column::Column;
use bytes::{Buf, BufMut};

impl Decoder for Column {
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        Ok(Self {
            name: String::decode(buf)?,
            datatype: DataType::decode(buf)?,
            primary_key: bool::decode(buf)?,
            nullable: Option::<bool>::decode(buf)?,
            default: Option::<Value>::decode(buf)?,
            unique: bool::decode(buf)?,
            index: bool::decode(buf)?,
            references: Option::<String>::decode(buf)?,
        })
    }
}
impl Encoder for Column {
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
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

impl EncodedSize for Column {
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
