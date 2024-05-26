use crate::encoding::{Decoder, Encoder};
use crate::error::RustDBError;
use crate::sql::types::expression::Expression;
use crate::sql::types::DataType;
use crate::storage::page::column::Column;
use bytes::{Buf, BufMut};

impl Decoder for Column {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        Ok(Self {
            name: String::decode(buf)?,
            datatype: DataType::decode(buf)?,
            primary_key: bool::decode(buf)?,
            nullable: Option::<bool>::decode(buf)?,
            default: Option::<Expression>::decode(buf)?,
            unique: bool::decode(buf)?,
            index: bool::decode(buf)?,
            references: Option::<String>::decode(buf)?,
        })
    }
}
impl Encoder for Column {
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
