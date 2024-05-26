use crate::encoding::{Decoder, Encoder};
use crate::error::RustDBError;
use crate::storage::page::column::Column;
use crate::storage::page::table::{Table, TableHeader, TupleReference};
use crate::storage::{PageId, NULL_PAGE};
use bytes::{Buf, BufMut};

impl Encoder for TupleReference {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        self.offset.encode(buf)?;
        self.size.encode(buf)
    }
}

impl Decoder for TupleReference {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        Ok(Self {
            offset: u16::decode(buf)?,
            size: u16::decode(buf)?,
        })
    }
}

impl Decoder for TableHeader {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        Ok(Self {
            start: PageId::decode(buf)?,
            end: PageId::decode(buf)?,
            next_table: {
                let next_page = PageId::decode(buf)?;
                match next_page {
                    NULL_PAGE => None,
                    next_page => Some(next_page),
                }
            },
            tuple_references: {
                let len = u32::decode(buf)?;
                let mut tuple_references = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    tuple_references.push(TupleReference::decode(buf)?);
                }
                tuple_references
            },
        })
    }
}
impl Encoder for TableHeader {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        self.start.encode(buf)?;
        self.end.encode(buf)?;
        match self.next_table {
            None => NULL_PAGE.encode(buf)?,
            Some(next_page) => next_page.encode(buf)?,
        };
        (self.tuple_references.len() as u32).encode(buf)?;
        for tuple_reference in &self.tuple_references {
            tuple_reference.encode(buf)?;
        }
        Ok(())
    }
}

impl Decoder for Table {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        Ok(Self {
            header: TableHeader::decode(buf)?,
            name: String::decode(buf)?,
            columns: {
                let len = u8::decode(buf)?;
                let mut columns = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    columns.push(Column::decode(buf)?)
                }
                columns
            },
        })
    }
}

impl Encoder for Table {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        self.header.encode(buf)?;
        self.name.encode(buf)?;
        (self.columns.len() as u8).encode(buf)?;
        for column in &self.columns {
            column.encode(buf)?;
        }
        Ok(())
    }
}
