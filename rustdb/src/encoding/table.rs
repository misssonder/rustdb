use crate::encoding::encoded_size::EncodedSize;
use crate::encoding::error::Error;
use crate::encoding::{Decoder, Encoder};
use crate::sql::catalog::TableId;
use crate::sql::types::Value;
use crate::storage::page::column::Column;
use crate::storage::page::table::{Table, TableNode, Tuple};
use crate::storage::PageId;
use bytes::{Buf, BufMut};

impl Decoder for Tuple {
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        Ok(Self {
            values: Vec::<Value>::decode(buf)?,
        })
    }
}

impl Encoder for Tuple {
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut,
    {
        self.values.encode(buf)?;
        Ok(())
    }
}

impl EncodedSize for Tuple {
    fn encoded_size(&self) -> usize {
        self.values.encoded_size()
    }
}

impl Decoder for TableNode {
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        Ok(Self {
            page_id: PageId::decode(buf)?,
            next: Option::<PageId>::decode(buf)?,
            tuples: Vec::<Tuple>::decode(buf)?,
        })
    }
}
impl Encoder for TableNode {
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut,
    {
        self.page_id.encode(buf)?;
        self.next.encode(buf)?;
        self.tuples.encode(buf)?;
        Ok(())
    }
}

impl EncodedSize for TableNode {
    fn encoded_size(&self) -> usize {
        self.page_id.encoded_size() + self.page_id.encoded_size() + self.tuples.encoded_size()
    }
}

impl Decoder for Table {
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        Ok(Self {
            id: TableId::decode(buf)?,
            name: String::decode(buf)?,
            page_id: PageId::decode(buf)?,
            start: PageId::decode(buf)?,
            end: PageId::decode(buf)?,
            columns: Vec::<Column>::decode(buf)?,
        })
    }
}

impl Encoder for Table {
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut,
    {
        self.id.encode(buf)?;
        self.name.encode(buf)?;
        self.page_id.encode(buf)?;
        self.start.encode(buf)?;
        self.end.encode(buf)?;
        self.columns.encode(buf)?;
        Ok(())
    }
}

impl EncodedSize for Table {
    fn encoded_size(&self) -> usize {
        self.id.encoded_size()
            + self.name.encoded_size()
            + self.page_id.encoded_size()
            + self.start.encoded_size()
            + self.end.encoded_size()
            + self.columns.encoded_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::sql::types::{DataType, Value};
    use crate::storage::PAGE_SIZE;

    #[test]
    fn encode_decode_table() {
        let mut buffer = [0; PAGE_SIZE];
        let table = Table::new(
            0,
            "table_1",
            1,
            1,
            vec![Column::new("id", DataType::Bigint)
                .with_primary(true)
                .with_nullable(false)
                .with_default(Value::Double(2.0))
                .with_unique(true)
                .with_index(true)
                .with_references("table_2")],
        );
        table.encode(&mut buffer.as_mut()).unwrap();
        let decoded = Table::decode(&mut buffer[..table.encoded_size()].as_ref()).unwrap();
        assert_eq!(
            buffer[table.encoded_size()..],
            [0; PAGE_SIZE][table.encoded_size()..]
        );
        assert_eq!(decoded, table)
    }

    #[test]
    fn encode_decode_table_node() {
        let mut buffer = [0; PAGE_SIZE];
        let table_node = TableNode {
            page_id: 256,
            next: None,
            tuples: vec![Tuple::new(vec![
                Value::Null,
                Value::Bigint(1024),
                Value::String("Hello world".into()),
                Value::Double(0.5),
            ])],
        };
        table_node.encode(&mut buffer.as_mut()).unwrap();
        assert_eq!(
            TableNode::decode(&mut buffer[..table_node.encoded_size()].as_ref()).unwrap(),
            table_node
        )
    }
}
