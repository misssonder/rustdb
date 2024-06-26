use crate::encoding::encoded_size::EncodedSize;
use crate::encoding::error::Error;
use crate::encoding::{Decoder, Encoder};
use crate::sql::types::Value;
use crate::storage::page::column::Column;
use crate::storage::page::table::{Table, TableNode, Tuple};
use crate::storage::{PageId, TimeStamp};
use bytes::{Buf, BufMut};

impl Decoder for Tuple {
    fn decode<B>(buf: &mut B) -> Result<Self, Error>
    where
        B: Buf,
    {
        Ok(Self {
            timestamp: TimeStamp::decode(buf)?,
            deleted: bool::decode(buf)?,
            values: Vec::<Value>::decode(buf)?,
        })
    }
}

impl Encoder for Tuple {
    fn encode<B>(&self, buf: &mut B) -> Result<(), Error>
    where
        B: BufMut,
    {
        self.timestamp.encode(buf)?;
        self.deleted.encode(buf)?;
        self.values.encode(buf)?;
        Ok(())
    }
}

impl EncodedSize for Tuple {
    fn encoded_size(&self) -> usize {
        self.timestamp.encoded_size() + self.deleted.encoded_size() + self.values.encoded_size()
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
        self.name.encoded_size()
            + self.page_id.encoded_size()
            + self.start.encoded_size()
            + self.end.encoded_size()
            + self.columns.encoded_size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ordered_float::OrderedFloat;

    use crate::sql::types::DataType;
    use crate::storage::PAGE_SIZE;

    #[test]
    fn encode_decode_table() {
        let mut buffer = [0; PAGE_SIZE];
        let table = Table::new(
            "table_1",
            1,
            1,
            vec![Column::new("id", DataType::Bigint)
                .with_primary(true)
                .with_nullable(false)
                .with_default(Value::Double(2.0.into()))
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
            tuples: vec![Tuple::new(
                vec![
                    Value::Null,
                    Value::Tinyint(2),
                    Value::Smallint(4),
                    Value::Integer(6),
                    Value::Bigint(1024),
                    Value::Float(1.2.into()),
                    Value::Double(OrderedFloat(1.2)),
                    Value::String("Hello world".into()),
                ],
                0,
            )],
        };
        table_node.encode(&mut buffer.as_mut()).unwrap();
        assert_eq!(
            TableNode::decode(&mut buffer[..table_node.encoded_size()].as_ref()).unwrap(),
            table_node
        )
    }
}
