use crate::encoding::encoded_size::EncodedSize;
use crate::encoding::{Decoder, Encoder};
use crate::error::RustDBError;
use crate::sql::types::Value;
use crate::storage::page::column::ColumnDesc;
use crate::storage::page::table::{Table, TableNode, Tuple};
use crate::storage::{PageId, RecordId};
use bytes::{Buf, BufMut};

impl Decoder for Tuple {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        Ok(Self {
            record_id: RecordId::decode(buf)?,
            values: Vec::<Value>::decode(buf)?,
        })
    }
}

impl Encoder for Tuple {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        self.record_id.encode(buf)?;
        self.values.encode(buf)?;
        Ok(())
    }
}

impl EncodedSize for Tuple {
    fn encoded_size(&self) -> usize {
        self.record_id.encoded_size() + self.values.encoded_size()
    }
}

impl Decoder for TableNode {
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
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
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
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
    type Error = RustDBError;

    fn decode<B>(buf: &mut B) -> Result<Self, Self::Error>
    where
        B: Buf,
    {
        let page_id = PageId::decode(buf)?;
        let start = PageId::decode(buf)?;
        let end = PageId::decode(buf)?;
        Ok(Self {
            page_id,
            start,
            end,
            columns: Vec::<ColumnDesc>::decode(buf)?,
        })
    }
}

impl Encoder for Table {
    type Error = RustDBError;

    fn encode<B>(&self, buf: &mut B) -> Result<(), Self::Error>
    where
        B: BufMut,
    {
        self.page_id.encode(buf)?;
        self.start.encode(buf)?;
        self.end.encode(buf)?;
        self.columns.encode(buf)?;
        Ok(())
    }
}

impl EncodedSize for Table {
    fn encoded_size(&self) -> usize {
        self.page_id.encoded_size()
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
        let table = Table {
            page_id: 1,
            start: 0,
            end: 1024,
            columns: vec![ColumnDesc {
                name: "id".to_string(),
                datatype: DataType::Bigint,
                primary_key: true,
                nullable: Some(false),
                default: Some(Value::Double(2.0)),
                unique: true,
                index: true,
                references: Some("table_2".into()),
            }],
        };
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
            tuples: vec![Tuple {
                record_id: RecordId {
                    page_id: 512,
                    slot_num: 36,
                },
                values: vec![
                    Value::Null,
                    Value::Bigint(1024),
                    Value::String("Hello world".into()),
                    Value::Double(0.5),
                ],
            }],
        };
        table_node.encode(&mut buffer.as_mut()).unwrap();
        assert_eq!(
            TableNode::decode(&mut buffer[..table_node.encoded_size()].as_ref()).unwrap(),
            table_node
        )
    }
}
