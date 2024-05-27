use crate::encoding::{Decoder, Encoder};
use crate::error::RustDBError;
use crate::sql::types::Value;
use crate::storage::page::column::Column;
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
            values: {
                let len = u16::decode(buf)?;
                let mut values = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    values.push(Value::decode(buf)?)
                }
                values
            },
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
        (self.values.len() as u16).encode(buf)?;
        for value in &self.values {
            value.encode(buf)?;
        }
        Ok(())
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
            tuples: {
                let len = u32::decode(buf)?;
                let mut tuples = Vec::with_capacity(len as usize);
                for _ in 0..len {
                    tuples.push(Tuple::decode(buf)?);
                }
                tuples
            },
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
        (self.tuples.len() as u32).encode(buf)?;
        for tuple in &self.tuples {
            tuple.encode(buf)?;
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
            start: PageId::decode(buf)?,
            end: PageId::decode(buf)?,
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
        self.start.encode(buf)?;
        self.end.encode(buf)?;
        (self.columns.len() as u8).encode(buf)?;
        for column in &self.columns {
            column.encode(buf)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::types::expression::Expression;
    use crate::sql::types::{DataType, Value};
    use crate::storage::PAGE_SIZE;

    #[test]
    fn encode_decode_table() {
        let mut buffer = [0; PAGE_SIZE];
        let table = Table {
            start: 0,
            end: 1024,
            columns: vec![Column {
                name: "id".to_string(),
                datatype: DataType::Bigint,
                primary_key: true,
                nullable: Some(false),
                default: Some(Expression::Add(
                    Box::new(Expression::Const(Value::Integer(1))),
                    Box::new(Expression::Const(Value::Integer(1))),
                )),
                unique: true,
                index: true,
                references: Some("table_2".into()),
            }],
        };
        table.encode(&mut buffer.as_mut()).unwrap();
        let decoded = Table::decode(&mut buffer.as_ref()).unwrap();
        assert_eq!(
            decoded,
            Table {
                start: 0,
                end: 1024,
                columns: vec![Column {
                    name: "id".to_string(),
                    datatype: DataType::Bigint,
                    primary_key: true,
                    nullable: Some(false),
                    default: Some(Expression::Const(Value::Integer(2))),
                    unique: true,
                    index: true,
                    references: Some("table_2".into()),
                }],
            }
        )
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
        assert_eq!(TableNode::decode(&mut buffer.as_ref()).unwrap(), table_node)
    }
}
