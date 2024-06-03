pub(crate) mod expression;

use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::{Formatter};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum DataType {
    Boolean,
    Tinyint,
    Smallint,
    Integer,
    Bigint,
    Float,
    Double,
    String,
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DataType::Boolean => "BOOLEAN",
            DataType::Tinyint => "TINYINT",
            DataType::Smallint => "SMALLINT",
            DataType::Integer => "INTEGER",
            DataType::Bigint => "BIGINT",
            DataType::Float => "FLOAT",
            DataType::Double => "DOUBLE",
            DataType::String => "STRING",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Tinyint(i16),
    Smallint(i32),
    Integer(i64),
    Bigint(i128),
    Float(OrderedFloat<f32>),
    Double(OrderedFloat<f64>),
    String(String),
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            match self {
                Value::Null => Cow::Borrowed("NULL"),
                Value::Boolean(b) if *b => Cow::Borrowed("TRUE"),
                Value::Boolean(_) => Cow::Borrowed("FALSE"),
                Value::Tinyint(i) => Cow::Owned(i.to_string()),
                Value::Smallint(i) => Cow::Owned(i.to_string()),
                Value::Integer(i) => Cow::Owned(i.to_string()),
                Value::Bigint(i) => Cow::Owned(i.to_string()),
                Value::Float(f) => Cow::Owned(f.0.to_string()),
                Value::Double(f) => Cow::Owned(f.0.to_string()),
                Value::String(s) => Cow::Borrowed(s.as_str()),
            }
            .as_ref(),
        )
    }
}

impl Value {
    pub fn datatype(&self) -> Option<DataType> {
        Some(match self {
            Value::Null => return None,
            Value::Boolean(_) => DataType::Boolean,
            Value::Tinyint(_) => DataType::Tinyint,
            Value::Smallint(_) => DataType::Smallint,
            Value::Integer(_) => DataType::Integer,
            Value::Bigint(_) => DataType::Bigint,
            Value::Float(_) => DataType::Float,
            Value::Double(_) => DataType::Double,
            Value::String(_) => DataType::String,
        })
    }

    pub fn check_int(&self) -> bool {
        matches!(
            self,
            Value::Tinyint(_) | Value::Smallint(_) | Value::Integer(_) | Value::Bigint(_)
        )
    }

    pub fn check_float(&self) -> bool {
        matches!(self, Value::Float(_) | Value::Double(_))
    }

    pub fn check_zero(&self) -> bool {
        matches!(
            self,
            Value::Tinyint(0) | Value::Smallint(0) | Value::Integer(0) | Value::Bigint(0)
        )
    }
}
