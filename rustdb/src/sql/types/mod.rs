pub(crate) mod expression;

use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::{Formatter, Write};

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    Boolean(bool),
    Tinyint(i16),
    Smallint(i32),
    Integer(i64),
    Bigint(i128),
    Float(f32),
    Double(f64),
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
                Value::Float(f) => Cow::Owned(f.to_string()),
                Value::Double(f) => Cow::Owned(f.to_string()),
                Value::String(s) => Cow::Borrowed(s.as_str()),
            }
            .as_ref(),
        )
    }
}

impl Value {
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
