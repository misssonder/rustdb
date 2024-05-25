use crate::sql::types::expression::Expression;
use crate::sql::types::DataType;

#[derive(Debug)]
pub struct Table {
    name: String,
    columns: Vec<Column>,
}

impl Table {
    pub fn new<N: Into<String>>(name: N, columns: Vec<Column>) -> Self {
        Self {
            name: name.into(),
            columns,
        }
    }
}

#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: Option<bool>,
    pub default: Option<Expression>,
    pub unique: bool,
    pub index: bool,
    pub references: Option<String>,
}
