use crate::sql::types::{DataType, Value};
use crate::sql::SqlResult;
use std::future::Future;

pub trait Catalog {
    fn create_table(&self, table: Table) -> impl Future<Output = SqlResult<()>>;

    fn drop_table(&self, name: &str) -> impl Future<Output = SqlResult<Option<Table>>>;

    fn read_table(&self, name: &str) -> impl Future<Output = SqlResult<Option<Table>>>;
}

/// Logical table.
/// If want to check the physical table in page, check [`crate::storage::page::table::Table`]
#[derive(Clone, Debug, PartialEq)]
pub struct Table {
    /// Table name
    name: String,
    /// Columns
    columns: Vec<Column>,
}

/// Logical table.
/// If you want to check physical column in page, check [`crate::storage::page::column::Column`]
#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    /// Column name
    pub name: String,
    /// Column datatype
    pub datatype: DataType,
    /// Whether the column is a primary key
    pub primary_key: bool,
    /// Whether the column allows null values
    pub nullable: bool,
    /// The default value of the column
    pub default: Option<Value>,
    /// Whether the column should only take unique values
    pub unique: bool,
    /// The table which is referenced by this foreign key
    pub references: Option<String>,
    /// Whether the column should be indexed
    pub index: bool,
}
