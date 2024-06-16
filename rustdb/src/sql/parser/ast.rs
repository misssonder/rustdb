use crate::sql::parser::arithmetic::ArithmeticExpression;
use crate::sql::parser::ddl::CreateTable;
use crate::sql::types::DataType;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    Begin {
        read_only: bool,
        as_of: Option<u64>,
    },
    Commit,
    Rollback,
    Explain(Box<Statement>),

    CreateTable(CreateTable),
    DropTable {
        name: String,
        if_exists: bool,
    },

    Delete {
        table: String,
        r#where: Option<ArithmeticExpression>,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<ArithmeticExpression>>,
    },
    Update {
        table: String,
        set: BTreeMap<String, ArithmeticExpression>,
        r#where: Option<ArithmeticExpression>,
    },

    Select {
        select: Vec<(ArithmeticExpression, Option<String>)>,
        from: Vec<FromItem>,
        r#where: Option<ArithmeticExpression>,
        group_by: Vec<ArithmeticExpression>,
        having: Option<ArithmeticExpression>,
        order: Vec<(ArithmeticExpression, Order)>,
        offset: Option<ArithmeticExpression>,
        limit: Option<ArithmeticExpression>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    pub name: String,
    pub datatype: DataType,
    pub primary_key: bool,
    pub nullable: Option<bool>,
    pub default: Option<ArithmeticExpression>,
    pub unique: bool,
    pub index: bool,
    pub references: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum FromItem {
    Table {
        name: String,
        alias: Option<String>,
    },
    Join {
        left: Box<FromItem>,
        right: Box<FromItem>,
        r#type: JoinType,
        predicate: Option<ArithmeticExpression>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Order {
    Ascending,
    Descending,
}
