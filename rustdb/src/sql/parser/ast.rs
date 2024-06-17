use crate::sql::parser::ddl::CreateTable;
use crate::sql::parser::expression::Expression;
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
        r#where: Option<Expression>,
    },
    Insert {
        table: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expression>>,
    },
    Update {
        table: String,
        set: BTreeMap<String, Expression>,
        r#where: Option<Expression>,
    },

    Select {
        select: Vec<(Expression, Option<String>)>,
        from: Vec<FromItem>,
        r#where: Option<Expression>,
        group_by: Vec<Expression>,
        having: Option<Expression>,
        order: Vec<(Expression, Order)>,
        offset: Option<Expression>,
        limit: Option<Expression>,
    },
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
        predicate: Option<Expression>,
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
