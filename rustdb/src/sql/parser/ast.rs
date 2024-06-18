use crate::sql::parser::ddl::{CreateTable, DropTable};
use crate::sql::parser::dml::{Delete, Insert, Update};
use crate::sql::parser::expression::Expression;
use crate::sql::parser::tcl::Begin;

#[derive(Clone, Debug, PartialEq)]
pub enum Statement {
    Begin(Begin),
    Commit,
    Rollback,
    Explain(Box<Statement>),

    CreateTable(CreateTable),
    DropTable(DropTable),

    Delete(Delete),
    Insert(Insert),
    Update(Update),

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
