use crate::sql::parser::ddl::{CreateTable, DropTable};
use crate::sql::parser::dml::{Delete, Insert, Update};
use crate::sql::parser::dql::Select;
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

    Select(Select),
}
