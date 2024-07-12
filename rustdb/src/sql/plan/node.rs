use crate::sql::catalog::Table;
use crate::sql::types::expression::Expression;

#[derive(Debug)]
pub enum Node {
    CreateTable {
        schema: Table,
    },
    DropTable {
        table: String,
        if_exists: bool,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<Expression>>,
    },
}
