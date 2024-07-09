use crate::sql::catalog::Table;

#[derive(Debug)]
pub enum Node {
    CreateTable { schema: Table },
    DropTable { table: String, if_exists: bool },
}
