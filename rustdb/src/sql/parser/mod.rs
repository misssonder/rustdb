use crate::sql::parser::keyword::keyword;
use futures::StreamExt;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while1};
use nom::character::complete::alpha1;
use nom::combinator::{map, not, peek};
use nom::error::{context, convert_error, VerboseError};
use nom::sequence::{delimited, preceded, tuple};
use nom::Finish;
use thiserror::Error;

mod ast;
mod ddl;
mod dml;
mod dql;
mod expression;
mod keyword;
mod tcl;

type IResult<I, O> = nom::IResult<I, O, VerboseError<I>>;

#[derive(Error, Debug)]
pub enum Error {
    #[error("parse sql error: {0}")]
    Parse(String),
}

pub fn parse(sql: &str) -> Result<ast::Statement, Error> {
    match statement(sql).finish() {
        Ok((_, statement)) => Ok(statement),
        Err(err) => Err(Error::Parse(convert_error(sql, err))),
    }
}
fn statement(i: &str) -> IResult<&str, ast::Statement> {
    context(
        "parse sql statement",
        alt((
            tcl::transaction,
            map(ddl::create, |create_table| {
                ast::Statement::CreateTable(create_table)
            }),
            map(ddl::drop_table, |drop_table| {
                ast::Statement::DropTable(drop_table)
            }),
            map(dml::delete, ast::Statement::Delete),
            map(dml::insert, ast::Statement::Insert),
            map(dml::update, ast::Statement::Update),
            map(dql::select, ast::Statement::Select),
        )),
    )(i)
}

pub fn identifier(i: &str) -> IResult<&str, &str> {
    context(
        "identifier",
        alt((
            preceded(
                not(keyword),
                tuple((peek(alpha1), take_while1(is_identifier))),
            ),
            delimited(
                tag("`"),
                tuple((peek(alpha1), take_while1(is_identifier))),
                tag("`"),
            ),
            delimited(
                tag("["),
                tuple((peek(alpha1), take_while1(is_identifier))),
                tag("]"),
            ),
        )),
    )(i)
    .map(|(remaining, ident)| (remaining, ident.1))
}

fn is_identifier(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '@'
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::ast;

    #[test]
    fn parse() {
        let input = "Insert into user(id, name) values(1,'Mike');";
        assert!(matches!(
            super::parse(input).unwrap(),
            ast::Statement::Insert(_)
        ))
    }
}
