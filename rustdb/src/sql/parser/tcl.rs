use crate::sql::parser::keyword::Keyword;
use crate::sql::parser::{ast, identifier, IResult};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::{map, opt};
use nom::error::context;
use nom::sequence::{delimited, preceded, tuple};

#[derive(Debug, Clone, PartialEq)]
pub struct Begin {
    pub name: Option<String>,
    pub read_only: bool,
}
pub fn transaction(i: &str) -> IResult<&str, ast::Statement> {
    context(
        "transaction",
        delimited(
            multispace0,
            alt((
                map(begin, ast::Statement::Begin),
                map(tag_no_case(Keyword::Commit.to_str()), |_| {
                    ast::Statement::Commit
                }),
                map(tag_no_case(Keyword::Rollback.to_str()), |_| {
                    ast::Statement::Rollback
                }),
            )),
            preceded(multispace0, tag(";")),
        ),
    )(i)
}
fn begin(i: &str) -> IResult<&str, Begin> {
    context(
        "begin",
        map(
            tuple((
                preceded(multispace0, tag_no_case(Keyword::Begin.to_str())),
                preceded(multispace1, tag_no_case(Keyword::Transaction.to_str())),
                opt(preceded(multispace1, identifier)),
                readonly,
            )),
            |(_, _, name, readonly)| Begin {
                name: name.map(|name| name.to_string()),
                read_only: readonly,
            },
        ),
    )(i)
}

fn readonly(i: &str) -> IResult<&str, bool> {
    context(
        "readonly",
        map(
            opt(tuple((
                preceded(multispace0, tag_no_case(Keyword::Read.to_str())),
                preceded(
                    multispace0,
                    alt((
                        map(tag_no_case(Keyword::Only.to_str()), |_| true),
                        map(tag_no_case(Keyword::Write.to_str()), |_| false),
                    )),
                ),
            ))),
            |readonly| readonly.map(|(_, readonly)| readonly).unwrap_or_default(),
        ),
    )(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::multi::many1;

    #[test]
    fn begin() {
        let transaction = "BEGIN Transaction test read only";
        assert_eq!(
            super::begin(transaction).unwrap().1,
            Begin {
                name: Some("test".to_string()),
                read_only: true,
            }
        );

        let transaction = "BEGIN Transaction read write";
        assert_eq!(
            super::begin(transaction).unwrap().1,
            Begin {
                name: None,
                read_only: false,
            }
        );

        let transaction = "BEGIN Transaction";
        assert_eq!(
            super::begin(transaction).unwrap().1,
            Begin {
                name: None,
                read_only: false,
            }
        );
    }

    #[test]
    fn transaction() {
        let sql = "BEGIN Transaction;Commit;ROLLBACK;";
        let transactions = many1(super::transaction)(sql);
        assert_eq!(
            transactions.unwrap().1,
            vec![
                ast::Statement::Begin(Begin {
                    name: None,
                    read_only: false,
                }),
                ast::Statement::Commit,
                ast::Statement::Rollback
            ]
        )
    }
}
