use crate::sql::parser::expression::{expression, Expression};
use crate::sql::parser::keyword::Keyword;
use crate::sql::parser::{identifier, IResult};
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::{map, opt};
use nom::error::context;
use nom::multi::separated_list1;
use nom::sequence::{delimited, preceded, separated_pair, terminated, tuple};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct Delete {
    table: String,
    r#where: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Insert {
    table: String,
    columns: Option<Vec<String>>,
    values: Vec<Vec<Expression>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Update {
    table: String,
    set: BTreeMap<String, Expression>,
    r#where: Option<Expression>,
}

pub fn insert(i: &str) -> IResult<&str, Insert> {
    context(
        "insert",
        terminated(
            map(
                tuple((
                    preceded(
                        tuple((
                            preceded(multispace0, tag_no_case(Keyword::Insert.to_str())),
                            preceded(multispace1, tag_no_case(Keyword::Into.to_str())),
                        )),
                        preceded(multispace1, identifier),
                    ),
                    opt(columns),
                    values,
                )),
                |(name, columns, values)| Insert {
                    table: name.to_string(),
                    columns,
                    values,
                },
            ),
            preceded(multispace0, tag(";")),
        ),
    )(i)
}

pub fn delete(i: &str) -> IResult<&str, Delete> {
    context(
        "delete",
        terminated(
            map(
                tuple((
                    preceded(
                        tuple((
                            preceded(multispace0, tag_no_case(Keyword::Delete.to_str())),
                            preceded(multispace1, tag_no_case(Keyword::From.to_str())),
                        )),
                        preceded(multispace1, identifier),
                    ),
                    opt(r#where),
                )),
                |(name, r#where)| Delete {
                    table: name.to_string(),
                    r#where,
                },
            ),
            preceded(multispace0, tag(";")),
        ),
    )(i)
}

pub fn update(i: &str) -> IResult<&str, Update> {
    context(
        "update",
        terminated(
            map(
                tuple((
                    preceded(
                        tuple((multispace0, tag_no_case(Keyword::Update.to_str()))),
                        preceded(multispace1, identifier),
                    ),
                    set,
                    opt(r#where),
                )),
                |(name, set, r#where)| Update {
                    table: name.to_string(),
                    set,
                    r#where,
                },
            ),
            preceded(multispace0, tag(";")),
        ),
    )(i)
}

/// Parse 'WHERE some_expression'
fn r#where(i: &str) -> IResult<&str, Expression> {
    context(
        "where",
        preceded(
            tuple((multispace0, tag_no_case(Keyword::Where.to_str()))),
            expression(0),
        ),
    )(i)
}

/// Parse the set clause of update
fn set(i: &str) -> IResult<&str, BTreeMap<String, Expression>> {
    context(
        "set",
        map(
            preceded(
                tuple((multispace0, tag_no_case(Keyword::Set.to_str()))),
                separated_list1(tag(","), key_value),
            ),
            |values| values.into_iter().collect(),
        ),
    )(i)
}

/// Parse `key = value`
fn key_value(i: &str) -> IResult<&str, (String, Expression)> {
    context(
        "key value",
        map(
            separated_pair(
                delimited(multispace0, identifier, multispace0),
                tag("="),
                delimited(multispace0, expression(0), multispace0),
            ),
            |(key, value)| (key.to_string(), value),
        ),
    )(i)
}

/// Parse `Values (Value1, Value2,Value3,…..), (Value1, Value2,Value3,…..), (Value1, Value2,Value3,…..),`
fn values(i: &str) -> IResult<&str, Vec<Vec<Expression>>> {
    context(
        "values",
        preceded(
            tuple((multispace0, tag_no_case(Keyword::Values.to_str()))),
            separated_list1(delimited(multispace0, tag(","), multispace0), value),
        ),
    )(i)
}

/// Parse `(Value1, Value2,Value3,…..)`
fn value(i: &str) -> IResult<&str, Vec<Expression>> {
    context(
        "value",
        delimited(
            delimited(multispace0, tag("("), multispace0),
            separated_list1(delimited(multispace0, tag(","), multispace0), expression(0)),
            delimited(multispace0, tag(")"), multispace0),
        ),
    )(i)
}

fn columns(i: &str) -> IResult<&str, Vec<String>> {
    context(
        "insert columns",
        map(
            delimited(
                delimited(multispace0, tag("("), multispace0),
                separated_list1(delimited(multispace0, tag(","), multispace0), identifier),
                delimited(multispace0, tag(")"), multispace0),
            ),
            |column| {
                column
                    .into_iter()
                    .map(|column| column.to_string())
                    .collect()
            },
        ),
    )(i)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::expression::Literal::Float;
    use crate::sql::parser::expression::{Literal, Operation};
    use std::vec;

    #[test]
    fn insert() {
        let sql = "INSERT INTO user (id, name, grade) values( 1, 'John',3.0),(2, 'Mike',3.8) ;";
        assert_eq!(
            super::insert(sql).unwrap().1,
            Insert {
                table: "user".to_string(),
                columns: Some(vec![
                    "id".to_string(),
                    "name".to_string(),
                    "grade".to_string()
                ]),
                values: vec![
                    vec![
                        Expression::Literal(Literal::Integer(1)),
                        Expression::Literal(Literal::String("John".to_string())),
                        Expression::Literal(Float(3.0))
                    ],
                    vec![
                        Expression::Literal(Literal::Integer(2)),
                        Expression::Literal(Literal::String("Mike".to_string())),
                        Expression::Literal(Float(3.8))
                    ]
                ],
            }
        );
        let sql = "INSERT INTO user values( 1, 'John',3.0),(2, 'Mike',3.8) ;";
        assert!(super::insert(sql).unwrap().1.columns.is_none());
    }

    #[test]
    fn delete() {
        let sql = "DELETE FROM user where id = 1 ;";
        assert_eq!(
            super::delete(sql).unwrap().1,
            Delete {
                table: "user".to_string(),
                r#where: Some(Expression::Operation(Operation::Equal(
                    Box::new(Expression::Field(None, "id".to_string())),
                    Box::new(Expression::Literal(Literal::Integer(1)))
                ))),
            }
        )
    }

    #[test]
    fn update() {
        let sql = "UPDATE user set grade = grade + 1.0, name = 'John' where id = 2 ;";
        assert_eq!(
            super::update(sql).unwrap().1,
            Update {
                table: "user".to_string(),
                set: BTreeMap::from([
                    (
                        "grade".to_string(),
                        Expression::Operation(Operation::Add(
                            Box::new(Expression::Field(None, "grade".to_string())),
                            Box::new(Expression::Literal(Literal::Float(1.0))),
                        ))
                    ),
                    (
                        "name".to_string(),
                        Expression::Literal(Literal::String("John".to_string()))
                    ),
                ]),
                r#where: Some(Expression::Operation(Operation::Equal(
                    Box::new(Expression::Field(None, "id".to_string())),
                    Box::new(Expression::Literal(Literal::Integer(2)))
                ))),
            }
        );
        let sql = "UPDATE user set grade = grade + 1.0, name = 'John' ;";
        assert!(super::update(sql).unwrap().1.r#where.is_none());
    }
}
