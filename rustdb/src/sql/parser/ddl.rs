use crate::sql::parser::arithmetic::{arith_expression, ArithmeticExpression};
use crate::sql::parser::keyword::Keyword;
use crate::sql::parser::{identifier, IResult};
use crate::sql::types::DataType;
use futures::StreamExt;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{multispace0, multispace1};
use nom::combinator::{map, opt};
use nom::error::context;
use nom::multi::separated_list1;
use nom::sequence::{delimited, preceded, tuple};
use nom::Parser;
use std::fmt::{Debug, Formatter};

#[derive(Clone, Debug, PartialEq)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<Column>,
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

impl std::fmt::Display for CreateTable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE TABLE {}", self.name)?;
        write!(f, "(")?;
        for column in &self.columns {
            write!(f, "{}", column)?;
        }
        write!(f, ")")
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ", self.name)?;
        write!(f, "{} ", self.datatype.as_str())?;
        if self.primary_key {
            write!(f, "PRIMARY ")?;
        }
        if self.nullable.unwrap_or_default() {
            write!(f, "NOT NULL ")?;
        }
        if let Some(ArithmeticExpression::Literal(ref default)) = self.default {
            write!(f, "DEFAULT {}", default)?;
        }
        if self.unique {
            write!(f, "UNIQUE ")?;
        }
        if self.index {
            write!(f, "INDEX ")?;
        }
        if let Some(ref references) = self.references {
            write!(f, "REFERENCE {}", references)?;
        }
        Ok(())
    }
}

pub fn create(i: &str) -> IResult<&str, CreateTable> {
    context(
        "create",
        map(
            tuple((
                preceded(multispace0, tag_no_case(Keyword::Create.to_str())),
                preceded(multispace1, tag_no_case(Keyword::Table.to_str())),
                preceded(multispace1, identifier),
                delimited(
                    space_open_paren,
                    separated_list1(space_comma, column),
                    space_close_paren,
                ),
            )),
            |(_, _, name, columns)| CreateTable {
                name: name.to_string(),
                columns,
            },
        ),
    )(i)
}

fn column(i: &str) -> IResult<&str, Column> {
    context(
        "column",
        map(
            tuple((
                preceded(multispace0, identifier),
                preceded(multispace1, datatype),
                opt(preceded(multispace1, primary_key)),
                opt(preceded(multispace1, nullable)),
                opt(preceded(multispace1, default)),
                opt(preceded(multispace1, unique)),
                opt(preceded(multispace1, index)),
                opt(preceded(multispace1, references)),
            )),
            |(name, datatype, primary, null, default, unique, index, references)| Column {
                name: name.to_string(),
                datatype,
                primary_key: primary.is_some(),
                nullable: null,
                default,
                unique: unique.is_some(),
                index: index.is_some(),
                references,
            },
        ),
    )(i)
}

fn primary_key(i: &str) -> IResult<&str, bool> {
    tag_no_case(Keyword::Primary.to_str())(i).map(|(remaining, primary)| (remaining, true))
}

fn nullable(i: &str) -> IResult<&str, bool> {
    tuple((
        tag_no_case(Keyword::Not.to_str()),
        multispace1,
        tag_no_case(Keyword::Null.to_str()),
    ))(i)
    .map(|(remaining, _)| (remaining, true))
}

fn default(i: &str) -> IResult<&str, ArithmeticExpression> {
    tuple((
        tag_no_case(Keyword::Default.to_str()),
        multispace1,
        arith_expression(0),
    ))(i)
    .map(|(remaining, expression)| (remaining, expression.2))
}

fn unique(i: &str) -> IResult<&str, bool> {
    tag_no_case(Keyword::Unique.to_str())(i).map(|(remaining, unique)| (remaining, true))
}

fn index(i: &str) -> IResult<&str, bool> {
    tag_no_case(Keyword::Index.to_str())(i).map(|(remaining, index)| (remaining, true))
}

fn references(i: &str) -> IResult<&str, String> {
    tuple((tag_no_case(Keyword::References.to_str()), identifier))(i)
        .map(|(remaining, references)| (remaining, references.1.to_string()))
}

pub fn datatype(i: &str) -> IResult<&str, DataType> {
    alt((
        map(tag_no_case(DataType::Boolean.as_str()), |_| {
            DataType::Boolean
        }),
        map(tag_no_case(DataType::Tinyint.as_str()), |_| {
            DataType::Tinyint
        }),
        map(tag_no_case(DataType::Smallint.as_str()), |_| {
            DataType::Smallint
        }),
        map(tag_no_case(DataType::Integer.as_str()), |_| {
            DataType::Integer
        }),
        map(tag_no_case(DataType::Bigint.as_str()), |_| DataType::Bigint),
        map(tag_no_case(DataType::Float.as_str()), |_| DataType::Float),
        map(tag_no_case(DataType::Double.as_str()), |_| DataType::Double),
        map(tag_no_case(DataType::String.as_str()), |_| DataType::String),
    ))(i)
}

pub(crate) fn space_comma(i: &str) -> IResult<&str, &str> {
    delimited(multispace0, tag(","), multispace0)(i)
}

pub(crate) fn space_open_paren(i: &str) -> IResult<&str, &str> {
    delimited(multispace0, tag("("), multispace0)(i)
}

pub(crate) fn space_close_paren(i: &str) -> IResult<&str, &str> {
    delimited(multispace0, tag(")"), multispace0)(i)
}

#[cfg(test)]
mod tests {
    use crate::sql::parser::arithmetic::{ArithmeticExpression, Literal};
    use crate::sql::parser::ddl::{create, Column, CreateTable};
    use crate::sql::types::DataType;

    use nom::Finish;

    #[test]
    fn column() {
        let input = " EmployeeID INTEGER PRIMARY NOT NULL DEFAULT 1 UNIQUE INDEX";
        let column = super::column(input).finish().unwrap().1;
        assert_eq!(
            column,
            Column {
                name: "EmployeeID".to_string(),
                datatype: DataType::Integer,
                primary_key: true,
                nullable: Some(true),
                default: Some(ArithmeticExpression::Literal(Literal::Integer(1))),
                unique: true,
                index: true,
                references: None,
            }
        );
    }
    #[test]
    fn create_table() {
        let sql = "CREATE TABLE Employee (EmployeeID INTEGER PRIMARY,FirstName STRING INDEX,LastName STRING,Department STRING,Salary DOUBLE NOT NULL DEFAULT 1.0);";
        let table = create(sql).unwrap().1;
        assert_eq!(
            table,
            CreateTable {
                name: "Employee".to_string(),
                columns: vec![
                    Column {
                        name: "EmployeeID".to_string(),
                        datatype: DataType::Integer,
                        primary_key: true,
                        nullable: None,
                        default: None,
                        unique: false,
                        index: false,
                        references: None,
                    },
                    Column {
                        name: "FirstName".to_string(),
                        datatype: DataType::String,
                        primary_key: false,
                        nullable: None,
                        default: None,
                        unique: false,
                        index: true,
                        references: None,
                    },
                    Column {
                        name: "LastName".to_string(),
                        datatype: DataType::String,
                        primary_key: false,
                        nullable: None,
                        default: None,
                        unique: false,
                        index: false,
                        references: None,
                    },
                    Column {
                        name: "Department".to_string(),
                        datatype: DataType::String,
                        primary_key: false,
                        nullable: None,
                        default: None,
                        unique: false,
                        index: false,
                        references: None,
                    },
                    Column {
                        name: "Salary".to_string(),
                        datatype: DataType::Double,
                        primary_key: false,
                        nullable: Some(true),
                        default: Some(ArithmeticExpression::Literal(Literal::Float(1.0))),
                        unique: false,
                        index: false,
                        references: None,
                    },
                ],
            }
        )
    }
}
