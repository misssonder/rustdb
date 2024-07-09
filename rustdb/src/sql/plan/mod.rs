use super::{
    parser::{self},
    types::expression,
    SqlResult,
};
use crate::sql::catalog::{Column, Table};
use crate::sql::parser::ast;
use crate::sql::parser::ddl::CreateTable;
use crate::sql::plan::node::Node;
use crate::sql::types::Value;
use ordered_float::OrderedFloat;

mod node;

pub struct Planner {}

impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn build_statement(&self, statement: ast::Statement) -> SqlResult<Node> {
        match statement {
            ast::Statement::CreateTable(CreateTable { name, columns }) => Ok(Node::CreateTable {
                schema: Table::new(
                    name,
                    columns
                        .into_iter()
                        .map(|c| {
                            let mut column = Column::new(c.name, c.datatype)
                                .with_primary(c.primary_key)
                                .with_unique(c.unique)
                                .with_index(c.index);
                            if let Some(nullable) = c.nullable {
                                column = column.with_nullable(nullable);
                            }
                            if let Some(default) = c.default {
                                column = column
                                    .with_default(self.build_expression(default)?.evaluate()?);
                            }
                            if let Some(references) = c.references {
                                column = column.with_references(references)
                            }
                            Ok(column)
                        })
                        .collect::<SqlResult<_>>()?,
                ),
            }),
            _ => unimplemented!(),
        }
    }

    pub fn build_expression(
        &self,
        expression: parser::expression::Expression,
    ) -> SqlResult<expression::Expression> {
        use super::types::expression::*;
        Ok(match expression {
            parser::expression::Expression::Literal(literal) => Expression::Const(match literal {
                parser::expression::Literal::Null => Value::Null,
                parser::expression::Literal::Boolean(boolean) => Value::Boolean(boolean),
                parser::expression::Literal::Tinyint(integer) => Value::Tinyint(integer),
                parser::expression::Literal::Smallint(integer) => Value::Smallint(integer),
                parser::expression::Literal::Integer(integer) => Value::Integer(integer),
                parser::expression::Literal::Bigint(integer) => Value::Bigint(integer),
                parser::expression::Literal::Float(float) => Value::Float(OrderedFloat(float)),
                parser::expression::Literal::Double(float) => Value::Double(OrderedFloat(float)),
                parser::expression::Literal::String(string) => Value::String(string),
            }),
            parser::expression::Expression::Field(_, _) => todo!(),
            parser::expression::Expression::Column(_) => todo!(),
            parser::expression::Expression::Operation(operation) => match operation {
                parser::expression::Operation::And(lhs, rhs) => Expression::And(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Not(expr) => {
                    Expression::Not(Box::new(self.build_expression(*expr)?))
                }
                parser::expression::Operation::Or(lhs, rhs) => Expression::Or(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Equal(lhs, rhs) => Expression::Equal(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::GreaterThan(lhs, rhs) => Expression::GreaterThan(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::GreaterThanOrEqual(lhs, rhs) => Expression::Or(
                    Box::new(Expression::Equal(
                        Box::new(self.build_expression(*lhs.clone())?),
                        Box::new(self.build_expression(*rhs.clone())?),
                    )),
                    Box::new(Expression::GreaterThan(
                        Box::new(self.build_expression(*lhs)?),
                        Box::new(self.build_expression(*rhs)?),
                    )),
                ),
                parser::expression::Operation::IsNull(expr) => {
                    Expression::IsNull(Box::new(self.build_expression(*expr)?))
                }
                parser::expression::Operation::LessThan(lhs, rhs) => Expression::LessThan(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::LessThanOrEqual(lhs, rhs) => Expression::Or(
                    Box::new(Expression::Equal(
                        Box::new(self.build_expression(*lhs.clone())?),
                        Box::new(self.build_expression(*rhs.clone())?),
                    )),
                    Box::new(Expression::LessThan(
                        Box::new(self.build_expression(*lhs)?),
                        Box::new(self.build_expression(*rhs)?),
                    )),
                ),
                parser::expression::Operation::NotEqual(lhs, rhs) => {
                    Expression::Not(Box::new(Expression::Equal(
                        Box::new(self.build_expression(*lhs)?),
                        Box::new(self.build_expression(*rhs)?),
                    )))
                }
                parser::expression::Operation::Add(lhs, rhs) => Expression::Add(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Assert(expr) => {
                    Expression::Assert(Box::new(self.build_expression(*expr)?))
                }
                parser::expression::Operation::Divide(lhs, rhs) => Expression::Divide(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Exponentiate(lhs, rhs) => Expression::Exponentiate(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Factorial(expr) => {
                    Expression::Factorial(Box::new(self.build_expression(*expr)?))
                }
                parser::expression::Operation::Modulo(lhs, rhs) => Expression::Modulo(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Multiply(lhs, rhs) => Expression::Multiply(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Negate(expr) => {
                    Expression::Negate(Box::new(self.build_expression(*expr)?))
                }
                parser::expression::Operation::Subtract(lhs, rhs) => Expression::Subtract(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Like(lhs, rhs) => Expression::Like(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
            },
        })
    }
}
