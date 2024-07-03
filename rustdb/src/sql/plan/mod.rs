use ordered_float::OrderedFloat;

use crate::sql::types::Value;

use super::{
    parser::{self},
    types::expression,
    SqlResult,
};

mod node;

pub struct Planner {}

impl Planner {
    pub fn new() -> Self {
        Self {}
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
                parser::expression::Literal::Integer(integer) => Value::Integer(integer),
                parser::expression::Literal::Float(float) => Value::Double(OrderedFloat(float)),
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
                parser::expression::Operation::Equal(_, _) => todo!(),
                parser::expression::Operation::GreaterThan(_, _) => todo!(),
                parser::expression::Operation::GreaterThanOrEqual(_, _) => todo!(),
                parser::expression::Operation::IsNull(_) => todo!(),
                parser::expression::Operation::LessThan(_, _) => todo!(),
                parser::expression::Operation::LessThanOrEqual(_, _) => todo!(),
                parser::expression::Operation::NotEqual(_, _) => todo!(),
                parser::expression::Operation::Add(lhs, rhs) => Expression::Add(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Assert(_) => todo!(),
                parser::expression::Operation::Divide(lhs, rhs) => Expression::Divide(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Exponentiate(lhs, rhs) => Expression::Exponentiate(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Factorial(_) => todo!(),
                parser::expression::Operation::Modulo(_, _) => todo!(),
                parser::expression::Operation::Multiply(lhs, rhs) => Expression::Multiply(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Negate(_) => todo!(),
                parser::expression::Operation::Subtract(lhs, rhs) => Expression::Subtract(
                    Box::new(self.build_expression(*lhs)?),
                    Box::new(self.build_expression(*rhs)?),
                ),
                parser::expression::Operation::Like(_, _) => todo!(),
            },
        })
    }
}
