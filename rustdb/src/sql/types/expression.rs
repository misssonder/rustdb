use crate::error::{RustDBError, RustDBResult};
use crate::sql::types::Value;
use crate::sql::{Error, SqlResult};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Const(Value),

    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),

    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    IsNull(Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),

    Add(Box<Expression>, Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),

    Like(Box<Expression>, Box<Expression>),
}

impl Expression {
    // TODO cast integer
    pub fn evaluate(&self) -> SqlResult<Value> {
        match self {
            Expression::Const(value) => Ok(value.clone()),
            Expression::And(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Boolean(lhs), Value::Boolean(rhs)) => Value::Boolean(lhs && rhs),
                (Value::Null, Value::Null) => Value::Null,
                (lhs, rhs) => {
                    return Err(Error::ValuesNotMatch(
                        "and",
                        lhs.to_string(),
                        rhs.to_string(),
                    ))
                }
            }),
            Expression::Or(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Boolean(lhs), Value::Boolean(rhs)) => Value::Boolean(lhs || rhs),
                (Value::Null, Value::Null) => Value::Null,
                (lhs, rhs) => {
                    return Err(Error::ValuesNotMatch(
                        "or",
                        lhs.to_string(),
                        rhs.to_string(),
                    ))
                }
            }),
            Expression::Not(expr) => Ok(match expr.evaluate()? {
                Value::Null => Value::Null,
                Value::Boolean(expr) => Value::Boolean(!expr),
                expr => return Err(Error::ValueNotMatch("not", expr.to_string())),
            }),
            Expression::Equal(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Boolean(lhs), Value::Boolean(rhs)) => Value::Boolean(lhs == rhs),
                (Value::Tinyint(lhs), Value::Tinyint(rhs)) => Value::Boolean(lhs == rhs),
                (Value::Tinyint(lhs), Value::Smallint(rhs)) => Value::Boolean((lhs as i32) == rhs),
                (Value::Tinyint(lhs), Value::Integer(rhs)) => Value::Boolean(lhs as i64 == rhs),
                (Value::Tinyint(lhs), Value::Bigint(rhs)) => Value::Boolean(lhs as i128 == rhs),
                (Value::Smallint(lhs), Value::Tinyint(rhs)) => Value::Boolean(lhs == rhs as i32),
                (Value::Smallint(lhs), Value::Smallint(rhs)) => Value::Boolean(lhs == rhs),
                (Value::Smallint(lhs), Value::Integer(rhs)) => Value::Boolean(lhs as i64 == rhs),
                (Value::Smallint(lhs), Value::Bigint(rhs)) => Value::Boolean(lhs as i128 == rhs),
                (Value::Integer(lhs), Value::Tinyint(rhs)) => Value::Boolean(lhs == rhs as i64),
                (Value::Integer(lhs), Value::Smallint(rhs)) => Value::Boolean(lhs == rhs as i64),
                (Value::Integer(lhs), Value::Integer(rhs)) => Value::Boolean(lhs == rhs),
                (Value::Integer(lhs), Value::Bigint(rhs)) => Value::Boolean(lhs as i128 == rhs),
                (Value::Bigint(lhs), Value::Tinyint(rhs)) => Value::Boolean(lhs == rhs as i128),
                (Value::Bigint(lhs), Value::Smallint(rhs)) => Value::Boolean(lhs == rhs as i128),
                (Value::Bigint(lhs), Value::Integer(rhs)) => Value::Boolean(lhs == rhs as i128),
                (Value::Bigint(lhs), Value::Bigint(rhs)) => Value::Boolean(lhs == rhs),
                (Value::Float(lhs), Value::Float(rhs)) => Value::Boolean(lhs == rhs),
                (Value::Float(OrderedFloat(lhs)), Value::Double(rhs)) => {
                    Value::Boolean(OrderedFloat(lhs as f64) == rhs)
                }
                (Value::Double(lhs), Value::Float(OrderedFloat(rhs))) => {
                    Value::Boolean(lhs == OrderedFloat(rhs as f64))
                }
                (Value::Double(lhs), Value::Double(rhs)) => Value::Boolean(lhs == rhs),
                (Value::String(lhs), Value::String(rhs)) => Value::Boolean(lhs == rhs),
                (lhs, rhs) => {
                    return Err(Error::ValuesNotMatch(
                        "equal",
                        lhs.to_string(),
                        rhs.to_string(),
                    ))
                }
            }),
            Expression::GreaterThan(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Boolean(lhs), Value::Boolean(rhs)) => Value::Boolean(lhs & !rhs),
                (Value::Tinyint(lhs), Value::Tinyint(rhs)) => Value::Boolean(lhs > rhs),
                (Value::Tinyint(lhs), Value::Smallint(rhs)) => Value::Boolean((lhs as i32) > rhs),
                (Value::Tinyint(lhs), Value::Integer(rhs)) => Value::Boolean(lhs as i64 > rhs),
                (Value::Tinyint(lhs), Value::Bigint(rhs)) => Value::Boolean(lhs as i128 > rhs),
                (Value::Smallint(lhs), Value::Tinyint(rhs)) => Value::Boolean(lhs > rhs as i32),
                (Value::Smallint(lhs), Value::Smallint(rhs)) => Value::Boolean(lhs > rhs),
                (Value::Smallint(lhs), Value::Integer(rhs)) => Value::Boolean(lhs as i64 > rhs),
                (Value::Smallint(lhs), Value::Bigint(rhs)) => Value::Boolean(lhs as i128 > rhs),
                (Value::Integer(lhs), Value::Tinyint(rhs)) => Value::Boolean(lhs > rhs as i64),
                (Value::Integer(lhs), Value::Smallint(rhs)) => Value::Boolean(lhs > rhs as i64),
                (Value::Integer(lhs), Value::Integer(rhs)) => Value::Boolean(lhs > rhs),
                (Value::Integer(lhs), Value::Bigint(rhs)) => Value::Boolean(lhs as i128 > rhs),
                (Value::Bigint(lhs), Value::Tinyint(rhs)) => Value::Boolean(lhs > rhs as i128),
                (Value::Bigint(lhs), Value::Smallint(rhs)) => Value::Boolean(lhs > rhs as i128),
                (Value::Bigint(lhs), Value::Integer(rhs)) => Value::Boolean(lhs > rhs as i128),
                (Value::Bigint(lhs), Value::Bigint(rhs)) => Value::Boolean(lhs > rhs),
                (Value::Float(lhs), Value::Float(rhs)) => Value::Boolean(lhs > rhs),
                (Value::Float(OrderedFloat(lhs)), Value::Double(rhs)) => {
                    Value::Boolean(OrderedFloat(lhs as f64) > rhs)
                }
                (Value::Double(lhs), Value::Float(OrderedFloat(rhs))) => {
                    Value::Boolean(lhs > OrderedFloat(rhs as f64))
                }
                (Value::Double(lhs), Value::Double(rhs)) => Value::Boolean(lhs > rhs),
                (Value::String(lhs), Value::String(rhs)) => Value::Boolean(lhs > rhs),
                (lhs, rhs) => {
                    return Err(Error::ValuesNotMatch(
                        "great than",
                        lhs.to_string(),
                        rhs.to_string(),
                    ))
                }
            }),
            Expression::IsNull(expr) => Ok(match expr.evaluate()? {
                Value::Null => Value::Boolean(true),
                _ => Value::Boolean(false),
            }),
            Expression::LessThan(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Boolean(lhs), Value::Boolean(rhs)) => Value::Boolean(!lhs & rhs),
                (Value::Tinyint(lhs), Value::Tinyint(rhs)) => Value::Boolean(lhs < rhs),
                (Value::Tinyint(lhs), Value::Smallint(rhs)) => Value::Boolean((lhs as i32) < rhs),
                (Value::Tinyint(lhs), Value::Integer(rhs)) => Value::Boolean((lhs as i64) < rhs),
                (Value::Tinyint(lhs), Value::Bigint(rhs)) => Value::Boolean((lhs as i128) < rhs),
                (Value::Smallint(lhs), Value::Tinyint(rhs)) => Value::Boolean(lhs < rhs as i32),
                (Value::Smallint(lhs), Value::Smallint(rhs)) => Value::Boolean(lhs < rhs),
                (Value::Smallint(lhs), Value::Integer(rhs)) => Value::Boolean((lhs as i64) < rhs),
                (Value::Smallint(lhs), Value::Bigint(rhs)) => Value::Boolean((lhs as i128) < rhs),
                (Value::Integer(lhs), Value::Tinyint(rhs)) => Value::Boolean(lhs < rhs as i64),
                (Value::Integer(lhs), Value::Smallint(rhs)) => Value::Boolean(lhs < rhs as i64),
                (Value::Integer(lhs), Value::Integer(rhs)) => Value::Boolean(lhs < rhs),
                (Value::Integer(lhs), Value::Bigint(rhs)) => Value::Boolean((lhs as i128) < rhs),
                (Value::Bigint(lhs), Value::Tinyint(rhs)) => Value::Boolean(lhs < rhs as i128),
                (Value::Bigint(lhs), Value::Smallint(rhs)) => Value::Boolean(lhs < rhs as i128),
                (Value::Bigint(lhs), Value::Integer(rhs)) => Value::Boolean(lhs < rhs as i128),
                (Value::Bigint(lhs), Value::Bigint(rhs)) => Value::Boolean(lhs < rhs),
                (Value::Float(lhs), Value::Float(rhs)) => Value::Boolean(lhs < rhs),
                (Value::Float(OrderedFloat(lhs)), Value::Double(rhs)) => {
                    Value::Boolean(OrderedFloat(lhs as f64) < rhs)
                }
                (Value::Double(lhs), Value::Float(OrderedFloat(rhs))) => {
                    Value::Boolean(lhs < OrderedFloat(rhs as f64))
                }
                (Value::Double(lhs), Value::Double(rhs)) => Value::Boolean(lhs < rhs),
                (Value::String(lhs), Value::String(rhs)) => Value::Boolean(lhs < rhs),
                (lhs, rhs) => {
                    return Err(Error::ValuesNotMatch(
                        "less than",
                        lhs.to_string(),
                        rhs.to_string(),
                    ))
                }
            }),
            Expression::Add(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Tinyint(lhs), Value::Tinyint(rhs)) => Value::Tinyint(
                    lhs.checked_add(rhs)
                        .ok_or(Error::OutOfBound("Tinyint", "overflow"))?,
                ),
                (Value::Smallint(lhs), Value::Smallint(rhs)) => Value::Smallint(
                    lhs.checked_add(rhs)
                        .ok_or(Error::OutOfBound("Smallint", "overflow"))?,
                ),
                (Value::Integer(lhs), Value::Integer(rhs)) => Value::Integer(
                    lhs.checked_add(rhs)
                        .ok_or(Error::OutOfBound("Integer", "overflow"))?,
                ),
                (Value::Bigint(lhs), Value::Bigint(rhs)) => Value::Bigint(
                    lhs.checked_add(rhs)
                        .ok_or(Error::OutOfBound("Bigint", "overflow"))?,
                ),
                (Value::Float(lhs), Value::Float(rhs)) => Value::Float(OrderedFloat(lhs.0 + rhs.0)),
                (Value::Double(lhs), Value::Double(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0 + rhs.0))
                }
                (Value::Null, Value::Null) => Value::Null,
                // cast float
                (Value::Float(lhs), Value::Double(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0 as f64 + rhs.0))
                }
                (Value::Double(lhs), Value::Float(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0 + rhs.0 as f64))
                }
                (lhs, rhs) => {
                    return Err(Error::ValuesNotMatch(
                        "add",
                        lhs.to_string(),
                        rhs.to_string(),
                    ))
                }
            }),
            Expression::Subtract(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Tinyint(lhs), Value::Tinyint(rhs)) => Value::Tinyint(
                    lhs.checked_sub(rhs)
                        .ok_or(Error::OutOfBound("Tinyint", "underflow"))?,
                ),
                (Value::Smallint(lhs), Value::Smallint(rhs)) => Value::Smallint(
                    lhs.checked_sub(rhs)
                        .ok_or(Error::OutOfBound("Smallint", "underflow"))?,
                ),
                (Value::Integer(lhs), Value::Integer(rhs)) => Value::Integer(
                    lhs.checked_sub(rhs)
                        .ok_or(Error::OutOfBound("Integer", "underflow"))?,
                ),
                (Value::Bigint(lhs), Value::Bigint(rhs)) => Value::Bigint(
                    lhs.checked_sub(rhs)
                        .ok_or(Error::OutOfBound("Bigint", "underflow"))?,
                ),
                (Value::Float(lhs), Value::Float(rhs)) => Value::Float(OrderedFloat(lhs.0 - rhs.0)),
                (Value::Double(lhs), Value::Double(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0 - rhs.0))
                }
                (Value::Null, Value::Null) => Value::Null,
                // cast float
                (Value::Float(lhs), Value::Double(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0 as f64 - rhs.0))
                }
                (Value::Double(lhs), Value::Float(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0 - rhs.0 as f64))
                }
                (lhs, rhs) => {
                    return Err(Error::ValuesNotMatch(
                        "substract",
                        lhs.to_string(),
                        rhs.to_string(),
                    ))
                }
            }),
            Expression::Multiply(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Tinyint(lhs), Value::Tinyint(rhs)) => Value::Tinyint(
                    lhs.checked_mul(rhs)
                        .ok_or(Error::OutOfBound("Tinyint", "overflow"))?,
                ),
                (Value::Smallint(lhs), Value::Smallint(rhs)) => Value::Smallint(
                    lhs.checked_mul(rhs)
                        .ok_or(Error::OutOfBound("Smallint", "overflow"))?,
                ),
                (Value::Integer(lhs), Value::Integer(rhs)) => Value::Integer(
                    lhs.checked_mul(rhs)
                        .ok_or(Error::OutOfBound("Integer", "overflow"))?,
                ),
                (Value::Bigint(lhs), Value::Bigint(rhs)) => Value::Bigint(
                    lhs.checked_sub(rhs)
                        .ok_or(Error::OutOfBound("Bigint", "overflow"))?,
                ),
                (Value::Float(lhs), Value::Float(rhs)) => Value::Float(OrderedFloat(lhs.0 * rhs.0)),
                (Value::Double(lhs), Value::Double(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0 * rhs.0))
                }
                (Value::Null, Value::Null) => Value::Null,
                // cast float
                (Value::Float(lhs), Value::Double(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0 as f64 * rhs.0))
                }
                (Value::Double(lhs), Value::Float(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0 * rhs.0 as f64))
                }
                (lhs, rhs) => {
                    return Err(Error::ValuesNotMatch(
                        "multiple",
                        lhs.to_string(),
                        rhs.to_string(),
                    ))
                }
            }),
            Expression::Divide(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                // check zero
                (lhs, rhs) if (lhs.check_int() || lhs.check_float()) && rhs.check_zero() => {
                    return Err(Error::ValuesNotMatch(
                        "divide",
                        lhs.to_string(),
                        rhs.to_string(),
                    ))
                }
                (Value::Tinyint(lhs), Value::Tinyint(rhs)) => Value::Tinyint(
                    lhs.checked_div(rhs)
                        .ok_or(Error::OutOfBound("Tinyint", "underflow"))?,
                ),
                (Value::Smallint(lhs), Value::Smallint(rhs)) => Value::Smallint(
                    lhs.checked_div(rhs)
                        .ok_or(Error::OutOfBound("Smallint", "underflow"))?,
                ),
                (Value::Integer(lhs), Value::Integer(rhs)) => Value::Integer(
                    lhs.checked_div(rhs)
                        .ok_or(Error::OutOfBound("Integer", "underflow"))?,
                ),
                (Value::Bigint(lhs), Value::Bigint(rhs)) => Value::Bigint(
                    lhs.checked_div(rhs)
                        .ok_or(Error::OutOfBound("Bigint", "underflow"))?,
                ),
                (Value::Float(lhs), Value::Float(rhs)) => Value::Float(OrderedFloat(lhs.0 / rhs.0)),
                (Value::Double(lhs), Value::Double(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0 / rhs.0))
                }
                (Value::Null, Value::Null) => Value::Null,
                // cast float
                (Value::Float(lhs), Value::Double(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0 as f64 / rhs.0))
                }
                (Value::Double(lhs), Value::Float(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0 / rhs.0 as f64))
                }
                (lhs, rhs) => {
                    return Err(Error::ValuesNotMatch(
                        "divide",
                        lhs.to_string(),
                        rhs.to_string(),
                    ))
                }
            }),
            Expression::Exponentiate(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Tinyint(lhs), Value::Tinyint(rhs)) => {
                    Value::Double(OrderedFloat((lhs as f64).powf(rhs as f64)))
                }
                (Value::Smallint(lhs), Value::Smallint(rhs)) => {
                    Value::Double(OrderedFloat((lhs as f64).powf(rhs as f64)))
                }
                (Value::Integer(lhs), Value::Integer(rhs)) => {
                    Value::Double(OrderedFloat((lhs as f64).powf(rhs as f64)))
                }
                (Value::Bigint(lhs), Value::Bigint(rhs)) => {
                    Value::Double(OrderedFloat((lhs as f64).powf(rhs as f64)))
                }
                (Value::Float(lhs), Value::Float(rhs)) => {
                    Value::Float(OrderedFloat(lhs.0.powf(rhs.0)))
                }
                (Value::Double(lhs), Value::Double(rhs)) => {
                    Value::Double(OrderedFloat(lhs.powf(rhs.0)))
                }
                (Value::Null, Value::Null) => Value::Null,
                // cast float
                (Value::Double(lhs), Value::Float(rhs)) => {
                    Value::Double(OrderedFloat(lhs.0.powf(rhs.0 as f64)))
                }
                (lhs, rhs) => {
                    return Err(Error::ValuesNotMatch(
                        "exponentiate",
                        lhs.to_string(),
                        rhs.to_string(),
                    ))
                }
            }),
            Expression::Like(_, _) => todo!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn evaluate() {
        {
            let expression = Expression::Add(
                Box::new(Expression::Const(Value::Integer(1))),
                Box::new(Expression::Const(Value::Integer(1))),
            );
            assert_eq!(expression.evaluate().unwrap(), Value::Integer(2))
        }
        {
            let expression = Expression::Subtract(
                Box::new(Expression::Const(Value::Integer(2))),
                Box::new(Expression::Const(Value::Integer(1))),
            );
            assert_eq!(expression.evaluate().unwrap(), Value::Integer(1))
        }
        {
            let expression = Expression::Multiply(
                Box::new(Expression::Const(Value::Integer(2))),
                Box::new(Expression::Const(Value::Integer(2))),
            );
            assert_eq!(expression.evaluate().unwrap(), Value::Integer(4))
        }
        {
            let expression = Expression::Divide(
                Box::new(Expression::Const(Value::Integer(2))),
                Box::new(Expression::Const(Value::Integer(1))),
            );
            assert_eq!(expression.evaluate().unwrap(), Value::Integer(2))
        }
    }
}
