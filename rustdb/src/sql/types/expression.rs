use crate::error::{RustDBError, RustDBResult};
use crate::sql::types::Value;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Const(Value),

    And(Box<Expression>, Box<Expression>),
    Or(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),

    Add(Box<Expression>, Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
}

impl Expression {
    // TODO cast integer
    pub fn evaluate(&self) -> RustDBResult<Value> {
        match self {
            Expression::Const(value) => Ok(value.clone()),
            Expression::And(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Boolean(lhs), Value::Boolean(rhs)) => Value::Boolean(lhs && rhs),
                (Value::Null, Value::Null) => Value::Null,
                (lhs, rhs) => {
                    return Err(RustDBError::Value(format!("Can't and {} and {}", lhs, rhs)))
                }
            }),
            Expression::Or(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Boolean(lhs), Value::Boolean(rhs)) => Value::Boolean(lhs || rhs),
                (Value::Null, Value::Null) => Value::Null,
                (lhs, rhs) => {
                    return Err(RustDBError::Value(format!("Can't or {} and {}", lhs, rhs)))
                }
            }),
            Expression::Not(expr) => Ok(match expr.evaluate()? {
                Value::Null => Value::Null,
                Value::Boolean(expr) => Value::Boolean(!expr),
                expr => return Err(RustDBError::Value(format!("Can't not {}", expr))),
            }),
            Expression::Add(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Tinyint(lhs), Value::Tinyint(rhs)) => Value::Tinyint(
                    lhs.checked_add(rhs)
                        .ok_or(RustDBError::Value("Tinyint overflow".into()))?,
                ),
                (Value::Smallint(lhs), Value::Smallint(rhs)) => Value::Smallint(
                    lhs.checked_add(rhs)
                        .ok_or(RustDBError::Value("Smallint overflow".into()))?,
                ),
                (Value::Integer(lhs), Value::Integer(rhs)) => Value::Integer(
                    lhs.checked_add(rhs)
                        .ok_or(RustDBError::Value("Integer overflow".into()))?,
                ),
                (Value::Bigint(lhs), Value::Bigint(rhs)) => Value::Bigint(
                    lhs.checked_add(rhs)
                        .ok_or(RustDBError::Value("Bigint overflow".into()))?,
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
                    return Err(RustDBError::Value(format!("Can't add {} and {}", lhs, rhs)))
                }
            }),
            Expression::Subtract(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Tinyint(lhs), Value::Tinyint(rhs)) => Value::Tinyint(
                    lhs.checked_sub(rhs)
                        .ok_or(RustDBError::Value("Tinyint underflow".into()))?,
                ),
                (Value::Smallint(lhs), Value::Smallint(rhs)) => Value::Smallint(
                    lhs.checked_sub(rhs)
                        .ok_or(RustDBError::Value("Smallint underflow".into()))?,
                ),
                (Value::Integer(lhs), Value::Integer(rhs)) => Value::Integer(
                    lhs.checked_sub(rhs)
                        .ok_or(RustDBError::Value("Integer underflow".into()))?,
                ),
                (Value::Bigint(lhs), Value::Bigint(rhs)) => Value::Bigint(
                    lhs.checked_sub(rhs)
                        .ok_or(RustDBError::Value("Bigint underflow".into()))?,
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
                    return Err(RustDBError::Value(format!(
                        "Can't subtract {} and {}",
                        lhs, rhs
                    )))
                }
            }),
            Expression::Multiply(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                (Value::Tinyint(lhs), Value::Tinyint(rhs)) => Value::Tinyint(
                    lhs.checked_mul(rhs)
                        .ok_or(RustDBError::Value("Tinyint overflow".into()))?,
                ),
                (Value::Smallint(lhs), Value::Smallint(rhs)) => Value::Smallint(
                    lhs.checked_mul(rhs)
                        .ok_or(RustDBError::Value("Smallint overflow".into()))?,
                ),
                (Value::Integer(lhs), Value::Integer(rhs)) => Value::Integer(
                    lhs.checked_mul(rhs)
                        .ok_or(RustDBError::Value("Integer overflow".into()))?,
                ),
                (Value::Bigint(lhs), Value::Bigint(rhs)) => Value::Bigint(
                    lhs.checked_sub(rhs)
                        .ok_or(RustDBError::Value("Bigint overflow".into()))?,
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
                    return Err(RustDBError::Value(format!(
                        "Can't multiply {} and {}",
                        lhs, rhs
                    )))
                }
            }),
            Expression::Divide(lhs, rhs) => Ok(match (lhs.evaluate()?, rhs.evaluate()?) {
                // check zero
                (lhs, rhs) if (lhs.check_int() || lhs.check_float()) && rhs.check_zero() => {
                    return Err(RustDBError::Value(format!(
                        "Can't divide {} and {}",
                        lhs, rhs
                    )))
                }
                (Value::Tinyint(lhs), Value::Tinyint(rhs)) => Value::Tinyint(
                    lhs.checked_div(rhs)
                        .ok_or(RustDBError::Value("Tinyint underflow".into()))?,
                ),
                (Value::Smallint(lhs), Value::Smallint(rhs)) => Value::Smallint(
                    lhs.checked_div(rhs)
                        .ok_or(RustDBError::Value("Smallint underflow".into()))?,
                ),
                (Value::Integer(lhs), Value::Integer(rhs)) => Value::Integer(
                    lhs.checked_div(rhs)
                        .ok_or(RustDBError::Value("Integer underflow".into()))?,
                ),
                (Value::Bigint(lhs), Value::Bigint(rhs)) => Value::Bigint(
                    lhs.checked_div(rhs)
                        .ok_or(RustDBError::Value("Bigint underflow".into()))?,
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
                    return Err(RustDBError::Value(format!(
                        "Can't divide {} and {}",
                        lhs, rhs
                    )))
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
                    return Err(RustDBError::Value(format!(
                        "Can't exponentiate {} and {}",
                        lhs, rhs
                    )))
                }
            }),
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
