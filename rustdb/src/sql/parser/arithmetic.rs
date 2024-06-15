use crate::sql::parser::IResult;
use crate::sql::types::expression::Expression;
use nom::branch::alt;
use nom::bytes::complete::tag_no_case;
use nom::combinator::{map, opt};
use nom::error::context;
use nom::multi::many0;

#[derive(Clone, Debug, PartialEq)]
pub enum ArithmeticExpression {
    Literal(Literal),
    Operation(Operation),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Literal {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    // Logical operators
    And(Box<ArithmeticExpression>, Box<ArithmeticExpression>),
    Not(Box<ArithmeticExpression>),
    Or(Box<ArithmeticExpression>, Box<ArithmeticExpression>),

    // Comparison operators
    Equal(Box<ArithmeticExpression>, Box<ArithmeticExpression>),
    GreaterThan(Box<ArithmeticExpression>, Box<ArithmeticExpression>),
    GreaterThanOrEqual(Box<ArithmeticExpression>, Box<ArithmeticExpression>),
    IsNull(Box<ArithmeticExpression>),
    LessThan(Box<ArithmeticExpression>, Box<ArithmeticExpression>),
    LessThanOrEqual(Box<ArithmeticExpression>, Box<ArithmeticExpression>),
    NotEqual(Box<ArithmeticExpression>, Box<ArithmeticExpression>),

    // Mathematical operators
    Add(Box<ArithmeticExpression>, Box<ArithmeticExpression>),
    Assert(Box<ArithmeticExpression>),
    Divide(Box<ArithmeticExpression>, Box<ArithmeticExpression>),
    Exponentiate(Box<ArithmeticExpression>, Box<ArithmeticExpression>),
    Factorial(Box<ArithmeticExpression>),
    Modulo(Box<ArithmeticExpression>, Box<ArithmeticExpression>),
    Multiply(Box<ArithmeticExpression>, Box<ArithmeticExpression>),
    Negate(Box<ArithmeticExpression>),
    Subtract(Box<ArithmeticExpression>, Box<ArithmeticExpression>),

    // String operators
    Like(Box<ArithmeticExpression>, Box<ArithmeticExpression>),
}

impl From<Operation> for ArithmeticExpression {
    fn from(operation: Operation) -> Self {
        ArithmeticExpression::Operation(operation)
    }
}

/// An operator trait, to help with parsing of operators
trait Operator: Sized {
    /// Returns the operator's associativity
    fn assoc(&self) -> u8;
    /// Returns the operator's precedence
    fn prec(&self) -> u8;
}

const ASSOC_LEFT: u8 = 1;
const ASSOC_RIGHT: u8 = 0;

enum PrefixOperator {
    Minus,
    Plus,
}

impl PrefixOperator {
    fn build(&self, lhs: ArithmeticExpression) -> ArithmeticExpression {
        let lhs = Box::new(lhs);
        match self {
            PrefixOperator::Minus => Operation::Negate(lhs),
            PrefixOperator::Plus => Operation::Assert(lhs),
        }
        .into()
    }
}

impl Operator for PrefixOperator {
    fn assoc(&self) -> u8 {
        ASSOC_RIGHT
    }

    fn prec(&self) -> u8 {
        9
    }
}

enum InfixOperator {
    Add,
    Divide,
    Exponentiate,
    Multiply,
    Subtract,
    Modulo,
}

impl InfixOperator {
    fn build(&self, lhs: ArithmeticExpression, rhs: ArithmeticExpression) -> ArithmeticExpression {
        let lhs = Box::new(lhs);
        let rhs = Box::new(rhs);
        match self {
            InfixOperator::Add => Operation::Add(lhs, rhs),
            InfixOperator::Divide => Operation::Divide(lhs, rhs),
            InfixOperator::Exponentiate => Operation::Exponentiate(lhs, rhs),
            InfixOperator::Multiply => Operation::Multiply(lhs, rhs),
            InfixOperator::Subtract => Operation::Subtract(lhs, rhs),
            InfixOperator::Modulo => Operation::Modulo(lhs, rhs),
        }
        .into()
    }
}

impl Operator for InfixOperator {
    fn assoc(&self) -> u8 {
        match self {
            Self::Exponentiate => ASSOC_RIGHT,
            _ => ASSOC_LEFT,
        }
    }

    fn prec(&self) -> u8 {
        match self {
            Self::Add | Self::Subtract => 5,
            Self::Multiply | Self::Divide | Self::Modulo => 6,
            Self::Exponentiate => 7,
        }
    }
}

enum PostfixOperator {
    Factorial,
}

impl PostfixOperator {
    fn build(&self, lhs: ArithmeticExpression) -> ArithmeticExpression {
        let lhs = Box::new(lhs);
        match self {
            PostfixOperator::Factorial => Operation::Factorial(lhs),
        }
        .into()
    }
}

impl Operator for PostfixOperator {
    fn assoc(&self) -> u8 {
        ASSOC_LEFT
    }

    fn prec(&self) -> u8 {
        8
    }
}

fn pre_operator(i: &[u8]) -> IResult<&[u8], PrefixOperator> {
    context(
        "PrefixOperator",
        alt((
            map(tag_no_case("-"), |_| PrefixOperator::Minus),
            map(tag_no_case("+"), |_| PrefixOperator::Plus),
        )),
    )(i)
}

fn infix_operator(i: &[u8]) -> IResult<&[u8], InfixOperator> {
    context(
        "InfixOperator",
        alt((
            map(tag_no_case("+"), |_| InfixOperator::Add),
            map(tag_no_case("-"), |_| InfixOperator::Subtract),
            map(tag_no_case("*"), |_| InfixOperator::Multiply),
            map(tag_no_case("/"), |_| InfixOperator::Divide),
            map(tag_no_case("^"), |_| InfixOperator::Exponentiate),
            map(tag_no_case("%"), |_| InfixOperator::Modulo),
        )),
    )(i)
}

fn post_operator(i: &[u8]) -> IResult<&[u8], PostfixOperator> {
    context(
        "InfixOperator",
        map(tag_no_case("!"), |_| PostfixOperator::Factorial),
    )(i)
}
