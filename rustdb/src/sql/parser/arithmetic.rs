use crate::sql::parser::IResult;
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::i64;
use nom::combinator::{map, not, opt};
use nom::error::context;
use nom::multi::many0;
use nom::number::complete::double;
use nom::sequence::{delimited, tuple};
use nom::Parser;

#[derive(Clone, Debug, PartialEq)]
pub enum ArithmeticExpression {
    Literal(Literal),
    Operation(Operation),
}

#[derive(Clone, Debug, PartialEq)]
enum Literal {
    Integer(i64),
    Float(f64),
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

fn arith_expression(prec_min: u8) -> impl FnMut(&[u8]) -> IResult<&[u8], ArithmeticExpression> {
    move |i| {
        let (i, prefix) = opt(pre_operator)(i)?;
        let (i, mut lhs) = if let Some(prefix) = prefix {
            let (i, expression) = arith_expression(prefix.prec() + prefix.assoc())(i)?;
            (i, prefix.build(expression))
        } else {
            arith_expression_atom(i)?
        };
        let (i, postfixes) = many0(post_operator)(i)?;
        for postfix in postfixes {
            lhs = postfix.build(lhs);
        }
        let (mut i, infixes) = many0(infix_operator)(i)?;
        let input = i;
        for infix in infixes {
            let (input, expression) = arith_expression(infix.prec() + infix.assoc())(input)?;
            lhs = infix.build(lhs, expression);
            i = input;
        }
        Ok((i, lhs))
    }
}

fn arith_expression_atom(i: &[u8]) -> IResult<&[u8], ArithmeticExpression> {
    context(
        "expression atom",
        alt((
            map(literal, ArithmeticExpression::Literal),
            delimited(tag("("), arith_expression(0), tag(")")),
        )),
    )(i)
}

fn literal(i: &[u8]) -> IResult<&[u8], Literal> {
    context(
        "literal",
        alt((
            map(
                tuple((i64, not(alt((tag("."), tag_no_case("e")))))),
                |(integer, _)| Literal::Integer(integer),
            ),
            map(double, Literal::Float),
        )),
    )(i)
}

fn pre_operator(i: &[u8]) -> IResult<&[u8], PrefixOperator> {
    context(
        "prefix operator",
        alt((
            map(tag_no_case("-"), |_| PrefixOperator::Minus),
            map(tag_no_case("+"), |_| PrefixOperator::Plus),
        )),
    )(i)
}

fn infix_operator(i: &[u8]) -> IResult<&[u8], InfixOperator> {
    context(
        "infix operator",
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
        "post operator",
        map(tag_no_case("!"), |_| PostfixOperator::Factorial),
    )(i)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expression(input: &[u8]) -> IResult<&[u8], ArithmeticExpression> {
        super::arith_expression(0)(input)
    }
    #[test]
    fn literal() {
        assert_eq!(super::literal(b"1.0").unwrap().1, Literal::Float(1.0));
        assert_eq!(super::literal(b"1").unwrap().1, Literal::Integer(1));
    }
    #[test]
    fn arith_expression() {
        let input = vec!["1+2*3", "(1+2)*3", "(1.0+2)*3"];
        let output = vec![
            Ok(ArithmeticExpression::Operation(Operation::Add(
                Box::new(ArithmeticExpression::Literal(Literal::Integer(1))),
                Box::new(ArithmeticExpression::Operation(Operation::Multiply(
                    Box::new(ArithmeticExpression::Literal(Literal::Integer(2))),
                    Box::new(ArithmeticExpression::Literal(Literal::Integer(3))),
                ))),
            ))),
            Ok(ArithmeticExpression::Operation(Operation::Multiply(
                Box::new(ArithmeticExpression::Operation(Operation::Add(
                    Box::new(ArithmeticExpression::Literal(Literal::Integer(1))),
                    Box::new(ArithmeticExpression::Literal(Literal::Integer(2))),
                ))),
                Box::new(ArithmeticExpression::Literal(Literal::Integer(3))),
            ))),
            Ok(ArithmeticExpression::Operation(Operation::Multiply(
                Box::new(ArithmeticExpression::Operation(Operation::Add(
                    Box::new(ArithmeticExpression::Literal(Literal::Float(1.0))),
                    Box::new(ArithmeticExpression::Literal(Literal::Integer(2))),
                ))),
                Box::new(ArithmeticExpression::Literal(Literal::Integer(3))),
            ))),
        ];
        assert_eq!(
            input
                .into_iter()
                .map(|i| expression(i.as_bytes()).map(|(_, expression)| expression))
                .collect::<Vec<_>>(),
            output
        )
    }
}
