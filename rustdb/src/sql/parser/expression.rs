use crate::sql::parser::keyword::Keyword;
use crate::sql::parser::{identifier, IResult};
use nom::branch::alt;
use nom::bytes::complete::{tag, tag_no_case};
use nom::character::complete::{alpha1, i64, multispace0};
use nom::combinator::{map, not, opt, peek};
use nom::error::context;
use nom::number::complete::double;
use nom::sequence::{delimited, preceded, tuple};
use nom::Parser;
use std::fmt::{Debug, Formatter};

#[derive(Clone, Debug, PartialEq)]
pub enum Expression {
    Literal(Literal),
    Field(Option<String>, String),
    Column(usize),
    Operation(Operation),
}

impl Default for Expression {
    fn default() -> Self {
        Self::Literal(Literal::default())
    }
}

#[derive(Default, Clone, Debug, PartialEq)]
pub enum Literal {
    #[default]
    Null,
    Boolean(bool),
    Integer(i64),
    Float(f64),
    String(String),
}

impl std::fmt::Display for Literal {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Integer(i) => write!(f, "{}", i),
            Literal::Float(float) => write!(f, "{}", float),
            Literal::String(s) => write!(f, "{}", s),
            Literal::Null => write!(f, "NULL"),
            Literal::Boolean(bool) => write!(f, "{}", bool),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Operation {
    // Logical operators
    And(Box<Expression>, Box<Expression>),
    Not(Box<Expression>),
    Or(Box<Expression>, Box<Expression>),

    // Comparison operators
    Equal(Box<Expression>, Box<Expression>),
    GreaterThan(Box<Expression>, Box<Expression>),
    GreaterThanOrEqual(Box<Expression>, Box<Expression>),
    IsNull(Box<Expression>),
    LessThan(Box<Expression>, Box<Expression>),
    LessThanOrEqual(Box<Expression>, Box<Expression>),
    NotEqual(Box<Expression>, Box<Expression>),

    // Mathematical operators
    Add(Box<Expression>, Box<Expression>),
    Assert(Box<Expression>),
    Divide(Box<Expression>, Box<Expression>),
    Exponentiate(Box<Expression>, Box<Expression>),
    Factorial(Box<Expression>),
    Modulo(Box<Expression>, Box<Expression>),
    Multiply(Box<Expression>, Box<Expression>),
    Negate(Box<Expression>),
    Subtract(Box<Expression>, Box<Expression>),

    // String operators
    Like(Box<Expression>, Box<Expression>),
}

impl From<Operation> for Expression {
    fn from(operation: Operation) -> Self {
        Expression::Operation(operation)
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

/// Prefix operators
#[derive(Debug)]
enum PrefixOperator {
    Minus,
    Not,
    Plus,
}
impl PrefixOperator {
    fn build(&self, lhs: Expression) -> Expression {
        let lhs = Box::new(lhs);
        match self {
            PrefixOperator::Minus => Operation::Negate(lhs),
            PrefixOperator::Plus => Operation::Assert(lhs),
            PrefixOperator::Not => Operation::Not(lhs),
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

#[derive(Debug)]
enum InfixOperator {
    Add,
    And,
    Divide,
    Equal,
    Exponentiate,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Like,
    Modulo,
    Multiply,
    NotEqual,
    Or,
    Subtract,
}

impl InfixOperator {
    fn build(&self, lhs: Expression, rhs: Expression) -> Expression {
        let lhs = Box::new(lhs);
        let rhs = Box::new(rhs);
        match self {
            InfixOperator::Add => Operation::Add(lhs, rhs),
            InfixOperator::Divide => Operation::Divide(lhs, rhs),
            InfixOperator::Exponentiate => Operation::Exponentiate(lhs, rhs),
            InfixOperator::Multiply => Operation::Multiply(lhs, rhs),
            InfixOperator::Subtract => Operation::Subtract(lhs, rhs),
            InfixOperator::Modulo => Operation::Modulo(lhs, rhs),
            InfixOperator::And => Operation::And(lhs, rhs),
            InfixOperator::Equal => Operation::Equal(lhs, rhs),
            InfixOperator::GreaterThan => Operation::GreaterThan(lhs, rhs),
            InfixOperator::GreaterThanOrEqual => Operation::GreaterThanOrEqual(lhs, rhs),
            InfixOperator::LessThan => Operation::LessThan(lhs, rhs),
            InfixOperator::LessThanOrEqual => Operation::LessThanOrEqual(lhs, rhs),
            InfixOperator::Like => Operation::Like(lhs, rhs),
            InfixOperator::NotEqual => Operation::NotEqual(lhs, rhs),
            InfixOperator::Or => Operation::Or(lhs, rhs),
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
            Self::Or => 1,
            Self::And => 2,
            Self::Equal | Self::NotEqual | Self::Like => 3,
            Self::GreaterThan
            | Self::GreaterThanOrEqual
            | Self::LessThan
            | Self::LessThanOrEqual => 4,
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
    fn build(&self, lhs: Expression) -> Expression {
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

/// Ugly implement precedence climbing
pub fn expression(prec_min: u8) -> impl FnMut(&str) -> IResult<&str, Expression> {
    move |i| {
        let (i, prefix) = min_prec_pre_operator(prec_min)(i)?;
        let (mut i, mut lhs) = if let Some(prefix) = prefix {
            let (i, expression) = expression(prefix.prec() + prefix.assoc())(i)?;
            (i, prefix.build(expression))
        } else {
            expression_atom(i)?
        };
        let mut postfix = None;
        loop {
            (i, postfix) = min_prec_post_operator(prec_min)(i)?;
            if let Some(postfix) = postfix {
                lhs = postfix.build(lhs);
            } else {
                break;
            }
        }
        let mut infix = None;
        let mut rhs = Expression::default();
        loop {
            (i, infix) = min_prec_infix_operator(prec_min)(i)?;
            if let Some(infix) = infix {
                (i, rhs) = expression(infix.prec() + infix.assoc())(i)?;
                lhs = infix.build(lhs, rhs);
            } else {
                break;
            }
        }
        Ok((i, lhs))
    }
}

fn expression_atom(i: &str) -> IResult<&str, Expression> {
    context(
        "expression atom",
        preceded(
            multispace0,
            alt((
                map(literal, Expression::Literal),
                delimited(tag("("), expression(0), tag(")")),
                map(
                    tuple((identifier, opt(preceded(tag("."), identifier)))),
                    |(field, relation)| {
                        Expression::Field(
                            relation.map(|relation| relation.to_string()),
                            field.to_string(),
                        )
                    },
                ),
            )),
        ),
    )(i)
}

fn literal(i: &str) -> IResult<&str, Literal> {
    context(
        "literal",
        alt((
            map(
                tuple((i64, not(alt((tag("."), tag_no_case("e")))))),
                |(integer, _)| Literal::Integer(integer),
            ),
            map(double, Literal::Float),
            map(delimited(tag("'"), alpha1, tag("'")), |s: &str| {
                Literal::String(s.to_string())
            }),
            map(tag_no_case(Keyword::Null.to_str()), |_| Literal::Null),
            map(tag_no_case(Keyword::False.to_str()), |_| {
                Literal::Boolean(false)
            }),
            map(tag_no_case(Keyword::True.to_str()), |_| {
                Literal::Boolean(true)
            }),
        )),
    )(i)
}

fn pre_operator(i: &str) -> IResult<&str, PrefixOperator> {
    context(
        "prefix operator",
        preceded(
            multispace0,
            alt((
                map(tag_no_case("-"), |_| PrefixOperator::Minus),
                map(tag_no_case(Keyword::Not.to_str()), |_| PrefixOperator::Not),
                map(tag_no_case("+"), |_| PrefixOperator::Plus),
            )),
        ),
    )(i)
}

fn infix_operator(i: &str) -> IResult<&str, InfixOperator> {
    context(
        "infix operator",
        preceded(
            multispace0,
            alt((
                map(tag_no_case("+"), |_| InfixOperator::Add),
                map(tag_no_case(Keyword::And.to_str()), |_| InfixOperator::And),
                map(tag_no_case("/"), |_| InfixOperator::Divide),
                map(tag_no_case("="), |_| InfixOperator::Equal),
                map(tag_no_case("^"), |_| InfixOperator::Exponentiate),
                map(tag_no_case(">"), |_| InfixOperator::GreaterThan),
                map(tag_no_case(">="), |_| InfixOperator::GreaterThanOrEqual),
                map(tag_no_case("<"), |_| InfixOperator::LessThan),
                map(tag_no_case("<="), |_| InfixOperator::LessThanOrEqual),
                map(tag_no_case(Keyword::Like.to_str()), |_| InfixOperator::Like),
                map(tag_no_case("%"), |_| InfixOperator::Modulo),
                map(tag_no_case("*"), |_| InfixOperator::Multiply),
                map(tag_no_case("!="), |_| InfixOperator::NotEqual),
                map(tag_no_case(Keyword::Or.to_str()), |_| InfixOperator::Or),
                map(tag_no_case("-"), |_| InfixOperator::Subtract),
            )),
        ),
    )(i)
}

fn post_operator(i: &str) -> IResult<&str, PostfixOperator> {
    context(
        "post operator",
        preceded(
            multispace0,
            alt((map(tag_no_case("!"), |_| PostfixOperator::Factorial),)),
        ),
    )(i)
}

fn min_prec_pre_operator(
    min_prec: u8,
) -> impl FnMut(&str) -> IResult<&str, Option<PrefixOperator>> {
    move |i| {
        opt(peek(pre_operator))(i).and_then(|(remaining, operator)| match operator {
            None => Ok((i, None)),
            Some(operator) => {
                if operator.prec() >= min_prec {
                    pre_operator(i).map(|(remaining, operator)| (remaining, Some(operator)))
                } else {
                    Ok((i, None))
                }
            }
        })
    }
}

fn min_prec_infix_operator(
    min_prec: u8,
) -> impl FnMut(&str) -> IResult<&str, Option<InfixOperator>> {
    move |i| {
        opt(peek(infix_operator))(i).and_then(|(remaining, operator)| match operator {
            None => Ok((i, None)),
            Some(operator) => {
                if operator.prec() >= min_prec {
                    infix_operator(i).map(|(remaining, operator)| (remaining, Some(operator)))
                } else {
                    Ok((i, None))
                }
            }
        })
    }
}

fn min_prec_post_operator(
    min_prec: u8,
) -> impl FnMut(&str) -> IResult<&str, Option<PostfixOperator>> {
    move |i| {
        opt(peek(post_operator))(i).and_then(|(remaining, operator)| match operator {
            None => Ok((i, None)),
            Some(operator) => {
                if operator.prec() >= min_prec {
                    post_operator(i).map(|(remaining, operator)| (remaining, Some(operator)))
                } else {
                    Ok((i, None))
                }
            }
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    fn expression(input: &str) -> IResult<&str, Expression> {
        super::expression(0)(input)
    }
    #[test]
    fn literal() {
        assert_eq!(super::literal("1.0").unwrap().1, Literal::Float(1.0));
        assert_eq!(super::literal("1").unwrap().1, Literal::Integer(1));
    }
    #[test]
    fn arith_expression() {
        let input = vec!["1+2*3", "(1+2)*3", "(1.0+2)*3"];
        let output = vec![
            Ok(Expression::Operation(Operation::Add(
                Box::new(Expression::Literal(Literal::Integer(1))),
                Box::new(Expression::Operation(Operation::Multiply(
                    Box::new(Expression::Literal(Literal::Integer(2))),
                    Box::new(Expression::Literal(Literal::Integer(3))),
                ))),
            ))),
            Ok(Expression::Operation(Operation::Multiply(
                Box::new(Expression::Operation(Operation::Add(
                    Box::new(Expression::Literal(Literal::Integer(1))),
                    Box::new(Expression::Literal(Literal::Integer(2))),
                ))),
                Box::new(Expression::Literal(Literal::Integer(3))),
            ))),
            Ok(Expression::Operation(Operation::Multiply(
                Box::new(Expression::Operation(Operation::Add(
                    Box::new(Expression::Literal(Literal::Float(1.0))),
                    Box::new(Expression::Literal(Literal::Integer(2))),
                ))),
                Box::new(Expression::Literal(Literal::Integer(3))),
            ))),
        ];
        assert_eq!(
            input
                .into_iter()
                .map(|i| expression(i).map(|(_, expression)| expression))
                .collect::<Vec<_>>(),
            output
        );
        assert_eq!(expression("1 + 2 * 3"), expression("1 + (2 * 3)"));
        assert_eq!(
            expression("a.user = 'John' and b.id = 2"),
            expression("(a.user = 'John') and (b.id = 2)"),
        );
    }
}
