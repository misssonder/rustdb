use crate::sql::parser::keyword::keyword;
use futures::StreamExt;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while1};
use nom::character::complete::alpha1;
use nom::combinator::{not, peek};
use nom::error::{context, VerboseError};
use nom::sequence::{delimited, preceded, tuple};

mod ast;
mod ddl;
mod expression;
mod keyword;
mod tcl;

type IResult<I, O> = nom::IResult<I, O, VerboseError<I>>;

pub fn identifier(i: &str) -> IResult<&str, &str> {
    context(
        "identifier",
        alt((
            preceded(
                not(keyword),
                tuple((peek(alpha1), take_while1(is_identifier))),
            ),
            delimited(
                tag("`"),
                tuple((peek(alpha1), take_while1(is_identifier))),
                tag("`"),
            ),
            delimited(
                tag("["),
                tuple((peek(alpha1), take_while1(is_identifier))),
                tag("]"),
            ),
        )),
    )(i)
    .map(|(remaining, ident)| (remaining, ident.1))
}

fn is_identifier(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '@'
}

#[cfg(test)]
mod tests {

    #[test]
    fn identifier() {
        let input = "Employee_ID";
        println!("{:?}", super::identifier(input));
    }
}
