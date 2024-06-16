use crate::sql::parser::keyword::keyword;
use nom::branch::alt;
use nom::bytes::complete::{tag, take_while1};
use nom::combinator::not;
use nom::error::{context, VerboseError};
use nom::sequence::{delimited, preceded};

mod arithmetic;
mod ast;
mod ddl;
mod keyword;

type IResult<I, O> = nom::IResult<I, O, VerboseError<I>>;

pub fn identifier(i: &str) -> IResult<&str, &str> {
    context(
        "identifier",
        alt((
            preceded(not(keyword), take_while1(is_identifier)),
            delimited(tag("`"), take_while1(is_identifier), tag("`")),
            delimited(tag("["), take_while1(is_identifier), tag("]")),
        )),
    )(i)
}

fn is_identifier(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '@'
}

#[cfg(test)]
mod tests {

    #[test]
    fn identifier() {
        let input = "EmployeeID";
        println!("{:?}", super::identifier(input));
    }
}
