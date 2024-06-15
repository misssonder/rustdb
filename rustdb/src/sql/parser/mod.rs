use nom::error::VerboseError;

mod arithmetic;
mod ast;
mod ddl;

type IResult<I, O> = nom::IResult<I, O, VerboseError<I>>;
