use thiserror::Error;

mod catalog;
mod engine;
mod execution;
pub mod parser;
mod plan;
mod transaction;
pub mod types;

pub type SqlResult<T> = Result<T, Error>;
#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    FromStr(String),
    #[error("can't {0} {1} and {2}")]
    ValuesNotMatch(&'static str, String, String),
    #[error("can't {0} {1}")]
    ValueNotMatch(&'static str, String),
    #[error("can't {0} {1}")]
    OutOfBound(&'static str, &'static str),
}
