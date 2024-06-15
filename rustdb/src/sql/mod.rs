use thiserror::Error;

mod catalog;
mod engine;
mod execution;
mod parser;
mod transaction;
pub mod types;

pub type SqlResult<T> = Result<T, Error>;
#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}")]
    FromStr(String),
}
