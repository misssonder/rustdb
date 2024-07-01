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
}
