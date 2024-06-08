use thiserror::Error;

mod catalog;
mod engine;
mod execution;
mod transaction;
pub mod types;

pub type SqlResult<T> = Result<T, Error>;
#[derive(Error, Debug)]
pub enum Error {}
