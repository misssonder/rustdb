use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0} duplicated")]
    Duplicated(String),
}
