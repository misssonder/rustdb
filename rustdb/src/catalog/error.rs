use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("{0}: {1} duplicated")]
    Duplicated(&'static str, String),
}
