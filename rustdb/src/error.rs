use crate::{encoding, sql};
use thiserror::Error;

pub type RustDBResult<T> = Result<T, RustDBError>;

#[derive(Error, Debug)]
pub enum RustDBError {
    #[error("[BufferPool]: {0}")]
    BufferPool(String),
    #[error("[IO]: {0}")]
    IO(#[from] std::io::Error),
    #[error("[Encode]: {0}")]
    Encoding(#[from] encoding::error::Error),
    #[error("[TryLock]: {0}")]
    TryLock(#[from] tokio::sync::TryLockError),
    #[error("[Value]: {0}")]
    Value(String),
    #[error("[Catalog]: {0}")]
    Catalog(#[from] sql::catalog::error::Error),
}
