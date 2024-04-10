use thiserror::Error;

pub type RustDBResult<T> = Result<T, RustDBError>;

#[derive(Error, Debug)]
pub enum RustDBError {
    #[error("[BufferPool]: {0}")]
    BufferPool(String),
    #[error("[IO]: {0}")]
    IO(#[from] std::io::Error),
    #[error("[Encode]: {0}")]
    Encode(String),
    #[error("[Decode]: {0}")]
    Decode(String),
    #[error("[TryLock]: {0}")]
    TryLock(#[from] tokio::sync::TryLockError),
}
