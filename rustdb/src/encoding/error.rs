use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("encode error: {0}")]
    Encode(String),
    #[error("decode error: {0}")]
    Decode(String),
}
