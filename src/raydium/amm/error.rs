use thiserror::Error;

#[derive(Debug, Error)]
pub enum AmmError {
    #[error("Conversion to u64 failed with an overflow or underflow")]
    ConversionFailure,
}
