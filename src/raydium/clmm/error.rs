use thiserror::Error;

#[derive(Debug, Error, PartialEq)]
pub enum ErrorCode {
    #[error("The tick must be lesser than, or equal to the maximum tick(443636)")]
    TickUpperOverflow,
    #[error("sqrt_price_x64 out of range")]
    SqrtPriceX64,
    #[error("Liquidity sub delta L must be smaller than before")]
    LiquiditySubValueErr,
    #[error("Liquidity add delta L must be greater, or equal to before")]
    LiquidityAddValueErr,
}
