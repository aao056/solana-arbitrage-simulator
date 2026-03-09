use crate::Rounding;

/// (x * y) / denominator
pub fn mul_div(x: u128, y: u128, denominator: u128, rounding: Rounding) -> Option<u128> {
    if denominator == 0 {
        return None;
    }

    let prod = x.checked_mul(y)?;

    match rounding {
        Rounding::Up => {
            let num = prod.checked_add(denominator.checked_sub(1)?)?;
            num.checked_div(denominator)
        }
        Rounding::Down => prod.checked_div(denominator),
    }
}

/// (x * y) >> offset
#[inline]
pub fn mul_shr(x: u128, y: u128, offset: u8, rounding: Rounding) -> Option<u128> {
    let denominator = 1u128.checked_shl(offset.into())?;
    mul_div(x, y, denominator, rounding)
}

/// (x << offset) / y
#[inline]
pub fn shl_div(x: u128, y: u128, offset: u8, rounding: Rounding) -> Option<u128> {
    let scale = 1u128.checked_shl(offset.into())?;
    mul_div(x, scale, y, rounding)
}
