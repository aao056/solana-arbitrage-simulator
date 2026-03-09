use super::error::ErrorCode;
use anyhow::Result;

pub fn add_delta(x: u128, y: i128) -> Result<u128> {
    if y < 0 {
        let delta =
            u128::try_from(y.wrapping_neg()).map_err(|_| ErrorCode::LiquiditySubValueErr)?;

        let z = x
            .checked_sub(delta)
            .ok_or(ErrorCode::LiquiditySubValueErr)?;

        if z >= x {
            return Err(ErrorCode::LiquiditySubValueErr.into());
        }

        Ok(z)
    } else {
        let delta = u128::try_from(y).map_err(|_| ErrorCode::LiquidityAddValueErr)?;

        let z = x
            .checked_add(delta)
            .ok_or(ErrorCode::LiquidityAddValueErr)?;

        if z < x {
            return Err(ErrorCode::LiquidityAddValueErr.into());
        }

        Ok(z)
    }
}

// Gets the delta amount_0 for given liquidity and price range
//
// # Formula
//
// * `Δx = L * (1 / √P_lower - 1 / √P_upper)`
// * i.e. `L * (√P_upper - √P_lower) / (√P_upper * √P_lower)`
// pub fn get_delta_amount_0_unsigned(
//     mut sqrt_ratio_a_x64: u128,
//     mut sqrt_ratio_b_x64: u128,
//     liquidity: u128,
//     round_up: bool,
// ) -> Result<u64> {
//     // sqrt_ratio_a_x64 should hold the smaller value
//     if sqrt_ratio_a_x64 > sqrt_ratio_b_x64 {
//         std::mem::swap(&mut sqrt_ratio_a_x64, &mut sqrt_ratio_b_x64);
//     };

//     let numerator_1 = liquidity << fixed_point_64::RESOLUTION;
//     let numerator_2 = sqrt_ratio_b_x64 - sqrt_ratio_a_x64;

//     assert!(sqrt_ratio_a_x64 > 0);

//     let result = if round_up {
//         u128::div_rounding_up(
//             numerator_1
//                 .mul_div_ceil(numerator_2, sqrt_ratio_b_x64)
//                 .unwrap(),
//             sqrt_ratio_a_x64,
//         )
//     } else {
//         numerator_1
//             .mul_div_floor(numerator_2, sqrt_ratio_b_x64)
//             .unwrap()
//             / sqrt_ratio_a_x64
//     };
//     if result > u128::from(u64::MAX) {
//         return Err(ErrorCode::MaxTokenOverflow.into());
//     }
//     Ok(result as u64)
// }

// Gets the delta amount_1 for given liquidity and price range
// * `Δy = L (√P_upper - √P_lower)`
// pub fn get_delta_amount_1_unsigned(
//     mut sqrt_ratio_a_x64: u128,
//     mut sqrt_ratio_b_x64: u128,
//     liquidity: u128,
//     round_up: bool,
// ) -> Result<u64> {
//     // sqrt_ratio_a_x64 should hold the smaller value
//     if sqrt_ratio_a_x64 > sqrt_ratio_b_x64 {
//         std::mem::swap(&mut sqrt_ratio_a_x64, &mut sqrt_ratio_b_x64);
//     };

//     let result = if round_up {
//         liquidity.mul_div_ceil(sqrt_ratio_b_x64 - sqrt_ratio_a_x64, fixed_point_64::Q64)
//     } else {
//         liquidity.mul_div_floor(sqrt_ratio_b_x64 - sqrt_ratio_a_x64, fixed_point_64::Q64)
//     }
//     .unwrap();
//     if result > u128::from(u64::MAX) {
//         return Err(ErrorCode::MaxTokenOverflow.into());
//     }
//     Ok(result as u64)
// }

// Helper function to get signed delta amount_0 for given liquidity and price range
// pub fn get_delta_amount_0_signed(
//     sqrt_ratio_a_x64: u128,
//     sqrt_ratio_b_x64: u128,
//     liquidity: i128,
// ) -> Result<u64> {
//     if liquidity < 0 {
//         get_delta_amount_0_unsigned(
//             sqrt_ratio_a_x64,
//             sqrt_ratio_b_x64,
//             u128::try_from(-liquidity).unwrap(),
//             false,
//         )
//     } else {
//         get_delta_amount_0_unsigned(
//             sqrt_ratio_a_x64,
//             sqrt_ratio_b_x64,
//             u128::try_from(liquidity).unwrap(),
//             true,
//         )
//     }
// }

// /// Helper function to get signed delta amount_1 for given liquidity and price range
// pub fn get_delta_amount_1_signed(
//     sqrt_ratio_a_x64: u128,
//     sqrt_ratio_b_x64: u128,
//     liquidity: i128,
// ) -> Result<u64> {
//     if liquidity < 0 {
//         get_delta_amount_1_unsigned(
//             sqrt_ratio_a_x64,
//             sqrt_ratio_b_x64,
//             u128::try_from(-liquidity).unwrap(),
//             false,
//         )
//     } else {
//         get_delta_amount_1_unsigned(
//             sqrt_ratio_a_x64,
//             sqrt_ratio_b_x64,
//             u128::try_from(liquidity).unwrap(),
//             true,
//         )
//     }
// }

// pub fn get_delta_amounts_signed(
//     tick_current: i32,
//     sqrt_price_x64_current: u128,
//     tick_lower: i32,
//     tick_upper: i32,
//     liquidity_delta: i128,
// ) -> Result<(u64, u64)> {
//     let mut amount_0 = 0;
//     let mut amount_1 = 0;
//     if tick_current < tick_lower {
//         amount_0 = get_delta_amount_0_signed(
//             tick_math::get_sqrt_price_at_tick(tick_lower)?,
//             tick_math::get_sqrt_price_at_tick(tick_upper)?,
//             liquidity_delta,
//         )
//         .unwrap();
//     } else if tick_current < tick_upper {
//         amount_0 = get_delta_amount_0_signed(
//             sqrt_price_x64_current,
//             tick_math::get_sqrt_price_at_tick(tick_upper)?,
//             liquidity_delta,
//         )
//         .unwrap();
//         amount_1 = get_delta_amount_1_signed(
//             tick_math::get_sqrt_price_at_tick(tick_lower)?,
//             sqrt_price_x64_current,
//             liquidity_delta,
//         )
//         .unwrap();
//     } else {
//         amount_1 = get_delta_amount_1_signed(
//             tick_math::get_sqrt_price_at_tick(tick_lower)?,
//             tick_math::get_sqrt_price_at_tick(tick_upper)?,
//             liquidity_delta,
//         )
//         .unwrap();
//     }
//     Ok((amount_0, amount_1))
// }
