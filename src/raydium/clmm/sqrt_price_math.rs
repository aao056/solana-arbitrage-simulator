// pub fn get_next_sqrt_price_from_amount_0_rounding_up(
//     sqrt_price_x64: u128,
//     liquidity: u128,
//     amount: u64,
//     add: bool,
// ) -> u128 {
//     if amount == 0 {
//         return sqrt_price_x64;
//     };
//     let numerator_1 = liquidity << fixed_point_64::RESOLUTION;

//     if add {
//         if let Some(product) = u128::from(amount).checked_mul(sqrt_price_x64) {
//             let denominator = numerator_1 + product;
//             if denominator >= numerator_1 {
//                 return numerator_1
//                     .mul_div_ceil(sqrt_price_x64, denominator)
//                     .unwrap();
//             };
//         }

//         u128::div_rounding_up(
//             numerator_1,
//             (numerator_1 / sqrt_price_x64)
//                 .checked_add(u128::from(amount))
//                 .unwrap(),
//         )
//     } else {
//         let product = u128::from(amount).checked_mul(sqrt_price_x64).unwrap();
//         let denominator = numerator_1.checked_sub(product).unwrap();
//         numerator_1
//             .mul_div_ceil(sqrt_price_x64, denominator)
//             .unwrap()
//     }
// }

// pub fn get_next_sqrt_price_from_amount_1_rounding_down(
//     sqrt_price_x64: u128,
//     liquidity: u128,
//     amount: u64,
//     add: bool,
// ) -> u128 {
//     if add {
//         let quotient = (u128::from(amount) << fixed_point_64::RESOLUTION) / liquidity;
//         sqrt_price_x64.checked_add(quotient).unwrap()
//     } else {
//         let quotient =
//             u128::div_rounding_up(u128::from(amount) << fixed_point_64::RESOLUTION, liquidity);
//         sqrt_price_x64.checked_sub(quotient).unwrap()
//     }
// }

// /// Gets the next sqrt price given an input amount of token_0 or token_1
// /// Throws if price or liquidity are 0, or if the next price is out of bounds
// pub fn get_next_sqrt_price_from_input(
//     sqrt_price_x64: u128,
//     liquidity: u128,
//     amount_in: u64,
//     zero_for_one: bool,
// ) -> u128 {
//     assert!(sqrt_price_x64 > 0);
//     assert!(liquidity > 0);

//     // round to make sure that we don't pass the target price
//     if zero_for_one {
//         get_next_sqrt_price_from_amount_0_rounding_up(sqrt_price_x64, liquidity, amount_in, true)
//     } else {
//         get_next_sqrt_price_from_amount_1_rounding_down(sqrt_price_x64, liquidity, amount_in, true)
//     }
// }

// /// Gets the next sqrt price given an output amount of token0 or token1
// ///
// /// Throws if price or liquidity are 0 or the next price is out of bounds
// ///
// pub fn get_next_sqrt_price_from_output(
//     sqrt_price_x64: u128,
//     liquidity: u128,
//     amount_out: u64,
//     zero_for_one: bool,
// ) -> u128 {
//     assert!(sqrt_price_x64 > 0);
//     assert!(liquidity > 0);

//     if zero_for_one {
//         get_next_sqrt_price_from_amount_1_rounding_down(
//             sqrt_price_x64,
//             liquidity,
//             amount_out,
//             false,
//         )
//     } else {
//         get_next_sqrt_price_from_amount_0_rounding_up(sqrt_price_x64, liquidity, amount_out, false)
//     }
// }
