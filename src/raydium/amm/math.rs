use super::core::{Calculator, CheckedCeilDiv, SwapDirection};
use anyhow::Result;

pub fn swap_exact_amount(
    pc_vault_amount: u64,
    coin_vault_amount: u64,
    swap_fee_numerator: u64,
    swap_fee_denominator: u64,
    swap_direction: SwapDirection,
    amount_specified: u64,
    swap_base_in: bool,
) -> Result<u128> {
    let other_amount_threshold = if swap_base_in {
        let swap_fee = u128::from(amount_specified)
            .checked_mul(swap_fee_numerator.into())
            .unwrap()
            .checked_ceil_div(swap_fee_denominator.into())
            .unwrap();
        let swap_in_after_deduct_fee = u128::from(amount_specified).checked_sub(swap_fee).unwrap();
        Calculator::swap_token_amount_base_in(
            swap_in_after_deduct_fee,
            pc_vault_amount.into(),
            coin_vault_amount.into(),
            swap_direction,
        )
    } else {
        let swap_in_before_add_fee = Calculator::swap_token_amount_base_out(
            amount_specified.into(),
            pc_vault_amount.into(),
            coin_vault_amount.into(),
            swap_direction,
        );
        swap_in_before_add_fee
            .checked_mul(swap_fee_denominator.into())
            .unwrap()
            .checked_ceil_div(
                (swap_fee_denominator
                    .checked_sub(swap_fee_numerator)
                    .unwrap())
                .into(),
            )
            .unwrap()
    };

    Ok(other_amount_threshold)
}
