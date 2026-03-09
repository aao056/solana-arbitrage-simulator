#[derive(Debug)]
pub struct SwapState {
    // the amount remaining to be swapped in/out of the input/output asset
    pub amount_specified_remaining: u64,
    // the amount already swapped out/in of the output/input asset
    pub amount_calculated: u64,
    // current sqrt(price)
    pub sqrt_price_x64: u128,
    // the tick associated with the current price
    pub tick: i32,
    // the current liquidity in range
    pub liquidity: u128,
}
#[derive(Default)]
pub struct StepComputations {
    // the price at the beginning of the step
    pub sqrt_price_start_x64: u128,
    // the next tick to swap to from the current tick in the swap direction
    pub tick_next: i32,
    // whether tick_next is initialized or not
    pub initialized: bool,
    // sqrt(price) for the next tick (1/0)
    pub sqrt_price_next_x64: u128,
    // how much is being swapped in in this step
    pub amount_in: u64,
    // how much is being swapped out
    pub amount_out: u64,
    // how much fee is being paid in
    pub fee_amount: u64,
}
