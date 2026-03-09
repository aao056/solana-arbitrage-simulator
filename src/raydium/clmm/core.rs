use super::{
    liquidity_math, tick_math,
    types::{StepComputations, SwapState},
};
use anchor_lang::AccountDeserialize;
use anyhow::Result;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::account::Account as CliAccount;
use std::ops::Neg;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{collections::VecDeque, ops::DerefMut};
use tokio::time::sleep;
use tracing::warn;

pub fn deserialize_anchor_from_bytes<T: AccountDeserialize>(bytes: &[u8]) -> Result<T> {
    let mut slice = bytes;
    Ok(T::try_deserialize(&mut slice)?)
}

pub fn deserialize_anchor_account<T: AccountDeserialize>(account: &CliAccount) -> Result<T> {
    let mut data: &[u8] = &account.data;
    T::try_deserialize(&mut data).map_err(Into::into)
}

pub fn derive_tick_array_window(
    raydium_v3_program: Pubkey,
    pool_id: Pubkey,
    pool_state: &raydium_amm_v3::states::PoolState,
    tickarray_bitmap_extension: &raydium_amm_v3::states::TickArrayBitmapExtension,
    k: usize,
) -> Vec<(i32, Pubkey)> {
    let (_is_init, cur_start) = match pool_state
        .get_first_initialized_tick_array(&Some(*tickarray_bitmap_extension), false)
    {
        Ok(v) => v,
        Err(err) => {
            tracing::debug!(
                pool = %pool_id,
                err = ?err,
                "CLMM derive_tick_array_window: no initialized tick array for direction"
            );
            return Vec::new();
        }
    };

    let derive = |start: i32| {
        (
            start,
            Pubkey::find_program_address(
                &[
                    raydium_amm_v3::states::TICK_ARRAY_SEED.as_bytes(),
                    pool_id.as_ref(),
                    &start.to_be_bytes(),
                ],
                &raydium_v3_program,
            )
            .0,
        )
    };

    let mut back: Vec<(i32, Pubkey)> = Vec::new();
    let mut idx = cur_start;

    for _ in 0..k {
        let prev = match pool_state.next_initialized_tick_array_start_index(
            &Some(*tickarray_bitmap_extension),
            idx,
            false,
        ) {
            Ok(v) => v,
            Err(err) => {
                tracing::debug!(
                    pool = %pool_id,
                    from_start = idx,
                    err = ?err,
                    "CLMM derive_tick_array_window: previous tick-array lookup failed"
                );
                break;
            }
        };
        let Some(prev_idx) = prev else {
            break;
        };

        idx = prev_idx;
        back.push(derive(idx));
    }

    back.reverse();

    let mut out: Vec<(i32, Pubkey)> = Vec::with_capacity(back.len() + 1 + k);
    out.extend(back);
    out.push(derive(cur_start));

    let mut idx = cur_start;
    for _ in 0..k {
        let next = match pool_state.next_initialized_tick_array_start_index(
            &Some(*tickarray_bitmap_extension),
            idx,
            true,
        ) {
            Ok(v) => v,
            Err(err) => {
                tracing::debug!(
                    pool = %pool_id,
                    from_start = idx,
                    err = ?err,
                    "CLMM derive_tick_array_window: next tick-array lookup failed"
                );
                break;
            }
        };
        let Some(next_idx) = next else {
            break;
        };

        idx = next_idx;
        out.push(derive(idx));
    }

    out
}

pub async fn load_tick_array_states(
    rpc_client: &RpcClient,
    tick_array_keys: &[Pubkey],
) -> anyhow::Result<VecDeque<raydium_amm_v3::states::TickArrayState>> {
    let accounts = rpc_client.get_multiple_accounts(tick_array_keys).await?;

    let mut tick_arrays = VecDeque::new();

    for acc in accounts {
        let acc = acc.ok_or_else(|| anyhow::anyhow!("tick array account missing"))?;

        let tick_array_state =
            deserialize_anchor_account::<raydium_amm_v3::states::TickArrayState>(&acc)?;

        tick_arrays.push_back(tick_array_state);
    }

    Ok(tick_arrays)
}

pub async fn load_tick_array_states_with_retry(
    rpc_client: &RpcClient,
    tick_array_keys: &[Pubkey],
    max_attempts: usize,
    base_delay_ms: u64,
) -> anyhow::Result<VecDeque<raydium_amm_v3::states::TickArrayState>> {
    let attempts = max_attempts.max(1);

    for attempt in 1..=attempts {
        match load_tick_array_states(rpc_client, tick_array_keys).await {
            Ok(v) => return Ok(v),
            Err(err) => {
                let should_retry = is_rate_limited_error(&err) && attempt < attempts;
                if !should_retry {
                    return Err(err);
                }

                let delay_ms = exponential_backoff_with_jitter(base_delay_ms, attempt as u32);
                warn!(
                    attempt,
                    attempts,
                    delay_ms,
                    err = ?err,
                    "CLMM tick array RPC load rate-limited; retrying"
                );
                sleep(Duration::from_millis(delay_ms)).await;
            }
        }
    }

    Err(anyhow::anyhow!(
        "unreachable retry state for tick array load"
    ))
}

fn is_rate_limited_error(err: &anyhow::Error) -> bool {
    let msg = err.to_string();
    let msg_lc = msg.to_ascii_lowercase();
    msg.contains("429") || msg_lc.contains("too many requests")
}

fn exponential_backoff_with_jitter(base_delay_ms: u64, attempt: u32) -> u64 {
    let factor = 1u64 << attempt.saturating_sub(1).min(5);
    let base = base_delay_ms.saturating_mul(factor);
    let jitter = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_millis() % 125))
        .unwrap_or(0);
    base.saturating_add(jitter)
}

#[allow(clippy::too_many_arguments)]
pub fn swap_compute(
    zero_for_one: bool,
    is_base_input: bool,
    is_pool_current_tick_array: bool,
    trade_fee_rate: u32,
    amount_specified: u64,
    current_vaild_tick_array_start_index: i32,
    sqrt_price_limit_x64: u128,
    pool_state: &raydium_amm_v3::states::PoolState,
    tickarray_bitmap_extension: &raydium_amm_v3::states::TickArrayBitmapExtension,
    tick_arrays: &mut VecDeque<raydium_amm_v3::states::TickArrayState>,
) -> Result<(u64, VecDeque<i32>), &'static str> {
    if amount_specified == 0 {
        return Result::Err("amountSpecified must not be 0");
    }
    let sqrt_price_limit_x64 = if sqrt_price_limit_x64 == 0 {
        if zero_for_one {
            tick_math::MIN_SQRT_PRICE_X64 + 1
        } else {
            tick_math::MAX_SQRT_PRICE_X64 - 1
        }
    } else {
        sqrt_price_limit_x64
    };
    if zero_for_one {
        if sqrt_price_limit_x64 < tick_math::MIN_SQRT_PRICE_X64 {
            return Result::Err("sqrt_price_limit_x64 must greater than MIN_SQRT_PRICE_X64");
        }
        if sqrt_price_limit_x64 >= pool_state.sqrt_price_x64 {
            return Result::Err("sqrt_price_limit_x64 must smaller than current");
        }
    } else {
        if sqrt_price_limit_x64 > tick_math::MAX_SQRT_PRICE_X64 {
            return Result::Err("sqrt_price_limit_x64 must smaller than MAX_SQRT_PRICE_X64");
        }
        if sqrt_price_limit_x64 <= pool_state.sqrt_price_x64 {
            return Result::Err("sqrt_price_limit_x64 must greater than current");
        }
    }
    let mut tick_match_current_tick_array = is_pool_current_tick_array;

    let mut state = SwapState {
        amount_specified_remaining: amount_specified,
        amount_calculated: 0,
        sqrt_price_x64: pool_state.sqrt_price_x64,
        tick: pool_state.tick_current,
        liquidity: pool_state.liquidity,
    };

    let mut tick_array_current = tick_arrays.pop_front().unwrap();
    if tick_array_current.start_tick_index != current_vaild_tick_array_start_index {
        return Result::Err("tick array start tick index does not match");
    }
    let mut tick_array_start_index_vec = VecDeque::new();
    tick_array_start_index_vec.push_back(tick_array_current.start_tick_index);
    let mut loop_count = 0;
    // loop across ticks until input liquidity is consumed, or the limit price is reached
    while state.amount_specified_remaining != 0
        && state.sqrt_price_x64 != sqrt_price_limit_x64
        && state.tick < tick_math::MAX_TICK
        && state.tick > tick_math::MIN_TICK
    {
        if loop_count > 100 {
            return Result::Err("loop_count limit");
        }
        let mut step = StepComputations {
            sqrt_price_start_x64: state.sqrt_price_x64,
            ..Default::default()
        };
        step.sqrt_price_start_x64 = state.sqrt_price_x64;
        // save the bitmap, and the tick account if it is initialized
        let mut next_initialized_tick = if let Some(tick_state) = tick_array_current
            .next_initialized_tick(state.tick, pool_state.tick_spacing, zero_for_one)
            .unwrap()
        {
            Box::new(*tick_state)
        } else if !tick_match_current_tick_array {
            tick_match_current_tick_array = true;
            Box::new(
                *tick_array_current
                    .first_initialized_tick(zero_for_one)
                    .unwrap(),
            )
        } else {
            Box::new(raydium_amm_v3::states::TickState::default())
        };
        if !next_initialized_tick.is_initialized() {
            let current_vaild_tick_array_start_index = pool_state
                .next_initialized_tick_array_start_index(
                    &Some(*tickarray_bitmap_extension),
                    current_vaild_tick_array_start_index,
                    zero_for_one,
                )
                .unwrap();
            tick_array_current = tick_arrays.pop_front().unwrap();
            if current_vaild_tick_array_start_index.is_none() {
                return Result::Err("tick array start tick index out of range limit");
            }
            if tick_array_current.start_tick_index != current_vaild_tick_array_start_index.unwrap()
            {
                return Result::Err("tick array start tick index does not match");
            }
            tick_array_start_index_vec.push_back(tick_array_current.start_tick_index);
            let mut first_initialized_tick = tick_array_current
                .first_initialized_tick(zero_for_one)
                .unwrap();

            *next_initialized_tick = *first_initialized_tick.deref_mut();
        }
        step.tick_next = next_initialized_tick.tick;
        step.initialized = next_initialized_tick.is_initialized();
        step.tick_next = step
            .tick_next
            .clamp(tick_math::MIN_TICK, tick_math::MAX_TICK);

        step.sqrt_price_next_x64 = tick_math::get_sqrt_price_at_tick(step.tick_next).unwrap();

        let target_price = if (zero_for_one && step.sqrt_price_next_x64 < sqrt_price_limit_x64)
            || (!zero_for_one && step.sqrt_price_next_x64 > sqrt_price_limit_x64)
        {
            sqrt_price_limit_x64
        } else {
            step.sqrt_price_next_x64
        };
        let swap_step = raydium_amm_v3::libraries::swap_math::compute_swap_step(
            state.sqrt_price_x64,
            target_price,
            state.liquidity,
            state.amount_specified_remaining,
            trade_fee_rate,
            is_base_input,
            zero_for_one,
            1,
        )
        .unwrap();
        state.sqrt_price_x64 = swap_step.sqrt_price_next_x64;
        step.amount_in = swap_step.amount_in;
        step.amount_out = swap_step.amount_out;
        step.fee_amount = swap_step.fee_amount;

        if is_base_input {
            state.amount_specified_remaining = state
                .amount_specified_remaining
                .checked_sub(step.amount_in + step.fee_amount)
                .unwrap();
            state.amount_calculated = state
                .amount_calculated
                .checked_add(step.amount_out)
                .unwrap();
        } else {
            state.amount_specified_remaining = state
                .amount_specified_remaining
                .checked_sub(step.amount_out)
                .unwrap();
            state.amount_calculated = state
                .amount_calculated
                .checked_add(step.amount_in + step.fee_amount)
                .unwrap();
        }

        if state.sqrt_price_x64 == step.sqrt_price_next_x64 {
            // if the tick is initialized, run the tick transition
            if step.initialized {
                let mut liquidity_net = next_initialized_tick.liquidity_net;
                if zero_for_one {
                    liquidity_net = liquidity_net.neg();
                }
                state.liquidity =
                    liquidity_math::add_delta(state.liquidity, liquidity_net).unwrap();
            }

            state.tick = if zero_for_one {
                step.tick_next - 1
            } else {
                step.tick_next
            };
        } else if state.sqrt_price_x64 != step.sqrt_price_start_x64 {
            // recompute unless we're on a lower tick boundary (i.e. already transitioned ticks), and haven't moved
            state.tick = tick_math::get_tick_at_sqrt_price(state.sqrt_price_x64).unwrap();
        }
        loop_count += 1;
    }

    Ok((state.amount_calculated, tick_array_start_index_vec))
}
