#![allow(clippy::manual_div_ceil)]

use anyhow::{Context, Result};
use solana_pubkey::Pubkey;
use solana_sdk::instruction::{AccountMeta, Instruction};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::str::FromStr;
use uint::construct_uint;

construct_uint! {
    pub struct U256(4);
}

#[allow(dead_code)] // execution wiring will use this constant next
pub const METEORA_DAMM_PROGRAM_ID_STR: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";
pub const METEORA_DAMM_POOL_DISCRIMINATOR: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];
pub const METEORA_DAMM_SWAP2_IX_DISCRIMINATOR: [u8; 8] = [65, 75, 63, 76, 235, 91, 91, 136];
pub const METEORA_DAMM_POOL_AUTHORITY_STR: &str = "HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC";

const FEE_DENOMINATOR: u64 = 1_000_000_000;
const MAX_FEE_NUMERATOR_V0: u64 = 500_000_000;
const MAX_FEE_NUMERATOR_V1: u64 = 990_000_000;
const BASIS_POINT_MAX: u64 = 10_000;
const ONE_Q64: u128 = 1u128 << 64;
const DYNAMIC_FEE_SCALING_FACTOR: u128 = 100_000_000_000;
const DYNAMIC_FEE_ROUNDING_OFFSET: u128 = 99_999_999_999;
const SCALE_SHIFT: u32 = 128; // SCALE_OFFSET(64) * 2

// Account offsets (absolute, including 8-byte discriminator) for cp_amm::state::Pool zero-copy.
const POOL_FEES_OFFSET: usize = 8;
const POOL_FEES_LEN: usize = 160;
const TOKEN_A_MINT_OFFSET: usize = 168;
const TOKEN_B_MINT_OFFSET: usize = 200;
const TOKEN_A_VAULT_OFFSET: usize = 232;
const TOKEN_B_VAULT_OFFSET: usize = 264;
const PARTNER_OFFSET: usize = 328;
const LIQUIDITY_OFFSET: usize = 360;
const SQRT_MIN_PRICE_OFFSET: usize = 424;
const SQRT_MAX_PRICE_OFFSET: usize = 440;
const SQRT_PRICE_OFFSET: usize = 456;
const ACTIVATION_POINT_OFFSET: usize = 472;
const ACTIVATION_TYPE_OFFSET: usize = 480;
const POOL_STATUS_OFFSET: usize = 481;
const COLLECT_FEE_MODE_OFFSET: usize = 484;
const VERSION_OFFSET: usize = 486;
const POOL_MIN_LEN: usize = 488;

// pool_fees (relative to POOL_FEES_OFFSET)
const BASE_FEE_INFO_OFFSET: usize = 0; // 32 bytes
const PROTOCOL_FEE_PERCENT_OFFSET: usize = 40;
const PARTNER_FEE_PERCENT_OFFSET: usize = 41;
const REFERRAL_FEE_PERCENT_OFFSET: usize = 42;
const DYNAMIC_FEE_OFFSET: usize = 48; // 96 bytes
const INIT_SQRT_PRICE_OFFSET: usize = 144;

// base_fee_info.data pod-aligned fee schedulers share these prefixes
const BASE_FEE_CLIFF_NUMERATOR_OFFSET: usize = 0;
const BASE_FEE_MODE_OFFSET: usize = 8;
const BASE_FEE_U16_0_OFFSET: usize = 14;
const BASE_FEE_U32_0_OFFSET: usize = 16;
const BASE_FEE_U32_1_OFFSET: usize = 20;
const BASE_FEE_U64_0_OFFSET: usize = 16;
const BASE_FEE_U64_1_OFFSET: usize = 24;

// dynamic_fee (relative to DYNAMIC_FEE_OFFSET)
const DYN_INITIALIZED_OFFSET: usize = 0;
const DYN_VARIABLE_FEE_CONTROL_OFFSET: usize = 12;
const DYN_BIN_STEP_OFFSET: usize = 16;
const DYN_VOLATILITY_ACCUMULATOR_OFFSET: usize = 64;
const SWAP2_MODE_EXACT_IN: u8 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MeteoraDammBaseFee {
    FeeTimeScheduler {
        number_of_period: u16,
        period_frequency: u64,
        reduction_factor: u64,
        mode: u8,
    },
    RateLimiter {
        fee_increment_bps: u16,
        max_limiter_duration: u32,
        max_fee_bps: u32,
        reference_amount: u64,
    },
    FeeMarketCapScheduler {
        number_of_period: u16,
        sqrt_price_step_bps: u32,
        scheduler_expiration_duration: u32,
        reduction_factor: u64,
        mode: u8,
    },
    Unknown {
        mode: u8,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MeteoraDammFeeApprox {
    pub cliff_fee_numerator: u64,
    pub base_fee_mode: u8,
    pub base_fee: MeteoraDammBaseFee,
    pub protocol_fee_percent: u8,
    pub partner_fee_percent: u8,
    pub referral_fee_percent: u8,
    pub dynamic_initialized: u8,
    pub dynamic_variable_fee_control: u32,
    pub dynamic_bin_step: u16,
    pub dynamic_volatility_accumulator: u128,
    pub init_sqrt_price: u128,
}

#[derive(Clone, Debug)]
pub struct MeteoraDammStatic {
    pub token_a_mint: Pubkey,
    pub token_b_mint: Pubkey,
    #[allow(dead_code)] // execution wiring later
    pub token_a_vault: Pubkey,
    #[allow(dead_code)] // execution wiring later
    pub token_b_vault: Pubkey,
    #[allow(dead_code)] // execution wiring later
    pub partner: Pubkey,
    #[allow(dead_code)] // execution wiring later
    pub token_a_program: Pubkey,
    #[allow(dead_code)] // execution wiring later
    pub token_b_program: Pubkey,
    #[allow(dead_code)] // quote logging / execution later
    pub token_a_decimals: u8,
    #[allow(dead_code)] // quote logging / execution later
    pub token_b_decimals: u8,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MeteoraDammSnapshot {
    pub token_a_vault_amount: u64,
    pub token_b_vault_amount: u64,
    pub liquidity: u128,
    pub sqrt_min_price: u128,
    pub sqrt_max_price: u128,
    pub sqrt_price: u128,
    pub activation_point: u64,
    pub activation_type: u8,
    pub pool_status: u8,
    pub collect_fee_mode: u8,
    pub version: u8,
    pub fee_approx: MeteoraDammFeeApprox,
    pub last_observed_slot: u64,
}

fn meteora_damm_program_id() -> Result<Pubkey> {
    Pubkey::from_str(METEORA_DAMM_PROGRAM_ID_STR).context("invalid meteora damm program id")
}

fn meteora_damm_pool_authority() -> Result<Pubkey> {
    Pubkey::from_str(METEORA_DAMM_POOL_AUTHORITY_STR).context("invalid meteora damm pool authority")
}

fn meteora_damm_event_authority(program_id: Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"__event_authority"], &program_id).0
}

pub fn build_swap2_exact_in_ix(
    payer: Pubkey,
    pool_id: Pubkey,
    st: &MeteoraDammStatic,
    input_mint: Pubkey,
    amount_in: u64,
    min_amount_out: u64,
) -> Result<Instruction> {
    let program_id = meteora_damm_program_id()?;
    let pool_authority = meteora_damm_pool_authority()?;
    let event_authority = meteora_damm_event_authority(program_id);

    let (input_token_program, output_token_program) = if input_mint == st.token_a_mint {
        (st.token_a_program, st.token_b_program)
    } else if input_mint == st.token_b_mint {
        (st.token_b_program, st.token_a_program)
    } else {
        anyhow::bail!("input mint not in meteora damm pool");
    };

    let user_in_ata =
        get_associated_token_address_with_program_id(&payer, &input_mint, &input_token_program);
    let output_mint = if input_mint == st.token_a_mint {
        st.token_b_mint
    } else {
        st.token_a_mint
    };
    let user_out_ata =
        get_associated_token_address_with_program_id(&payer, &output_mint, &output_token_program);

    let accounts = vec![
        AccountMeta::new_readonly(pool_authority, false), // pool_authority
        AccountMeta::new(pool_id, false),                 // pool
        AccountMeta::new(user_in_ata, false),             // input_token_account
        AccountMeta::new(user_out_ata, false),            // output_token_account
        AccountMeta::new(st.token_a_vault, false),        // token_a_vault
        AccountMeta::new(st.token_b_vault, false),        // token_b_vault
        AccountMeta::new_readonly(st.token_a_mint, false), // token_a_mint
        AccountMeta::new_readonly(st.token_b_mint, false), // token_b_mint
        AccountMeta::new_readonly(payer, true),           // payer
        AccountMeta::new_readonly(st.token_a_program, false), // token_a_program
        AccountMeta::new_readonly(st.token_b_program, false), // token_b_program
        // Anchor optional account placeholder (referral_token_account = None).
        AccountMeta::new_readonly(program_id, false),
        AccountMeta::new_readonly(event_authority, false), // event_authority
        AccountMeta::new_readonly(program_id, false),      // program
    ];

    let mut data = Vec::with_capacity(8 + 8 + 8 + 1);
    data.extend_from_slice(&METEORA_DAMM_SWAP2_IX_DISCRIMINATOR);
    data.extend_from_slice(&amount_in.to_le_bytes()); // amount_0 (exact-in input)
    data.extend_from_slice(&min_amount_out.to_le_bytes()); // amount_1 (min out)
    data.push(SWAP2_MODE_EXACT_IN);

    Ok(Instruction {
        program_id,
        accounts,
        data,
    })
}

fn read_pubkey_at(data: &[u8], offset: usize) -> Result<Pubkey> {
    let end = offset.checked_add(32).context("pubkey offset overflow")?;
    let bytes: [u8; 32] = data
        .get(offset..end)
        .context("pubkey slice out of bounds")?
        .try_into()
        .context("invalid pubkey slice len")?;
    Ok(Pubkey::new_from_array(bytes))
}

fn read_u8_at(data: &[u8], offset: usize) -> Result<u8> {
    data.get(offset).copied().context("u8 out of bounds")
}

fn read_u16_le_at(data: &[u8], offset: usize) -> Result<u16> {
    let end = offset.checked_add(2).context("u16 offset overflow")?;
    let bytes: [u8; 2] = data
        .get(offset..end)
        .context("u16 slice out of bounds")?
        .try_into()
        .context("invalid u16 slice len")?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32_le_at(data: &[u8], offset: usize) -> Result<u32> {
    let end = offset.checked_add(4).context("u32 offset overflow")?;
    let bytes: [u8; 4] = data
        .get(offset..end)
        .context("u32 slice out of bounds")?
        .try_into()
        .context("invalid u32 slice len")?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64_le_at(data: &[u8], offset: usize) -> Result<u64> {
    let end = offset.checked_add(8).context("u64 offset overflow")?;
    let bytes: [u8; 8] = data
        .get(offset..end)
        .context("u64 slice out of bounds")?
        .try_into()
        .context("invalid u64 slice len")?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u128_le_at(data: &[u8], offset: usize) -> Result<u128> {
    let end = offset.checked_add(16).context("u128 offset overflow")?;
    let bytes: [u8; 16] = data
        .get(offset..end)
        .context("u128 slice out of bounds")?
        .try_into()
        .context("invalid u128 slice len")?;
    Ok(u128::from_le_bytes(bytes))
}

pub fn parse_pool_static_layout(data: &[u8]) -> Result<MeteoraDammStaticLayout> {
    if data.len() < POOL_MIN_LEN {
        anyhow::bail!(
            "invalid meteora damm pool account len={}, need at least {}",
            data.len(),
            POOL_MIN_LEN
        );
    }
    if data.get(..8) != Some(&METEORA_DAMM_POOL_DISCRIMINATOR) {
        anyhow::bail!("unexpected meteora damm pool discriminator");
    }

    let fee_approx = parse_fee_approx(data)?;

    Ok(MeteoraDammStaticLayout {
        token_a_mint: read_pubkey_at(data, TOKEN_A_MINT_OFFSET)?,
        token_b_mint: read_pubkey_at(data, TOKEN_B_MINT_OFFSET)?,
        token_a_vault: read_pubkey_at(data, TOKEN_A_VAULT_OFFSET)?,
        token_b_vault: read_pubkey_at(data, TOKEN_B_VAULT_OFFSET)?,
        partner: read_pubkey_at(data, PARTNER_OFFSET)?,
        liquidity: read_u128_le_at(data, LIQUIDITY_OFFSET)?,
        sqrt_min_price: read_u128_le_at(data, SQRT_MIN_PRICE_OFFSET)?,
        sqrt_max_price: read_u128_le_at(data, SQRT_MAX_PRICE_OFFSET)?,
        sqrt_price: read_u128_le_at(data, SQRT_PRICE_OFFSET)?,
        activation_point: read_u64_le_at(data, ACTIVATION_POINT_OFFSET)?,
        activation_type: read_u8_at(data, ACTIVATION_TYPE_OFFSET)?,
        pool_status: read_u8_at(data, POOL_STATUS_OFFSET)?,
        collect_fee_mode: read_u8_at(data, COLLECT_FEE_MODE_OFFSET)?,
        version: read_u8_at(data, VERSION_OFFSET)?,
        fee_approx,
    })
}

#[derive(Clone, Copy, Debug)]
pub struct MeteoraDammStaticLayout {
    pub token_a_mint: Pubkey,
    pub token_b_mint: Pubkey,
    pub token_a_vault: Pubkey,
    pub token_b_vault: Pubkey,
    pub partner: Pubkey,
    pub liquidity: u128,
    pub sqrt_min_price: u128,
    pub sqrt_max_price: u128,
    pub sqrt_price: u128,
    pub activation_point: u64,
    pub activation_type: u8,
    pub pool_status: u8,
    pub collect_fee_mode: u8,
    pub version: u8,
    pub fee_approx: MeteoraDammFeeApprox,
}

#[derive(Clone, Copy, Debug)]
pub struct MeteoraDammPoolQuoteState {
    pub liquidity: u128,
    pub sqrt_min_price: u128,
    pub sqrt_max_price: u128,
    pub sqrt_price: u128,
    pub activation_point: u64,
    pub activation_type: u8,
    pub pool_status: u8,
    pub collect_fee_mode: u8,
    pub version: u8,
    pub fee_approx: MeteoraDammFeeApprox,
}

fn parse_fee_approx(data: &[u8]) -> Result<MeteoraDammFeeApprox> {
    let pf = POOL_FEES_OFFSET;
    let pool_fees = data
        .get(pf..pf + POOL_FEES_LEN)
        .context("pool_fees slice out of bounds")?;

    let base_fee_info = pool_fees
        .get(BASE_FEE_INFO_OFFSET..BASE_FEE_INFO_OFFSET + 32)
        .context("base_fee_info slice out of bounds")?;

    let cliff_fee_numerator = read_u64_le_at(base_fee_info, BASE_FEE_CLIFF_NUMERATOR_OFFSET)?;
    let base_fee_mode = read_u8_at(base_fee_info, BASE_FEE_MODE_OFFSET)?;
    let base_fee = parse_base_fee_info(base_fee_info, cliff_fee_numerator, base_fee_mode)?;

    let dyn_off = DYNAMIC_FEE_OFFSET;
    let dyn_slice = pool_fees
        .get(dyn_off..dyn_off + 96)
        .context("dynamic_fee slice out of bounds")?;

    Ok(MeteoraDammFeeApprox {
        cliff_fee_numerator,
        base_fee_mode,
        base_fee,
        protocol_fee_percent: read_u8_at(pool_fees, PROTOCOL_FEE_PERCENT_OFFSET)?,
        partner_fee_percent: read_u8_at(pool_fees, PARTNER_FEE_PERCENT_OFFSET)?,
        referral_fee_percent: read_u8_at(pool_fees, REFERRAL_FEE_PERCENT_OFFSET)?,
        dynamic_initialized: read_u8_at(dyn_slice, DYN_INITIALIZED_OFFSET)?,
        dynamic_variable_fee_control: read_u32_le_at(dyn_slice, DYN_VARIABLE_FEE_CONTROL_OFFSET)?,
        dynamic_bin_step: read_u16_le_at(dyn_slice, DYN_BIN_STEP_OFFSET)?,
        dynamic_volatility_accumulator: read_u128_le_at(
            dyn_slice,
            DYN_VOLATILITY_ACCUMULATOR_OFFSET,
        )?,
        init_sqrt_price: read_u128_le_at(pool_fees, INIT_SQRT_PRICE_OFFSET)?,
    })
}

fn parse_base_fee_info(
    base_fee_info: &[u8],
    cliff_fee_numerator: u64,
    base_fee_mode: u8,
) -> Result<MeteoraDammBaseFee> {
    let parsed = match base_fee_mode {
        // FeeTimeSchedulerLinear / FeeTimeSchedulerExponential
        0 | 1 => MeteoraDammBaseFee::FeeTimeScheduler {
            number_of_period: read_u16_le_at(base_fee_info, BASE_FEE_U16_0_OFFSET)?,
            period_frequency: read_u64_le_at(base_fee_info, BASE_FEE_U64_0_OFFSET)?,
            reduction_factor: read_u64_le_at(base_fee_info, BASE_FEE_U64_1_OFFSET)?,
            mode: base_fee_mode,
        },
        // RateLimiter
        2 => MeteoraDammBaseFee::RateLimiter {
            fee_increment_bps: read_u16_le_at(base_fee_info, BASE_FEE_U16_0_OFFSET)?,
            max_limiter_duration: read_u32_le_at(base_fee_info, BASE_FEE_U32_0_OFFSET)?,
            max_fee_bps: read_u32_le_at(base_fee_info, BASE_FEE_U32_1_OFFSET)?,
            reference_amount: read_u64_le_at(base_fee_info, BASE_FEE_U64_1_OFFSET)?,
        },
        // FeeMarketCapSchedulerLinear / FeeMarketCapSchedulerExponential
        3 | 4 => MeteoraDammBaseFee::FeeMarketCapScheduler {
            number_of_period: read_u16_le_at(base_fee_info, BASE_FEE_U16_0_OFFSET)?,
            sqrt_price_step_bps: read_u32_le_at(base_fee_info, BASE_FEE_U32_0_OFFSET)?,
            scheduler_expiration_duration: read_u32_le_at(base_fee_info, BASE_FEE_U32_1_OFFSET)?,
            reduction_factor: read_u64_le_at(base_fee_info, BASE_FEE_U64_1_OFFSET)?,
            mode: base_fee_mode,
        },
        _ => {
            let _ = cliff_fee_numerator; // retained for debugging parity if needed later
            MeteoraDammBaseFee::Unknown {
                mode: base_fee_mode,
            }
        }
    };
    Ok(parsed)
}

pub fn parse_pool_quote_state(data: &[u8]) -> Result<MeteoraDammPoolQuoteState> {
    if data.len() < POOL_MIN_LEN {
        anyhow::bail!(
            "invalid meteora damm pool account len={}, need at least {}",
            data.len(),
            POOL_MIN_LEN
        );
    }
    if data.get(..8) != Some(&METEORA_DAMM_POOL_DISCRIMINATOR) {
        anyhow::bail!("unexpected meteora damm pool discriminator");
    }

    Ok(MeteoraDammPoolQuoteState {
        liquidity: read_u128_le_at(data, LIQUIDITY_OFFSET)?,
        sqrt_min_price: read_u128_le_at(data, SQRT_MIN_PRICE_OFFSET)?,
        sqrt_max_price: read_u128_le_at(data, SQRT_MAX_PRICE_OFFSET)?,
        sqrt_price: read_u128_le_at(data, SQRT_PRICE_OFFSET)?,
        activation_point: read_u64_le_at(data, ACTIVATION_POINT_OFFSET)?,
        activation_type: read_u8_at(data, ACTIVATION_TYPE_OFFSET)?,
        pool_status: read_u8_at(data, POOL_STATUS_OFFSET)?,
        collect_fee_mode: read_u8_at(data, COLLECT_FEE_MODE_OFFSET)?,
        version: read_u8_at(data, VERSION_OFFSET)?,
        fee_approx: parse_fee_approx(data)?,
    })
}

fn max_fee_numerator(version: u8) -> u64 {
    match version {
        0 => MAX_FEE_NUMERATOR_V0,
        _ => MAX_FEE_NUMERATOR_V1,
    }
}

fn current_unix_timestamp_u64() -> u64 {
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => d.as_secs(),
        Err(_) => 0,
    }
}

fn current_point_for_snapshot(snap: &MeteoraDammSnapshot) -> u64 {
    match snap.activation_type {
        0 => {
            if snap.last_observed_slot != 0 {
                snap.last_observed_slot
            } else {
                snap.activation_point
            }
        }
        1 => current_unix_timestamp_u64(),
        _ => snap.activation_point,
    }
}

fn mul_div_u64_floor(x: u64, y: u64, d: u64) -> Result<u64> {
    let out = (U256::from(x) * U256::from(y)) / U256::from(d);
    u64::try_from(out.low_u128()).context("u64 overflow in floor mul_div")
}

fn to_numerator_bps(bps: u64) -> Result<u64> {
    mul_div_u64_floor(bps, FEE_DENOMINATOR, BASIS_POINT_MAX)
}

fn mul_q64_floor(x: U256, y: U256) -> U256 {
    (x * y) >> 64
}

fn pow_q64_floor(mut base_q64: U256, mut exp: u16) -> U256 {
    let mut result = U256::from(1u128 << 64);
    while exp != 0 {
        if (exp & 1) != 0 {
            result = mul_q64_floor(result, base_q64);
        }
        exp >>= 1;
        if exp != 0 {
            base_q64 = mul_q64_floor(base_q64, base_q64);
        }
    }
    result
}

fn fee_in_period_exponential(
    cliff_fee_numerator: u64,
    reduction_factor_bps: u64,
    period: u16,
) -> u64 {
    if period == 0 {
        return cliff_fee_numerator;
    }
    let bps_q64 = (U256::from(reduction_factor_bps) << 64) / U256::from(BASIS_POINT_MAX);
    let one_q64 = U256::from(ONE_Q64);
    let base = one_q64.saturating_sub(bps_q64);
    let result_q64 = pow_q64_floor(base, period);
    let out = (U256::from(cliff_fee_numerator) * result_q64) / one_q64;
    u64::try_from(out.low_u128()).unwrap_or(0)
}

fn base_fee_time_from_included(
    cliff_fee_numerator: u64,
    mode: u8,
    number_of_period: u16,
    period_frequency: u64,
    reduction_factor: u64,
    current_point: u64,
    activation_point: u64,
) -> Result<u64> {
    if period_frequency == 0 {
        return Ok(cliff_fee_numerator);
    }
    let mut period = if current_point < activation_point {
        number_of_period as u64
    } else {
        current_point
            .checked_sub(activation_point)
            .context("current_point < activation_point underflow")?
            / period_frequency
    };
    period = period.min(number_of_period as u64);
    match mode {
        0 => cliff_fee_numerator
            .checked_sub(reduction_factor.saturating_mul(period))
            .context("time scheduler linear underflow"),
        1 => {
            let p = u16::try_from(period).context("time scheduler period overflow")?;
            Ok(fee_in_period_exponential(
                cliff_fee_numerator,
                reduction_factor,
                p,
            ))
        }
        _ => Ok(cliff_fee_numerator),
    }
}

#[allow(clippy::too_many_arguments)]
fn base_fee_market_cap_from_included(
    cliff_fee_numerator: u64,
    mode: u8,
    number_of_period: u16,
    sqrt_price_step_bps: u32,
    scheduler_expiration_duration: u32,
    reduction_factor: u64,
    current_point: u64,
    activation_point: u64,
    init_sqrt_price: u128,
    current_sqrt_price: u128,
) -> Result<u64> {
    let scheduler_expiration_point = activation_point
        .checked_add(scheduler_expiration_duration as u64)
        .context("scheduler_expiration_point overflow")?;

    let mut period =
        if current_point > scheduler_expiration_point || current_point < activation_point {
            number_of_period as u64
        } else if current_sqrt_price <= init_sqrt_price {
            0
        } else {
            let passed = (U256::from(current_sqrt_price - init_sqrt_price)
                * U256::from(BASIS_POINT_MAX))
                / U256::from(init_sqrt_price)
                / U256::from(sqrt_price_step_bps);
            let passed_u64 = u64::try_from(passed.low_u128()).unwrap_or(u64::MAX);
            passed_u64.min(number_of_period as u64)
        };
    period = period.min(number_of_period as u64);

    match mode {
        3 => cliff_fee_numerator
            .checked_sub(reduction_factor.saturating_mul(period))
            .context("market-cap scheduler linear underflow"),
        4 => {
            let p = u16::try_from(period).context("market-cap scheduler period overflow")?;
            Ok(fee_in_period_exponential(
                cliff_fee_numerator,
                reduction_factor,
                p,
            ))
        }
        _ => Ok(cliff_fee_numerator),
    }
}

fn is_rate_limiter_applied(
    reference_amount: u64,
    max_limiter_duration: u32,
    max_fee_bps: u32,
    fee_increment_bps: u16,
    current_point: u64,
    activation_point: u64,
    a_to_b: bool,
) -> bool {
    let zero = reference_amount == 0
        && max_limiter_duration == 0
        && max_fee_bps == 0
        && fee_increment_bps == 0;
    if zero {
        return false;
    }
    if a_to_b {
        return false;
    }
    if current_point < activation_point {
        return false;
    }
    let last = activation_point.saturating_add(max_limiter_duration as u64);
    current_point <= last
}

fn rate_limiter_max_index(
    max_fee_bps: u32,
    cliff_fee_numerator: u64,
    fee_increment_bps: u16,
) -> Result<u64> {
    let max_fee_numerator = to_numerator_bps(max_fee_bps as u64)?;
    let delta = max_fee_numerator
        .checked_sub(cliff_fee_numerator)
        .context("cliff_fee_numerator > max_fee_numerator")?;
    let inc = to_numerator_bps(fee_increment_bps as u64)?;
    if inc == 0 {
        anyhow::bail!("fee_increment_numerator is zero");
    }
    Ok(delta / inc)
}

fn rate_limiter_fee_numerator_from_included(
    input_amount: u64,
    cliff_fee_numerator: u64,
    fee_increment_bps: u16,
    max_fee_bps: u32,
    reference_amount: u64,
) -> Result<u64> {
    if input_amount <= reference_amount {
        return Ok(cliff_fee_numerator);
    }

    let max_fee_numerator = to_numerator_bps(max_fee_bps as u64)?;
    let c = U256::from(cliff_fee_numerator);
    let input_minus_ref = input_amount
        .checked_sub(reference_amount)
        .context("input < reference")?;
    let a = U256::from(input_minus_ref / reference_amount);
    let b = U256::from(input_minus_ref % reference_amount);
    let max_index = U256::from(rate_limiter_max_index(
        max_fee_bps,
        cliff_fee_numerator,
        fee_increment_bps,
    )?);
    let i = U256::from(to_numerator_bps(fee_increment_bps as u64)?);
    let x0 = U256::from(reference_amount);
    let one = U256::from(1u8);
    let two = U256::from(2u8);

    let trading_fee_num = if a < max_index {
        let numerator1 = c + c * a + i * a * (a + one) / two;
        let numerator2 = c + i * (a + one);
        x0 * numerator1 + b * numerator2
    } else {
        let numerator1 = c + c * max_index + i * max_index * (max_index + one) / two;
        let first_fee = x0 * numerator1;
        let d = a - max_index;
        let left_amount = d * x0 + b;
        first_fee + left_amount * U256::from(max_fee_numerator)
    };

    let denom = U256::from(FEE_DENOMINATOR);
    let trading_fee = (trading_fee_num + denom - one) / denom;
    let trading_fee_u64 = u64::try_from(trading_fee.low_u128()).context("trading_fee overflow")?;
    ceil_mul_div_u64(trading_fee_u64, FEE_DENOMINATOR, input_amount)
}

fn base_fee_numerator_from_included_exact(
    snap: &MeteoraDammSnapshot,
    a_to_b: bool,
    included_amount: u64,
) -> Result<u64> {
    let current_point = current_point_for_snapshot(snap);
    match snap.fee_approx.base_fee {
        MeteoraDammBaseFee::FeeTimeScheduler {
            number_of_period,
            period_frequency,
            reduction_factor,
            mode,
        } => base_fee_time_from_included(
            snap.fee_approx.cliff_fee_numerator,
            mode,
            number_of_period,
            period_frequency,
            reduction_factor,
            current_point,
            snap.activation_point,
        ),
        MeteoraDammBaseFee::RateLimiter {
            fee_increment_bps,
            max_limiter_duration,
            max_fee_bps,
            reference_amount,
        } => {
            if is_rate_limiter_applied(
                reference_amount,
                max_limiter_duration,
                max_fee_bps,
                fee_increment_bps,
                current_point,
                snap.activation_point,
                a_to_b,
            ) {
                rate_limiter_fee_numerator_from_included(
                    included_amount,
                    snap.fee_approx.cliff_fee_numerator,
                    fee_increment_bps,
                    max_fee_bps,
                    reference_amount,
                )
            } else {
                Ok(snap.fee_approx.cliff_fee_numerator)
            }
        }
        MeteoraDammBaseFee::FeeMarketCapScheduler {
            number_of_period,
            sqrt_price_step_bps,
            scheduler_expiration_duration,
            reduction_factor,
            mode,
        } => base_fee_market_cap_from_included(
            snap.fee_approx.cliff_fee_numerator,
            mode,
            number_of_period,
            sqrt_price_step_bps,
            scheduler_expiration_duration,
            reduction_factor,
            current_point,
            snap.activation_point,
            snap.fee_approx.init_sqrt_price,
            snap.sqrt_price,
        ),
        MeteoraDammBaseFee::Unknown { .. } => Ok(snap.fee_approx.cliff_fee_numerator),
    }
}

fn dynamic_fee_numerator(fee: &MeteoraDammFeeApprox) -> u128 {
    if fee.dynamic_initialized == 0 {
        return 0;
    }
    let v = U256::from(fee.dynamic_volatility_accumulator);
    let bin = U256::from(fee.dynamic_bin_step);
    let vb = v * bin;
    let square = vb * vb;
    let out = U256::from(fee.dynamic_variable_fee_control) * square;
    let out =
        (out + U256::from(DYNAMIC_FEE_ROUNDING_OFFSET)) / U256::from(DYNAMIC_FEE_SCALING_FACTOR);
    u64::try_from(out.low_u128()).unwrap_or(u64::MAX) as u128
}

fn trade_fee_numerator_exact_in(
    snap: &MeteoraDammSnapshot,
    a_to_b: bool,
    amount_in_included: u64,
) -> Result<u64> {
    let base = u128::from(base_fee_numerator_from_included_exact(
        snap,
        a_to_b,
        amount_in_included,
    )?);
    let dyn_fee = dynamic_fee_numerator(&snap.fee_approx);
    let capped = (base + dyn_fee).min(u128::from(max_fee_numerator(snap.version)));
    Ok(capped as u64)
}

fn fee_mode_fees_on_input(collect_fee_mode: u8, a_to_b: bool) -> bool {
    // 0=BothToken, 1=OnlyB per cp-amm.
    match (collect_fee_mode, a_to_b) {
        (1, false) => true, // OnlyB + B->A
        _ => false,
    }
}

fn ceil_mul_div_u64(x: u64, y: u64, d: u64) -> Result<u64> {
    let x = U256::from(x);
    let y = U256::from(y);
    let d = U256::from(d);
    let q = (x * y) / d;
    let r = (x * y) % d;
    let out = if r.is_zero() { q } else { q + U256::from(1u8) };
    u64::try_from(out.low_u128()).context("u64 overflow in ceil_mul_div")
}

fn apply_fee_excluded_amount(amount_included: u64, fee_numerator: u64) -> Result<u64> {
    if fee_numerator == 0 {
        return Ok(amount_included);
    }
    if fee_numerator >= FEE_DENOMINATOR {
        anyhow::bail!("invalid fee numerator >= denominator");
    }
    let fee = ceil_mul_div_u64(amount_included, fee_numerator, FEE_DENOMINATOR)?;
    amount_included
        .checked_sub(fee)
        .context("fee exceeds amount in meteora damm quote")
}

fn mul_div_u256_round_up(x: U256, y: U256, d: U256) -> U256 {
    let prod = x * y;
    let q = prod / d;
    let r = prod % d;
    if r.is_zero() { q } else { q + U256::from(1u8) }
}

fn get_next_sqrt_price_from_input(
    sqrt_price: u128,
    liquidity: u128,
    amount_in: u64,
    a_to_b: bool,
) -> Result<u128> {
    if amount_in == 0 {
        return Ok(sqrt_price);
    }
    if liquidity == 0 || sqrt_price == 0 {
        anyhow::bail!("invalid pool state for meteora damm quote");
    }

    let s = U256::from(sqrt_price);
    let l = U256::from(liquidity);
    let a = U256::from(amount_in);

    let next = if a_to_b {
        // ceil(liquidity * sqrt / (liquidity + amount * sqrt))
        let den = l + a * s;
        if den.is_zero() {
            anyhow::bail!("zero denominator in damm a->b quote");
        }
        mul_div_u256_round_up(l, s, den)
    } else {
        // sqrt + floor((amount << 128) / liquidity)
        let q = (a << SCALE_SHIFT) / l;
        s + q
    };

    Ok(next.low_u128())
}

fn get_amount_b_from_liquidity_delta(
    lower_sqrt: u128,
    upper_sqrt: u128,
    liquidity: u128,
    round_up: bool,
) -> Result<u64> {
    let delta = upper_sqrt
        .checked_sub(lower_sqrt)
        .context("invalid sqrt ordering for amount_b")?;
    let prod = U256::from(liquidity) * U256::from(delta);
    let den = U256::from(1u8) << SCALE_SHIFT;
    let out = if round_up {
        let q = prod / den;
        let r = prod % den;
        if r.is_zero() { q } else { q + U256::from(1u8) }
    } else {
        prod / den
    };
    u64::try_from(out.low_u128()).context("amount_b overflow")
}

fn get_amount_a_from_liquidity_delta(
    lower_sqrt: u128,
    upper_sqrt: u128,
    liquidity: u128,
    round_up: bool,
) -> Result<u64> {
    let delta = upper_sqrt
        .checked_sub(lower_sqrt)
        .context("invalid sqrt ordering for amount_a")?;
    let den = U256::from(lower_sqrt) * U256::from(upper_sqrt);
    if den.is_zero() {
        anyhow::bail!("zero denominator in amount_a");
    }
    let out = if round_up {
        mul_div_u256_round_up(U256::from(liquidity), U256::from(delta), den)
    } else {
        (U256::from(liquidity) * U256::from(delta)) / den
    };
    u64::try_from(out.low_u128()).context("amount_a overflow")
}

pub fn quote_exact_in_approx(
    st: &MeteoraDammStatic,
    snap: &MeteoraDammSnapshot,
    input_mint: Pubkey,
    amount_in: u64,
) -> Result<u64> {
    if amount_in == 0 {
        return Ok(0);
    }
    if snap.pool_status != 0 {
        anyhow::bail!("meteora damm pool disabled");
    }

    let a_to_b = if input_mint == st.token_a_mint {
        true
    } else if input_mint == st.token_b_mint {
        false
    } else {
        anyhow::bail!("input mint not in meteora damm pool");
    };

    let fee_num = trade_fee_numerator_exact_in(snap, a_to_b, amount_in)?;
    let fees_on_input = fee_mode_fees_on_input(snap.collect_fee_mode, a_to_b);
    let actual_in = if fees_on_input {
        apply_fee_excluded_amount(amount_in, fee_num)?
    } else {
        amount_in
    };

    let next_sqrt =
        get_next_sqrt_price_from_input(snap.sqrt_price, snap.liquidity, actual_in, a_to_b)?;
    let raw_out = if a_to_b {
        if next_sqrt < snap.sqrt_min_price {
            anyhow::bail!("price range violation a->b");
        }
        get_amount_b_from_liquidity_delta(next_sqrt, snap.sqrt_price, snap.liquidity, false)?
    } else {
        if next_sqrt > snap.sqrt_max_price {
            anyhow::bail!("price range violation b->a");
        }
        get_amount_a_from_liquidity_delta(snap.sqrt_price, next_sqrt, snap.liquidity, false)?
    };

    if fees_on_input {
        Ok(raw_out)
    } else {
        apply_fee_excluded_amount(raw_out, fee_num)
    }
}
