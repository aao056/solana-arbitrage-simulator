use anyhow::{Context, Result, anyhow};
use orca_whirlpools_client::{
    DynamicTick, Oracle, SwapV2Builder, Tick, TickArray, Whirlpool, get_tick_array_address,
};
use orca_whirlpools_core::{
    OracleFacade, TICK_ARRAY_SIZE, TickArrayFacade, TickArrays, TickFacade, TransferFee,
    WhirlpoolFacade, get_tick_array_start_tick_index, swap_quote_by_input_token,
};
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::instruction::Instruction;
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token_2022::extension::transfer_fee::TransferFeeConfig;
use spl_token_2022::extension::{BaseStateWithExtensions, StateWithExtensions};
use std::time::{SystemTime, UNIX_EPOCH};

pub const ORCA_TICK_WINDOW_LEN: usize = 5;

#[derive(Clone)]
pub struct OrcaWhirlpoolSnapshot {
    pub whirlpool: Whirlpool,
    // Order: [center, above +1, above +2, below -1, below -2]
    pub tick_arrays: [TickArrayFacade; ORCA_TICK_WINDOW_LEN],
    pub tick_array_pubkeys: [Pubkey; ORCA_TICK_WINDOW_LEN],
    pub tick_array_start_indexes: [i32; ORCA_TICK_WINDOW_LEN],
    pub token_program_a: Pubkey,
    pub token_program_b: Pubkey,
    pub oracle_pubkey: Pubkey,
    pub transfer_fee_a: Option<TransferFee>,
    pub transfer_fee_b: Option<TransferFee>,
    pub oracle: Option<OracleFacade>,
}

pub fn uninitialized_tick_array(start_tick_index: i32) -> TickArrayFacade {
    TickArrayFacade {
        start_tick_index,
        ticks: [TickFacade::default(); TICK_ARRAY_SIZE],
    }
}

fn tick_to_facade(tick: Tick) -> TickFacade {
    TickFacade {
        initialized: tick.initialized,
        liquidity_net: tick.liquidity_net,
        liquidity_gross: tick.liquidity_gross,
        fee_growth_outside_a: tick.fee_growth_outside_a,
        fee_growth_outside_b: tick.fee_growth_outside_b,
        reward_growths_outside: tick.reward_growths_outside,
    }
}

fn dynamic_tick_to_facade(tick: DynamicTick) -> TickFacade {
    match tick {
        DynamicTick::Uninitialized => TickFacade::default(),
        DynamicTick::Initialized(data) => TickFacade {
            initialized: true,
            liquidity_net: data.liquidity_net,
            liquidity_gross: data.liquidity_gross,
            fee_growth_outside_a: data.fee_growth_outside_a,
            fee_growth_outside_b: data.fee_growth_outside_b,
            reward_growths_outside: data.reward_growths_outside,
        },
    }
}

fn tick_array_to_facade(tick_array: TickArray) -> TickArrayFacade {
    match tick_array {
        TickArray::FixedTickArray(arr) => TickArrayFacade {
            start_tick_index: arr.start_tick_index,
            ticks: arr.ticks.map(tick_to_facade),
        },
        TickArray::DynamicTickArray(arr) => TickArrayFacade {
            start_tick_index: arr.start_tick_index,
            ticks: arr.ticks.map(dynamic_tick_to_facade),
        },
    }
}

pub fn decode_tick_array_facade(data: &[u8]) -> Option<TickArrayFacade> {
    let tick_array = TickArray::from_bytes(data).ok()?;
    Some(tick_array_to_facade(tick_array))
}

pub fn decode_tick_array_facade_or_default(
    data: &[u8],
    fallback_start_tick_index: i32,
) -> TickArrayFacade {
    decode_tick_array_facade(data)
        .unwrap_or_else(|| uninitialized_tick_array(fallback_start_tick_index))
}

fn whirlpool_to_facade(whirlpool: &Whirlpool) -> WhirlpoolFacade {
    WhirlpoolFacade {
        fee_tier_index_seed: whirlpool.fee_tier_index_seed,
        tick_spacing: whirlpool.tick_spacing,
        fee_rate: whirlpool.fee_rate,
        protocol_fee_rate: whirlpool.protocol_fee_rate,
        liquidity: whirlpool.liquidity,
        sqrt_price: whirlpool.sqrt_price,
        tick_current_index: whirlpool.tick_current_index,
        fee_growth_global_a: whirlpool.fee_growth_global_a,
        fee_growth_global_b: whirlpool.fee_growth_global_b,
        reward_last_updated_timestamp: whirlpool.reward_last_updated_timestamp,
        reward_infos: std::array::from_fn(|i| {
            let reward = &whirlpool.reward_infos[i];
            orca_whirlpools_core::WhirlpoolRewardInfoFacade {
                emissions_per_second_x64: reward.emissions_per_second_x64,
                growth_global_x64: reward.growth_global_x64,
            }
        }),
    }
}

pub fn oracle_to_facade(oracle: &Oracle) -> OracleFacade {
    OracleFacade {
        trade_enable_timestamp: oracle.trade_enable_timestamp,
        adaptive_fee_constants: orca_whirlpools_core::AdaptiveFeeConstantsFacade {
            filter_period: oracle.adaptive_fee_constants.filter_period,
            decay_period: oracle.adaptive_fee_constants.decay_period,
            reduction_factor: oracle.adaptive_fee_constants.reduction_factor,
            adaptive_fee_control_factor: oracle.adaptive_fee_constants.adaptive_fee_control_factor,
            max_volatility_accumulator: oracle.adaptive_fee_constants.max_volatility_accumulator,
            tick_group_size: oracle.adaptive_fee_constants.tick_group_size,
            major_swap_threshold_ticks: oracle.adaptive_fee_constants.major_swap_threshold_ticks,
        },
        adaptive_fee_variables: orca_whirlpools_core::AdaptiveFeeVariablesFacade {
            last_reference_update_timestamp: oracle
                .adaptive_fee_variables
                .last_reference_update_timestamp,
            last_major_swap_timestamp: oracle.adaptive_fee_variables.last_major_swap_timestamp,
            volatility_reference: oracle.adaptive_fee_variables.volatility_reference,
            tick_group_index_reference: oracle.adaptive_fee_variables.tick_group_index_reference,
            volatility_accumulator: oracle.adaptive_fee_variables.volatility_accumulator,
        },
    }
}

pub fn decode_oracle_facade(data: &[u8]) -> Option<OracleFacade> {
    let oracle = Oracle::from_bytes(data).ok()?;
    Some(oracle_to_facade(&oracle))
}

fn build_tick_array_indexes(tick_array_start_index: i32, start_tick_offset: i32) -> [i32; 5] {
    [
        tick_array_start_index,
        tick_array_start_index + start_tick_offset,
        tick_array_start_index + start_tick_offset * 2,
        tick_array_start_index - start_tick_offset,
        tick_array_start_index - start_tick_offset * 2,
    ]
}

pub fn get_tick_array_keys_and_indexes_from_whirlpool(
    pool_id: &Pubkey,
    whirlpool: &Whirlpool,
) -> Result<([Pubkey; ORCA_TICK_WINDOW_LEN], [i32; ORCA_TICK_WINDOW_LEN])> {
    let tick_array_start_index =
        get_tick_array_start_tick_index(whirlpool.tick_current_index, whirlpool.tick_spacing);
    let start_tick_offset = whirlpool.tick_spacing as i32 * TICK_ARRAY_SIZE as i32;
    let tick_array_indexes = build_tick_array_indexes(tick_array_start_index, start_tick_offset);

    let addresses: Result<Vec<Pubkey>> = tick_array_indexes
        .iter()
        .map(|&idx| {
            let (pk, _) = get_tick_array_address(pool_id, idx)?;
            Ok(pk)
        })
        .collect();
    let addresses = addresses?;
    let keys_array: [Pubkey; ORCA_TICK_WINDOW_LEN] = addresses
        .try_into()
        .map_err(|_| anyhow!("expected exactly {} tick arrays", ORCA_TICK_WINDOW_LEN))?;

    Ok((keys_array, tick_array_indexes))
}

pub async fn fetch_tick_arrays(
    rpc: &RpcClient,
    keys: &[Pubkey; ORCA_TICK_WINDOW_LEN],
    start_tick_indexes: &[i32; ORCA_TICK_WINDOW_LEN],
) -> Result<[TickArrayFacade; ORCA_TICK_WINDOW_LEN]> {
    let accounts = rpc
        .get_multiple_accounts(keys)
        .await
        .context("failed to fetch Orca tick arrays")?;

    let mut out: [TickArrayFacade; ORCA_TICK_WINDOW_LEN] = [
        uninitialized_tick_array(start_tick_indexes[0]),
        uninitialized_tick_array(start_tick_indexes[1]),
        uninitialized_tick_array(start_tick_indexes[2]),
        uninitialized_tick_array(start_tick_indexes[3]),
        uninitialized_tick_array(start_tick_indexes[4]),
    ];

    for (i, account) in accounts.iter().enumerate() {
        let Some(account) = account else {
            continue;
        };
        let Ok(tick_array) = TickArray::from_bytes(&account.data) else {
            continue;
        };
        out[i] = tick_array_to_facade(tick_array);
    }

    Ok(out)
}

pub fn transfer_fee_from_mint_account_data(owner: Pubkey, data: &[u8]) -> Option<TransferFee> {
    if owner != spl_token_2022::ID {
        return None;
    }

    let mint = StateWithExtensions::<spl_token_2022::state::Mint>::unpack(data).ok()?;
    let fee_cfg = mint.get_extension::<TransferFeeConfig>().ok()?;

    Some(TransferFee {
        fee_bps: u16::from(fee_cfg.newer_transfer_fee.transfer_fee_basis_points),
        max_fee: u64::from(fee_cfg.newer_transfer_fee.maximum_fee),
    })
}

pub async fn fetch_transfer_fee(
    rpc: &RpcClient,
    mint: Pubkey,
    token_program: Pubkey,
) -> Option<TransferFee> {
    let account = rpc.get_account(&mint).await.ok()?;
    transfer_fee_from_mint_account_data(token_program, &account.data)
}

pub async fn build_snapshot(
    rpc: &RpcClient,
    pool_id: Pubkey,
    whirlpool: Whirlpool,
    token_program_a: Pubkey,
    token_program_b: Pubkey,
    oracle_pubkey: Pubkey,
) -> Result<OrcaWhirlpoolSnapshot> {
    let (tick_array_pubkeys, tick_array_start_indexes) =
        get_tick_array_keys_and_indexes_from_whirlpool(&pool_id, &whirlpool)?;
    let tick_arrays =
        fetch_tick_arrays(rpc, &tick_array_pubkeys, &tick_array_start_indexes).await?;

    let transfer_fee_a = fetch_transfer_fee(rpc, whirlpool.token_mint_a, token_program_a).await;
    let transfer_fee_b = fetch_transfer_fee(rpc, whirlpool.token_mint_b, token_program_b).await;

    let oracle = match rpc.get_account(&oracle_pubkey).await {
        Ok(account) => Oracle::from_bytes(&account.data)
            .ok()
            .map(|oracle| oracle_to_facade(&oracle)),
        Err(_) => None,
    };

    Ok(OrcaWhirlpoolSnapshot {
        whirlpool,
        tick_arrays,
        tick_array_pubkeys,
        tick_array_start_indexes,
        token_program_a,
        token_program_b,
        oracle_pubkey,
        transfer_fee_a,
        transfer_fee_b,
        oracle,
    })
}

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn is_a_to_b(whirlpool: &Whirlpool, input_mint: Pubkey) -> Result<bool> {
    if input_mint == whirlpool.token_mint_a {
        Ok(true)
    } else if input_mint == whirlpool.token_mint_b {
        Ok(false)
    } else {
        anyhow::bail!("input mint not in Orca Whirlpool")
    }
}

pub fn quote_exact_in(
    snapshot: &OrcaWhirlpoolSnapshot,
    amount_in: u64,
    input_mint: Pubkey,
) -> Result<u64> {
    let a_to_b = is_a_to_b(&snapshot.whirlpool, input_mint)?;
    let tick_arrays = TickArrays::Five(
        snapshot.tick_arrays[0],
        snapshot.tick_arrays[1],
        snapshot.tick_arrays[2],
        snapshot.tick_arrays[3],
        snapshot.tick_arrays[4],
    );

    let quote = swap_quote_by_input_token(
        amount_in,
        a_to_b,
        0,
        whirlpool_to_facade(&snapshot.whirlpool),
        snapshot.oracle,
        tick_arrays,
        unix_timestamp(),
        snapshot.transfer_fee_a,
        snapshot.transfer_fee_b,
    )
    .map_err(|e| anyhow!("orca quote failed: {e:?}"))?;

    Ok(quote.token_est_out)
}

fn instruction_tick_array_pubkeys(snapshot: &OrcaWhirlpoolSnapshot, a_to_b: bool) -> [Pubkey; 3] {
    if a_to_b {
        // A->B walks downward in tick index.
        [
            snapshot.tick_array_pubkeys[0],
            snapshot.tick_array_pubkeys[3],
            snapshot.tick_array_pubkeys[4],
        ]
    } else {
        // B->A walks upward in tick index.
        [
            snapshot.tick_array_pubkeys[0],
            snapshot.tick_array_pubkeys[1],
            snapshot.tick_array_pubkeys[2],
        ]
    }
}

pub fn build_swap_exact_in_ix(
    payer: Pubkey,
    pool_id: Pubkey,
    snapshot: &OrcaWhirlpoolSnapshot,
    input_mint: Pubkey,
    amount_in: u64,
    min_amount_out: u64,
) -> Result<Instruction> {
    let a_to_b = is_a_to_b(&snapshot.whirlpool, input_mint)?;
    let tick_arrays = instruction_tick_array_pubkeys(snapshot, a_to_b);

    let token_owner_account_a = get_associated_token_address_with_program_id(
        &payer,
        &snapshot.whirlpool.token_mint_a,
        &snapshot.token_program_a,
    );
    let token_owner_account_b = get_associated_token_address_with_program_id(
        &payer,
        &snapshot.whirlpool.token_mint_b,
        &snapshot.token_program_b,
    );

    let ix = SwapV2Builder::new()
        .token_program_a(snapshot.token_program_a)
        .token_program_b(snapshot.token_program_b)
        .token_authority(payer)
        .whirlpool(pool_id)
        .token_mint_a(snapshot.whirlpool.token_mint_a)
        .token_mint_b(snapshot.whirlpool.token_mint_b)
        .token_owner_account_a(token_owner_account_a)
        .token_vault_a(snapshot.whirlpool.token_vault_a)
        .token_owner_account_b(token_owner_account_b)
        .token_vault_b(snapshot.whirlpool.token_vault_b)
        .tick_array0(tick_arrays[0])
        .tick_array1(tick_arrays[1])
        .tick_array2(tick_arrays[2])
        .oracle(snapshot.oracle_pubkey)
        .amount(amount_in)
        .other_amount_threshold(min_amount_out)
        .sqrt_price_limit(0)
        .amount_specified_is_input(true)
        .a_to_b(a_to_b)
        .instruction();

    Ok(ix)
}
