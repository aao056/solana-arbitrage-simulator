use anyhow::{Context, Result, anyhow};
use meteora::meteora_dlmm::types::{ActivationType, PairStatus, PairType};
use meteora::{
    BinArrayExtension, BinExtension, LbPairExtension, SwapResult, derive_bin_array_pda,
    derive_event_authority_pda, get_bin_array_pubkeys_for_swap, utils::decode_pod_anchor_account,
};
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::instruction::{AccountMeta, Instruction};
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

pub type MeteoraBinArray = meteora::meteora_dlmm::accounts::BinArray;
pub type MeteoraLbPair = meteora::meteora_dlmm::accounts::LbPair;
pub type MeteoraPubkey = meteora::solana_sdk::pubkey::Pubkey;

pub const DLMM_BIN_ARRAY_WINDOW_RADIUS_DEFAULT: i32 = 1;
pub const DLMM_BIN_ARRAY_WINDOW_RADIUS_MAX: i32 = 8;

const DLMM_SWAP_DISCRIMINATOR: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

#[derive(Clone)]
pub struct MeteoraDlmmSnapshot {
    pub lb_pair: MeteoraLbPair,
    pub center_bin_array_index: i32,
    pub window_bin_array_indices: Vec<i32>,
    pub window_bin_array_pubkeys: Vec<Pubkey>,
    pub bin_arrays_by_pubkey: HashMap<MeteoraPubkey, MeteoraBinArray>,
}

pub fn to_meteora_pubkey(pk: Pubkey) -> MeteoraPubkey {
    MeteoraPubkey::new_from_array(pk.to_bytes())
}

pub fn from_meteora_pubkey(pk: MeteoraPubkey) -> Pubkey {
    Pubkey::new_from_array(pk.to_bytes())
}

pub fn decode_lb_pair(data: &[u8]) -> Result<MeteoraLbPair> {
    decode_pod_anchor_account::<MeteoraLbPair>(data)
}

pub fn decode_bin_array(data: &[u8]) -> Result<MeteoraBinArray> {
    decode_pod_anchor_account::<MeteoraBinArray>(data)
}

fn is_skippable_bin_array_decode_error(err: &anyhow::Error) -> bool {
    let msg = format!("{err:#}").to_ascii_lowercase();
    msg.contains("account too small for type")
        || msg.contains("invalid account discriminator")
        || msg.contains("failed to fill whole buffer")
}

pub fn dlmm_bin_array_window_radius() -> i32 {
    static RADIUS: OnceLock<i32> = OnceLock::new();
    *RADIUS.get_or_init(|| {
        let parsed = std::env::var("ARB_DLMM_BIN_ARRAY_WINDOW_RADIUS")
            .ok()
            .and_then(|v| v.parse::<i32>().ok())
            .unwrap_or(DLMM_BIN_ARRAY_WINDOW_RADIUS_DEFAULT);
        parsed.clamp(1, DLMM_BIN_ARRAY_WINDOW_RADIUS_MAX)
    })
}

pub fn dlmm_bin_array_window_len() -> usize {
    usize::try_from(
        dlmm_bin_array_window_radius()
            .saturating_mul(2)
            .saturating_add(1),
    )
    .unwrap_or(3)
}

pub fn derive_window_bin_array_indices(center: i32) -> Result<Vec<i32>> {
    let radius = dlmm_bin_array_window_radius();
    let mut out = Vec::with_capacity(dlmm_bin_array_window_len());
    for d in -radius..=radius {
        out.push(center.checked_add(d).context("bin array index overflow")?);
    }
    Ok(out)
}

pub fn derive_window_bin_array_pubkeys(pool_id: Pubkey, indices: &[i32]) -> Vec<Pubkey> {
    let pool_id_m = to_meteora_pubkey(pool_id);
    indices
        .iter()
        .map(|idx| from_meteora_pubkey(derive_bin_array_pda(pool_id_m, i64::from(*idx)).0))
        .collect()
}

pub async fn build_snapshot(
    rpc: &RpcClient,
    pool_id: Pubkey,
    lb_pair: MeteoraLbPair,
) -> Result<MeteoraDlmmSnapshot> {
    let center_bin_array_index = MeteoraBinArray::bin_id_to_bin_array_index(lb_pair.active_id)?;
    let window_bin_array_indices = derive_window_bin_array_indices(center_bin_array_index)?;
    let window_bin_array_pubkeys =
        derive_window_bin_array_pubkeys(pool_id, &window_bin_array_indices);

    let accounts = rpc
        .get_multiple_accounts(&window_bin_array_pubkeys)
        .await
        .context("failed to fetch DLMM bin arrays")?;

    let mut bin_arrays_by_pubkey: HashMap<MeteoraPubkey, MeteoraBinArray> =
        HashMap::with_capacity(window_bin_array_pubkeys.len());
    let mut missing_non_center = 0usize;
    let mut missing_center_pk: Option<Pubkey> = None;

    for ((idx, pk), acc_opt) in window_bin_array_indices
        .iter()
        .copied()
        .zip(window_bin_array_pubkeys.iter())
        .zip(accounts.into_iter())
    {
        let Some(acc) = acc_opt else {
            if idx == center_bin_array_index {
                missing_center_pk = Some(*pk);
                continue;
            }
            missing_non_center = missing_non_center.saturating_add(1);
            continue;
        };
        if acc.data.is_empty() {
            if idx == center_bin_array_index {
                missing_center_pk = Some(*pk);
            } else {
                missing_non_center = missing_non_center.saturating_add(1);
            }
            continue;
        }
        let arr = match decode_pod_anchor_account::<MeteoraBinArray>(&acc.data) {
            Ok(v) => v,
            Err(e) => {
                if is_skippable_bin_array_decode_error(&e) {
                    if idx == center_bin_array_index {
                        missing_center_pk = Some(*pk);
                    } else {
                        missing_non_center = missing_non_center.saturating_add(1);
                    }
                    continue;
                }
                return Err(anyhow!("failed to decode DLMM bin array {pk}: {e:?}"));
            }
        };
        bin_arrays_by_pubkey.insert(to_meteora_pubkey(*pk), arr);
    }

    if bin_arrays_by_pubkey.is_empty() {
        if let Some(center_pk) = missing_center_pk {
            anyhow::bail!(
                "missing DLMM center bin array account {center_pk} (and no other watched bin arrays exist)"
            );
        }
        anyhow::bail!("missing all watched DLMM bin arrays for pool {pool_id}");
    }

    if let Some(center_pk) = missing_center_pk {
        tracing::debug!(
            pool = %pool_id,
            center_bin_array_index,
            center_bin_array = %center_pk,
            window_radius = dlmm_bin_array_window_radius(),
            available_bin_arrays = bin_arrays_by_pubkey.len(),
            "DLMM snapshot built without center bin array; relying on bitmap-guided traversal"
        );
    }

    if missing_non_center > 0 {
        tracing::debug!(
            pool = %pool_id,
            center_bin_array_index,
            window_radius = dlmm_bin_array_window_radius(),
            missing_non_center,
            total_window = window_bin_array_pubkeys.len(),
            "DLMM snapshot built with partial bin-array window"
        );
    }

    Ok(MeteoraDlmmSnapshot {
        lb_pair,
        center_bin_array_index,
        window_bin_array_indices,
        window_bin_array_pubkeys,
        bin_arrays_by_pubkey,
    })
}

fn shift_active_bin_if_empty_gap(
    lb_pair: &mut MeteoraLbPair,
    active_bin_array: &MeteoraBinArray,
    swap_for_y: bool,
) -> Result<()> {
    let lb_pair_bin_array_index = MeteoraBinArray::bin_id_to_bin_array_index(lb_pair.active_id)?;

    if i64::from(lb_pair_bin_array_index) != active_bin_array.index {
        if swap_for_y {
            let (_, upper_bin_id) =
                MeteoraBinArray::get_bin_array_lower_upper_bin_id(active_bin_array.index as i32)?;
            lb_pair.active_id = upper_bin_id;
        } else {
            let (lower_bin_id, _) =
                MeteoraBinArray::get_bin_array_lower_upper_bin_id(active_bin_array.index as i32)?;
            lb_pair.active_id = lower_bin_id;
        }
    }

    Ok(())
}

fn quote_exact_in_window(
    lb_pair_pubkey: MeteoraPubkey,
    lb_pair: &MeteoraLbPair,
    amount_in: u64,
    swap_for_y: bool,
    bin_arrays: HashMap<MeteoraPubkey, MeteoraBinArray>,
) -> Result<u64> {
    let mut lb_pair = *lb_pair;
    apply_quote_time_state(&mut lb_pair)?;
    let mut amount_left = amount_in;
    let mut total_amount_out: u64 = 0;

    while amount_left > 0 {
        let active_bin_array_pubkey =
            get_bin_array_pubkeys_for_swap(lb_pair_pubkey, &lb_pair, None, swap_for_y, 1)?
                .pop()
                .context("DLMM pool out of liquidity in watched window")?;

        let mut active_bin_array = bin_arrays
            .get(&active_bin_array_pubkey)
            .cloned()
            .context("active DLMM bin array not found in watched window")?;

        shift_active_bin_if_empty_gap(&mut lb_pair, &active_bin_array, swap_for_y)?;

        loop {
            if !active_bin_array.is_bin_id_within_range(lb_pair.active_id)? || amount_left == 0 {
                break;
            }

            lb_pair.update_volatility_accumulator()?;

            let active_bin = active_bin_array.get_bin_mut(lb_pair.active_id)?;
            let price = active_bin.get_or_store_bin_price(lb_pair.active_id, lb_pair.bin_step)?;

            if !active_bin.is_empty(!swap_for_y) {
                let SwapResult {
                    amount_in_with_fees,
                    amount_out,
                    ..
                } = active_bin.swap(amount_left, price, swap_for_y, &lb_pair, None)?;

                amount_left = amount_left
                    .checked_sub(amount_in_with_fees)
                    .context("overflow")?;
                total_amount_out = total_amount_out
                    .checked_add(amount_out)
                    .context("overflow")?;
            }

            if amount_left > 0 {
                lb_pair.advance_active_bin(swap_for_y)?;
            }
        }
    }

    Ok(total_amount_out)
}

fn apply_quote_time_state(lb_pair: &mut MeteoraLbPair) -> Result<()> {
    // Keep DLMM quote behavior closer to on-chain swap math by updating time-dependent
    // fee/reference state before traversing bins. This is still an approximation because
    // we don't yet pass full clock + mint account transfer-fee context like meteora::quote.
    let now_ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs();

    // Lightweight activation guard for timestamp-gated pools. Slot-gated pools remain
    // approximated until we thread slot/clock through the quote path.
    if !matches!(lb_pair.status()?, PairStatus::Enabled) {
        anyhow::bail!("DLMM pair is disabled");
    }
    if matches!(lb_pair.pair_type()?, PairType::Permission)
        && matches!(lb_pair.activation_type()?, ActivationType::Timestamp)
        && now_ts < lb_pair.activation_point
    {
        anyhow::bail!("DLMM pair not yet active (timestamp gate)");
    }

    lb_pair.update_references(now_ts as i64)?;
    Ok(())
}

pub fn quote_exact_in(
    snapshot: &MeteoraDlmmSnapshot,
    pool_id: Pubkey,
    amount_in: u64,
    input_mint: Pubkey,
) -> Result<u64> {
    let input_mint = to_meteora_pubkey(input_mint);

    let swap_for_y = if input_mint == snapshot.lb_pair.token_x_mint {
        true
    } else if input_mint == snapshot.lb_pair.token_y_mint {
        false
    } else {
        anyhow::bail!("input mint is not part of this DLMM pair");
    };

    quote_exact_in_window(
        to_meteora_pubkey(pool_id),
        &snapshot.lb_pair,
        amount_in,
        swap_for_y,
        snapshot.bin_arrays_by_pubkey.clone(),
    )
}

fn ordered_bin_arrays_for_direction(
    snapshot: &MeteoraDlmmSnapshot,
    swap_for_y: bool,
) -> Vec<Pubkey> {
    let mut by_idx: HashMap<i32, Pubkey> =
        HashMap::with_capacity(snapshot.window_bin_array_indices.len());
    for (idx, pk) in snapshot
        .window_bin_array_indices
        .iter()
        .copied()
        .zip(snapshot.window_bin_array_pubkeys.iter().copied())
    {
        by_idx.insert(idx, pk);
    }

    let c = snapshot.center_bin_array_index;
    let mut out = Vec::with_capacity(snapshot.window_bin_array_pubkeys.len());

    if let Some(pk) = by_idx.get(&c) {
        out.push(*pk);
    }

    if swap_for_y {
        if let Some(pk) = by_idx.get(&(c - 1)) {
            out.push(*pk);
        }
        if let Some(pk) = by_idx.get(&(c + 1)) {
            out.push(*pk);
        }
    } else {
        if let Some(pk) = by_idx.get(&(c + 1)) {
            out.push(*pk);
        }
        if let Some(pk) = by_idx.get(&(c - 1)) {
            out.push(*pk);
        }
    }

    for pk in snapshot.window_bin_array_pubkeys.iter().copied() {
        if !out.contains(&pk) {
            out.push(pk);
        }
    }

    out.into_iter()
        .filter(|pk| {
            snapshot
                .bin_arrays_by_pubkey
                .contains_key(&to_meteora_pubkey(*pk))
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub fn build_swap_exact_in_ix(
    payer: Pubkey,
    pool_id: Pubkey,
    snapshot: &MeteoraDlmmSnapshot,
    user_token_in: Pubkey,
    user_token_out: Pubkey,
    input_mint: Pubkey,
    amount_in: u64,
    min_amount_out: u64,
) -> Result<Instruction> {
    let input_mint = to_meteora_pubkey(input_mint);
    let swap_for_y = if input_mint == snapshot.lb_pair.token_x_mint {
        true
    } else if input_mint == snapshot.lb_pair.token_y_mint {
        false
    } else {
        anyhow::bail!("input mint is not part of this DLMM pair");
    };

    let program_id = from_meteora_pubkey(meteora::meteora_dlmm::ID);
    let event_authority = from_meteora_pubkey(derive_event_authority_pda().0);
    let reserve_x = from_meteora_pubkey(snapshot.lb_pair.reserve_x);
    let reserve_y = from_meteora_pubkey(snapshot.lb_pair.reserve_y);
    let token_x_mint = from_meteora_pubkey(snapshot.lb_pair.token_x_mint);
    let token_y_mint = from_meteora_pubkey(snapshot.lb_pair.token_y_mint);
    let oracle = from_meteora_pubkey(snapshot.lb_pair.oracle);
    let [token_x_program, token_y_program] = snapshot.lb_pair.get_token_programs()?;
    let token_x_program = from_meteora_pubkey(token_x_program);
    let token_y_program = from_meteora_pubkey(token_y_program);

    let mut accounts = vec![
        AccountMeta::new(pool_id, false),                  // lb_pair
        AccountMeta::new_readonly(program_id, false), // bin_array_bitmap_extension (None placeholder)
        AccountMeta::new(reserve_x, false),           // reserve_x
        AccountMeta::new(reserve_y, false),           // reserve_y
        AccountMeta::new(user_token_in, false),       // user_token_in
        AccountMeta::new(user_token_out, false),      // user_token_out
        AccountMeta::new_readonly(token_x_mint, false), // token_x_mint
        AccountMeta::new_readonly(token_y_mint, false), // token_y_mint
        AccountMeta::new(oracle, false),              // oracle
        AccountMeta::new_readonly(program_id, false), // host_fee_in (None placeholder)
        AccountMeta::new_readonly(payer, true),       // user
        AccountMeta::new_readonly(token_x_program, false), // token_x_program
        AccountMeta::new_readonly(token_y_program, false), // token_y_program
        AccountMeta::new_readonly(event_authority, false), // event_authority
        AccountMeta::new_readonly(program_id, false), // program
    ];

    for ta in ordered_bin_arrays_for_direction(snapshot, swap_for_y) {
        accounts.push(AccountMeta::new(ta, false));
    }

    let mut data = Vec::with_capacity(8 + 8 + 8);
    data.extend_from_slice(&DLMM_SWAP_DISCRIMINATOR);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());

    Ok(Instruction {
        program_id,
        accounts,
        data,
    })
}
