use crate::*;
use anyhow::{Context, Result};
use bytemuck::pod_read_unaligned;
use solana_client::{nonblocking::rpc_client::RpcClient as RpcClientAsync, rpc_client::RpcClient};
use solana_sdk::pubkey::Pubkey;
use std::result::Result::Ok;

pub fn fetch_and_decode_bin_arrays_sync(rpc: &RpcClient, keys: &[Pubkey]) -> Result<Vec<BinArray>> {
    let accounts = rpc
        .get_multiple_accounts(keys)
        .context("DLMM: get_multiple_accounts(bin_arrays)")?;

    let mut arrays = Vec::with_capacity(accounts.len());

    for (i, acc_opt) in accounts.into_iter().enumerate() {
        match acc_opt {
            Some(acc) => match decode_pod_anchor_account::<BinArray>(&acc.data) {
                Ok(ba_ref) => arrays.push(ba_ref),
                Err(err) => eprintln!(
                    "DLMM: Failed to deserialize BinArray for {} ({}): {err:?}",
                    i, keys[i]
                ),
            },
            None => eprintln!(
                "DLMM: Missing account for bin array pubkey {} ({})",
                i, keys[i]
            ),
        }
    }

    Ok(arrays)
}

pub async fn fetch_and_decode_bin_arrays(
    rpc: &RpcClientAsync,
    keys: &[Pubkey],
) -> Result<Vec<BinArray>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }

    let accounts = rpc
        .get_multiple_accounts(keys)
        .await
        .context("DLMM: get_multiple_accounts(bin_arrays)")?;

    let mut arrays = Vec::with_capacity(accounts.len());

    for (i, acc_opt) in accounts.into_iter().enumerate() {
        match acc_opt {
            Some(acc) => match decode_pod_anchor_account::<BinArray>(&acc.data) {
                Ok(ba_ref) => arrays.push(ba_ref),
                Err(err) => eprintln!(
                    "DLMM: Failed to deserialize BinArray for {} ({}): {err:?}",
                    i, keys[i]
                ),
            },
            None => eprintln!(
                "DLMM: Missing account for bin array pubkey {} ({})",
                i, keys[i]
            ),
        }
    }

    Ok(arrays)
}

pub fn derive_binarrays(
    pair_pubkey: Pubkey,
    center_index: i32,
    n_forward: i32,
    n_backward: i32,
) -> (Vec<Pubkey>, Vec<Pubkey>) {
    let mut forward = Vec::with_capacity((n_forward + 1).max(1) as usize);
    if let Some((pda, _)) = derive_bin_array_pda_safe(pair_pubkey, center_index) {
        forward.push(pda);
    }

    for step in 1..=n_forward {
        if let Some(next_index) = center_index.checked_add(step) {
            if let Some((pda, _)) = derive_bin_array_pda_safe(pair_pubkey, next_index) {
                forward.push(pda);
            }
        }
    }

    let mut backward = Vec::with_capacity((n_backward + 1).max(1) as usize);
    if let Some((pda, _)) = derive_bin_array_pda_safe(pair_pubkey, center_index) {
        backward.push(pda);
    }

    for step in 1..=n_backward {
        if let Some(prev_index) = center_index.checked_sub(step) {
            if let Some((pda, _)) = derive_bin_array_pda_safe(pair_pubkey, prev_index) {
                backward.push(pda);
            }
        }
    }

    (forward, backward)
}

fn derive_bin_array_pda_safe(pair_pubkey: Pubkey, idx: i32) -> Option<(Pubkey, u8)> {
    Some(derive_bin_array_pda(pair_pubkey, idx as i64))
}

pub fn decode_pod_anchor_account<T: bytemuck::Pod>(data: &[u8]) -> Result<T> {
    const DISC_SIZE: usize = 8;
    let need = DISC_SIZE + size_of::<T>();

    if data.len() < need {
        anyhow::bail!(
            "account too small for type (have {}, need at least {})",
            data.len(),
            need
        );
    }

    let val = pod_read_unaligned(&data[DISC_SIZE..DISC_SIZE + size_of::<T>()]);
    Ok(val)
}
