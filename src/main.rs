mod core;
mod dynamic_candidates;
mod execute;
mod meteora_damm;
mod meteora_damm_watch_worker;
mod meteora_dlmm;
mod meteora_dlmm_watch_worker;
mod models;
mod orca_whirlpool;
mod orca_whirlpool_watch_worker;
mod pumpswap;
mod pumpswap_watch_worker;
mod raydium;
mod raydium_amm_watch_worker;
mod raydium_clmm_tick_watch_worker;
mod raydium_clmm_watch_worker;
mod raydium_cpmm;
mod raydium_cpmm_watch_worker;
mod runtime;
mod simulate;
mod telegram_send;
mod utils;

use crate::dynamic_candidates::fetch_dynamic_candidate_pools;
use crate::meteora_damm::{
    MeteoraDammSnapshot, MeteoraDammStatic,
    parse_pool_static_layout as parse_meteora_damm_pool_static_layout,
};
use crate::meteora_damm_watch_worker::spawn_meteora_damm_pool_watcher_task;
use crate::meteora_dlmm::{build_snapshot as build_meteora_snapshot, decode_lb_pair};
use crate::meteora_dlmm_watch_worker::spawn_meteora_dlmm_pool_watcher_task;
use crate::models::{ClmmStatic, Config, PoolConfig, PoolType, PoolUpdate};
use crate::orca_whirlpool::build_snapshot as build_orca_snapshot;
use crate::orca_whirlpool_watch_worker::spawn_orca_whirlpool_pool_watcher_task;
use crate::pumpswap::{
    PumpAmmSnapshot, PumpPoolStatic, decode_mint_decimals_any_program,
    decode_token_account_amount_any_program, parse_fee_config, parse_pool_static_layout,
    pumpswap_fee_config_account,
};
use crate::pumpswap_watch_worker::spawn_pumpswap_pool_watcher_task;
use crate::raydium::clmm::core::deserialize_anchor_account;
use crate::raydium_cpmm::{
    RaydiumCpmmSnapshot, parse_amm_config_trade_fee_rate,
    parse_pool_dynamic as parse_raydium_cpmm_pool_dynamic,
    parse_pool_static_layout as parse_raydium_cpmm_pool_static_layout,
};
use crate::raydium_cpmm_watch_worker::spawn_raydium_cpmm_pool_watcher_task;
use crate::runtime::{RuntimeCtx, RuntimeState, VenueKind};
use anyhow::{Context, Result};
use bytemuck::from_bytes;
use futures::FutureExt;
use orca_whirlpools_client::{Whirlpool, get_oracle_address};
use raydium::amm::core::AmmInfo;
use raydium_amm_v3::states::{AmmConfig, PoolState, TickArrayBitmapExtension};
use raydium_amm_watch_worker::spawn_raydium_amm_pool_watcher_task;
use raydium_clmm_watch_worker::spawn_raydium_clmm_pool_watcher_task;
use solana_address_lookup_table_interface::program::id as alt_program_id;
use solana_address_lookup_table_interface::state::AddressLookupTable;
use solana_program::program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_types::config::{CommitmentConfig, RpcAccountInfoConfig, UiAccountEncoding};
use solana_sdk::account::Account;
use solana_sdk::message::AddressLookupTableAccount;
use solana_sdk::signature::read_keypair_file;
use solana_sdk::signer::Signer;
use spl_associated_token_account::get_associated_token_address;
use spl_token::state::Account as TokenAccount;
use std::collections::HashSet;
use std::mem;
use std::sync::Arc;
use std::{fs, str::FromStr, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

fn parse_cfg(path: &str) -> Config {
    let config_str = fs::read_to_string(path).expect("invalid file path");
    toml::from_str(&config_str).expect("error parsing toml file")
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(default)
}

fn parse_bool_env(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "y" | "on"
        ),
        Err(_) => default,
    }
}

fn parse_pubkey_list_env(name: &str) -> Vec<Pubkey> {
    let Ok(raw) = std::env::var(name) else {
        return Vec::new();
    };

    let mut keys = Vec::new();
    for token in raw.split(',') {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            continue;
        }
        match Pubkey::from_str(trimmed) {
            Ok(pk) => keys.push(pk),
            Err(err) => tracing::warn!(env = name, value = trimmed, ?err, "invalid pubkey"),
        }
    }

    keys
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct WatchedPoolKey {
    pool_type: PoolType,
    pool_id: Pubkey,
}

const RAYDIUM_CLMM_PROGRAM_ID_STR: &str = "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK";
const PANCAKESWAP_CLMM_PROGRAM_ID_STR: &str = "HpNfyc2Saw7RKkQd8nEL4khUcuPhQ7WwY1B2qjx8jxFq";

async fn load_lookup_table_accounts(
    rpc: &RpcClient,
    table_keys: &[Pubkey],
) -> Vec<AddressLookupTableAccount> {
    let mut out = Vec::with_capacity(table_keys.len());
    for table_key in table_keys {
        let account = match rpc.get_account(table_key).await {
            Ok(account) => account,
            Err(err) => {
                tracing::warn!(table = %table_key, ?err, "failed to fetch ALT account");
                continue;
            }
        };

        if account.owner != alt_program_id() {
            tracing::warn!(
                table = %table_key,
                owner = %account.owner,
                expected_owner = %alt_program_id(),
                "account is not an Address Lookup Table program account"
            );
            continue;
        }

        match AddressLookupTable::deserialize(&account.data) {
            Ok(table) => {
                out.push(AddressLookupTableAccount {
                    key: *table_key,
                    addresses: table.addresses.to_vec(),
                });
            }
            Err(err) => {
                tracing::warn!(table = %table_key, ?err, "failed to decode ALT account");
            }
        }
    }

    out
}

async fn get_account_with_retry(
    rpc_client: &RpcClient,
    pubkey: &Pubkey,
    label: &str,
) -> Result<Account> {
    let attempts = parse_env_u64("ARB_ONBOARD_ACCOUNT_RETRIES", 4).max(1);
    let base_delay_ms = parse_env_u64("ARB_ONBOARD_ACCOUNT_RETRY_MS", 250).max(1);
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 1..=attempts {
        match rpc_client.get_account(pubkey).await {
            Ok(account) => {
                if attempt > 1 {
                    tracing::info!(
                        %label,
                        account = %pubkey,
                        attempt,
                        attempts,
                        "dynamic onboarding get_account succeeded after retry"
                    );
                }
                return Ok(account);
            }
            Err(err) => {
                let msg = format!("{err:#}");
                let lower = msg.to_ascii_lowercase();
                let retryable = lower.contains("accountnotfound")
                    || lower.contains("not found")
                    || lower.contains("timeout")
                    || lower.contains("429")
                    || lower.contains("node is behind");
                last_err = Some(anyhow::anyhow!(err));
                if attempt >= attempts || !retryable {
                    break;
                }
                tracing::warn!(
                    %label,
                    account = %pubkey,
                    attempt,
                    attempts,
                    error = %msg,
                    "dynamic onboarding get_account retry"
                );
                tokio::time::sleep(Duration::from_millis(base_delay_ms.saturating_mul(attempt)))
                    .await;
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("unknown get_account failure")))
        .with_context(|| format!("failed to fetch account {} ({pubkey}) with retry", label))
}

async fn build_dlmm_snapshot_with_retry(
    rpc_client: &RpcClient,
    pool_id: Pubkey,
    lb_pair: crate::meteora_dlmm::MeteoraLbPair,
) -> Result<crate::meteora_dlmm::MeteoraDlmmSnapshot> {
    let attempts = parse_env_u64("ARB_ONBOARD_SNAPSHOT_RETRIES", 4).max(1);
    let base_delay_ms = parse_env_u64("ARB_ONBOARD_SNAPSHOT_RETRY_MS", 250).max(1);
    let mut last_err: Option<anyhow::Error> = None;

    for attempt in 1..=attempts {
        match build_meteora_snapshot(rpc_client, pool_id, lb_pair).await {
            Ok(snapshot) => {
                if attempt > 1 {
                    tracing::info!(
                        pool_id = %pool_id,
                        attempt,
                        attempts,
                        "dynamic onboarding DLMM snapshot build succeeded after retry"
                    );
                }
                return Ok(snapshot);
            }
            Err(err) => {
                let msg = format!("{err:#}");
                let lower = msg.to_ascii_lowercase();
                let retryable = lower.contains("missing dlmm bin array account")
                    || lower.contains("accountnotfound")
                    || lower.contains("not found")
                    || lower.contains("timeout")
                    || lower.contains("429")
                    || lower.contains("node is behind");
                last_err = Some(anyhow::anyhow!(err));
                if attempt >= attempts || !retryable {
                    break;
                }
                tracing::warn!(
                    pool_id = %pool_id,
                    attempt,
                    attempts,
                    error = %msg,
                    "dynamic onboarding DLMM snapshot retry"
                );
                tokio::time::sleep(Duration::from_millis(base_delay_ms.saturating_mul(attempt)))
                    .await;
            }
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("unknown DLMM snapshot build failure")))
}

#[allow(clippy::too_many_arguments)]
async fn initialize_pool_watcher(
    runtime: &mut RuntimeState,
    rpc_client: &Arc<RpcClient>,
    ps_client: &Arc<PubsubClient>,
    tx: &mpsc::Sender<PoolUpdate>,
    account_cfg: &RpcAccountInfoConfig,
    wss_url: &str,
    pool: &PoolConfig,
    fast_startup: bool,
) -> Result<(Pubkey, PoolType, CancellationToken)> {
    let pool_id = Pubkey::from_str(&pool.pool_id)?;
    let pool_type = pool.pool_type();

    let stop = CancellationToken::new();
    let account_cfg = account_cfg.clone();
    let ps_client_clone = Arc::clone(ps_client);

    match pool_type {
        PoolType::RaydiumAmm => {
            let market_data_acc = rpc_client
                .get_account(&pool_id)
                .await
                .expect("coudnt get market data acc");

            let amm_info: &AmmInfo =
                from_bytes::<AmmInfo>(&market_data_acc.data[0..mem::size_of::<AmmInfo>()]);
            let coin_vault_acc = rpc_client
                .get_account(&amm_info.coin_vault)
                .await
                .expect("couldnt get amm coin vault");
            let pc_vault_acc = rpc_client
                .get_account(&amm_info.pc_vault)
                .await
                .expect("couldnt get amm pc vault");
            let coin_vault = TokenAccount::unpack(&coin_vault_acc.data)
                .expect("couldnt decode coin vault token account");
            let pc_vault = TokenAccount::unpack(&pc_vault_acc.data)
                .expect("couldnt decode pc vault token account");

            runtime.register_amm_pool(pool_id, *amm_info, coin_vault.mint, pc_vault.mint);
            spawn_raydium_amm_pool_watcher_task(
                ps_client_clone,
                account_cfg,
                pool_id,
                amm_info.coin_vault,
                amm_info.pc_vault,
                tx.clone(),
                stop.clone(),
            )
            .await;
        }

        PoolType::RaydiumCpmm => {
            let pool_acc =
                get_account_with_retry(rpc_client.as_ref(), &pool_id, "raydium cpmm pool").await?;
            let mut st = parse_raydium_cpmm_pool_static_layout(&pool_acc.data)
                .with_context(|| format!("bad raydium cpmm pool layout for {pool_id}"))?;
            let pool_dyn = parse_raydium_cpmm_pool_dynamic(&pool_acc.data)
                .with_context(|| format!("bad raydium cpmm pool dynamic fields for {pool_id}"))?;

            let amm_config_acc = get_account_with_retry(
                rpc_client.as_ref(),
                &st.amm_config,
                "raydium cpmm amm config",
            )
            .await
            .with_context(|| {
                format!(
                    "couldnt get raydium cpmm amm config account {} for pool {}",
                    st.amm_config, pool_id
                )
            })?;
            st.trade_fee_rate = parse_amm_config_trade_fee_rate(&amm_config_acc.data)
                .with_context(|| {
                    format!(
                        "bad raydium cpmm amm config {} for pool {}",
                        st.amm_config, pool_id
                    )
                })?;

            let token_0_vault_acc = get_account_with_retry(
                rpc_client.as_ref(),
                &st.token_0_vault,
                "raydium cpmm token_0 vault",
            )
            .await
            .with_context(|| {
                format!(
                    "couldnt get raydium cpmm token_0 vault account {} for pool {}",
                    st.token_0_vault, pool_id
                )
            })?;
            let token_1_vault_acc = get_account_with_retry(
                rpc_client.as_ref(),
                &st.token_1_vault,
                "raydium cpmm token_1 vault",
            )
            .await
            .with_context(|| {
                format!(
                    "couldnt get raydium cpmm token_1 vault account {} for pool {}",
                    st.token_1_vault, pool_id
                )
            })?;

            let token_0_vault_amount = decode_token_account_amount_any_program(
                &token_0_vault_acc.data,
            )
            .with_context(|| {
                format!(
                    "couldnt decode raydium cpmm token_0 vault {} for pool {}",
                    st.token_0_vault, pool_id
                )
            })?;
            let token_1_vault_amount = decode_token_account_amount_any_program(
                &token_1_vault_acc.data,
            )
            .with_context(|| {
                format!(
                    "couldnt decode raydium cpmm token_1 vault {} for pool {}",
                    st.token_1_vault, pool_id
                )
            })?;

            runtime.register_raydium_cpmm_pool(pool_id, st.clone());
            runtime.insert_initial_raydium_cpmm_snapshot(
                pool_id,
                RaydiumCpmmSnapshot {
                    token_0_vault_amount,
                    token_1_vault_amount,
                    protocol_fees_token_0: pool_dyn.protocol_fees_token_0,
                    protocol_fees_token_1: pool_dyn.protocol_fees_token_1,
                    fund_fees_token_0: pool_dyn.fund_fees_token_0,
                    fund_fees_token_1: pool_dyn.fund_fees_token_1,
                    status: pool_dyn.status,
                    open_time: pool_dyn.open_time,
                },
            );

            spawn_raydium_cpmm_pool_watcher_task(
                ps_client_clone,
                account_cfg,
                pool_id,
                pool_id,
                st.token_0_vault,
                st.token_1_vault,
                tx.clone(),
                stop.clone(),
            )
            .await;
        }

        PoolType::PumpAmm => {
            let pool_acc = rpc_client
                .as_ref()
                .get_account(&pool_id)
                .await
                .expect("couldnt get pumpswap pool account");
            let (
                base_mint,
                quote_mint,
                pool_base_token_account,
                pool_quote_token_account,
                coin_creator,
            ) = parse_pool_static_layout(&pool_acc.data).expect("bad pumpswap pool layout");

            let base_mint_acc = rpc_client
                .as_ref()
                .get_account(&base_mint)
                .await
                .expect("couldnt get pumpswap base mint account");
            let quote_mint_acc = rpc_client
                .as_ref()
                .get_account(&quote_mint)
                .await
                .expect("couldnt get pumpswap quote mint account");
            let base_token_program = base_mint_acc.owner;
            let quote_token_program = quote_mint_acc.owner;
            let base_decimals = decode_mint_decimals_any_program(&base_mint_acc.data)
                .expect("couldnt decode pumpswap base mint decimals");
            let quote_decimals = decode_mint_decimals_any_program(&quote_mint_acc.data)
                .expect("couldnt decode pumpswap quote mint decimals");

            let base_vault_acc = rpc_client
                .as_ref()
                .get_account(&pool_base_token_account)
                .await
                .expect("couldnt get pumpswap base vault account");
            let quote_vault_acc = rpc_client
                .as_ref()
                .get_account(&pool_quote_token_account)
                .await
                .expect("couldnt get pumpswap quote vault account");
            let base_vault_amount = decode_token_account_amount_any_program(&base_vault_acc.data)
                .expect("couldnt decode pumpswap base vault token account");
            let quote_vault_amount = decode_token_account_amount_any_program(&quote_vault_acc.data)
                .expect("couldnt decode pumpswap quote vault token account");

            let base_supply = rpc_client
                .as_ref()
                .get_token_supply(&base_mint)
                .await
                .expect("couldnt get pumpswap base mint supply")
                .amount
                .parse::<u64>()
                .expect("invalid pumpswap base mint supply amount");

            let fee_cfg_acc = rpc_client
                .as_ref()
                .get_account(&pumpswap_fee_config_account())
                .await
                .expect("couldnt get pumpswap fee config account");
            let fee_cfg = parse_fee_config(&fee_cfg_acc.data).expect("bad pumpswap fee config");
            runtime.set_pumpswap_fee_config(fee_cfg);

            runtime.register_pumpswap_pool(
                pool_id,
                PumpPoolStatic {
                    base_mint,
                    quote_mint,
                    coin_creator,
                    pool_base_token_account,
                    pool_quote_token_account,
                    base_token_program,
                    quote_token_program,
                    base_decimals,
                    quote_decimals,
                    base_mint_supply: base_supply,
                },
            );
            runtime.insert_initial_pumpswap_snapshot(
                pool_id,
                PumpAmmSnapshot {
                    base_vault_amount,
                    quote_vault_amount,
                },
            );

            spawn_pumpswap_pool_watcher_task(
                ps_client_clone,
                account_cfg,
                pool_id,
                pool_base_token_account,
                pool_quote_token_account,
                tx.clone(),
                stop.clone(),
            )
            .await;
        }

        PoolType::MeteoraDamm => {
            let pool_acc =
                get_account_with_retry(rpc_client.as_ref(), &pool_id, "meteora damm pool")
                    .await
                    .with_context(|| format!("couldnt get meteora damm pool account {pool_id}"))?;
            let parsed = parse_meteora_damm_pool_static_layout(&pool_acc.data)
                .with_context(|| format!("bad meteora damm pool layout for {pool_id}"))?;

            let token_a_mint_acc = get_account_with_retry(
                rpc_client.as_ref(),
                &parsed.token_a_mint,
                "meteora damm token_a mint",
            )
            .await
            .with_context(|| {
                format!(
                    "couldnt get meteora damm token_a mint account {} for pool {}",
                    parsed.token_a_mint, pool_id
                )
            })?;
            let token_b_mint_acc = get_account_with_retry(
                rpc_client.as_ref(),
                &parsed.token_b_mint,
                "meteora damm token_b mint",
            )
            .await
            .with_context(|| {
                format!(
                    "couldnt get meteora damm token_b mint account {} for pool {}",
                    parsed.token_b_mint, pool_id
                )
            })?;
            let token_a_program = token_a_mint_acc.owner;
            let token_b_program = token_b_mint_acc.owner;
            let token_a_decimals = decode_mint_decimals_any_program(&token_a_mint_acc.data)
                .with_context(|| {
                    format!(
                        "couldnt decode meteora damm token_a mint decimals {} for pool {}",
                        parsed.token_a_mint, pool_id
                    )
                })?;
            let token_b_decimals = decode_mint_decimals_any_program(&token_b_mint_acc.data)
                .with_context(|| {
                    format!(
                        "couldnt decode meteora damm token_b mint decimals {} for pool {}",
                        parsed.token_b_mint, pool_id
                    )
                })?;

            let initial_damm_snapshot = if fast_startup {
                None
            } else {
                let token_a_vault_acc = get_account_with_retry(
                    rpc_client.as_ref(),
                    &parsed.token_a_vault,
                    "meteora damm token_a vault",
                )
                .await
                .with_context(|| {
                    format!(
                        "couldnt get meteora damm token_a vault account {} for pool {}",
                        parsed.token_a_vault, pool_id
                    )
                })?;
                let token_b_vault_acc = get_account_with_retry(
                    rpc_client.as_ref(),
                    &parsed.token_b_vault,
                    "meteora damm token_b vault",
                )
                .await
                .with_context(|| {
                    format!(
                        "couldnt get meteora damm token_b vault account {} for pool {}",
                        parsed.token_b_vault, pool_id
                    )
                })?;
                let token_a_vault_amount = decode_token_account_amount_any_program(
                    &token_a_vault_acc.data,
                )
                .with_context(|| {
                    format!(
                        "couldnt decode meteora damm token_a vault token account {} for pool {}",
                        parsed.token_a_vault, pool_id
                    )
                })?;
                let token_b_vault_amount = decode_token_account_amount_any_program(
                    &token_b_vault_acc.data,
                )
                .with_context(|| {
                    format!(
                        "couldnt decode meteora damm token_b vault token account {} for pool {}",
                        parsed.token_b_vault, pool_id
                    )
                })?;
                let boot_slot = rpc_client.as_ref().get_slot().await.unwrap_or(0);

                Some(MeteoraDammSnapshot {
                    token_a_vault_amount,
                    token_b_vault_amount,
                    liquidity: parsed.liquidity,
                    sqrt_min_price: parsed.sqrt_min_price,
                    sqrt_max_price: parsed.sqrt_max_price,
                    sqrt_price: parsed.sqrt_price,
                    activation_point: parsed.activation_point,
                    activation_type: parsed.activation_type,
                    pool_status: parsed.pool_status,
                    collect_fee_mode: parsed.collect_fee_mode,
                    version: parsed.version,
                    fee_approx: parsed.fee_approx,
                    last_observed_slot: boot_slot,
                })
            };

            runtime.register_meteora_damm_pool(
                pool_id,
                MeteoraDammStatic {
                    token_a_mint: parsed.token_a_mint,
                    token_b_mint: parsed.token_b_mint,
                    token_a_vault: parsed.token_a_vault,
                    token_b_vault: parsed.token_b_vault,
                    partner: parsed.partner,
                    token_a_program,
                    token_b_program,
                    token_a_decimals,
                    token_b_decimals,
                },
            );

            if let Some(snapshot) = initial_damm_snapshot {
                runtime.insert_initial_meteora_damm_snapshot(pool_id, snapshot);
            } else {
                tracing::info!(
                    pool_id = %pool_id,
                    "fast-startup enabled: skipping initial Meteora DAMM vault bootstrap snapshot"
                );
            }

            tracing::warn!(
                pool_id = %pool_id,
                cliff_fee_numerator = parsed.fee_approx.cliff_fee_numerator,
                base_fee_mode = parsed.fee_approx.base_fee_mode,
                dynamic_initialized = parsed.fee_approx.dynamic_initialized,
                "meteora damm exact-in quotes enabled (base scheduler + dynamic fee); execution still gated"
            );

            spawn_meteora_damm_pool_watcher_task(
                ps_client_clone,
                account_cfg,
                pool_id,
                parsed.token_a_vault,
                parsed.token_b_vault,
                tx.clone(),
                stop.clone(),
            )
            .await;
        }

        PoolType::RaydiumClmm | PoolType::PancakeswapClmm => {
            let (clmm_program_id, clmm_kind) = match pool_type {
                PoolType::RaydiumClmm => (
                    Pubkey::from_str(RAYDIUM_CLMM_PROGRAM_ID_STR)?,
                    VenueKind::RaydiumClmm,
                ),
                PoolType::PancakeswapClmm => (
                    Pubkey::from_str(PANCAKESWAP_CLMM_PROGRAM_ID_STR)?,
                    VenueKind::PancakeswapClmm,
                ),
                _ => unreachable!(),
            };
            let pool_state_acc = rpc_client
                .as_ref()
                .get_account(&pool_id)
                .await
                .with_context(|| {
                    format!("failed to fetch CLMM pool state account for {pool_id}")
                })?;

            if pool_state_acc.owner != clmm_program_id {
                anyhow::bail!(
                    "pool {} owner {} does not match expected CLMM program {} (wrong dex/kind for this pool?)",
                    pool_id,
                    pool_state_acc.owner,
                    clmm_program_id
                );
            }

            let pool_state = deserialize_anchor_account::<PoolState>(&pool_state_acc)
                .with_context(|| {
                    format!(
                        "failed to decode CLMM PoolState for {} under program {}",
                        pool_id, clmm_program_id
                    )
                })?;

            let amm_config_key = Pubkey::new_from_array(pool_state.amm_config.to_bytes());
            let amm_cfg_acc = rpc_client
                .as_ref()
                .get_account(&amm_config_key)
                .await
                .with_context(|| {
                    format!(
                        "failed to fetch CLMM amm config account {} for pool {}",
                        amm_config_key, pool_id
                    )
                })?;
            let amm_cfg =
                deserialize_anchor_account::<AmmConfig>(&amm_cfg_acc).with_context(|| {
                    format!(
                        "failed to decode CLMM AmmConfig {} for pool {}",
                        amm_config_key, pool_id
                    )
                })?;

            let bitmap_pda = Pubkey::find_program_address(
                &[
                    raydium_amm_v3::states::POOL_TICK_ARRAY_BITMAP_SEED.as_bytes(),
                    pool_id.as_ref(),
                ],
                &clmm_program_id,
            )
            .0;

            let mint_0 = Pubkey::new_from_array(pool_state.token_mint_0.to_bytes());
            let mint_1 = Pubkey::new_from_array(pool_state.token_mint_1.to_bytes());
            runtime.register_clmm_pool_with_kind(
                clmm_kind,
                pool_id,
                ClmmStatic {
                    program_id: clmm_program_id,
                    amm_config: amm_config_key,
                    bitmap_pda,
                },
                mint_0,
                mint_1,
            );

            let bitmap_pda_acc = rpc_client.get_account(&bitmap_pda).await.with_context(|| {
                format!(
                    "failed to fetch CLMM bitmap PDA {} for pool {}",
                    bitmap_pda, pool_id
                )
            })?;
            let bitmap_pda_state =
                deserialize_anchor_account::<TickArrayBitmapExtension>(&bitmap_pda_acc)
                    .with_context(|| {
                        format!(
                            "failed to decode CLMM TickArrayBitmapExtension {} for pool {}",
                            bitmap_pda, pool_id
                        )
                    })?;

            let ps_base = Arc::new(PubsubClient::new(wss_url).await?);
            let ps_ta = PubsubClient::new(wss_url).await?;

            spawn_raydium_clmm_pool_watcher_task(
                Arc::clone(rpc_client),
                clmm_program_id,
                ps_base,
                ps_ta,
                account_cfg,
                pool_id,
                pool_state,
                amm_config_key,
                amm_cfg,
                bitmap_pda,
                bitmap_pda_state,
                tx.clone(),
                stop.clone(),
            )
            .await;
        }

        PoolType::MeteoraDlmm => {
            let lb_pair_acc = get_account_with_retry(rpc_client.as_ref(), &pool_id, "dlmm lb_pair")
                .await
                .with_context(|| format!("coudnt get dlmm lb_pair acc {pool_id}"))?;
            let lb_pair = decode_lb_pair(&lb_pair_acc.data)
                .with_context(|| format!("coudnt deserialize dlmm lb_pair {pool_id}"))?;

            let mint_x = Pubkey::new_from_array(lb_pair.token_x_mint.to_bytes());
            let mint_y = Pubkey::new_from_array(lb_pair.token_y_mint.to_bytes());
            runtime.register_meteora_pool(pool_id, mint_x, mint_y);

            if fast_startup {
                tracing::info!(
                    pool_id = %pool_id,
                    "fast-startup enabled: skipping initial DLMM snapshot bootstrap"
                );
            } else {
                let snapshot =
                    build_dlmm_snapshot_with_retry(rpc_client.as_ref(), pool_id, lb_pair)
                        .await
                        .with_context(|| {
                            format!("coudnt build initial dlmm snapshot for {pool_id}")
                        })?;
                runtime.insert_initial_meteora_snapshot(pool_id, Box::new(snapshot));
            }

            spawn_meteora_dlmm_pool_watcher_task(
                Arc::clone(rpc_client),
                ps_client_clone,
                account_cfg,
                pool_id,
                tx.clone(),
                stop.clone(),
            )
            .await;
        }

        PoolType::OrcaWhirlpool => {
            let whirlpool_acc = rpc_client
                .as_ref()
                .get_account(&pool_id)
                .await
                .expect("couldnt get orca whirlpool account");
            let whirlpool = Whirlpool::from_bytes(&whirlpool_acc.data)
                .expect("couldnt deserialize orca whirlpool");

            let mint_a_acc = rpc_client
                .as_ref()
                .get_account(&whirlpool.token_mint_a)
                .await
                .expect("couldnt get orca token_mint_a account");
            let mint_b_acc = rpc_client
                .as_ref()
                .get_account(&whirlpool.token_mint_b)
                .await
                .expect("couldnt get orca token_mint_b account");

            let token_program_a = mint_a_acc.owner;
            let token_program_b = mint_b_acc.owner;

            let (oracle_pubkey, _) =
                get_oracle_address(&pool_id).expect("couldnt derive orca oracle PDA");

            runtime.register_orca_pool(pool_id, whirlpool.token_mint_a, whirlpool.token_mint_b);

            let snapshot = build_orca_snapshot(
                rpc_client.as_ref(),
                pool_id,
                whirlpool,
                token_program_a,
                token_program_b,
                oracle_pubkey,
            )
            .await
            .expect("couldnt build initial orca snapshot");
            runtime.insert_initial_orca_snapshot(pool_id, Box::new(snapshot));

            spawn_orca_whirlpool_pool_watcher_task(
                Arc::clone(rpc_client),
                ps_client_clone,
                account_cfg,
                pool_id,
                token_program_a,
                token_program_b,
                oracle_pubkey,
                tx.clone(),
                stop.clone(),
            )
            .await;
        }
    }

    Ok((pool_id, pool_type, stop))
}

fn panic_payload_message(panic: Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = panic.downcast_ref::<&str>() {
        return (*s).to_string();
    }
    if let Some(s) = panic.downcast_ref::<String>() {
        return s.clone();
    }
    "unknown panic payload".to_string()
}

fn venue_kind_from_pool_type(pool_type: PoolType) -> VenueKind {
    match pool_type {
        PoolType::RaydiumAmm => VenueKind::RaydiumAmm,
        PoolType::RaydiumCpmm => VenueKind::RaydiumCpmm,
        PoolType::RaydiumClmm => VenueKind::RaydiumClmm,
        PoolType::PancakeswapClmm => VenueKind::PancakeswapClmm,
        PoolType::MeteoraDlmm => VenueKind::MeteoraDlmm,
        PoolType::MeteoraDamm => VenueKind::MeteoraDamm,
        PoolType::OrcaWhirlpool => VenueKind::OrcaWhirlpool,
        PoolType::PumpAmm => VenueKind::PumpAmm,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = match std::env::var("RUST_LOG").as_deref() {
        Ok("true") | Ok("1") => EnvFilter::new("debug"),
        Ok("false") | Ok("0") => EnvFilter::new("info"),
        Ok(other) => EnvFilter::new(other),
        Err(_) => EnvFilter::new("info"),
    };

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    dotenvy::dotenv().ok();
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let tg_token = std::env::var("TG_BOT_API_TOKEN").ok();
    let tg_chat_id = std::env::var("TG_CHAT_ID").ok();

    let payer =
        read_keypair_file(std::env::var("PAYER_KEYPAIR").expect("PAYER_KEYPAIR env not set"))
            .expect("failed to read payer keypair");
    let payer_pubkey = payer.try_pubkey().expect("failed to derive payer pubkey");

    let usdc_mint = Pubkey::from_str("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")?;
    let wsol_mint = Pubkey::from_str("So11111111111111111111111111111111111111112")?;
    let usdc_ata = get_associated_token_address(&payer_pubkey, &usdc_mint);
    let wsol_ata = get_associated_token_address(&payer_pubkey, &wsol_mint);

    tracing::info!(
        payer = %payer_pubkey,
        usdc_ata = %usdc_ata,
        wsol_ata = %wsol_ata,
        "Derived payer ATAs"
    );

    let cfg = parse_cfg("cfg.toml");
    let rpc_url = std::env::var("RPC_URL").expect("RPC_URL env not set");
    let wss_url = std::env::var("WSS_URL").expect("WSS_URL env not set");

    tracing::info!(
        rpc_url = %rpc_url,
        wss_url = %wss_url,
        "rpc endpoints loaded from environment"
    );

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        rpc_url,
        CommitmentConfig::confirmed(),
    ));
    let ps_client = Arc::new(PubsubClient::new(&wss_url).await?);
    let alt_table_keys = parse_pubkey_list_env("ARB_ALT_TABLES");
    let lookup_tables = load_lookup_table_accounts(rpc_client.as_ref(), &alt_table_keys).await;
    tracing::info!(
        requested_alt_tables = alt_table_keys.len(),
        loaded_alt_tables = lookup_tables.len(),
        "address lookup table configuration loaded"
    );

    let account_cfg = RpcAccountInfoConfig {
        commitment: Some(CommitmentConfig::confirmed()),
        encoding: Some(UiAccountEncoding::Base64),
        data_slice: None,
        min_context_slot: None,
    };

    let (tx, mut rx) = mpsc::channel::<PoolUpdate>(1024);
    let mut runtime = RuntimeState::new(cfg.start_mints.clone())?;
    let mut watched_pools: HashSet<WatchedPoolKey> = HashSet::new();
    let fast_startup = parse_bool_env("ARB_FAST_STARTUP", false);
    let watcher_startup_delay_default = if fast_startup { 0 } else { 75 };
    let watcher_startup_delay_ms =
        parse_env_u64("WATCHER_STARTUP_DELAY_MS", watcher_startup_delay_default);
    let dynamic_db_enabled = parse_bool_env("ARB_DYNAMIC_DB_ENABLED", false);
    let dynamic_db_path =
        std::env::var("ARB_LISTENER_DB_PATH").unwrap_or_else(|_| "./data/pools.db".to_string());
    let dynamic_db_poll_secs = parse_env_u64("ARB_DYNAMIC_DB_POLL_SECS", 8).max(1);
    let dynamic_db_max_rows = parse_env_u64("ARB_DYNAMIC_DB_MAX_ROWS", 300).max(1) as usize;
    let dynamic_watcher_startup_delay_ms =
        parse_env_u64("ARB_DYNAMIC_WATCHER_STARTUP_DELAY_MS", 30);
    let mut dynamic_poll_interval =
        tokio::time::interval(Duration::from_secs(dynamic_db_poll_secs));
    dynamic_poll_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tracing::info!(
        watcher_startup_delay_ms,
        fast_startup,
        "pool watcher startup delay configured"
    );
    tracing::info!(
        dynamic_db_enabled,
        dynamic_db_path = %dynamic_db_path,
        dynamic_db_poll_secs,
        dynamic_db_max_rows,
        dynamic_watcher_startup_delay_ms,
        "dynamic DB onboarding configuration"
    );

    let mut static_added = 0usize;
    let mut static_failed = 0usize;
    for (i, pool) in cfg.pools.into_iter().enumerate() {
        if i > 0 && watcher_startup_delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(watcher_startup_delay_ms)).await;
        }
        let pool_type = pool.pool_type();
        let pool_id = Pubkey::from_str(&pool.pool_id)?;
        let key = WatchedPoolKey { pool_type, pool_id };
        if watched_pools.contains(&key) {
            tracing::info!(
                pool_id = %pool_id,
                kind = ?pool_type,
                symbol = %pool.symbol,
                "skipping duplicate static pool during startup"
            );
            continue;
        }
        tracing::info!(
            dex = ?pool.dex,
            symbol = %pool.symbol,
            pool_id = %pool_id,
            kind = ?pool_type,
            "initializing pool watcher"
        );
        let init_result = std::panic::AssertUnwindSafe(initialize_pool_watcher(
            &mut runtime,
            &rpc_client,
            &ps_client,
            &tx,
            &account_cfg,
            &wss_url,
            &pool,
            fast_startup,
        ))
        .catch_unwind()
        .await;

        match init_result {
            Ok(Ok((initialized_pool_id, initialized_pool_type, _stop))) => {
                watched_pools.insert(WatchedPoolKey {
                    pool_type: initialized_pool_type,
                    pool_id: initialized_pool_id,
                });
                static_added += 1;
            }
            Ok(Err(err)) => {
                static_failed += 1;
                tracing::warn!(
                    pool_id = %pool.pool_id,
                    kind = ?pool_type,
                    symbol = %pool.symbol,
                    err = %format!("{err:#}"),
                    "static pool watcher initialization failed; skipping pool"
                );
            }
            Err(panic_payload) => {
                static_failed += 1;
                let panic_msg = panic_payload_message(panic_payload);
                tracing::error!(
                    pool_id = %pool.pool_id,
                    kind = ?pool_type,
                    symbol = %pool.symbol,
                    panic = %panic_msg,
                    "static pool watcher initialization panicked; skipping pool"
                );
            }
        }
    }
    tracing::info!(
        static_added,
        static_failed,
        watched_total = watched_pools.len(),
        "static pool watcher initialization completed"
    );

    loop {
        tokio::select! {
            maybe_msg = rx.recv() => {
                let Some(msg) = maybe_msg else {
                    tracing::warn!("pool update channel closed; exiting runtime loop");
                    break;
                };
                let ctx = RuntimeCtx {
                    rpc: rpc_client.as_ref(),
                    payer: &payer,
                    lookup_tables: &lookup_tables,
                    tg_token: tg_token.as_deref(),
                    tg_chat_id: tg_chat_id.as_deref(),
                };
                runtime.handle_pool_update(msg, &ctx).await?;
            }
            _ = dynamic_poll_interval.tick(), if dynamic_db_enabled => {
                let candidates = match fetch_dynamic_candidate_pools(&dynamic_db_path, dynamic_db_max_rows) {
                    Ok(rows) => rows,
                    Err(err) => {
                        tracing::warn!(
                            db_path = %dynamic_db_path,
                            err = %format!("{err:#}"),
                            "failed to fetch dynamic candidate pools"
                        );
                        continue;
                    }
                };

                let mut dynamic_added = 0usize;
                let mut dynamic_failed = 0usize;

                for (idx, candidate) in candidates.into_iter().enumerate() {
                    if idx > 0 && dynamic_watcher_startup_delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(dynamic_watcher_startup_delay_ms)).await;
                    }

                    let pool_type = candidate.pool.pool_type();
                    let pool_id = match Pubkey::from_str(&candidate.pool.pool_id) {
                        Ok(pk) => pk,
                        Err(err) => {
                            dynamic_failed += 1;
                            tracing::warn!(
                                pool_id = %candidate.pool.pool_id,
                                kind = %candidate.pool.kind,
                                err = %err,
                                "invalid dynamic candidate pool id; skipping"
                            );
                            continue;
                        }
                    };

                    let key = WatchedPoolKey { pool_type, pool_id };
                    if watched_pools.contains(&key) {
                        continue;
                    }

                    tracing::info!(
                        pool_id = %pool_id,
                        kind = ?pool_type,
                        token_mint = %candidate.token_mint,
                        quote_mint = %candidate.quote_mint,
                        dex_count = candidate.dex_count,
                        pool_count = candidate.pool_count,
                        updated_unix = candidate.updated_unix,
                        "initializing dynamic pool watcher from listener DB"
                    );

                    let init_result = std::panic::AssertUnwindSafe(initialize_pool_watcher(
                        &mut runtime,
                        &rpc_client,
                        &ps_client,
                        &tx,
                        &account_cfg,
                        &wss_url,
                        &candidate.pool,
                        true,
                    ))
                    .catch_unwind()
                    .await;

                    match init_result {
                        Ok(Ok((initialized_pool_id, initialized_pool_type, _stop))) => {
                            watched_pools.insert(WatchedPoolKey {
                                pool_type: initialized_pool_type,
                                pool_id: initialized_pool_id,
                            });
                            runtime.mark_dynamic_pool(
                                venue_kind_from_pool_type(initialized_pool_type),
                                initialized_pool_id,
                            );
                            dynamic_added += 1;
                            tracing::info!(
                                pool_id = %initialized_pool_id,
                                kind = ?initialized_pool_type,
                                watched_total = watched_pools.len(),
                                "dynamic pool watcher initialized"
                            );
                        }
                        Ok(Err(err)) => {
                            dynamic_failed += 1;
                            tracing::warn!(
                                pool_id = %candidate.pool.pool_id,
                                kind = %candidate.pool.kind,
                                err = %format!("{err:#}"),
                                "dynamic pool watcher initialization failed"
                            );
                        }
                        Err(panic_payload) => {
                            dynamic_failed += 1;
                            let panic_msg = panic_payload_message(panic_payload);
                            tracing::error!(
                                pool_id = %candidate.pool.pool_id,
                                kind = %candidate.pool.kind,
                                panic = %panic_msg,
                                "dynamic pool watcher initialization panicked"
                            );
                        }
                    }
                }

                if dynamic_added > 0 || dynamic_failed > 0 {
                    tracing::info!(
                        dynamic_added,
                        dynamic_failed,
                        watched_total = watched_pools.len(),
                        "dynamic pool onboarding pass completed"
                    );
                }
            }
        }
    }

    Ok(())
}
