use crate::models::{
    CLMM_TICK_WINDOW_CENTER_INDEX, CLMM_TICK_WINDOW_LEN, CLMM_TICK_WINDOW_RADIUS, ClmmSnapshot,
    PoolUpdate, SnapshotDedupeKey,
};
use crate::raydium::clmm::core::{
    deserialize_anchor_from_bytes, load_tick_array_states_with_retry,
};
use crate::raydium_clmm_tick_watch_worker::{
    TaCmd, TaUpdate, spawn_raydium_clmm_tick_watcher_task,
};
use crate::utils::{destructure_start_pks_safe, tick_array_start_index, window_has_pair};

use futures_util::StreamExt;
use raydium_amm_v3::states::{AmmConfig, PoolState, TickArrayBitmapExtension, TickArrayState};
use solana_pubkey::Pubkey;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_types::config::RpcAccountInfoConfig;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use tokio::time::{self, Duration, Instant};

const DEFAULT_CLMM_TA_RPC_MAX_ATTEMPTS: usize = 4;
const DEFAULT_CLMM_TA_RPC_BASE_DELAY_MS: u64 = 75;
const DEFAULT_CLMM_WINDOW_RECOMPUTE_DEBOUNCE_MS: u64 = 75;

fn parse_env_u64(name: &str, default: u64) -> u64 {
    let Ok(raw) = std::env::var(name) else {
        return default;
    };

    match raw.parse::<u64>() {
        Ok(v) => v,
        Err(_) => {
            tracing::warn!(env = name, value = %raw, "invalid integer env override");
            default
        }
    }
}

fn parse_env_usize(name: &str, default: usize) -> usize {
    let Ok(raw) = std::env::var(name) else {
        return default;
    };

    match raw.parse::<usize>() {
        Ok(v) if v > 0 => v,
        _ => {
            tracing::warn!(env = name, value = %raw, "invalid integer env override");
            default
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn spawn_raydium_clmm_pool_watcher_task(
    rpc_client: Arc<RpcClient>,
    clmm_program_id: Pubkey,
    ps_client: Arc<PubsubClient>,
    ps_tick_client: PubsubClient,
    account_cfg: RpcAccountInfoConfig,
    pool_id: Pubkey,
    pool_state_init: PoolState,
    amm_id: Pubkey,
    amm_cfg: AmmConfig,
    bitmap_ext_latest_pk: Pubkey,
    bitmap_ext_latest_init: TickArrayBitmapExtension,
    tx: mpsc::Sender<PoolUpdate>,
    stop: CancellationToken,
) {
    tokio::spawn(async move {
        let ta_rpc_max_attempts =
            parse_env_usize("CLMM_TA_RPC_MAX_ATTEMPTS", DEFAULT_CLMM_TA_RPC_MAX_ATTEMPTS);
        let ta_rpc_base_delay_ms = parse_env_u64(
            "CLMM_TA_RPC_BASE_DELAY_MS",
            DEFAULT_CLMM_TA_RPC_BASE_DELAY_MS,
        );
        let window_recompute_debounce_ms = parse_env_u64(
            "CLMM_WINDOW_RECOMPUTE_DEBOUNCE_MS",
            DEFAULT_CLMM_WINDOW_RECOMPUTE_DEBOUNCE_MS,
        );

        tracing::info!(
            pool = %pool_id,
            ta_rpc_max_attempts,
            ta_rpc_base_delay_ms,
            window_recompute_debounce_ms,
            "CLMM watcher tuning loaded"
        );

        let pool_state_size = core::mem::size_of::<PoolState>();
        let tick_arr_size = core::mem::size_of::<TickArrayState>();

        let program_id = clmm_program_id;

        let (mut pool_sub, _) = ps_client
            .account_subscribe(&pool_id, Some(account_cfg.clone()))
            .await
            .expect("pool subscribe failed");
        let (mut amm_sub, _) = ps_client
            .account_subscribe(&amm_id, Some(account_cfg.clone()))
            .await
            .expect("amm subscribe failed");
        let (mut bitmap_sub, _) = ps_client
            .account_subscribe(&bitmap_ext_latest_pk, Some(account_cfg.clone()))
            .await
            .expect("bitmap ext subscribe failed");

        // Latest state
        let mut pool_state_latest: Option<PoolState> = Some(pool_state_init);
        let mut bitmap_ext_latest: Option<TickArrayBitmapExtension> = Some(bitmap_ext_latest_init);
        let mut trade_fee_rate_latest: Option<u32> = Some(amm_cfg.trade_fee_rate);

        // ordered window metadata
        let mut window_pks: Vec<Pubkey> = Vec::new();

        let mut tick_window_slots: Vec<Option<TickArrayState>> = vec![None; CLMM_TICK_WINDOW_LEN];
        let mut tick_window_update_slots: Vec<u64> = vec![0; CLMM_TICK_WINDOW_LEN];

        // initial window + initial RPC load
        if let (Some(ps), Some(bm)) = (pool_state_latest.as_ref(), bitmap_ext_latest.as_ref()) {
            let (_starts, pks) =
                destructure_start_pks_safe(program_id, pool_id, ps, bm, CLMM_TICK_WINDOW_RADIUS);
            window_pks = pks;

            if window_pks.len() == CLMM_TICK_WINDOW_LEN {
                match load_tick_array_states_with_retry(
                    &rpc_client,
                    &window_pks,
                    ta_rpc_max_attempts,
                    ta_rpc_base_delay_ms,
                )
                .await
                {
                    Ok(states) => {
                        tick_window_slots.fill(None);
                        for (i, st) in states.into_iter().enumerate().take(CLMM_TICK_WINDOW_LEN) {
                            tick_window_slots[i] = Some(st);
                        }
                    }
                    Err(e) => {
                        tracing::warn!(pool=%pool_id, err=?e, "initial RPC tick arrays load failed");
                    }
                }
            }
        }

        let (sender_cmd, mut tick_update_receiver) = spawn_raydium_clmm_tick_watcher_task(
            ps_tick_client,
            window_pks.clone(),
            account_cfg.clone(),
            stop.clone(),
        );

        let debounce = Duration::from_millis(window_recompute_debounce_ms);
        let mut pending_recompute = false;

        let far_future = Instant::now() + Duration::from_secs(3600);
        let recompute_timer = time::sleep_until(far_future);
        tokio::pin!(recompute_timer);

        fn schedule_recompute(
            pending: &mut bool,
            timer: &mut std::pin::Pin<&mut tokio::time::Sleep>,
            debounce: Duration,
        ) {
            *pending = true;
            let at = Instant::now() + debounce;
            timer.as_mut().reset(at);
        }

        // Dedupe snapshots (include tick-array update slots!)
        let mut last_sent_key: Option<SnapshotDedupeKey> = None;

        loop {
            tokio::select! {
                _ = stop.cancelled() => break,

                _ = &mut recompute_timer, if pending_recompute => {
                    pending_recompute = false;

                    if let (Some(ps), Some(bm)) = (pool_state_latest.as_ref(), bitmap_ext_latest.as_ref()) {
                        let (_starts, new_pks) = destructure_start_pks_safe(
                            program_id,
                            pool_id,
                            ps,
                            bm,
                            CLMM_TICK_WINDOW_RADIUS,
                        );
                        if new_pks.len() == CLMM_TICK_WINDOW_LEN && new_pks != window_pks {
                            tracing::info!(pool=%pool_id, "debounced recompute: window changed");
                            window_pks = new_pks.clone();

                            if let Ok(states) = load_tick_array_states_with_retry(
                                &rpc_client,
                                &window_pks,
                                ta_rpc_max_attempts,
                                ta_rpc_base_delay_ms,
                            )
                            .await
                            {
                                tick_window_slots.fill(None);
                                for (i, st) in states.into_iter().enumerate().take(CLMM_TICK_WINDOW_LEN) {
                                    tick_window_slots[i] = Some(st);
                                }
                                tick_window_update_slots = vec![0; CLMM_TICK_WINDOW_LEN];
                            } else {
                                // Window metadata changed; avoid mixing old tick-array states with
                                // the new window until a fresh RPC load / subscriptions refill it.
                                tick_window_slots.fill(None);
                                tick_window_update_slots = vec![0; CLMM_TICK_WINDOW_LEN];
                            }

                            let _ = sender_cmd
                                .send(TaCmd::TickWindowChanged { new_ticks: new_pks })
                                .await;
                        }
                    }
                }

                m = pool_sub.next() => {
                    let Some(resp) = m else { break; };
                    let Some(pool_bytes) = resp.value.data.decode() else { continue; };
                    if pool_bytes.len() < pool_state_size { continue; }

                    let ps_new = match deserialize_anchor_from_bytes::<PoolState>(&pool_bytes) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    let old_ps = pool_state_latest.as_ref().copied();

                    let boundary_crossed = old_ps.map(|old| {
                        let old_start = tick_array_start_index(old.tick_current, old.tick_spacing);
                        let new_start = tick_array_start_index(ps_new.tick_current, ps_new.tick_spacing);
                        old_start != new_start
                    }).unwrap_or(true);

                    let tick_or_spacing_changed = old_ps.map(|old| {
                        old.tick_current != ps_new.tick_current || old.tick_spacing != ps_new.tick_spacing
                    }).unwrap_or(true);

                    pool_state_latest = Some(ps_new);

                    if boundary_crossed {
                        if let (Some(ps), Some(bm)) = (pool_state_latest.as_ref(), bitmap_ext_latest.as_ref()) {
                            let (_starts, new_pks) = destructure_start_pks_safe(
                                program_id,
                                pool_id,
                                ps,
                                bm,
                                CLMM_TICK_WINDOW_RADIUS,
                            );
                            if new_pks.len() == CLMM_TICK_WINDOW_LEN && new_pks != window_pks {
                                window_pks = new_pks.clone();

                                if let Ok(states) = load_tick_array_states_with_retry(
                                    &rpc_client,
                                    &window_pks,
                                    ta_rpc_max_attempts,
                                    ta_rpc_base_delay_ms,
                                )
                                .await
                                {
                                    tick_window_slots.fill(None);
                                    for (i, st) in states.into_iter().enumerate().take(CLMM_TICK_WINDOW_LEN) {
                                        tick_window_slots[i] = Some(st);
                                    }
                                    tick_window_update_slots = vec![0; CLMM_TICK_WINDOW_LEN];
                                } else {
                                    tick_window_slots.fill(None);
                                    tick_window_update_slots = vec![0; CLMM_TICK_WINDOW_LEN];
                                }

                                let _ = sender_cmd.send(TaCmd::TickWindowChanged { new_ticks: new_pks }).await;
                            }
                        }
                    } else if tick_or_spacing_changed {
                        schedule_recompute(&mut pending_recompute, &mut recompute_timer, debounce);
                    }
                }

                m = amm_sub.next() => {
                    let Some(resp) = m else { break; };
                    let Some(bytes) = resp.value.data.decode() else { continue; };

                    let cfg = match deserialize_anchor_from_bytes::<AmmConfig>(&bytes) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    if trade_fee_rate_latest != Some(cfg.trade_fee_rate) {
                        trade_fee_rate_latest = Some(cfg.trade_fee_rate);
                    }
                }

                m = bitmap_sub.next() => {
                    let Some(resp) = m else { break; };
                    let Some(bytes) = resp.value.data.decode() else { continue; };

                    let bm_new = match deserialize_anchor_from_bytes::<TickArrayBitmapExtension>(&bytes) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    bitmap_ext_latest = Some(bm_new);

                    schedule_recompute(&mut pending_recompute, &mut recompute_timer, debounce);
                }

                Some(msg) = tick_update_receiver.recv() => {
                    let TaUpdate::Update { index, tick_pk, data } = msg;

                    let ctx_slot = data.context.slot;

                    let Some(bytes) = data.value.data.decode() else { continue; };
                    if bytes.len() < tick_arr_size { continue; }

                    let st = match deserialize_anchor_from_bytes::<TickArrayState>(&bytes) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };

                    let idx = index as usize;
                    if idx >= CLMM_TICK_WINDOW_LEN { continue; }

                    if window_pks.get(idx) != Some(&tick_pk) {
                        tracing::debug!(
                            pool=%pool_id,
                            idx,
                            expected=?window_pks.get(idx),
                            got=%tick_pk,
                            "tick update index mismatch"
                        );
                        continue;
                    }

                    tick_window_slots[idx] = Some(st);
                    tick_window_update_slots[idx] = ctx_slot;
                }
            }

            if !tick_window_slots.iter().all(|x| x.is_some()) {
                continue;
            }

            if let (Some(ps), Some(fee), Some(bm)) =
                (pool_state_latest, trade_fee_rate_latest, bitmap_ext_latest)
            {
                let Some(win) = tick_window_slots
                    .iter()
                    .copied()
                    .collect::<Option<Vec<_>>>()
                else {
                    continue;
                };

                if !window_has_pair(&ps, &win, /*zero_for_one=*/ false)
                    && !window_has_pair(&ps, &win, /*zero_for_one=*/ true)
                {
                    tracing::debug!(pool=%pool_id, "skipping CLMM snapshot: missing current/adjacent tick array");
                    continue;
                }

                let center_start_tick = win
                    .get(CLMM_TICK_WINDOW_CENTER_INDEX)
                    .map(|x| x.start_tick_index)
                    .unwrap_or_default();

                let tick_update_slots: [u64; CLMM_TICK_WINDOW_LEN] =
                    match tick_window_update_slots.as_slice().try_into() {
                        Ok(v) => v,
                        Err(_) => {
                            tracing::warn!(pool=%pool_id, "tick_update_slots wrong len");
                            continue;
                        }
                    };

                let key = SnapshotDedupeKey {
                    sqrt_price_x64: ps.sqrt_price_x64,
                    liquidity: ps.liquidity,
                    tick_current: ps.tick_current,
                    tick_spacing: ps.tick_spacing,
                    trade_fee_rate: fee,
                    center_start_tick,
                    tick_update_slots,
                };

                if last_sent_key == Some(key) {
                    continue;
                }

                last_sent_key = Some(key);

                let snap = ClmmSnapshot {
                    pool_state: ps,
                    trade_fee_rate: fee,
                    tick_array_bitmap_ext: bm,
                    ticks_array_window: win,
                    tick_array_pubkeys: window_pks.clone(),
                };

                if tx
                    .send(PoolUpdate::RaydiumClmm {
                        pool_id,
                        snapshot: Box::new(snap),
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });
}
