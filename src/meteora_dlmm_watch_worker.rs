use crate::meteora_dlmm::{
    MeteoraBinArray, MeteoraDlmmSnapshot, MeteoraLbPair, build_snapshot, decode_bin_array,
    decode_lb_pair, derive_window_bin_array_indices, derive_window_bin_array_pubkeys,
    to_meteora_pubkey,
};
use crate::models::PoolUpdate;
use anyhow::{Result, anyhow};
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures_util::StreamExt;
use meteora::BinArrayExtension;
use solana_pubkey::Pubkey;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_types::config::RpcAccountInfoConfig;
use solana_rpc_client_types::response::{Response, UiAccount};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_stream::StreamMap;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

type UnsubscribeFn = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

struct Subs<'a> {
    streams: StreamMap<Pubkey, BoxStream<'a, Response<UiAccount>>>,
    unsubs: HashMap<Pubkey, UnsubscribeFn>,
}

impl<'a> Subs<'a> {
    fn new() -> Self {
        Self {
            streams: StreamMap::new(),
            unsubs: HashMap::new(),
        }
    }

    async fn add(
        &mut self,
        ps: &'a PubsubClient,
        key: Pubkey,
        cfg: RpcAccountInfoConfig,
        label: &str,
    ) {
        match ps.account_subscribe(&key, Some(cfg)).await {
            Ok((stream, unsub)) => {
                self.unsubs.insert(key, unsub);
                self.streams.insert(key, stream);
                tracing::info!(label, account = %key, "DLMM watcher: subscribed");
            }
            Err(e) => {
                tracing::warn!(label, account = %key, err = ?e, "DLMM watcher: subscribe failed");
            }
        }
    }

    async fn remove(&mut self, key: &Pubkey, label: &str) {
        if let Some(unsub) = self.unsubs.remove(key) {
            unsub().await;
            tracing::info!(label, account = %key, "DLMM watcher: unsubscribed");
        }
        self.streams.remove(key);
    }

    async fn rebuild(
        &mut self,
        ps: &'a PubsubClient,
        cfg: &RpcAccountInfoConfig,
        new_keys: &[Pubkey],
        label: &str,
    ) {
        let new: HashSet<_> = new_keys.iter().copied().collect();
        let old: HashSet<_> = self.unsubs.keys().copied().collect();

        for k in old.difference(&new) {
            self.remove(k, label).await;
        }
        for k in new.difference(&old) {
            self.add(ps, *k, cfg.clone(), label).await;
        }
    }

    async fn next(&mut self) -> Option<(Pubkey, Response<UiAccount>)> {
        self.streams.next().await
    }
}

fn mint_pubkeys(lb_pair: &MeteoraLbPair) -> [Pubkey; 2] {
    [
        Pubkey::new_from_array(lb_pair.token_x_mint.to_bytes()),
        Pubkey::new_from_array(lb_pair.token_y_mint.to_bytes()),
    ]
}

fn derive_window_pubkeys(pool_id: Pubkey, lb_pair: &MeteoraLbPair) -> anyhow::Result<Vec<Pubkey>> {
    let center = MeteoraBinArray::bin_id_to_bin_array_index(lb_pair.active_id)?;
    let indices = derive_window_bin_array_indices(center)?;
    Ok(derive_window_bin_array_pubkeys(pool_id, &indices))
}

async fn emit_snapshot(
    tx: &mpsc::Sender<PoolUpdate>,
    pool_id: Pubkey,
    snapshot: &MeteoraDlmmSnapshot,
) -> bool {
    tx.send(PoolUpdate::MeteoraDlmm {
        pool_id,
        snapshot: Box::new(snapshot.clone()),
    })
    .await
    .is_ok()
}

async fn rebuild_snapshot_from_rpc(
    rpc_client: &RpcClient,
    tx: &mpsc::Sender<PoolUpdate>,
    pool_id: Pubkey,
    lb_pair: MeteoraLbPair,
) -> Result<MeteoraDlmmSnapshot> {
    let snapshot = build_snapshot(rpc_client, pool_id, lb_pair).await?;

    if !emit_snapshot(tx, pool_id, &snapshot).await {
        return Err(anyhow!("DLMM snapshot channel closed"));
    }

    Ok(snapshot)
}

fn is_missing_center_bin_array_error(err: &anyhow::Error) -> bool {
    let msg = format!("{err:#}").to_ascii_lowercase();
    msg.contains("missing dlmm center bin array account")
        || msg.contains("no other watched bin arrays exist")
}

fn dlmm_rebuild_backoff_ms(err: &anyhow::Error, failures: u32) -> u64 {
    const BASE_MS: u64 = 750;
    const MAX_MS: u64 = 30_000;
    const MISSING_CENTER_MS: u64 = 10_000;
    if is_missing_center_bin_array_error(err) {
        return MISSING_CENTER_MS;
    }
    let exp = failures.min(6).saturating_sub(1);
    BASE_MS.saturating_mul(1u64 << exp).min(MAX_MS)
}

fn should_skip_pool_for_missing_center(
    err: &anyhow::Error,
    missing_center_failures: &mut u32,
    max_missing_center_failures: u32,
) -> bool {
    if is_missing_center_bin_array_error(err) {
        *missing_center_failures = missing_center_failures.saturating_add(1);
        return max_missing_center_failures > 0
            && *missing_center_failures >= max_missing_center_failures;
    }
    *missing_center_failures = 0;
    false
}

pub async fn spawn_meteora_dlmm_pool_watcher_task(
    rpc_client: Arc<RpcClient>,
    ps_client: Arc<PubsubClient>,
    account_cfg: RpcAccountInfoConfig,
    pool_id: Pubkey,
    tx: mpsc::Sender<PoolUpdate>,
    stop: CancellationToken,
) {
    tokio::spawn(async move {
        let (mut lb_sub, lb_unsub) = match ps_client
            .account_subscribe(&pool_id, Some(account_cfg.clone()))
            .await
        {
            Ok(v) => v,
            Err(e) => {
                error!(pool = %pool_id, err = ?e, "DLMM lb_pair subscribe failed");
                return;
            }
        };

        let mut bin_subs = Subs::new();
        let mut mint_subs = Subs::new();
        let mut window_pks: Vec<Pubkey> = Vec::new();
        let mut mint_pks: Vec<Pubkey> = Vec::new();
        let mut last_lb_pair: Option<MeteoraLbPair> = None;
        let mut current_snapshot: Option<MeteoraDlmmSnapshot> = None;
        let mut snapshot_rebuild_backoff_until: Option<Instant> = None;
        let mut snapshot_rebuild_failures: u32 = 0;
        let mut snapshot_rebuild_last_log_at: Option<Instant> = None;
        let mut missing_center_failures: u32 = 0;
        let mut skip_pool_due_missing_center = false;
        let max_missing_center_failures = std::env::var("DLMM_SKIP_MISSING_CENTER_AFTER")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(3);

        if let Ok(lb_pair_acc) = rpc_client.get_account(&pool_id).await
            && let Ok(lb_pair) = decode_lb_pair(&lb_pair_acc.data)
        {
            if let Ok(new_window) = derive_window_pubkeys(pool_id, &lb_pair) {
                bin_subs
                    .rebuild(&ps_client, &account_cfg, &new_window, "DLMM BIN")
                    .await;
                window_pks = new_window;
            }
            let new_mints = mint_pubkeys(&lb_pair).to_vec();
            mint_subs
                .rebuild(&ps_client, &account_cfg, &new_mints, "DLMM MINT")
                .await;
            mint_pks = new_mints;
            last_lb_pair = Some(lb_pair);

            match rebuild_snapshot_from_rpc(rpc_client.as_ref(), &tx, pool_id, lb_pair).await {
                Ok(snapshot) => {
                    current_snapshot = Some(snapshot);
                    snapshot_rebuild_failures = 0;
                    snapshot_rebuild_backoff_until = None;
                }
                Err(err) => {
                    snapshot_rebuild_failures = 1;
                    let backoff_ms = dlmm_rebuild_backoff_ms(&err, snapshot_rebuild_failures);
                    snapshot_rebuild_backoff_until =
                        Some(Instant::now() + Duration::from_millis(backoff_ms));
                    warn!(
                        pool = %pool_id,
                        failures = snapshot_rebuild_failures,
                        backoff_ms,
                        err = %format!("{err:#}"),
                        "DLMM initial snapshot not ready; watcher will retry with backoff"
                    );
                    if should_skip_pool_for_missing_center(
                        &err,
                        &mut missing_center_failures,
                        max_missing_center_failures,
                    ) {
                        warn!(
                            pool = %pool_id,
                            failures = missing_center_failures,
                            max_missing_center_failures,
                            "DLMM pool skipped: center bin array missing for repeated retries"
                        );
                        skip_pool_due_missing_center = true;
                    }
                }
            }
        }

        loop {
            if skip_pool_due_missing_center {
                break;
            }
            tokio::select! {
                _ = stop.cancelled() => break,

                m = lb_sub.next() => {
                    let Some(resp) = m else {
                        warn!(pool = %pool_id, "DLMM lb_sub stream closed");
                        break;
                    };

                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %pool_id, "DLMM lb_pair data decode failed");
                        continue;
                    };

                    let lb_pair = match decode_lb_pair(&bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            error!(pool = %pool_id, err = ?e, "DLMM lb_pair deserialize failed");
                            continue;
                        }
                    };

                    last_lb_pair = Some(lb_pair);
                    let mut window_changed = false;

                    let new_mints = mint_pubkeys(&lb_pair).to_vec();
                    if new_mints != mint_pks {
                        tracing::info!(pool = %pool_id, "DLMM watcher: mint subscriptions changed");
                        mint_subs
                            .rebuild(&ps_client, &account_cfg, &new_mints, "DLMM MINT")
                            .await;
                        mint_pks = new_mints;
                    }

                    match derive_window_pubkeys(pool_id, &lb_pair) {
                        Ok(new_window) => {
                            if new_window != window_pks {
                                tracing::info!(pool = %pool_id, "DLMM watcher: bin window changed");
                                bin_subs
                                    .rebuild(&ps_client, &account_cfg, &new_window, "DLMM BIN")
                                    .await;
                                window_pks = new_window;
                                window_changed = true;
                            }
                        }
                        Err(e) => {
                            error!(pool = %pool_id, err = ?e, "DLMM watcher: failed deriving bin window");
                            continue;
                        }
                    }

                    if window_changed || current_snapshot.is_none() {
                        if snapshot_rebuild_backoff_until
                            .is_some_and(|until| Instant::now() < until)
                        {
                            continue;
                        }

                        match rebuild_snapshot_from_rpc(rpc_client.as_ref(), &tx, pool_id, lb_pair).await {
                            Ok(snapshot) => {
                                current_snapshot = Some(snapshot);
                                snapshot_rebuild_failures = 0;
                                snapshot_rebuild_backoff_until = None;
                            }
                            Err(err) if tx.is_closed() => {
                                let _ = err;
                                break;
                            }
                            Err(err) => {
                                // Avoid continuing to emit quotes against a stale bin window after
                                // active-bin movement if the rebuild failed (common on fresh pools).
                                current_snapshot = None;

                                snapshot_rebuild_failures =
                                    snapshot_rebuild_failures.saturating_add(1);
                                let backoff_ms =
                                    dlmm_rebuild_backoff_ms(&err, snapshot_rebuild_failures);
                                snapshot_rebuild_backoff_until =
                                    Some(Instant::now() + Duration::from_millis(backoff_ms));

                                let should_log = snapshot_rebuild_last_log_at
                                    .is_none_or(|t| t.elapsed() >= Duration::from_secs(15));
                                if should_log {
                                    snapshot_rebuild_last_log_at = Some(Instant::now());
                                    warn!(
                                        pool = %pool_id,
                                        failures = snapshot_rebuild_failures,
                                        backoff_ms,
                                        err = %format!("{err:#}"),
                                        "DLMM snapshot rebuild deferred; retrying with backoff"
                                    );
                                }
                                if should_skip_pool_for_missing_center(
                                    &err,
                                    &mut missing_center_failures,
                                    max_missing_center_failures,
                                ) {
                                    warn!(
                                        pool = %pool_id,
                                        failures = missing_center_failures,
                                        max_missing_center_failures,
                                        "DLMM pool skipped: center bin array missing for repeated retries"
                                    );
                                    break;
                                }
                            }
                        }
                        continue;
                    }

                    if let Some(snapshot) = current_snapshot.as_mut() {
                        snapshot.lb_pair = lb_pair;
                        match MeteoraBinArray::bin_id_to_bin_array_index(snapshot.lb_pair.active_id) {
                            Ok(center_idx) => {
                                snapshot.center_bin_array_index = center_idx;
                                if let Ok(indices) = derive_window_bin_array_indices(center_idx) {
                                    snapshot.window_bin_array_indices = indices;
                                }
                            }
                            Err(e) => {
                                warn!(pool = %pool_id, err = ?e, "DLMM watcher: failed to derive center index");
                            }
                        }

                        if !emit_snapshot(&tx, pool_id, snapshot).await {
                            break;
                        }
                    }
                }

                m = bin_subs.next(), if !bin_subs.streams.is_empty() => {
                    let Some((bin_pk, resp)) = m else {
                        continue;
                    };

                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %pool_id, bin_pk = %bin_pk, "DLMM bin data decode failed");
                        continue;
                    };

                    if bytes.is_empty() {
                        tracing::debug!(
                            pool = %pool_id,
                            bin_pk = %bin_pk,
                            "DLMM bin update ignored: empty account data (bin array likely uninitialized)"
                        );
                        continue;
                    }

                    let bin_array = match decode_bin_array(&bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            let msg = format!("{e:#}").to_ascii_lowercase();
                            if msg.contains("account too small for type")
                                || msg.contains("invalid account discriminator")
                                || msg.contains("failed to fill whole buffer")
                            {
                                tracing::debug!(
                                    pool = %pool_id,
                                    bin_pk = %bin_pk,
                                    err = %format!("{e:#}"),
                                    "DLMM bin update ignored: bin array account not initialized yet"
                                );
                                continue;
                            }
                            warn!(pool = %pool_id, bin_pk = %bin_pk, err = ?e, "DLMM bin decode failed");
                            continue;
                        }
                    };

                    if let Some(snapshot) = current_snapshot.as_mut() {
                        snapshot
                            .bin_arrays_by_pubkey
                            .insert(to_meteora_pubkey(bin_pk), bin_array);

                        if !emit_snapshot(&tx, pool_id, snapshot).await {
                            break;
                        }
                        continue;
                    }

                    let lb_pair = match last_lb_pair {
                        Some(v) => v,
                        None => continue,
                    };

                    if snapshot_rebuild_backoff_until
                        .is_some_and(|until| Instant::now() < until)
                    {
                        continue;
                    }

                    match rebuild_snapshot_from_rpc(rpc_client.as_ref(), &tx, pool_id, lb_pair).await {
                        Ok(snapshot) => {
                            current_snapshot = Some(snapshot);
                            snapshot_rebuild_failures = 0;
                            snapshot_rebuild_backoff_until = None;
                        }
                        Err(err) if tx.is_closed() => {
                            let _ = err;
                            break;
                        }
                        Err(err) => {
                            snapshot_rebuild_failures =
                                snapshot_rebuild_failures.saturating_add(1);
                            let backoff_ms =
                                dlmm_rebuild_backoff_ms(&err, snapshot_rebuild_failures);
                            snapshot_rebuild_backoff_until =
                                Some(Instant::now() + Duration::from_millis(backoff_ms));

                            let should_log = snapshot_rebuild_last_log_at
                                .is_none_or(|t| t.elapsed() >= Duration::from_secs(15));
                            if should_log {
                                snapshot_rebuild_last_log_at = Some(Instant::now());
                                warn!(
                                    pool = %pool_id,
                                    failures = snapshot_rebuild_failures,
                                    backoff_ms,
                                    err = %format!("{err:#}"),
                                    "DLMM snapshot rebuild deferred; retrying with backoff"
                                );
                            }
                            if should_skip_pool_for_missing_center(
                                &err,
                                &mut missing_center_failures,
                                max_missing_center_failures,
                            ) {
                                warn!(
                                    pool = %pool_id,
                                    failures = missing_center_failures,
                                    max_missing_center_failures,
                                    "DLMM pool skipped: center bin array missing for repeated retries"
                                );
                                break;
                            }
                        }
                    }
                }

                m = mint_subs.next(), if !mint_subs.streams.is_empty() => {
                    let Some((mint_pk, _resp)) = m else {
                        continue;
                    };

                    tracing::debug!(pool = %pool_id, mint = %mint_pk, "DLMM watcher: mint changed");

                    if let Some(snapshot) = current_snapshot.as_ref() {
                        if !emit_snapshot(&tx, pool_id, snapshot).await {
                            break;
                        }
                        continue;
                    }

                    let lb_pair = match last_lb_pair {
                        Some(v) => v,
                        None => continue,
                    };

                    if snapshot_rebuild_backoff_until
                        .is_some_and(|until| Instant::now() < until)
                    {
                        continue;
                    }

                    match rebuild_snapshot_from_rpc(rpc_client.as_ref(), &tx, pool_id, lb_pair).await {
                        Ok(snapshot) => {
                            current_snapshot = Some(snapshot);
                            snapshot_rebuild_failures = 0;
                            snapshot_rebuild_backoff_until = None;
                        }
                        Err(err) if tx.is_closed() => {
                            let _ = err;
                            break;
                        }
                        Err(err) => {
                            snapshot_rebuild_failures =
                                snapshot_rebuild_failures.saturating_add(1);
                            let backoff_ms =
                                dlmm_rebuild_backoff_ms(&err, snapshot_rebuild_failures);
                            snapshot_rebuild_backoff_until =
                                Some(Instant::now() + Duration::from_millis(backoff_ms));

                            let should_log = snapshot_rebuild_last_log_at
                                .is_none_or(|t| t.elapsed() >= Duration::from_secs(15));
                            if should_log {
                                snapshot_rebuild_last_log_at = Some(Instant::now());
                                warn!(
                                    pool = %pool_id,
                                    failures = snapshot_rebuild_failures,
                                    backoff_ms,
                                    err = %format!("{err:#}"),
                                    "DLMM snapshot rebuild deferred; retrying with backoff"
                                );
                            }
                            if should_skip_pool_for_missing_center(
                                &err,
                                &mut missing_center_failures,
                                max_missing_center_failures,
                            ) {
                                warn!(
                                    pool = %pool_id,
                                    failures = missing_center_failures,
                                    max_missing_center_failures,
                                    "DLMM pool skipped: center bin array missing for repeated retries"
                                );
                                break;
                            }
                        }
                    }
                }
            }
        }

        let keys: Vec<Pubkey> = bin_subs.unsubs.keys().copied().collect();
        for k in keys {
            bin_subs.remove(&k, "DLMM BIN").await;
        }
        let mint_keys: Vec<Pubkey> = mint_subs.unsubs.keys().copied().collect();
        for k in mint_keys {
            mint_subs.remove(&k, "DLMM MINT").await;
        }
        lb_unsub().await;
    });
}
