use crate::models::PoolUpdate;
use crate::orca_whirlpool::{
    OrcaWhirlpoolSnapshot, build_snapshot, decode_oracle_facade,
    decode_tick_array_facade_or_default, get_tick_array_keys_and_indexes_from_whirlpool,
    transfer_fee_from_mint_account_data,
};
use anyhow::Context;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures_util::StreamExt;
use orca_whirlpools_client::Whirlpool;
use orca_whirlpools_core::get_tick_array_start_tick_index;
use solana_pubkey::Pubkey;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_types::config::RpcAccountInfoConfig;
use solana_rpc_client_types::response::{Response, UiAccount};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::StreamMap;
use tokio_util::sync::CancellationToken;

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
                tracing::info!(label, account = %key, "ORCA watcher: subscribed");
            }
            Err(err) => {
                tracing::warn!(label, account = %key, ?err, "ORCA watcher: subscribe failed");
            }
        }
    }

    async fn remove(&mut self, key: &Pubkey, label: &str) {
        if let Some(unsub) = self.unsubs.remove(key) {
            unsub().await;
            tracing::info!(label, account = %key, "ORCA watcher: unsubscribed");
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

        for key in old.difference(&new) {
            self.remove(key, label).await;
        }
        for key in new.difference(&old) {
            self.add(ps, *key, cfg.clone(), label).await;
        }
    }

    async fn next(&mut self) -> Option<(Pubkey, Response<UiAccount>)> {
        self.streams.next().await
    }
}

async fn emit_snapshot(
    tx: &mpsc::Sender<PoolUpdate>,
    pool_id: Pubkey,
    snapshot: &OrcaWhirlpoolSnapshot,
) -> bool {
    tx.send(PoolUpdate::OrcaWhirlpool {
        pool_id,
        snapshot: Box::new(snapshot.clone()),
    })
    .await
    .is_ok()
}

#[allow(clippy::too_many_arguments)]
pub async fn spawn_orca_whirlpool_pool_watcher_task(
    rpc_client: Arc<RpcClient>,
    ps_client: Arc<PubsubClient>,
    account_cfg: RpcAccountInfoConfig,
    pool_id: Pubkey,
    token_program_a: Pubkey,
    token_program_b: Pubkey,
    oracle_pubkey: Pubkey,
    tx: mpsc::Sender<PoolUpdate>,
    stop: CancellationToken,
) {
    tokio::spawn(async move {
        let (mut whirlpool_sub, whirlpool_unsub) = match ps_client
            .account_subscribe(&pool_id, Some(account_cfg.clone()))
            .await
        {
            Ok(v) => v,
            Err(err) => {
                tracing::error!(pool = %pool_id, ?err, "ORCA whirlpool subscribe failed");
                return;
            }
        };

        let mut oracle_unsub: Option<UnsubscribeFn> = None;
        let mut oracle_sub = match ps_client
            .account_subscribe(&oracle_pubkey, Some(account_cfg.clone()))
            .await
        {
            Ok((stream, unsub)) => {
                oracle_unsub = Some(unsub);
                Some(stream)
            }
            Err(err) => {
                tracing::debug!(
                    pool = %pool_id,
                    oracle = %oracle_pubkey,
                    ?err,
                    "ORCA oracle subscribe skipped"
                );
                None
            }
        };

        let mut tick_subs = Subs::new();
        let mut mint_subs = Subs::new();
        let mut tick_key_to_index: HashMap<Pubkey, usize> = HashMap::new();
        let mut snapshot: Option<OrcaWhirlpoolSnapshot> = None;

        if let Ok(account) = rpc_client.get_account(&pool_id).await
            && let Ok(whirlpool) = Whirlpool::from_bytes(&account.data)
        {
            match build_snapshot(
                rpc_client.as_ref(),
                pool_id,
                whirlpool,
                token_program_a,
                token_program_b,
                oracle_pubkey,
            )
            .await
            {
                Ok(initial) => {
                    let tick_keys = initial.tick_array_pubkeys.to_vec();
                    let mint_keys = vec![
                        initial.whirlpool.token_mint_a,
                        initial.whirlpool.token_mint_b,
                    ];

                    tick_subs
                        .rebuild(&ps_client, &account_cfg, &tick_keys, "ORCA TICK")
                        .await;
                    mint_subs
                        .rebuild(&ps_client, &account_cfg, &mint_keys, "ORCA MINT")
                        .await;

                    tick_key_to_index.clear();
                    for (idx, key) in tick_keys.into_iter().enumerate() {
                        tick_key_to_index.insert(key, idx);
                    }

                    if !emit_snapshot(&tx, pool_id, &initial).await {
                        return;
                    }
                    snapshot = Some(initial);
                }
                Err(err) => {
                    tracing::warn!(pool = %pool_id, ?err, "ORCA initial snapshot build failed");
                }
            }
        }

        loop {
            tokio::select! {
                _ = stop.cancelled() => break,

                m = whirlpool_sub.next() => {
                    let Some(resp) = m else {
                        tracing::warn!(pool = %pool_id, "ORCA whirlpool stream closed");
                        break;
                    };

                    let Some(bytes) = resp.value.data.decode() else {
                        tracing::warn!(pool = %pool_id, "ORCA whirlpool decode failed");
                        continue;
                    };

                    let whirlpool = match Whirlpool::from_bytes(&bytes) {
                        Ok(v) => v,
                        Err(err) => {
                            tracing::warn!(pool = %pool_id, ?err, "ORCA whirlpool parse failed");
                            continue;
                        }
                    };

                    if snapshot.is_none() {
                        match build_snapshot(
                            rpc_client.as_ref(),
                            pool_id,
                            whirlpool,
                            token_program_a,
                            token_program_b,
                            oracle_pubkey,
                        )
                        .await
                        {
                            Ok(new_snapshot) => {
                                let tick_keys = new_snapshot.tick_array_pubkeys.to_vec();
                                let mint_keys = vec![
                                    new_snapshot.whirlpool.token_mint_a,
                                    new_snapshot.whirlpool.token_mint_b,
                                ];
                                tick_subs
                                    .rebuild(&ps_client, &account_cfg, &tick_keys, "ORCA TICK")
                                    .await;
                                mint_subs
                                    .rebuild(&ps_client, &account_cfg, &mint_keys, "ORCA MINT")
                                    .await;

                                tick_key_to_index.clear();
                                for (idx, key) in tick_keys.into_iter().enumerate() {
                                    tick_key_to_index.insert(key, idx);
                                }

                                if !emit_snapshot(&tx, pool_id, &new_snapshot).await {
                                    break;
                                }
                                snapshot = Some(new_snapshot);
                            }
                            Err(err) => {
                                tracing::warn!(pool = %pool_id, ?err, "ORCA snapshot rebuild failed");
                            }
                        }
                        continue;
                    }

                    let snap = snapshot.as_mut().expect("snapshot checked above");
                    let prev_whirlpool = snap.whirlpool.clone();
                    let old_start = get_tick_array_start_tick_index(
                        snap.whirlpool.tick_current_index,
                        snap.whirlpool.tick_spacing,
                    );
                    let new_start = get_tick_array_start_tick_index(
                        whirlpool.tick_current_index,
                        whirlpool.tick_spacing,
                    );
                    let mints_changed = snap.whirlpool.token_mint_a != whirlpool.token_mint_a
                        || snap.whirlpool.token_mint_b != whirlpool.token_mint_b;

                    snap.whirlpool = whirlpool;
                    let mut ready_to_emit = true;

                    if old_start != new_start {
                        ready_to_emit = false;
                        match get_tick_array_keys_and_indexes_from_whirlpool(&pool_id, &snap.whirlpool)
                            .context("derive tick array keys failed")
                        {
                            Ok((keys, indexes)) => {
                                match crate::orca_whirlpool::fetch_tick_arrays(
                                    rpc_client.as_ref(),
                                    &keys,
                                    &indexes,
                                )
                                .await
                                {
                                    Ok(arrays) => {
                                        snap.tick_array_pubkeys = keys;
                                        snap.tick_array_start_indexes = indexes;
                                        snap.tick_arrays = arrays;

                                        let tick_keys = snap.tick_array_pubkeys.to_vec();
                                        tick_subs
                                            .rebuild(&ps_client, &account_cfg, &tick_keys, "ORCA TICK")
                                            .await;
                                        tick_key_to_index.clear();
                                        for (idx, key) in tick_keys.into_iter().enumerate() {
                                            tick_key_to_index.insert(key, idx);
                                        }
                                        ready_to_emit = true;
                                    }
                                    Err(err) => {
                                        tracing::warn!(pool = %pool_id, ?err, "ORCA tick array fetch failed");
                                    }
                                }
                            }
                            Err(err) => {
                                tracing::warn!(pool = %pool_id, ?err, "ORCA tick array derive failed");
                            }
                        }
                    }

                    if !ready_to_emit {
                        snap.whirlpool = prev_whirlpool;
                        continue;
                    }

                    if mints_changed {
                        let mint_keys = vec![snap.whirlpool.token_mint_a, snap.whirlpool.token_mint_b];
                        mint_subs
                            .rebuild(&ps_client, &account_cfg, &mint_keys, "ORCA MINT")
                            .await;
                    }

                    if !emit_snapshot(&tx, pool_id, snap).await {
                        break;
                    }
                }

                m = tick_subs.next(), if !tick_subs.streams.is_empty() => {
                    let Some((tick_pk, resp)) = m else {
                        continue;
                    };

                    let Some(snap) = snapshot.as_mut() else {
                        continue;
                    };

                    let Some(index) = tick_key_to_index.get(&tick_pk).copied() else {
                        continue;
                    };

                    let Some(bytes) = resp.value.data.decode() else {
                        tracing::warn!(pool = %pool_id, tick = %tick_pk, "ORCA tick decode failed");
                        continue;
                    };

                    let fallback_start = snap.tick_array_start_indexes[index];
                    snap.tick_arrays[index] = decode_tick_array_facade_or_default(&bytes, fallback_start);

                    if !emit_snapshot(&tx, pool_id, snap).await {
                        break;
                    }
                }

                m = mint_subs.next(), if !mint_subs.streams.is_empty() => {
                    let Some((mint_pk, resp)) = m else {
                        continue;
                    };
                    let Some(snap) = snapshot.as_mut() else {
                        continue;
                    };

                    let Some(bytes) = resp.value.data.decode() else {
                        tracing::warn!(pool = %pool_id, mint = %mint_pk, "ORCA mint decode failed");
                        continue;
                    };

                    let owner = match resp.value.owner.parse::<Pubkey>() {
                        Ok(pk) => pk,
                        Err(err) => {
                            tracing::warn!(pool = %pool_id, mint = %mint_pk, ?err, "ORCA mint owner parse failed");
                            continue;
                        }
                    };

                    let fee = transfer_fee_from_mint_account_data(owner, &bytes);
                    if mint_pk == snap.whirlpool.token_mint_a {
                        snap.transfer_fee_a = fee;
                    } else if mint_pk == snap.whirlpool.token_mint_b {
                        snap.transfer_fee_b = fee;
                    } else {
                        continue;
                    }

                    if !emit_snapshot(&tx, pool_id, snap).await {
                        break;
                    }
                }

                m = async {
                    let stream = oracle_sub.as_mut()?;
                    stream.next().await
                }, if oracle_sub.is_some() => {
                    let Some(resp) = m else {
                        tracing::warn!(pool = %pool_id, "ORCA oracle stream closed");
                        oracle_sub = None;
                        continue;
                    };
                    let Some(snap) = snapshot.as_mut() else {
                        continue;
                    };

                    let Some(bytes) = resp.value.data.decode() else {
                        tracing::warn!(pool = %pool_id, oracle = %oracle_pubkey, "ORCA oracle decode failed");
                        continue;
                    };

                    snap.oracle = decode_oracle_facade(&bytes);
                    if !emit_snapshot(&tx, pool_id, snap).await {
                        break;
                    }
                }
            }
        }

        let tick_keys: Vec<Pubkey> = tick_subs.unsubs.keys().copied().collect();
        for key in tick_keys {
            tick_subs.remove(&key, "ORCA TICK").await;
        }
        let mint_keys: Vec<Pubkey> = mint_subs.unsubs.keys().copied().collect();
        for key in mint_keys {
            mint_subs.remove(&key, "ORCA MINT").await;
        }
        if let Some(unsub) = oracle_unsub {
            unsub().await;
        }
        whirlpool_unsub().await;
    });
}
