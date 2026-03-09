use futures::future::BoxFuture;
use futures::stream::BoxStream;
use solana_pubkey::Pubkey;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client_types::response::Response;
use solana_rpc_client_types::{config::RpcAccountInfoConfig, response::UiAccount};
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, StreamMap};
use tokio_util::sync::CancellationToken;

type UnsubscribeFn = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>;

pub enum TaCmd {
    TickWindowChanged { new_ticks: Vec<Pubkey> },
}

pub enum TaUpdate {
    Update {
        index: u16,
        tick_pk: Pubkey,
        data: Response<UiAccount>,
    },
}

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

    async fn add(&mut self, ps: &'a PubsubClient, key: Pubkey, cfg: RpcAccountInfoConfig) {
        match ps.account_subscribe(&key, Some(cfg)).await {
            Ok((stream, unsub)) => {
                self.unsubs.insert(key, unsub);
                self.streams.insert(key, stream);
                tracing::info!(tick_array=%key, "tick watcher: subscribed");
            }
            Err(e) => {
                tracing::warn!(tick_array=%key, err=?e, "tick watcher: subscribe failed");
            }
        }
    }

    async fn remove(&mut self, key: &Pubkey) {
        if let Some(unsub) = self.unsubs.remove(key) {
            unsub().await;
            tracing::info!(tick_array=%key, "tick watcher: unsubscribed");
        }
        self.streams.remove(key);
    }

    async fn rebuild(
        &mut self,
        ps: &'a PubsubClient,
        cfg: &RpcAccountInfoConfig,
        new_keys: &[Pubkey],
    ) {
        let new: HashSet<_> = new_keys.iter().copied().collect();
        let old: HashSet<_> = self.unsubs.keys().copied().collect();

        for k in old.difference(&new) {
            self.remove(k).await;
        }
        for k in new.difference(&old) {
            self.add(ps, *k, cfg.clone()).await;
        }
    }

    async fn next(&mut self) -> Option<(Pubkey, Response<UiAccount>)> {
        self.streams.next().await
    }
}

pub fn spawn_raydium_clmm_tick_watcher_task(
    ps: PubsubClient,
    initial_ticks: Vec<Pubkey>,
    rpc_cfg: RpcAccountInfoConfig,
    stop: CancellationToken,
) -> (mpsc::Sender<TaCmd>, mpsc::Receiver<TaUpdate>) {
    let (cmd_tx, mut cmd_rx) = mpsc::channel::<TaCmd>(8);
    let (evt_tx, evt_rx) = mpsc::channel::<TaUpdate>(1024);

    tokio::spawn(async move {
        let mut subs = Subs::new();
        let mut window_pks: Vec<Pubkey> = Vec::new();
        let mut pk_to_index: HashMap<Pubkey, u16> = HashMap::new();

        async fn apply_window<'a>(
            subs: &mut Subs<'a>,
            ps: &'a PubsubClient,
            rpc_cfg: &RpcAccountInfoConfig,
            window_pks: &mut Vec<Pubkey>,
            pk_to_index: &mut HashMap<Pubkey, u16>,
            new_ticks: Vec<Pubkey>,
        ) {
            if new_ticks == *window_pks {
                tracing::debug!("tick watcher: apply_window noop (identical)");
                return;
            }

            tracing::info!(len = new_ticks.len(), "tick watcher: apply_window");

            subs.rebuild(ps, rpc_cfg, &new_ticks).await;

            *window_pks = new_ticks;
            pk_to_index.clear();
            for (i, pk) in window_pks.iter().copied().enumerate() {
                pk_to_index.insert(pk, i as u16);
            }

            tracing::info!(
                pk_to_index_size = pk_to_index.len(),
                "tick watcher: window updated"
            );
        }

        apply_window(
            &mut subs,
            &ps,
            &rpc_cfg,
            &mut window_pks,
            &mut pk_to_index,
            initial_ticks,
        )
        .await;

        loop {
            tokio::select! {
                _ = stop.cancelled() => {
                    tracing::info!("tick watcher: stop cancelled");
                    break;
                }

                Some((tick_pk, data)) = subs.next() => {
                    let Some(&index) = pk_to_index.get(&tick_pk) else {
                        tracing::debug!(tick_array=%tick_pk, "tick watcher: update for pk not in current window");
                        continue;
                    };
                    if evt_tx.send(TaUpdate::Update { index, tick_pk, data }).await.is_err() {
                        break;
                    }
                }

                Some(cmd) = cmd_rx.recv() => {
                    match cmd {
                        TaCmd::TickWindowChanged { new_ticks } => {
                            apply_window(&mut subs, &ps, &rpc_cfg, &mut window_pks, &mut pk_to_index, new_ticks).await;
                        }
                    }
                }
            }
        }

        let keys: Vec<Pubkey> = subs.unsubs.keys().copied().collect();
        for k in keys {
            subs.remove(&k).await;
        }
    });

    (cmd_tx, evt_rx)
}
