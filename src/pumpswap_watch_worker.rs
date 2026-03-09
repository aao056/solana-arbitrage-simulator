use crate::models::PoolUpdate;
use crate::pumpswap::{PumpAmmSnapshot, decode_token_account_amount_any_program};
use futures_util::StreamExt;
use solana_pubkey::Pubkey;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client_types::config::RpcAccountInfoConfig;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

#[allow(clippy::too_many_arguments)]
pub async fn spawn_pumpswap_pool_watcher_task(
    ps_client: Arc<PubsubClient>,
    account_cfg: RpcAccountInfoConfig,
    pool_id: Pubkey,
    base_vault: Pubkey,
    quote_vault: Pubkey,
    tx: mpsc::Sender<PoolUpdate>,
    stop: CancellationToken,
) {
    tokio::spawn(async move {
        let (mut base_sub, _) = ps_client
            .account_subscribe(&base_vault, Some(account_cfg.clone()))
            .await
            .expect("pumpswap base vault subscribe failed");

        let (mut quote_sub, _) = ps_client
            .account_subscribe(&quote_vault, Some(account_cfg))
            .await
            .expect("pumpswap quote vault subscribe failed");

        let mut base_vault_amount: Option<u64> = None;
        let mut quote_vault_amount: Option<u64> = None;
        let mut last_sent: Option<PumpAmmSnapshot> = None;

        loop {
            tokio::select! {
                _ = stop.cancelled() => break,

                m = base_sub.next() => {
                    let Some(resp) = m else {
                        warn!(pool = %pool_id, "pumpswap base_sub stream closed");
                        continue;
                    };

                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %pool_id, "failed to decode pumpswap base_vault data");
                        continue;
                    };

                    match decode_token_account_amount_any_program(&bytes) {
                        Ok(amt) => {
                            if base_vault_amount != Some(amt) {
                                base_vault_amount = Some(amt);
                                // info!(pool = %pool_id, base_vault_amount = amt, "pumpswap base vault updated");
                            }
                        }
                        Err(err) => {
                            error!(pool = %pool_id, error = ?err, "failed to unpack pumpswap base vault SPL account");
                            continue;
                        }
                    }
                }

                m = quote_sub.next() => {
                    let Some(resp) = m else {
                        warn!(pool = %pool_id, "pumpswap quote_sub stream closed");
                        continue;
                    };

                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %pool_id, "failed to decode pumpswap quote_vault data");
                        continue;
                    };

                    match decode_token_account_amount_any_program(&bytes) {
                        Ok(amt) => {
                            if quote_vault_amount != Some(amt) {
                                quote_vault_amount = Some(amt);
                                // info!(pool = %pool_id, quote_vault_amount = amt, "pumpswap quote vault updated");
                            }
                        }
                        Err(err) => {
                            error!(pool = %pool_id, error = ?err, "failed to unpack pumpswap quote vault SPL account");
                            continue;
                        }
                    }
                }
            }

            if let (Some(base), Some(quote)) = (base_vault_amount, quote_vault_amount) {
                let snap = PumpAmmSnapshot {
                    base_vault_amount: base,
                    quote_vault_amount: quote,
                };

                if last_sent != Some(snap) {
                    last_sent = Some(snap);
                    if tx
                        .send(PoolUpdate::PumpAmm {
                            pool_id,
                            snapshot: snap,
                        })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    });
}
