use crate::models::PoolUpdate;
use crate::pumpswap::decode_token_account_amount_any_program;
use crate::raydium_cpmm::{RaydiumCpmmSnapshot, parse_pool_dynamic};
use futures_util::StreamExt;
use solana_pubkey::Pubkey;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client_types::config::RpcAccountInfoConfig;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn};

#[allow(clippy::too_many_arguments)]
pub async fn spawn_raydium_cpmm_pool_watcher_task(
    ps_client: Arc<PubsubClient>,
    account_cfg: RpcAccountInfoConfig,
    pool_id: Pubkey,
    pool_account: Pubkey,
    token_0_vault: Pubkey,
    token_1_vault: Pubkey,
    tx: mpsc::Sender<PoolUpdate>,
    stop: CancellationToken,
) {
    tokio::spawn(async move {
        let (mut pool_sub, _) = ps_client
            .account_subscribe(&pool_account, Some(account_cfg.clone()))
            .await
            .expect("raydium cpmm pool subscribe failed");

        let (mut token_0_vault_sub, _) = ps_client
            .account_subscribe(&token_0_vault, Some(account_cfg.clone()))
            .await
            .expect("raydium cpmm token_0 vault subscribe failed");

        let (mut token_1_vault_sub, _) = ps_client
            .account_subscribe(&token_1_vault, Some(account_cfg))
            .await
            .expect("raydium cpmm token_1 vault subscribe failed");

        let mut pool_dynamic = None;
        let mut token_0_vault_amount = None;
        let mut token_1_vault_amount = None;
        let mut last_sent: Option<RaydiumCpmmSnapshot> = None;

        loop {
            tokio::select! {
                _ = stop.cancelled() => break,

                m = pool_sub.next() => {
                    let Some(resp) = m else {
                        warn!(pool = %pool_id, "raydium cpmm pool_sub stream closed");
                        continue;
                    };
                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %pool_id, "failed to decode raydium cpmm pool account data");
                        continue;
                    };
                    match parse_pool_dynamic(&bytes) {
                        Ok(dynamic) => {
                            pool_dynamic = Some(dynamic);
                        }
                        Err(err) => {
                            error!(pool = %pool_id, ?err, "failed to parse raydium cpmm pool account");
                            continue;
                        }
                    }
                }

                m = token_0_vault_sub.next() => {
                    let Some(resp) = m else {
                        warn!(pool = %pool_id, "raydium cpmm token_0_vault_sub stream closed");
                        continue;
                    };
                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %pool_id, "failed to decode raydium cpmm token_0 vault account");
                        continue;
                    };
                    match decode_token_account_amount_any_program(&bytes) {
                        Ok(amt) => token_0_vault_amount = Some(amt),
                        Err(err) => {
                            error!(pool = %pool_id, ?err, "failed to decode raydium cpmm token_0 vault amount");
                            continue;
                        }
                    }
                }

                m = token_1_vault_sub.next() => {
                    let Some(resp) = m else {
                        warn!(pool = %pool_id, "raydium cpmm token_1_vault_sub stream closed");
                        continue;
                    };
                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %pool_id, "failed to decode raydium cpmm token_1 vault account");
                        continue;
                    };
                    match decode_token_account_amount_any_program(&bytes) {
                        Ok(amt) => token_1_vault_amount = Some(amt),
                        Err(err) => {
                            error!(pool = %pool_id, ?err, "failed to decode raydium cpmm token_1 vault amount");
                            continue;
                        }
                    }
                }
            }

            if let (Some(dyn_state), Some(vault_0), Some(vault_1)) =
                (pool_dynamic, token_0_vault_amount, token_1_vault_amount)
            {
                let snap = RaydiumCpmmSnapshot {
                    token_0_vault_amount: vault_0,
                    token_1_vault_amount: vault_1,
                    protocol_fees_token_0: dyn_state.protocol_fees_token_0,
                    protocol_fees_token_1: dyn_state.protocol_fees_token_1,
                    fund_fees_token_0: dyn_state.fund_fees_token_0,
                    fund_fees_token_1: dyn_state.fund_fees_token_1,
                    status: dyn_state.status,
                    open_time: dyn_state.open_time,
                };

                if last_sent != Some(snap) {
                    last_sent = Some(snap);
                    if tx
                        .send(PoolUpdate::RaydiumCpmm {
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
