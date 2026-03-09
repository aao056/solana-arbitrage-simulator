use crate::meteora_damm::{MeteoraDammPoolQuoteState, MeteoraDammSnapshot, parse_pool_quote_state};
use crate::models::PoolUpdate;
use crate::pumpswap::decode_token_account_amount_any_program;
use futures_util::StreamExt;
use solana_pubkey::Pubkey;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client_types::config::RpcAccountInfoConfig;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[allow(clippy::too_many_arguments)]
pub async fn spawn_meteora_damm_pool_watcher_task(
    ps_client: Arc<PubsubClient>,
    account_cfg: RpcAccountInfoConfig,
    pool_id: Pubkey,
    token_a_vault: Pubkey,
    token_b_vault: Pubkey,
    tx: mpsc::Sender<PoolUpdate>,
    stop: CancellationToken,
) {
    tokio::spawn(async move {
        let (mut pool_sub, _) = ps_client
            .account_subscribe(&pool_id, Some(account_cfg.clone()))
            .await
            .expect("meteora damm pool subscribe failed");

        let (mut token_a_sub, _) = ps_client
            .account_subscribe(&token_a_vault, Some(account_cfg.clone()))
            .await
            .expect("meteora damm token_a vault subscribe failed");

        let (mut token_b_sub, _) = ps_client
            .account_subscribe(&token_b_vault, Some(account_cfg))
            .await
            .expect("meteora damm token_b vault subscribe failed");

        let mut token_a_amount: Option<u64> = None;
        let mut token_b_amount: Option<u64> = None;
        let mut pool_state: Option<MeteoraDammPoolQuoteState> = None;
        let mut pool_state_slot: Option<u64> = None;
        let mut last_sent: Option<MeteoraDammSnapshot> = None;

        loop {
            tokio::select! {
                _ = stop.cancelled() => break,

                m = pool_sub.next() => {
                    let Some(resp) = m else {
                        warn!(pool = %pool_id, "meteora damm pool_sub stream closed");
                        continue;
                    };
                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %pool_id, "failed to decode meteora damm pool account data");
                        continue;
                    };
                    match parse_pool_quote_state(&bytes) {
                        Ok(state) => {
                            pool_state_slot = Some(resp.context.slot);
                            pool_state = Some(state);
                            info!(
                                pool = %pool_id,
                                sqrt_price = state.sqrt_price,
                                liquidity = state.liquidity,
                                "meteora damm pool state updated"
                            );
                        }
                        Err(err) => {
                            error!(pool = %pool_id, error = ?err, "failed to parse meteora damm pool state");
                        }
                    }
                }

                m = token_a_sub.next() => {
                    let Some(resp) = m else {
                        warn!(pool = %pool_id, "meteora damm token_a_sub stream closed");
                        continue;
                    };
                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %pool_id, "failed to decode meteora damm token_a vault data");
                        continue;
                    };
                    match decode_token_account_amount_any_program(&bytes) {
                        Ok(amt) => {
                            if token_a_amount != Some(amt) {
                                token_a_amount = Some(amt);
                                info!(pool = %pool_id, token_a_vault_amount = amt, "meteora damm token_a vault updated");
                            }
                        }
                        Err(err) => {
                            error!(pool = %pool_id, error = ?err, "failed to unpack meteora damm token_a vault");
                        }
                    }
                }

                m = token_b_sub.next() => {
                    let Some(resp) = m else {
                        warn!(pool = %pool_id, "meteora damm token_b_sub stream closed");
                        continue;
                    };
                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %pool_id, "failed to decode meteora damm token_b vault data");
                        continue;
                    };
                    match decode_token_account_amount_any_program(&bytes) {
                        Ok(amt) => {
                            if token_b_amount != Some(amt) {
                                token_b_amount = Some(amt);
                                info!(pool = %pool_id, token_b_vault_amount = amt, "meteora damm token_b vault updated");
                            }
                        }
                        Err(err) => {
                            error!(pool = %pool_id, error = ?err, "failed to unpack meteora damm token_b vault");
                        }
                    }
                }
            }

            if let (Some(a), Some(b), Some(ps)) = (token_a_amount, token_b_amount, pool_state) {
                let snap = MeteoraDammSnapshot {
                    token_a_vault_amount: a,
                    token_b_vault_amount: b,
                    liquidity: ps.liquidity,
                    sqrt_min_price: ps.sqrt_min_price,
                    sqrt_max_price: ps.sqrt_max_price,
                    sqrt_price: ps.sqrt_price,
                    activation_point: ps.activation_point,
                    activation_type: ps.activation_type,
                    pool_status: ps.pool_status,
                    collect_fee_mode: ps.collect_fee_mode,
                    version: ps.version,
                    fee_approx: ps.fee_approx,
                    last_observed_slot: pool_state_slot.unwrap_or(0),
                };
                if last_sent != Some(snap) {
                    last_sent = Some(snap);
                    if tx
                        .send(PoolUpdate::MeteoraDamm {
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
