use crate::models::{AmmSnapshot, PoolUpdate};
use crate::raydium::amm::core::AmmInfo;
use bytemuck::from_bytes;
use futures_util::StreamExt;
use solana_program::program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client_types::config::RpcAccountInfoConfig;
use spl_token::state::Account as TokenAccount;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[allow(clippy::too_many_arguments)]
pub async fn spawn_raydium_amm_pool_watcher_task(
    ps_client: Arc<PubsubClient>,
    account_cfg: RpcAccountInfoConfig,
    amm_id: Pubkey,
    coin_vault: Pubkey,
    pc_vault: Pubkey,
    tx: mpsc::Sender<PoolUpdate>,
    stop: CancellationToken,
) {
    tokio::spawn(async move {
        let (mut amm_sub, _) = ps_client
            .account_subscribe(&amm_id, Some(account_cfg.clone()))
            .await
            .expect("amm subscribe failed");

        let (mut coin_sub, _) = ps_client
            .account_subscribe(&coin_vault, Some(account_cfg.clone()))
            .await
            .expect("coin vault subscribe failed");

        let (mut pc_sub, _) = ps_client
            .account_subscribe(&pc_vault, Some(account_cfg))
            .await
            .expect("pc vault subscribe failed");

        let mut coin_vault_amount: Option<u64> = None;
        let mut pc_vault_amount: Option<u64> = None;
        let mut fee_num: Option<u64> = None;
        let mut fee_den: Option<u64> = None;

        let mut last_sent: Option<AmmSnapshot> = None;

        loop {
            tokio::select! {
                _ = stop.cancelled() => break,

                m = amm_sub.next() => {
                    let Some(resp) = m else {
                        warn!(pool = %amm_id, "amm_sub stream returned no response");
                        continue;
                    };

                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %amm_id, "failed to decode amm data");
                        continue;
                    };

                    let need = core::mem::size_of::<AmmInfo>();
                    if bytes.len() < need {
                        warn!(
                            pool = %amm_id,
                            got = bytes.len(),
                            need = need,
                            "AMM account too small"
                        );
                        continue;
                    }

                    let amm_info_parsed: &AmmInfo = from_bytes::<AmmInfo>(&bytes[..need]);
                    let n = amm_info_parsed.fees.swap_fee_numerator;
                    let d = amm_info_parsed.fees.swap_fee_denominator;

                    if fee_num != Some(n) || fee_den != Some(d) {
                        fee_num = Some(n);
                        fee_den = Some(d);

                        info!(
                            pool = %amm_id,
                            fee_numerator = n,
                            fee_denominator = d,
                            "AMM fees updated"
                        );
                    }
                }

                m = coin_sub.next() => {
                    let Some(resp) = m else {
                        warn!(pool = %amm_id, "coin_sub stream closed");
                        continue;
                    };

                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %amm_id, "failed to decode coin_vault data");
                        continue;
                    };

                    match TokenAccount::unpack(&bytes) {
                        Ok(token_account) => {
                            let amt = token_account.amount;
                            if coin_vault_amount != Some(amt) {
                                coin_vault_amount = Some(amt);

                                info!(
                                    pool = %amm_id,
                                    coin_vault_amount = amt,
                                    "coin vault updated"
                                );
                            }
                        }
                        Err(err) => {
                            error!(
                                pool = %amm_id,
                                error = ?err,
                                "failed to unpack pc vault SPL account"
                            );
                            continue;
                        }
                    }
                }

                m = pc_sub.next() => {
                    let Some(resp) = m else {
                        warn!(pool = %amm_id, "pc_sub stream closed");
                        break;
                    };

                    let Some(bytes) = resp.value.data.decode() else {
                        warn!(pool = %amm_id, "failed to decode pc_vault data");
                        continue;
                    };

                    match TokenAccount::unpack(&bytes) {
                        Ok(token_account) => {
                            let amt = token_account.amount;
                            if pc_vault_amount != Some(amt) {
                                pc_vault_amount = Some(amt);

                                info!(
                                    pool = %amm_id,
                                    pc_vault_amount = amt,
                                    "pc vault updated"
                                );
                            }
                        }
                        Err(err) => {
                            error!(
                                pool = %amm_id,
                                error = ?err,
                                "failed to unpack pc vault SPL account"
                            );
                        }
                    }
                }
            }

            if let (Some(coin), Some(pc), Some(n), Some(d)) =
                (coin_vault_amount, pc_vault_amount, fee_num, fee_den)
            {
                let snap = AmmSnapshot {
                    coin_vault_amount: coin,
                    pc_vault_amount: pc,
                    fee_numerator: n,
                    fee_denominator: d,
                };

                if last_sent != Some(snap) {
                    last_sent = Some(snap);

                    if tx
                        .send(PoolUpdate::RaydiumAmm {
                            amm_id,
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
