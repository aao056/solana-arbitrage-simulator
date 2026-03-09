use anyhow::{Context, Result};
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_program::program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_types::config::{
    CommitmentConfig, RpcSimulateTransactionAccountsConfig, RpcSimulateTransactionConfig,
    UiAccountEncoding, UiTransactionEncoding,
};
use solana_rpc_client_types::response::RpcSimulateTransactionResult;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::{AddressLookupTableAccount, VersionedMessage, v0};
use solana_sdk::signer::{Signer, keypair::Keypair};
use solana_sdk::transaction::{Transaction, VersionedTransaction};
use spl_token::state::Account as SplTokenAccount;

pub async fn debug_missing_accounts(rpc: &RpcClient, ixs: &[Instruction]) -> Result<()> {
    let mut keys: Vec<Pubkey> = Vec::new();

    for ix in ixs {
        keys.push(ix.program_id);
        for meta in &ix.accounts {
            keys.push(meta.pubkey);
        }
    }

    keys.sort();
    keys.dedup();

    tracing::info!(
        total = keys.len(),
        "debug_missing_accounts: checking accounts..."
    );

    const CHUNK_SIZE: usize = 100;
    for chunk in keys.chunks(CHUNK_SIZE) {
        let accounts = rpc.get_multiple_accounts(chunk).await?;
        for (idx, account) in accounts.into_iter().enumerate() {
            if account.is_none() {
                tracing::error!(missing = %chunk[idx], "MISSING ACCOUNT");
            }
        }
    }

    Ok(())
}

pub async fn simulate_tx(
    rpc: &RpcClient,
    payer: &Keypair,
    ixs: Vec<Instruction>,
    lookup_tables: &[AddressLookupTableAccount],
    observed_accounts: &[Pubkey],
) -> anyhow::Result<RpcSimulateTransactionResult> {
    let mut all = Vec::with_capacity(ixs.len() + 2);
    all.push(ComputeBudgetInstruction::set_compute_unit_limit(800_000));
    all.push(ComputeBudgetInstruction::set_compute_unit_price(1));
    all.extend(ixs);

    let accounts = if observed_accounts.is_empty() {
        None
    } else {
        Some(RpcSimulateTransactionAccountsConfig {
            addresses: observed_accounts.iter().map(|pk| pk.to_string()).collect(),
            encoding: Some(UiAccountEncoding::Base64),
        })
    };

    let cfg = RpcSimulateTransactionConfig {
        sig_verify: false,
        replace_recent_blockhash: true,
        commitment: Some(CommitmentConfig::processed()),
        encoding: Some(UiTransactionEncoding::Base64),
        accounts,
        ..Default::default()
    };

    let latest_blockhash = rpc.get_latest_blockhash().await?;
    if lookup_tables.is_empty() {
        let tx = Transaction::new_signed_with_payer(
            &all,
            Some(&payer.pubkey()),
            &[payer],
            latest_blockhash,
        );
        Ok(rpc.simulate_transaction_with_config(&tx, cfg).await?.value)
    } else {
        let message =
            v0::Message::try_compile(&payer.pubkey(), &all, lookup_tables, latest_blockhash)
                .map_err(|e| {
                    anyhow::anyhow!("failed to compile v0 message for simulation: {e:?}")
                })?;
        let tx = VersionedTransaction::try_new(VersionedMessage::V0(message), &[payer])
            .map_err(|e| anyhow::anyhow!("failed to sign v0 simulation transaction: {e:?}"))?;
        Ok(rpc.simulate_transaction_with_config(&tx, cfg).await?.value)
    }
}

pub fn simulated_token_amount_at(sim: &RpcSimulateTransactionResult, idx: usize) -> Result<u64> {
    let accounts = sim
        .accounts
        .as_ref()
        .context("simulation response missing accounts payload")?;
    let ui_acc = accounts
        .get(idx)
        .context("simulation account index out of bounds")?
        .as_ref()
        .context("simulation account missing at requested index")?;
    let raw = ui_acc
        .data
        .decode()
        .context("simulation account data decode failed")?;
    let token_acc =
        SplTokenAccount::unpack(&raw).context("failed to decode simulated SPL token account")?;
    Ok(token_acc.amount)
}

pub fn dump_sim(label: &str, sim: &RpcSimulateTransactionResult) {
    tracing::info!(
        ?sim.err,
        units = ?sim.units_consumed,
        loaded = ?sim.loaded_accounts_data_size,
        "{label}"
    );

    match sim.logs.as_ref() {
        Some(logs) if !logs.is_empty() => {
            for line in logs.iter().take(200) {
                tracing::info!("log: {}", line);
            }
        }
        _ => tracing::warn!("(no logs returned)"),
    }
}
