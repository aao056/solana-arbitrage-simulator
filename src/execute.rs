use crate::telegram_send::tg_send;
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_rpc_client_types::config::{CommitmentLevel, RpcSendTransactionConfig};
use solana_sdk::instruction::Instruction;
use solana_sdk::message::{AddressLookupTableAccount, VersionedMessage, v0};
use solana_sdk::signer::{Signer, keypair::Keypair};
use solana_sdk::transaction::{Transaction, VersionedTransaction};

pub async fn send_tx(
    rpc: &RpcClient,
    payer: &Keypair,
    ixs: Vec<Instruction>,
    lookup_tables: &[AddressLookupTableAccount],
) -> anyhow::Result<String> {
    let mut all = Vec::with_capacity(ixs.len() + 2);
    all.push(ComputeBudgetInstruction::set_compute_unit_limit(800_000));
    all.push(ComputeBudgetInstruction::set_compute_unit_price(1));
    all.extend(ixs);

    let cfg = RpcSendTransactionConfig {
        skip_preflight: false,
        preflight_commitment: Some(CommitmentLevel::Processed),
        max_retries: Some(3),
        ..Default::default()
    };

    let bh = rpc.get_latest_blockhash().await?;
    let sig = if lookup_tables.is_empty() {
        let tx = Transaction::new_signed_with_payer(&all, Some(&payer.pubkey()), &[payer], bh);
        rpc.send_transaction_with_config(&tx, cfg).await?
    } else {
        let message = v0::Message::try_compile(&payer.pubkey(), &all, lookup_tables, bh)
            .map_err(|e| anyhow::anyhow!("failed to compile v0 message: {e:?}"))?;
        let tx = VersionedTransaction::try_new(VersionedMessage::V0(message), &[payer])
            .map_err(|e| anyhow::anyhow!("failed to sign v0 transaction: {e:?}"))?;
        rpc.send_transaction_with_config(&tx, cfg).await?
    };
    rpc.confirm_transaction(&sig).await?;

    Ok(sig.to_string())
}

pub async fn send_tx_and_notify(
    rpc: &RpcClient,
    payer: &Keypair,
    ixs: Vec<Instruction>,
    lookup_tables: &[AddressLookupTableAccount],
    tg_token: Option<&str>,
    tg_chat_id: Option<&str>,
    telegram_message: Option<String>,
) -> anyhow::Result<String> {
    let sig = send_tx(rpc, payer, ixs, lookup_tables).await?;

    match (tg_token, tg_chat_id, telegram_message) {
        (Some(token), Some(chat_id), Some(message)) => {
            let full_message = format!(
                "{message}\n\
<b>Signature</b>: <code>{sig}</code>\n\
<a href=\"https://solscan.io/tx/{sig}\">View on Solscan</a>"
            );
            if let Err(err) = tg_send(token, chat_id, &full_message).await {
                tracing::warn!(error = ?err, "telegram notify failed");
            }
        }
        _ => {
            tracing::debug!("telegram notify skipped (missing token/chat/message)");
        }
    }

    Ok(sig)
}
