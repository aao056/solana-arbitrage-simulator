use anyhow::{Result, anyhow};
use solana_pubkey::Pubkey;
use solana_sdk::instruction::{AccountMeta, Instruction};

pub fn derive_amm_authority(program_id: &Pubkey, nonce_u64: u64) -> Result<Pubkey> {
    let nonce_u8: u8 = nonce_u64
        .try_into()
        .map_err(|_| anyhow!("amm nonce out of u8 range: {nonce_u64}"))?;

    let seed = b"amm authority";

    Pubkey::create_program_address(&[seed.as_ref(), &[nonce_u8]], program_id)
        .map_err(|e| anyhow!("create_program_address failed: {e:?}"))
}

#[allow(clippy::too_many_arguments)]
pub fn build_swap_base_in_v2_ix(
    program_id: Pubkey,
    amm_id: Pubkey,
    amm_authority: Pubkey,
    amm_coin_vault: Pubkey,
    amm_pc_vault: Pubkey,
    user_source: Pubkey,
    user_destination: Pubkey,
    user_owner: Pubkey,
    amount_in: u128,
    min_amount_out: u128,
) -> Instruction {
    let mut data = Vec::with_capacity(1 + 8 + 8);
    data.push(16u8);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());

    let accounts = vec![
        AccountMeta::new_readonly(spl_token::ID, false), // 0 token program
        AccountMeta::new(amm_id, false),                 // 1 amm id (writable)
        AccountMeta::new_readonly(amm_authority, false), // 2 amm authority
        AccountMeta::new(amm_coin_vault, false),         // 3 coin vault (writable)
        AccountMeta::new(amm_pc_vault, false),           // 4 pc vault (writable)
        AccountMeta::new(user_source, false),            // 5 user source (writable)
        AccountMeta::new(user_destination, false),       // 6 user dest (writable)
        AccountMeta::new(user_owner, true),              // 7 user owner (signer)
    ];

    Instruction {
        program_id,
        accounts,
        data,
    }
}
