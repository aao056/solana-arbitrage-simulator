use solana_pubkey::Pubkey;
use solana_sdk::instruction::{AccountMeta, Instruction};

/// Build Raydium CLMM swap_v2 instruction.
///
/// - `is_base_input=true` => exact-in (amount_in, min_out)
/// - `is_base_input=false` => exact-out (amount_out, max_in)
///
/// Tick accounts:
/// - include bitmap extension (RO) first if provided
/// - then tick arrays (W) in correct order
#[allow(clippy::too_many_arguments)]
pub fn build_clmm_swap_v2_ix(
    program_id: Pubkey,
    payer: Pubkey,
    amm_config: Pubkey,
    pool_state: Pubkey,
    input_token_account: Pubkey,
    output_token_account: Pubkey,
    input_vault: Pubkey,
    output_vault: Pubkey,
    observation_state: Pubkey,
    input_vault_mint: Pubkey,
    output_vault_mint: Pubkey,
    // args
    amount: u64,
    other_amount_threshold: u64,
    sqrt_price_limit_x64: u128,
    is_base_input: bool,
    // remaining accounts
    tickarray_bitmap_extension: Option<Pubkey>,
    tick_arrays: &[Pubkey],
) -> Instruction {
    // Anchor discriminator for swap_v2 is the 8-byte "discriminator" in the IDL.
    // If you already have an Anchor discriminator helper in your project, use that.
    // Otherwise hardcode the 8 bytes from IDL:
    eprintln!("ticks {:#?}", tick_arrays);
    let discriminator: [u8; 8] = [43, 4, 237, 11, 26, 201, 30, 98];

    let mut data = Vec::with_capacity(8 + 8 + 8 + 16 + 1);
    data.extend_from_slice(&discriminator);
    data.extend_from_slice(&amount.to_le_bytes());
    data.extend_from_slice(&other_amount_threshold.to_le_bytes());
    data.extend_from_slice(&sqrt_price_limit_x64.to_le_bytes());
    data.push(if is_base_input { 1 } else { 0 });

    let mut accounts = Vec::with_capacity(13 + 1 + tick_arrays.len());

    accounts.push(AccountMeta::new(payer, true)); // 0 payer (signer, writable ok)
    accounts.push(AccountMeta::new_readonly(amm_config, false)); // 1 amm_config
    accounts.push(AccountMeta::new(pool_state, false)); // 2 pool_state (w)
    accounts.push(AccountMeta::new(input_token_account, false)); // 3 input_token_account (w)
    accounts.push(AccountMeta::new(output_token_account, false)); // 4 output_token_account (w)
    accounts.push(AccountMeta::new(input_vault, false)); // 5 input_vault (w)
    accounts.push(AccountMeta::new(output_vault, false)); // 6 output_vault (w)
    accounts.push(AccountMeta::new(observation_state, false)); // 7 observation_state (w)

    accounts.push(AccountMeta::new_readonly(spl_token::ID, false)); // 8 token_program
    accounts.push(AccountMeta::new_readonly(spl_token_2022::ID, false)); // 9 token_program_2022
    accounts.push(AccountMeta::new_readonly(
        Pubkey::new_from_array(spl_memo::ID.to_bytes()),
        false,
    )); // 10 memo_program

    accounts.push(AccountMeta::new_readonly(input_vault_mint, false)); // 11 input_vault_mint
    accounts.push(AccountMeta::new_readonly(output_vault_mint, false)); // 12 output_vault_mint

    if let Some(ext) = tickarray_bitmap_extension {
        accounts.push(AccountMeta::new_readonly(ext, false)); // remaining[0]
    }
    for &ta in tick_arrays {
        accounts.push(AccountMeta::new(ta, false)); // remaining tick arrays (w)
    }

    Instruction {
        program_id,
        accounts,
        data,
    }
}
