use anyhow::{Result, anyhow};
use raydium_amm_v3::states::PoolState;
use solana_pubkey::Pubkey as SolPubkey;

fn a2s(pk: anchor_lang::prelude::Pubkey) -> SolPubkey {
    SolPubkey::new_from_array(pk.to_bytes())
}

#[derive(Clone, Copy, Debug)]
pub struct ClmmRoute {
    pub input_vault: SolPubkey,
    pub output_vault: SolPubkey,
    pub input_mint: SolPubkey,
    pub output_mint: SolPubkey,
    pub user_input_ata: SolPubkey,
    pub user_output_ata: SolPubkey,
    pub zero_for_one: bool,
    pub observation_key: SolPubkey,
}

pub fn clmm_route_from_input_mint(
    ps: &PoolState,
    input_mint: SolPubkey,
    user_ata_for_mint0: SolPubkey,
    user_ata_for_mint1: SolPubkey,
) -> Result<ClmmRoute> {
    let mint0 = a2s(ps.token_mint_0);
    let mint1 = a2s(ps.token_mint_1);

    if input_mint == mint0 {
        Ok(ClmmRoute {
            input_vault: a2s(ps.token_vault_0),
            output_vault: a2s(ps.token_vault_1),
            input_mint: mint0,
            output_mint: mint1,
            user_input_ata: user_ata_for_mint0,
            user_output_ata: user_ata_for_mint1,
            zero_for_one: true,
            observation_key: a2s(ps.observation_key),
        })
    } else if input_mint == mint1 {
        Ok(ClmmRoute {
            input_vault: a2s(ps.token_vault_1),
            output_vault: a2s(ps.token_vault_0),
            input_mint: mint1,
            output_mint: mint0,
            user_input_ata: user_ata_for_mint1,
            user_output_ata: user_ata_for_mint0,
            zero_for_one: false,
            observation_key: a2s(ps.observation_key),
        })
    } else {
        Err(anyhow!(
            "input mint {} not in pool (mint0={}, mint1={})",
            input_mint,
            mint0,
            mint1
        ))
    }
}
