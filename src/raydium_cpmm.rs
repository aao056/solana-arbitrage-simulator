use anyhow::{Context, Result};
use solana_pubkey::Pubkey;
use solana_sdk::instruction::{AccountMeta, Instruction};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

pub const RAYDIUM_CPMM_PROGRAM_ID_STR: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
pub const RAYDIUM_CPMM_POOL_DISCRIMINATOR: [u8; 8] = [247, 237, 227, 245, 215, 195, 222, 70];
pub const RAYDIUM_CPMM_AMM_CONFIG_DISCRIMINATOR: [u8; 8] = [218, 244, 33, 104, 203, 203, 43, 111];
pub const RAYDIUM_CPMM_SWAP_BASE_INPUT_IX_DISCRIMINATOR: [u8; 8] =
    [143, 190, 90, 218, 196, 30, 51, 222];

const SWAP_FEE_RATE_DENOMINATOR: u64 = 1_000_000;
const SWAP_DISABLE_STATUS_MASK: u8 = 1 << 2;
const AUTHORITY_SEED: &[u8] = b"vault_and_lp_mint_auth_seed";

// PoolState offsets (absolute, including 8-byte discriminator), based on IDL packed layout.
const POOL_AMM_CONFIG_OFFSET: usize = 8;
const POOL_TOKEN_0_VAULT_OFFSET: usize = 72;
const POOL_TOKEN_1_VAULT_OFFSET: usize = 104;
const POOL_TOKEN_0_MINT_OFFSET: usize = 168;
const POOL_TOKEN_1_MINT_OFFSET: usize = 200;
const POOL_TOKEN_0_PROGRAM_OFFSET: usize = 232;
const POOL_TOKEN_1_PROGRAM_OFFSET: usize = 264;
const POOL_OBSERVATION_KEY_OFFSET: usize = 296;
const POOL_STATUS_OFFSET: usize = 329;
const POOL_MINT_0_DECIMALS_OFFSET: usize = 331;
const POOL_MINT_1_DECIMALS_OFFSET: usize = 332;
const POOL_PROTOCOL_FEES_TOKEN_0_OFFSET: usize = 341;
const POOL_PROTOCOL_FEES_TOKEN_1_OFFSET: usize = 349;
const POOL_FUND_FEES_TOKEN_0_OFFSET: usize = 357;
const POOL_FUND_FEES_TOKEN_1_OFFSET: usize = 365;
const POOL_OPEN_TIME_OFFSET: usize = 373;
const POOL_MIN_LEN: usize = 637;

// AmmConfig offsets (absolute, including 8-byte discriminator), Borsh layout from IDL.
const AMM_CONFIG_TRADE_FEE_RATE_OFFSET: usize = 12;
const AMM_CONFIG_MIN_LEN: usize = 236;

#[derive(Clone, Debug)]
pub struct RaydiumCpmmStatic {
    pub amm_config: Pubkey,
    pub observation_state: Pubkey,
    pub token_0_vault: Pubkey,
    pub token_1_vault: Pubkey,
    pub token_0_mint: Pubkey,
    pub token_1_mint: Pubkey,
    pub token_0_program: Pubkey,
    pub token_1_program: Pubkey,
    pub mint_0_decimals: u8,
    pub mint_1_decimals: u8,
    pub trade_fee_rate: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaydiumCpmmSnapshot {
    pub token_0_vault_amount: u64,
    pub token_1_vault_amount: u64,
    pub protocol_fees_token_0: u64,
    pub protocol_fees_token_1: u64,
    pub fund_fees_token_0: u64,
    pub fund_fees_token_1: u64,
    pub status: u8,
    pub open_time: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct RaydiumCpmmPoolDynamic {
    pub protocol_fees_token_0: u64,
    pub protocol_fees_token_1: u64,
    pub fund_fees_token_0: u64,
    pub fund_fees_token_1: u64,
    pub status: u8,
    pub open_time: u64,
}

fn cpmm_program_id() -> Result<Pubkey> {
    Pubkey::from_str(RAYDIUM_CPMM_PROGRAM_ID_STR).context("invalid raydium cpmm program id")
}

fn cpmm_authority(program_id: Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[AUTHORITY_SEED], &program_id).0
}

fn read_pubkey_at(data: &[u8], offset: usize) -> Result<Pubkey> {
    let end = offset.checked_add(32).context("pubkey offset overflow")?;
    let bytes: [u8; 32] = data
        .get(offset..end)
        .context("pubkey slice out of bounds")?
        .try_into()
        .context("invalid pubkey slice len")?;
    Ok(Pubkey::new_from_array(bytes))
}

fn read_u8_at(data: &[u8], offset: usize) -> Result<u8> {
    data.get(offset).copied().context("u8 out of bounds")
}

fn read_u64_le_at(data: &[u8], offset: usize) -> Result<u64> {
    let end = offset.checked_add(8).context("u64 offset overflow")?;
    let bytes: [u8; 8] = data
        .get(offset..end)
        .context("u64 slice out of bounds")?
        .try_into()
        .context("invalid u64 slice")?;
    Ok(u64::from_le_bytes(bytes))
}

pub fn parse_pool_static_layout(data: &[u8]) -> Result<RaydiumCpmmStatic> {
    if data.len() < POOL_MIN_LEN {
        anyhow::bail!(
            "invalid raydium cpmm pool data len={}, need >= {POOL_MIN_LEN}",
            data.len()
        );
    }
    if data.get(..8) != Some(&RAYDIUM_CPMM_POOL_DISCRIMINATOR) {
        anyhow::bail!("unexpected raydium cpmm pool discriminator");
    }

    Ok(RaydiumCpmmStatic {
        amm_config: read_pubkey_at(data, POOL_AMM_CONFIG_OFFSET)?,
        observation_state: read_pubkey_at(data, POOL_OBSERVATION_KEY_OFFSET)?,
        token_0_vault: read_pubkey_at(data, POOL_TOKEN_0_VAULT_OFFSET)?,
        token_1_vault: read_pubkey_at(data, POOL_TOKEN_1_VAULT_OFFSET)?,
        token_0_mint: read_pubkey_at(data, POOL_TOKEN_0_MINT_OFFSET)?,
        token_1_mint: read_pubkey_at(data, POOL_TOKEN_1_MINT_OFFSET)?,
        token_0_program: read_pubkey_at(data, POOL_TOKEN_0_PROGRAM_OFFSET)?,
        token_1_program: read_pubkey_at(data, POOL_TOKEN_1_PROGRAM_OFFSET)?,
        mint_0_decimals: read_u8_at(data, POOL_MINT_0_DECIMALS_OFFSET)?,
        mint_1_decimals: read_u8_at(data, POOL_MINT_1_DECIMALS_OFFSET)?,
        trade_fee_rate: 0,
    })
}

pub fn parse_pool_dynamic(data: &[u8]) -> Result<RaydiumCpmmPoolDynamic> {
    if data.len() < POOL_MIN_LEN {
        anyhow::bail!(
            "invalid raydium cpmm pool data len={}, need >= {POOL_MIN_LEN}",
            data.len()
        );
    }
    if data.get(..8) != Some(&RAYDIUM_CPMM_POOL_DISCRIMINATOR) {
        anyhow::bail!("unexpected raydium cpmm pool discriminator");
    }

    Ok(RaydiumCpmmPoolDynamic {
        protocol_fees_token_0: read_u64_le_at(data, POOL_PROTOCOL_FEES_TOKEN_0_OFFSET)?,
        protocol_fees_token_1: read_u64_le_at(data, POOL_PROTOCOL_FEES_TOKEN_1_OFFSET)?,
        fund_fees_token_0: read_u64_le_at(data, POOL_FUND_FEES_TOKEN_0_OFFSET)?,
        fund_fees_token_1: read_u64_le_at(data, POOL_FUND_FEES_TOKEN_1_OFFSET)?,
        status: read_u8_at(data, POOL_STATUS_OFFSET)?,
        open_time: read_u64_le_at(data, POOL_OPEN_TIME_OFFSET)?,
    })
}

pub fn parse_amm_config_trade_fee_rate(data: &[u8]) -> Result<u64> {
    if data.len() < AMM_CONFIG_MIN_LEN {
        anyhow::bail!(
            "invalid raydium cpmm amm config len={}, need >= {AMM_CONFIG_MIN_LEN}",
            data.len()
        );
    }
    if data.get(..8) != Some(&RAYDIUM_CPMM_AMM_CONFIG_DISCRIMINATOR) {
        anyhow::bail!("unexpected raydium cpmm amm config discriminator");
    }
    read_u64_le_at(data, AMM_CONFIG_TRADE_FEE_RATE_OFFSET)
}

fn effective_reserve(vault: u64, protocol_fee: u64, fund_fee: u64) -> Result<u128> {
    let reserve = vault
        .checked_sub(protocol_fee)
        .context("protocol fee exceeds vault amount")?
        .checked_sub(fund_fee)
        .context("fund fee exceeds vault amount")?;
    Ok(u128::from(reserve))
}

pub fn quote_exact_in(
    st: &RaydiumCpmmStatic,
    snap: &RaydiumCpmmSnapshot,
    input_mint: Pubkey,
    amount_in: u64,
) -> Result<u64> {
    if amount_in == 0 {
        return Ok(0);
    }
    if snap.status & SWAP_DISABLE_STATUS_MASK != 0 {
        anyhow::bail!("raydium cpmm pool swap disabled");
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before unix epoch")?
        .as_secs();
    if snap.open_time > 0 && now < snap.open_time {
        anyhow::bail!("raydium cpmm pool not open yet");
    }

    let (reserve_in, reserve_out) = if input_mint == st.token_0_mint {
        (
            effective_reserve(
                snap.token_0_vault_amount,
                snap.protocol_fees_token_0,
                snap.fund_fees_token_0,
            )?,
            effective_reserve(
                snap.token_1_vault_amount,
                snap.protocol_fees_token_1,
                snap.fund_fees_token_1,
            )?,
        )
    } else if input_mint == st.token_1_mint {
        (
            effective_reserve(
                snap.token_1_vault_amount,
                snap.protocol_fees_token_1,
                snap.fund_fees_token_1,
            )?,
            effective_reserve(
                snap.token_0_vault_amount,
                snap.protocol_fees_token_0,
                snap.fund_fees_token_0,
            )?,
        )
    } else {
        anyhow::bail!("input mint not in raydium cpmm pool");
    };

    if reserve_in == 0 || reserve_out == 0 {
        anyhow::bail!("raydium cpmm pool has zero effective reserve");
    }
    if st.trade_fee_rate >= SWAP_FEE_RATE_DENOMINATOR {
        anyhow::bail!("invalid raydium cpmm trade_fee_rate {}", st.trade_fee_rate);
    }

    let amount_in_less_fee = u128::from(amount_in)
        .checked_mul(u128::from(SWAP_FEE_RATE_DENOMINATOR - st.trade_fee_rate))
        .context("raydium cpmm quote fee mul overflow")?
        / u128::from(SWAP_FEE_RATE_DENOMINATOR);
    if amount_in_less_fee == 0 {
        return Ok(0);
    }

    let numerator = reserve_out
        .checked_mul(amount_in_less_fee)
        .context("raydium cpmm quote numerator overflow")?;
    let denominator = reserve_in
        .checked_add(amount_in_less_fee)
        .context("raydium cpmm quote denominator overflow")?;
    let out = numerator / denominator;
    u64::try_from(out).context("raydium cpmm quote out overflow")
}

pub fn build_swap_base_input_ix(
    payer: Pubkey,
    pool_id: Pubkey,
    st: &RaydiumCpmmStatic,
    input_mint: Pubkey,
    amount_in: u64,
    minimum_amount_out: u64,
) -> Result<Instruction> {
    let program_id = cpmm_program_id()?;
    let authority = cpmm_authority(program_id);

    let (input_vault, output_vault, input_token_program, output_token_program, output_mint) =
        if input_mint == st.token_0_mint {
            (
                st.token_0_vault,
                st.token_1_vault,
                st.token_0_program,
                st.token_1_program,
                st.token_1_mint,
            )
        } else if input_mint == st.token_1_mint {
            (
                st.token_1_vault,
                st.token_0_vault,
                st.token_1_program,
                st.token_0_program,
                st.token_0_mint,
            )
        } else {
            anyhow::bail!("input mint not in raydium cpmm pool");
        };

    let input_token_account =
        get_associated_token_address_with_program_id(&payer, &input_mint, &input_token_program);
    let output_token_account =
        get_associated_token_address_with_program_id(&payer, &output_mint, &output_token_program);

    let accounts = vec![
        AccountMeta::new_readonly(payer, true),
        AccountMeta::new_readonly(authority, false),
        AccountMeta::new_readonly(st.amm_config, false),
        AccountMeta::new(pool_id, false),
        AccountMeta::new(input_token_account, false),
        AccountMeta::new(output_token_account, false),
        AccountMeta::new(input_vault, false),
        AccountMeta::new(output_vault, false),
        AccountMeta::new_readonly(input_token_program, false),
        AccountMeta::new_readonly(output_token_program, false),
        AccountMeta::new_readonly(input_mint, false),
        AccountMeta::new_readonly(output_mint, false),
        AccountMeta::new(st.observation_state, false),
    ];

    let mut data = Vec::with_capacity(8 + 8 + 8);
    data.extend_from_slice(&RAYDIUM_CPMM_SWAP_BASE_INPUT_IX_DISCRIMINATOR);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&minimum_amount_out.to_le_bytes());

    Ok(Instruction {
        program_id,
        accounts,
        data,
    })
}
