use anyhow::{Context, Result};
use solana_program::program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_sdk::instruction::{AccountMeta, Instruction};
use spl_associated_token_account::get_associated_token_address_with_program_id;
use spl_token::state::Account as SplTokenAccount;
use spl_token::state::Mint as SplMint;
use spl_token_2022::extension::StateWithExtensions;
use std::str::FromStr;

pub const PUMPSWAP_PROGRAM_ID_STR: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
pub const PUMPSWAP_GLOBAL_CONFIG_STR: &str = "ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw";
pub const PUMPSWAP_PROTOCOL_FEE_RECIPIENT_STR: &str =
    "62qc2CNXwrYqQScmEdiZFFAnJR262PxWEuNQtxfafNgV";
pub const PUMPSWAP_EVENT_AUTHORITY_STR: &str = "GS4CU59F31iL7aR2Q8zVS8DRrcRnXX1yjQ66TqNVQnaR";
pub const PUMPSWAP_FEE_CONFIG_STR: &str = "5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx";
pub const PUMPSWAP_GLOBAL_VOLUME_ACCUMULATOR_STR: &str =
    "C2aFPdENg4A2HQsmrd5rTw5TaYBX5Ku887cWjbFKtZpw";
pub const PUMPSWAP_FEE_PROGRAM_STR: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";
pub const SYSTEM_PROGRAM_ID_STR: &str = "11111111111111111111111111111111";

pub const PUMPSWAP_POOL_DISCRIMINATOR: [u8; 8] = [241, 154, 109, 4, 17, 177, 109, 188];
pub const PUMPSWAP_BUY_IX_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];
pub const PUMPSWAP_SELL_IX_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];

const BASE_MINT_OFFSET: usize = 43;
const QUOTE_MINT_OFFSET: usize = 75;
const POOL_BASE_TOKEN_ACCOUNT_OFFSET: usize = 139;
const POOL_QUOTE_TOKEN_ACCOUNT_OFFSET: usize = 171;
const COIN_CREATOR_OFFSET: usize = 211;
const MIN_POOL_DATA_LENGTH: usize = POOL_QUOTE_TOKEN_ACCOUNT_OFFSET + 32;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PumpAmmSnapshot {
    pub base_vault_amount: u64,
    pub quote_vault_amount: u64,
}

#[derive(Clone, Debug)]
pub struct PumpPoolStatic {
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    #[allow(dead_code)] // kept in static pool state for PumpSwap execution account wiring
    pub coin_creator: Pubkey,
    pub pool_base_token_account: Pubkey,
    pub pool_quote_token_account: Pubkey,
    pub base_token_program: Pubkey,
    pub quote_token_program: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub base_mint_supply: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct PumpFees {
    pub lp_fee_bps: u64,
    pub protocol_fee_bps: u64,
    pub creator_fee_bps: u64,
}

impl PumpFees {
    pub fn as_multiplier_array(self) -> [u64; 3] {
        [self.lp_fee_bps, self.protocol_fee_bps, self.creator_fee_bps]
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PumpFeeTier {
    pub market_cap_lamports_threshold: u128,
    pub fees: PumpFees,
}

#[derive(Clone, Debug)]
pub struct PumpFeeConfig {
    pub fee_tiers: Vec<PumpFeeTier>,
}

#[derive(Clone, Copy, Debug)]
enum SwapDirection {
    Quote2Base,
    Base2Quote,
}

fn read_pubkey_at(data: &[u8], offset: usize) -> Result<Pubkey> {
    let end = offset
        .checked_add(32)
        .context("pubkey offset overflow in pumpswap layout")?;
    let bytes: [u8; 32] = data
        .get(offset..end)
        .context("pumpswap layout slice out of bounds")?
        .try_into()
        .context("invalid pubkey slice len")?;
    Ok(Pubkey::new_from_array(bytes))
}

pub fn parse_pool_static_layout(data: &[u8]) -> Result<(Pubkey, Pubkey, Pubkey, Pubkey, Pubkey)> {
    if data.len() < MIN_POOL_DATA_LENGTH {
        anyhow::bail!(
            "invalid pumpswap pool data len={}, need at least {}",
            data.len(),
            MIN_POOL_DATA_LENGTH
        );
    }

    if let Some(disc) = data.get(..8)
        && disc != PUMPSWAP_POOL_DISCRIMINATOR
    {
        anyhow::bail!("unexpected pumpswap pool discriminator");
    }

    Ok((
        read_pubkey_at(data, BASE_MINT_OFFSET)?,
        read_pubkey_at(data, QUOTE_MINT_OFFSET)?,
        read_pubkey_at(data, POOL_BASE_TOKEN_ACCOUNT_OFFSET)?,
        read_pubkey_at(data, POOL_QUOTE_TOKEN_ACCOUNT_OFFSET)?,
        read_pubkey_at(data, COIN_CREATOR_OFFSET)?,
    ))
}

pub fn decode_token_account_amount_any_program(data: &[u8]) -> Result<u64> {
    if let Ok(acc) = SplTokenAccount::unpack(data) {
        return Ok(acc.amount);
    }

    let acc_2022 = StateWithExtensions::<spl_token_2022::state::Account>::unpack(data)
        .context("failed to decode token account as SPL Token or Token-2022")?;
    Ok(acc_2022.base.amount)
}

pub fn decode_mint_decimals_any_program(data: &[u8]) -> Result<u8> {
    if let Ok(mint) = SplMint::unpack(data) {
        return Ok(mint.decimals);
    }

    let mint_2022 = StateWithExtensions::<spl_token_2022::state::Mint>::unpack(data)
        .context("failed to decode mint as SPL Token or Token-2022")?;
    Ok(mint_2022.base.decimals)
}

fn read_u32_le(data: &[u8], off: &mut usize) -> Result<u32> {
    let end = (*off).checked_add(4).context("u32 offset overflow")?;
    let bytes: [u8; 4] = data
        .get(*off..end)
        .context("read_u32 out of bounds")?
        .try_into()
        .context("invalid u32 slice")?;
    *off = end;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64_le(data: &[u8], off: &mut usize) -> Result<u64> {
    let end = (*off).checked_add(8).context("u64 offset overflow")?;
    let bytes: [u8; 8] = data
        .get(*off..end)
        .context("read_u64 out of bounds")?
        .try_into()
        .context("invalid u64 slice")?;
    *off = end;
    Ok(u64::from_le_bytes(bytes))
}

fn read_u128_le(data: &[u8], off: &mut usize) -> Result<u128> {
    let end = (*off).checked_add(16).context("u128 offset overflow")?;
    let bytes: [u8; 16] = data
        .get(*off..end)
        .context("read_u128 out of bounds")?
        .try_into()
        .context("invalid u128 slice")?;
    *off = end;
    Ok(u128::from_le_bytes(bytes))
}

fn skip_bytes(data: &[u8], off: &mut usize, n: usize) -> Result<()> {
    let end = off.checked_add(n).context("skip offset overflow")?;
    data.get(*off..end).context("skip out of bounds")?;
    *off = end;
    Ok(())
}

pub fn parse_fee_config(data: &[u8]) -> Result<PumpFeeConfig> {
    // Anchor account: [8-byte discriminator][borsh payload]
    let mut off = 8usize;
    skip_bytes(data, &mut off, 1)?; // bump
    skip_bytes(data, &mut off, 32)?; // admin
    let _flat_lp = read_u64_le(data, &mut off)?;
    let _flat_protocol = read_u64_le(data, &mut off)?;
    let _flat_creator = read_u64_le(data, &mut off)?;

    let tiers_len = read_u32_le(data, &mut off)? as usize;
    let mut fee_tiers = Vec::with_capacity(tiers_len);
    for _ in 0..tiers_len {
        let threshold = read_u128_le(data, &mut off)?;
        let lp_fee_bps = read_u64_le(data, &mut off)?;
        let protocol_fee_bps = read_u64_le(data, &mut off)?;
        let creator_fee_bps = read_u64_le(data, &mut off)?;
        fee_tiers.push(PumpFeeTier {
            market_cap_lamports_threshold: threshold,
            fees: PumpFees {
                lp_fee_bps,
                protocol_fee_bps,
                creator_fee_bps,
            },
        });
    }

    if fee_tiers.is_empty() {
        anyhow::bail!("pumpswap fee config has no tiers");
    }

    Ok(PumpFeeConfig { fee_tiers })
}

fn ceil_div(a: u128, b: u128) -> Result<u128> {
    if b == 0 {
        anyhow::bail!("division by zero");
    }
    a.checked_add(b)
        .and_then(|x| x.checked_sub(1))
        .map(|x| x / b)
        .context("overflow in ceil_div")
}

fn calc_fee(amount: u128, bps: u64) -> Result<u128> {
    ceil_div(
        amount
            .checked_mul(u128::from(bps))
            .context("overflow in pumpswap fee mul")?,
        10_000,
    )
}

fn pool_market_cap(base_supply: u64, base_reserve: u64, quote_reserve: u64) -> Result<u128> {
    if base_reserve == 0 {
        anyhow::bail!("base reserve is zero");
    }
    u128::from(quote_reserve)
        .checked_mul(u128::from(base_supply))
        .context("overflow in pumpswap market cap mul")?
        .checked_div(u128::from(base_reserve))
        .context("division by zero in pumpswap market cap")
}

fn select_fee_tier(cfg: &PumpFeeConfig, market_cap: u128) -> PumpFees {
    let first = cfg.fee_tiers[0];
    if market_cap < first.market_cap_lamports_threshold {
        return first.fees;
    }
    for tier in cfg.fee_tiers.iter().rev() {
        if market_cap >= tier.market_cap_lamports_threshold {
            return tier.fees;
        }
    }
    first.fees
}

fn calc_token_amount_exact_in(
    amount_in: u128,
    total_base: u128,
    total_quote: u128,
    fee_multiplier: &[u64; 3],
    swap_direction: SwapDirection,
) -> Result<u128> {
    match swap_direction {
        SwapDirection::Base2Quote => {
            let denominator = total_base
                .checked_add(amount_in)
                .context("overflow in pumpswap base2quote denominator")?;
            let amount_out = total_quote
                .checked_mul(amount_in)
                .context("overflow in pumpswap base2quote mul")?
                .checked_div(denominator)
                .context("division by zero in pumpswap base2quote")?;

            let total_fees = calc_fee(amount_out, fee_multiplier[0])?
                .checked_add(calc_fee(amount_out, fee_multiplier[1])?)
                .context("overflow in pumpswap fee add")?
                .checked_add(calc_fee(amount_out, fee_multiplier[2])?)
                .context("overflow in pumpswap fee add")?;

            amount_out
                .checked_sub(total_fees)
                .context("underflow: pumpswap fees exceed output")
        }
        SwapDirection::Quote2Base => {
            let total_fee_bps = u128::from(fee_multiplier[0])
                .checked_add(u128::from(fee_multiplier[1]))
                .context("overflow in pumpswap fee bps add")?
                .checked_add(u128::from(fee_multiplier[2]))
                .context("overflow in pumpswap fee bps add")?;

            let denominator = 10_000u128
                .checked_add(total_fee_bps)
                .context("overflow in pumpswap quote2base denominator")?;
            let effective_input = amount_in
                .checked_mul(10_000)
                .context("overflow in pumpswap effective input mul")?
                .checked_div(denominator)
                .context("division by zero in pumpswap effective input")?;

            let pool_denominator = total_quote
                .checked_add(effective_input)
                .context("overflow in pumpswap quote2base pool denominator")?;

            total_base
                .checked_mul(effective_input)
                .context("overflow in pumpswap quote2base mul")?
                .checked_div(pool_denominator)
                .context("division by zero in pumpswap quote2base")
        }
    }
}

pub fn quote_exact_in(
    st: &PumpPoolStatic,
    snap: &PumpAmmSnapshot,
    fee_cfg: &PumpFeeConfig,
    input_mint: Pubkey,
    amount_in: u64,
) -> Result<u64> {
    let (direction, total_base, total_quote) = if input_mint == st.quote_mint {
        (
            SwapDirection::Quote2Base,
            snap.base_vault_amount,
            snap.quote_vault_amount,
        )
    } else if input_mint == st.base_mint {
        (
            SwapDirection::Base2Quote,
            snap.base_vault_amount,
            snap.quote_vault_amount,
        )
    } else {
        anyhow::bail!("input mint not in pumpswap pool");
    };

    let market_cap = pool_market_cap(st.base_mint_supply, total_base, total_quote)?;
    let fees = select_fee_tier(fee_cfg, market_cap);
    let out = calc_token_amount_exact_in(
        u128::from(amount_in),
        u128::from(total_base),
        u128::from(total_quote),
        &fees.as_multiplier_array(),
        direction,
    )?;
    u64::try_from(out).context("pumpswap quote out overflow")
}

pub fn pumpswap_program_id() -> Pubkey {
    Pubkey::from_str(PUMPSWAP_PROGRAM_ID_STR).expect("invalid pumpswap program id")
}

pub fn pumpswap_global_config() -> Pubkey {
    Pubkey::from_str(PUMPSWAP_GLOBAL_CONFIG_STR).expect("invalid pumpswap global config")
}

pub fn pumpswap_protocol_fee_recipient() -> Pubkey {
    Pubkey::from_str(PUMPSWAP_PROTOCOL_FEE_RECIPIENT_STR).expect("invalid pumpswap fee recipient")
}

pub fn pumpswap_event_authority() -> Pubkey {
    Pubkey::from_str(PUMPSWAP_EVENT_AUTHORITY_STR).expect("invalid pumpswap event authority")
}

pub fn pumpswap_fee_config_account() -> Pubkey {
    Pubkey::from_str(PUMPSWAP_FEE_CONFIG_STR).expect("invalid pumpswap fee config account")
}

pub fn pumpswap_global_volume_accumulator() -> Pubkey {
    Pubkey::from_str(PUMPSWAP_GLOBAL_VOLUME_ACCUMULATOR_STR)
        .expect("invalid pumpswap global volume accumulator")
}

pub fn pumpswap_fee_program() -> Pubkey {
    Pubkey::from_str(PUMPSWAP_FEE_PROGRAM_STR).expect("invalid pumpswap fee program")
}

pub fn system_program_id() -> Pubkey {
    Pubkey::from_str(SYSTEM_PROGRAM_ID_STR).expect("invalid system program id")
}

fn build_ix_with_data(
    program_id: Pubkey,
    accounts: Vec<AccountMeta>,
    data: Vec<u8>,
) -> Instruction {
    Instruction {
        program_id,
        accounts,
        data,
    }
}

pub fn build_sell_exact_in_ix(
    payer: Pubkey,
    pool_id: Pubkey,
    st: &PumpPoolStatic,
    amount_in_base: u64,
    min_quote_out: u64,
) -> Instruction {
    let program_id = pumpswap_program_id();
    let global_config = pumpswap_global_config();
    let protocol_fee_recipient = pumpswap_protocol_fee_recipient();
    let event_authority = pumpswap_event_authority();
    let global_volume_accumulator = pumpswap_global_volume_accumulator();
    let fee_config = pumpswap_fee_config_account();
    let fee_program = pumpswap_fee_program();
    let (creator_vault_authority, _) =
        Pubkey::find_program_address(&[b"creator_vault", st.coin_creator.as_ref()], &program_id);
    let (user_volume_accumulator, _) =
        Pubkey::find_program_address(&[b"user_volume_accumulator", payer.as_ref()], &program_id);
    let creator_vault_ata = get_associated_token_address_with_program_id(
        &creator_vault_authority,
        &st.quote_mint,
        &st.quote_token_program,
    );
    let protocol_fee_recipient_token_account = get_associated_token_address_with_program_id(
        &protocol_fee_recipient,
        &st.quote_mint,
        &st.quote_token_program,
    );

    let user_base_token_account =
        get_associated_token_address_with_program_id(&payer, &st.base_mint, &st.base_token_program);
    let user_quote_token_account = get_associated_token_address_with_program_id(
        &payer,
        &st.quote_mint,
        &st.quote_token_program,
    );

    let mut data = Vec::with_capacity(8 + 8 + 8);
    data.extend_from_slice(&PUMPSWAP_SELL_IX_DISCRIMINATOR);
    data.extend_from_slice(&amount_in_base.to_le_bytes());
    data.extend_from_slice(&min_quote_out.to_le_bytes());

    let accounts = vec![
        AccountMeta::new(pool_id, false),                  // pool (mutable)
        AccountMeta::new(payer, true),                     // user
        AccountMeta::new_readonly(global_config, false),   // global_config
        AccountMeta::new_readonly(st.base_mint, false),    // base_mint
        AccountMeta::new_readonly(st.quote_mint, false),   // quote_mint
        AccountMeta::new(user_base_token_account, false),  // user_base_token_account
        AccountMeta::new(user_quote_token_account, false), // user_quote_token_account
        AccountMeta::new(st.pool_base_token_account, false), // pool_base_token_account
        AccountMeta::new(st.pool_quote_token_account, false), // pool_quote_token_account
        AccountMeta::new_readonly(protocol_fee_recipient, false), // protocol_fee_recipient
        AccountMeta::new(protocol_fee_recipient_token_account, false), // protocol_fee_recipient_token_account
        AccountMeta::new_readonly(st.base_token_program, false),       // base_token_program
        AccountMeta::new_readonly(st.quote_token_program, false),      // quote_token_program
        AccountMeta::new_readonly(system_program_id(), false),         // system_program
        AccountMeta::new_readonly(spl_associated_token_account::id(), false), // associated_token_program
        AccountMeta::new_readonly(event_authority, false),                    // event_authority
        AccountMeta::new_readonly(program_id, false),                         // program
        AccountMeta::new(creator_vault_ata, false),                           // creator_vault_ata
        AccountMeta::new_readonly(creator_vault_authority, false), // creator_vault_authority
        AccountMeta::new_readonly(global_volume_accumulator, false), // global_volume_accumulator
        AccountMeta::new_readonly(fee_config, false),              // fee_config
        AccountMeta::new(user_volume_accumulator, false),          // user_volume_accumulator
        AccountMeta::new_readonly(fee_program, false),             // fee_program
    ];

    build_ix_with_data(program_id, accounts, data)
}

pub fn build_buy_exact_out_ix(
    payer: Pubkey,
    pool_id: Pubkey,
    st: &PumpPoolStatic,
    base_amount_out: u64,
    max_quote_amount_in: u64,
) -> Instruction {
    let program_id = pumpswap_program_id();
    let global_config = pumpswap_global_config();
    let protocol_fee_recipient = pumpswap_protocol_fee_recipient();
    let event_authority = pumpswap_event_authority();
    let global_volume_accumulator = pumpswap_global_volume_accumulator();
    let fee_config = pumpswap_fee_config_account();
    let fee_program = pumpswap_fee_program();
    let (creator_vault_authority, _) =
        Pubkey::find_program_address(&[b"creator_vault", st.coin_creator.as_ref()], &program_id);
    let (user_volume_accumulator, _) =
        Pubkey::find_program_address(&[b"user_volume_accumulator", payer.as_ref()], &program_id);
    let creator_vault_ata = get_associated_token_address_with_program_id(
        &creator_vault_authority,
        &st.quote_mint,
        &st.quote_token_program,
    );
    let protocol_fee_recipient_token_account = get_associated_token_address_with_program_id(
        &protocol_fee_recipient,
        &st.quote_mint,
        &st.quote_token_program,
    );

    let user_base_token_account =
        get_associated_token_address_with_program_id(&payer, &st.base_mint, &st.base_token_program);
    let user_quote_token_account = get_associated_token_address_with_program_id(
        &payer,
        &st.quote_mint,
        &st.quote_token_program,
    );

    let mut data = Vec::with_capacity(8 + 8 + 8);
    data.extend_from_slice(&PUMPSWAP_BUY_IX_DISCRIMINATOR);
    data.extend_from_slice(&base_amount_out.to_le_bytes());
    data.extend_from_slice(&max_quote_amount_in.to_le_bytes());

    let accounts = vec![
        AccountMeta::new(pool_id, false),                  // pool (mutable)
        AccountMeta::new(payer, true),                     // user
        AccountMeta::new_readonly(global_config, false),   // global_config
        AccountMeta::new_readonly(st.base_mint, false),    // base_mint
        AccountMeta::new_readonly(st.quote_mint, false),   // quote_mint
        AccountMeta::new(user_base_token_account, false),  // user_base_token_account
        AccountMeta::new(user_quote_token_account, false), // user_quote_token_account
        AccountMeta::new(st.pool_base_token_account, false), // pool_base_token_account
        AccountMeta::new(st.pool_quote_token_account, false), // pool_quote_token_account
        AccountMeta::new_readonly(protocol_fee_recipient, false), // protocol_fee_recipient
        AccountMeta::new(protocol_fee_recipient_token_account, false), // protocol_fee_recipient_token_account
        AccountMeta::new_readonly(st.base_token_program, false),       // base_token_program
        AccountMeta::new_readonly(st.quote_token_program, false),      // quote_token_program
        AccountMeta::new_readonly(system_program_id(), false),         // system_program
        AccountMeta::new_readonly(spl_associated_token_account::id(), false), // associated_token_program
        AccountMeta::new_readonly(event_authority, false),                    // event_authority
        AccountMeta::new_readonly(program_id, false),                         // program
        AccountMeta::new(creator_vault_ata, false),                           // creator_vault_ata
        AccountMeta::new_readonly(creator_vault_authority, false), // creator_vault_authority
        AccountMeta::new_readonly(global_volume_accumulator, false), // global_volume_accumulator
        AccountMeta::new_readonly(fee_config, false),              // fee_config
        AccountMeta::new(user_volume_accumulator, false),          // user_volume_accumulator
        AccountMeta::new_readonly(fee_program, false),             // fee_program
    ];

    build_ix_with_data(program_id, accounts, data)
}
