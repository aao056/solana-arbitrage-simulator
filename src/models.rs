use crate::meteora_damm::MeteoraDammSnapshot;
use crate::meteora_dlmm::MeteoraDlmmSnapshot;
use crate::orca_whirlpool::OrcaWhirlpoolSnapshot;
use crate::pumpswap::PumpAmmSnapshot;
use crate::raydium_cpmm::RaydiumCpmmSnapshot;
use raydium_amm_v3::states::{PoolState, TickArrayBitmapExtension, TickArrayState};
use serde::Deserialize;
use solana_pubkey::Pubkey;

pub const CLMM_TICK_WINDOW_LEN: usize = 2;
pub const CLMM_TICK_WINDOW_RADIUS: usize = (CLMM_TICK_WINDOW_LEN - 1) / 2;
pub const CLMM_TICK_WINDOW_CENTER_INDEX: usize = CLMM_TICK_WINDOW_LEN / 2;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub start_mints: Option<Vec<StartMintConfig>>,
    #[serde(default)]
    pub pools: Vec<PoolConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct StartMintConfig {
    pub mint: String,
    pub decimals: u8,
    pub exec_amount: f64,
    pub min_profit: f64,
    pub probe_amounts: Option<Vec<f64>>,
    pub symbol: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PoolConfig {
    pub dex: Option<String>,
    pub kind: String,
    pub symbol: String,
    pub pool_id: String,
}

impl PoolConfig {
    pub fn pool_type(&self) -> PoolType {
        let kind = self.kind.to_lowercase();
        let dex = self.dex.as_ref().map(|d| d.to_lowercase());

        match (dex.as_deref(), kind.as_str()) {
            (Some("pump"), "amm") | (Some("pumpswap"), "amm") | (_, "pump_amm") => {
                PoolType::PumpAmm
            }
            (Some("meteora"), "damm")
            | (Some("meteora"), "cpamm")
            | (Some("meteora"), "cp_amm")
            | (_, "meteora_damm") => PoolType::MeteoraDamm,
            (Some("pancake"), "clmm")
            | (Some("pancakeswap"), "clmm")
            | (_, "pancakeswap_clmm")
            | (_, "pancake_clmm") => PoolType::PancakeswapClmm,
            (Some("raydium"), "cpmm") | (_, "raydium_cpmm") | (_, "cpmm") => PoolType::RaydiumCpmm,
            (_, "amm") => PoolType::RaydiumAmm,
            (_, "clmm") => PoolType::RaydiumClmm,
            (Some("orca"), "whirlpool")
            | (Some("orca"), "whirlpools")
            | (_, "orca_whirlpool")
            | (_, "orca_whirlpools") => PoolType::OrcaWhirlpool,
            (Some("meteora"), "dlmm") | (None, "dlmm") | (_, "meteora_dlmm") | (_, "dlmm") => {
                PoolType::MeteoraDlmm
            }
            (Some(d), k) => panic!("unsupported dex/kind in cfg.toml: dex={d}, kind={k}"),
            (None, other) => panic!("unknown pool kind in cfg.toml: {}", other),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PoolType {
    RaydiumAmm,
    RaydiumCpmm,
    RaydiumClmm,
    PancakeswapClmm,
    MeteoraDlmm,
    MeteoraDamm,
    OrcaWhirlpool,
    PumpAmm,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AmmSnapshot {
    pub coin_vault_amount: u64,
    pub pc_vault_amount: u64,
    pub fee_numerator: u64,
    pub fee_denominator: u64,
}

#[derive(Debug)]
pub struct ClmmStatic {
    pub program_id: Pubkey,
    pub amm_config: Pubkey,
    pub bitmap_pda: Pubkey,
}

#[derive(Clone)]
pub struct ClmmSnapshot {
    pub pool_state: PoolState,
    pub trade_fee_rate: u32,
    pub tick_array_bitmap_ext: TickArrayBitmapExtension,
    pub ticks_array_window: Vec<TickArrayState>,
    pub tick_array_pubkeys: Vec<Pubkey>,
}

pub enum PoolUpdate {
    RaydiumAmm {
        amm_id: Pubkey,
        snapshot: AmmSnapshot,
    },
    RaydiumCpmm {
        pool_id: Pubkey,
        snapshot: RaydiumCpmmSnapshot,
    },
    RaydiumClmm {
        pool_id: Pubkey,
        snapshot: Box<ClmmSnapshot>,
    },
    MeteoraDlmm {
        pool_id: Pubkey,
        snapshot: Box<MeteoraDlmmSnapshot>,
    },
    MeteoraDamm {
        pool_id: Pubkey,
        snapshot: MeteoraDammSnapshot,
    },
    OrcaWhirlpool {
        pool_id: Pubkey,
        snapshot: Box<OrcaWhirlpoolSnapshot>,
    },
    PumpAmm {
        pool_id: Pubkey,
        snapshot: PumpAmmSnapshot,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotDedupeKey {
    pub sqrt_price_x64: u128,
    pub liquidity: u128,
    pub tick_current: i32,
    pub tick_spacing: u16,
    pub trade_fee_rate: u32,
    pub center_start_tick: i32,
    pub tick_update_slots: [u64; CLMM_TICK_WINDOW_LEN],
}
