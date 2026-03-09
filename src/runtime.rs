use crate::core::raydium_clmm_quote;
use crate::execute::send_tx_and_notify;
use crate::meteora_damm::{
    MeteoraDammSnapshot, MeteoraDammStatic,
    build_swap2_exact_in_ix as build_meteora_damm_swap2_exact_in_ix,
    quote_exact_in_approx as meteora_damm_quote_exact_in,
};
use crate::meteora_dlmm::{
    MeteoraDlmmSnapshot, build_swap_exact_in_ix as build_meteora_swap_exact_in_ix,
    quote_exact_in as meteora_quote_exact_in,
};
use crate::models::{AmmSnapshot, ClmmSnapshot, ClmmStatic, PoolUpdate, StartMintConfig};
use crate::orca_whirlpool::{
    OrcaWhirlpoolSnapshot, build_swap_exact_in_ix as build_orca_swap_exact_in_ix,
    quote_exact_in as orca_quote_exact_in,
};
use crate::pumpswap::{
    PumpAmmSnapshot, PumpFeeConfig, PumpPoolStatic,
    build_buy_exact_out_ix as build_pumpswap_buy_ix,
    build_sell_exact_in_ix as build_pumpswap_sell_ix, quote_exact_in as pumpswap_quote_exact_in,
};
use crate::raydium::amm::build_swap_ix::{build_swap_base_in_v2_ix, derive_amm_authority};
use crate::raydium::amm::core::{AmmInfo, SwapDirection};
use crate::raydium::amm::math::swap_exact_amount;
use crate::raydium::clmm::build_swap_ix::build_clmm_swap_v2_ix;
use crate::raydium::clmm::sim::clmm_route_from_input_mint;
use crate::raydium_cpmm::{
    RaydiumCpmmSnapshot, RaydiumCpmmStatic,
    build_swap_base_input_ix as build_raydium_cpmm_swap_base_input_ix,
    quote_exact_in as raydium_cpmm_quote_exact_in,
};
use crate::simulate::{debug_missing_accounts, dump_sim, simulate_tx, simulated_token_amount_at};
use crate::telegram_send::tg_send;
use anyhow::{Context, Result};
use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_program::program_pack::Pack;
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::hash::Hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::message::AddressLookupTableAccount;
use solana_sdk::message::Message;
use solana_sdk::signer::{Signer, keypair::Keypair};
use solana_sdk::transaction::Transaction;
use spl_associated_token_account::{
    get_associated_token_address, get_associated_token_address_with_program_id,
    instruction::create_associated_token_account_idempotent,
};
use spl_token::state::Account as SplTokenAccount;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;
use tokio::time::Instant;

const USDC_MINT_STR: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT_STR: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
const WSOL_MINT_STR: &str = "So11111111111111111111111111111111111111112";
const RAYDIUM_AMM_PROGRAM_STR: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ROUTE_MIN_HOPS: usize = 2;
const DEFAULT_ROUTE_MAX_HOPS: usize = 3;
const DEFAULT_ROUTE_HAIRCUT_BPS: u64 = 10;
const DEFAULT_ROUTE_SLIPPAGE_BPS: u64 = 50;
const BPS_DENOMINATOR: u128 = 10_000;
const ESTIMATE_LOG_DELTA_UNITS: i128 = 500;
const LEGACY_ACCOUNT_META_SOFT_LIMIT: usize = 60;
const V0_ACCOUNT_META_SOFT_LIMIT: usize = 110;
const LEGACY_TX_MAX_RAW_BYTES: usize = 1232;
const TG_ERROR_NOTIFY_COOLDOWN: Duration = Duration::from_secs(20);
const TG_SIM_FOUND_NOTIFY_COOLDOWN: Duration = Duration::from_secs(20);
const DEFAULT_REQUOTE_MAX_DROP_BPS: u64 = 20;
const DEFAULT_NET_SAFETY_BPS: u64 = 5;
const DEFAULT_MAX_ROUTE_STALENESS_MS: u64 = 600;
const DEFAULT_DYNAMIC_AMOUNT_SELECTION: bool = true;
const DEFAULT_ROUTE_COOLDOWN_MS: u64 = 350;
const DEFAULT_SCAN_DEBOUNCE_MS: u64 = 40;
const DEFAULT_TRACE_SUMMARY_SECS: u64 = 60;
const PUMPSWAP_QUOTE_LOG_COOLDOWN: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug)]
struct PumpProbeConfig {
    mint: Pubkey,
    amount_in: u64,
    symbol: &'static str,
    decimals: u8,
}

const DEFAULT_STABLE_EXEC_AMOUNT: f64 = 10.0;
const DEFAULT_STABLE_MIN_PROFIT: f64 = 0.01;
const DEFAULT_STABLE_ESTIMATED_TX_COST: f64 = 0.001;
const DEFAULT_STABLE_PROBE_AMOUNTS: [f64; 7] = [1.0, 1.5, 2.0, 3.0, 5.0, 7.5, 10.0];

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum VenueKind {
    RaydiumAmm,
    RaydiumCpmm,
    RaydiumClmm,
    PancakeswapClmm,
    MeteoraDlmm,
    MeteoraDamm,
    OrcaWhirlpool,
    PumpAmm,
}

#[derive(Clone, Copy, Debug)]
pub struct VenueMeta {
    pub kind: VenueKind,
    pub pool_id: Pubkey,
    pub mint_a: Pubkey,
    pub mint_b: Pubkey,
}

#[derive(Clone, Copy, Debug)]
struct RouteLeg {
    kind: VenueKind,
    pool_id: Pubkey,
    input_mint: Pubkey,
    output_mint: Pubkey,
}

#[derive(Clone, Debug)]
struct RouteCandidate {
    start_mint: Pubkey,
    amount_in: u64,
    amount_out: u64,
    legs: Vec<RouteLeg>,
}

impl RouteCandidate {
    fn profit_units(&self) -> i128 {
        i128::from(self.amount_out) - i128::from(self.amount_in)
    }

    fn route_key(&self) -> String {
        let legs = self
            .legs
            .iter()
            .map(|l| {
                format!(
                    "{:?}:{}:{}:{}",
                    l.kind, l.pool_id, l.input_mint, l.output_mint
                )
            })
            .collect::<Vec<_>>()
            .join("|");
        format!("{}|{}", self.start_mint, legs)
    }
}

#[derive(Clone, Debug)]
struct StartMintPlan {
    mint: Pubkey,
    symbol: String,
    decimals: u8,
    exec_amount_units: u64,
    min_profit_units: u64,
    estimated_tx_cost_units: u64,
    probe_amounts_units: Vec<u64>,
}

#[derive(Clone, Copy, Debug)]
struct CandidateEval {
    exec_est_amount_out: u64,
    gross_profit_units: i128,
    estimated_cost_units: u64,
    net_profit_units: i128,
}

pub struct RuntimeCtx<'a> {
    pub rpc: &'a RpcClient,
    pub payer: &'a Keypair,
    pub lookup_tables: &'a [AddressLookupTableAccount],
    pub tg_token: Option<&'a str>,
    pub tg_chat_id: Option<&'a str>,
}

pub struct RuntimeState {
    pub amm_info_by_id: HashMap<Pubkey, AmmInfo>,
    pub clmm_static: HashMap<Pubkey, ClmmStatic>,
    pub latest_amm: HashMap<Pubkey, AmmSnapshot>,
    pub latest_raydium_cpmm: HashMap<Pubkey, RaydiumCpmmSnapshot>,
    pub latest_clmm: HashMap<Pubkey, Box<ClmmSnapshot>>,
    pub latest_meteora: HashMap<Pubkey, Box<MeteoraDlmmSnapshot>>,
    pub latest_meteora_damm: HashMap<Pubkey, MeteoraDammSnapshot>,
    pub latest_orca: HashMap<Pubkey, Box<OrcaWhirlpoolSnapshot>>,
    pub latest_pumpswap: HashMap<Pubkey, PumpAmmSnapshot>,
    pub venues: Vec<VenueMeta>,
    amm_pair_mints: HashMap<Pubkey, (Pubkey, Pubkey)>, // (coin_mint, pc_mint)
    meteora_damm_static: HashMap<Pubkey, MeteoraDammStatic>,
    pumpswap_static: HashMap<Pubkey, PumpPoolStatic>,
    raydium_cpmm_static: HashMap<Pubkey, RaydiumCpmmStatic>,
    pumpswap_fee_config: Option<PumpFeeConfig>,
    start_mint_plans: Vec<StartMintPlan>,
    last_route_exec: HashMap<String, Instant>,
    last_error_notify: HashMap<String, Instant>,
    last_sim_found_notify: HashMap<String, Instant>,
    last_scan_at: Option<Instant>,
    last_estimate_by_key: HashMap<String, (String, i128)>,
    last_pumpswap_quote_log: HashMap<Pubkey, Instant>,
    dynamic_pool_keys: HashSet<(VenueKind, Pubkey)>,
    dynamic_sim_only: bool,
    global_sim_only: bool,
    log_pumpswap_probes: bool,
    route_cooldown: Duration,
    scan_debounce: Duration,
    route_haircut_bps: u64,
    route_slippage_bps: u64,
    route_max_hops: usize,
    requote_max_drop_bps: u64,
    net_safety_bps: u64,
    max_route_staleness: Duration,
    dynamic_amount_selection: bool,
    debug_missing_accounts: bool,
    trace_summary_interval: Duration,
    last_trace_summary_at: Instant,
    trace_pool_updates: u64,
    trace_scan_runs: u64,
    trace_no_route_scans: u64,
    trace_route_estimates: u64,
    trace_positive_candidates: u64,
    trace_sim_attempts: u64,
    trace_sim_success: u64,
    trace_sim_failures: u64,
}

impl RuntimeState {
    pub fn new(cfg_start_mints: Option<Vec<StartMintConfig>>) -> Result<Self> {
        let start_mint_plans = build_start_mint_plans(cfg_start_mints)?;
        let debug_missing_accounts = parse_bool_env("ARB_DEBUG_MISSING_ACCOUNTS", false);
        let route_cooldown_ms = parse_env_u64("ARB_ROUTE_COOLDOWN_MS", DEFAULT_ROUTE_COOLDOWN_MS);
        let scan_debounce_ms = parse_env_u64("ARB_SCAN_DEBOUNCE_MS", DEFAULT_SCAN_DEBOUNCE_MS);
        let route_haircut_bps = parse_env_bps("ARB_ROUTE_HAIRCUT_BPS", DEFAULT_ROUTE_HAIRCUT_BPS);
        let route_slippage_bps =
            parse_env_bps("ARB_ROUTE_SLIPPAGE_BPS", DEFAULT_ROUTE_SLIPPAGE_BPS);
        let route_max_hops = parse_env_hops(
            "ARB_ROUTE_MAX_HOPS",
            DEFAULT_ROUTE_MAX_HOPS,
            ROUTE_MIN_HOPS,
            3,
        );
        let requote_max_drop_bps =
            parse_env_u64("ARB_REQUOTE_MAX_DROP_BPS", DEFAULT_REQUOTE_MAX_DROP_BPS);
        let net_safety_bps = parse_env_u64("ARB_NET_SAFETY_BPS", DEFAULT_NET_SAFETY_BPS);
        let max_route_staleness_ms =
            parse_env_u64("ARB_MAX_ROUTE_STALENESS_MS", DEFAULT_MAX_ROUTE_STALENESS_MS);
        let dynamic_amount_selection = parse_bool_env(
            "ARB_DYNAMIC_AMOUNT_SELECTION",
            DEFAULT_DYNAMIC_AMOUNT_SELECTION,
        );
        let dynamic_sim_only = parse_bool_env("ARB_DYNAMIC_SIM_ONLY", true);
        let global_sim_only = parse_bool_env("ARB_SIM_ONLY", true);
        let log_pumpswap_probes = parse_bool_env("ARB_LOG_PROBES", false);
        let trace_summary_secs =
            parse_env_u64("ARB_TRACE_SUMMARY_SECS", DEFAULT_TRACE_SUMMARY_SECS).max(5);

        tracing::info!(
            route_cooldown_ms,
            scan_debounce_ms,
            route_haircut_bps,
            route_slippage_bps,
            route_max_hops,
            requote_max_drop_bps,
            net_safety_bps,
            max_route_staleness_ms,
            dynamic_amount_selection,
            dynamic_sim_only,
            global_sim_only,
            log_pumpswap_probes,
            trace_summary_secs,
            "runtime tuning loaded"
        );

        Ok(Self {
            amm_info_by_id: HashMap::new(),
            clmm_static: HashMap::new(),
            latest_amm: HashMap::new(),
            latest_raydium_cpmm: HashMap::new(),
            latest_clmm: HashMap::new(),
            latest_meteora: HashMap::new(),
            latest_meteora_damm: HashMap::new(),
            latest_orca: HashMap::new(),
            latest_pumpswap: HashMap::new(),
            venues: Vec::new(),
            amm_pair_mints: HashMap::new(),
            meteora_damm_static: HashMap::new(),
            pumpswap_static: HashMap::new(),
            raydium_cpmm_static: HashMap::new(),
            pumpswap_fee_config: None,
            start_mint_plans,
            last_route_exec: HashMap::new(),
            last_error_notify: HashMap::new(),
            last_sim_found_notify: HashMap::new(),
            last_scan_at: None,
            last_estimate_by_key: HashMap::new(),
            last_pumpswap_quote_log: HashMap::new(),
            dynamic_pool_keys: HashSet::new(),
            dynamic_sim_only,
            global_sim_only,
            log_pumpswap_probes,
            route_cooldown: Duration::from_millis(route_cooldown_ms),
            scan_debounce: Duration::from_millis(scan_debounce_ms),
            route_haircut_bps,
            route_slippage_bps,
            route_max_hops,
            requote_max_drop_bps,
            net_safety_bps,
            max_route_staleness: Duration::from_millis(max_route_staleness_ms),
            dynamic_amount_selection,
            debug_missing_accounts,
            trace_summary_interval: Duration::from_secs(trace_summary_secs),
            last_trace_summary_at: Instant::now(),
            trace_pool_updates: 0,
            trace_scan_runs: 0,
            trace_no_route_scans: 0,
            trace_route_estimates: 0,
            trace_positive_candidates: 0,
            trace_sim_attempts: 0,
            trace_sim_success: 0,
            trace_sim_failures: 0,
        })
    }

    pub fn register_amm_pool(
        &mut self,
        pool_id: Pubkey,
        info: AmmInfo,
        coin_mint: Pubkey,
        pc_mint: Pubkey,
    ) {
        self.amm_info_by_id.insert(pool_id, info);
        self.amm_pair_mints.insert(pool_id, (coin_mint, pc_mint));
        self.venues.push(VenueMeta {
            kind: VenueKind::RaydiumAmm,
            pool_id,
            mint_a: coin_mint,
            mint_b: pc_mint,
        });
    }

    pub fn register_raydium_cpmm_pool(&mut self, pool_id: Pubkey, st: RaydiumCpmmStatic) {
        self.venues.push(VenueMeta {
            kind: VenueKind::RaydiumCpmm,
            pool_id,
            mint_a: st.token_0_mint,
            mint_b: st.token_1_mint,
        });
        self.raydium_cpmm_static.insert(pool_id, st);
    }

    pub fn register_clmm_pool_with_kind(
        &mut self,
        kind: VenueKind,
        pool_id: Pubkey,
        st: ClmmStatic,
        mint_0: Pubkey,
        mint_1: Pubkey,
    ) {
        self.clmm_static.insert(pool_id, st);
        self.venues.push(VenueMeta {
            kind,
            pool_id,
            mint_a: mint_0,
            mint_b: mint_1,
        });
    }

    pub fn register_meteora_pool(&mut self, pool_id: Pubkey, mint_x: Pubkey, mint_y: Pubkey) {
        self.venues.push(VenueMeta {
            kind: VenueKind::MeteoraDlmm,
            pool_id,
            mint_a: mint_x,
            mint_b: mint_y,
        });
    }

    pub fn register_meteora_damm_pool(&mut self, pool_id: Pubkey, st: MeteoraDammStatic) {
        self.venues.push(VenueMeta {
            kind: VenueKind::MeteoraDamm,
            pool_id,
            mint_a: st.token_a_mint,
            mint_b: st.token_b_mint,
        });
        self.meteora_damm_static.insert(pool_id, st);
    }

    pub fn register_orca_pool(&mut self, pool_id: Pubkey, mint_a: Pubkey, mint_b: Pubkey) {
        self.venues.push(VenueMeta {
            kind: VenueKind::OrcaWhirlpool,
            pool_id,
            mint_a,
            mint_b,
        });
    }

    pub fn set_pumpswap_fee_config(&mut self, cfg: PumpFeeConfig) {
        self.pumpswap_fee_config = Some(cfg);
    }

    pub fn register_pumpswap_pool(&mut self, pool_id: Pubkey, st: PumpPoolStatic) {
        self.venues.push(VenueMeta {
            kind: VenueKind::PumpAmm,
            pool_id,
            mint_a: st.base_mint,
            mint_b: st.quote_mint,
        });
        self.pumpswap_static.insert(pool_id, st);
    }

    pub fn insert_initial_pumpswap_snapshot(&mut self, pool_id: Pubkey, snapshot: PumpAmmSnapshot) {
        self.latest_pumpswap.insert(pool_id, snapshot);
    }

    pub fn insert_initial_raydium_cpmm_snapshot(
        &mut self,
        pool_id: Pubkey,
        snapshot: RaydiumCpmmSnapshot,
    ) {
        self.latest_raydium_cpmm.insert(pool_id, snapshot);
    }

    pub fn insert_initial_meteora_damm_snapshot(
        &mut self,
        pool_id: Pubkey,
        snapshot: MeteoraDammSnapshot,
    ) {
        self.latest_meteora_damm.insert(pool_id, snapshot);
    }

    pub fn insert_initial_meteora_snapshot(
        &mut self,
        pool_id: Pubkey,
        snapshot: Box<MeteoraDlmmSnapshot>,
    ) {
        self.latest_meteora.insert(pool_id, snapshot);
    }

    pub fn insert_initial_orca_snapshot(
        &mut self,
        pool_id: Pubkey,
        snapshot: Box<OrcaWhirlpoolSnapshot>,
    ) {
        self.latest_orca.insert(pool_id, snapshot);
    }

    pub fn mark_dynamic_pool(&mut self, kind: VenueKind, pool_id: Pubkey) {
        self.dynamic_pool_keys.insert((kind, pool_id));
    }

    pub async fn handle_pool_update(
        &mut self,
        msg: PoolUpdate,
        ctx: &RuntimeCtx<'_>,
    ) -> Result<()> {
        self.trace_pool_updates = self.trace_pool_updates.saturating_add(1);
        match msg {
            PoolUpdate::RaydiumAmm { amm_id, snapshot } => {
                self.latest_amm.insert(amm_id, snapshot);
            }
            PoolUpdate::RaydiumCpmm { pool_id, snapshot } => {
                self.latest_raydium_cpmm.insert(pool_id, snapshot);
            }
            PoolUpdate::RaydiumClmm { pool_id, snapshot } => {
                self.latest_clmm.insert(pool_id, snapshot);
            }
            PoolUpdate::MeteoraDlmm { pool_id, snapshot } => {
                self.latest_meteora.insert(pool_id, snapshot);
            }
            PoolUpdate::MeteoraDamm { pool_id, snapshot } => {
                self.latest_meteora_damm.insert(pool_id, snapshot);
            }
            PoolUpdate::OrcaWhirlpool { pool_id, snapshot } => {
                self.latest_orca.insert(pool_id, snapshot);
            }
            PoolUpdate::PumpAmm { pool_id, snapshot } => {
                self.latest_pumpswap.insert(pool_id, snapshot);
                self.maybe_log_pumpswap_probe_quotes(pool_id, snapshot);
            }
        }

        if self
            .last_scan_at
            .is_some_and(|at| at.elapsed() < self.scan_debounce)
        {
            return Ok(());
        }
        self.last_scan_at = Some(Instant::now());
        self.trace_scan_runs = self.trace_scan_runs.saturating_add(1);
        let out = self.scan_and_execute_graph_arbs(ctx).await;
        self.maybe_log_trace_summary();
        out
    }

    fn maybe_log_pumpswap_probe_quotes(&mut self, pool_id: Pubkey, snap: PumpAmmSnapshot) {
        if !self.log_pumpswap_probes {
            return;
        }
        let now = Instant::now();
        if self
            .last_pumpswap_quote_log
            .get(&pool_id)
            .is_some_and(|t| now.duration_since(*t) < PUMPSWAP_QUOTE_LOG_COOLDOWN)
        {
            return;
        }

        let Some(st) = self.pumpswap_static.get(&pool_id) else {
            return;
        };
        let Some(fee_cfg) = self.pumpswap_fee_config.as_ref() else {
            return;
        };

        let usdc_mint = Pubkey::from_str(USDC_MINT_STR).expect("invalid USDC mint");
        let wsol_mint = Pubkey::from_str(WSOL_MINT_STR).expect("invalid WSOL mint");

        let probes = [
            PumpProbeConfig {
                mint: usdc_mint,
                amount_in: 5_000_000,
                symbol: "USDC",
                decimals: 6,
            },
            PumpProbeConfig {
                mint: wsol_mint,
                amount_in: 50_000_000,
                symbol: "WSOL",
                decimals: 9,
            },
        ];

        let mut logged_any = false;
        for probe in probes {
            if self.log_one_pumpswap_probe_quote(pool_id, st, fee_cfg, snap, probe) {
                logged_any = true;
            }
        }

        if logged_any {
            self.last_pumpswap_quote_log.insert(pool_id, now);
        }
    }

    fn log_one_pumpswap_probe_quote(
        &self,
        pool_id: Pubkey,
        st: &PumpPoolStatic,
        fee_cfg: &PumpFeeConfig,
        snap: PumpAmmSnapshot,
        probe: PumpProbeConfig,
    ) -> bool {
        let probe_mint = probe.mint;
        let probe_amount_in = probe.amount_in;
        let probe_symbol = probe.symbol;
        let probe_decimals = probe.decimals;

        let (token_mint, token_decimals) = if st.base_mint == probe_mint {
            (st.quote_mint, st.quote_decimals)
        } else if st.quote_mint == probe_mint {
            (st.base_mint, st.base_decimals)
        } else {
            return false;
        };

        let ask_token_out =
            match pumpswap_quote_exact_in(st, &snap, fee_cfg, probe_mint, probe_amount_in) {
                Ok(v) => v,
                Err(_) => return false,
            };
        if ask_token_out == 0 {
            return false;
        }

        let bid_probe_out =
            pumpswap_quote_exact_in(st, &snap, fee_cfg, token_mint, ask_token_out).ok();

        let ask_input_ui = probe_amount_in as f64 / 10f64.powi(i32::from(probe_decimals));
        let ask_token_out_ui = ask_token_out as f64 / 10f64.powi(i32::from(token_decimals));
        let ask_price = if ask_token_out_ui > 0.0 {
            ask_input_ui / ask_token_out_ui
        } else {
            0.0
        };

        let (bid_out_ui, bid_price, roundtrip_bps) = if let Some(bid_out) = bid_probe_out {
            let bid_out_ui = bid_out as f64 / 10f64.powi(i32::from(probe_decimals));
            let bid_price = if ask_token_out_ui > 0.0 {
                bid_out_ui / ask_token_out_ui
            } else {
                0.0
            };
            let roundtrip_bps = if probe_amount_in > 0 {
                ((bid_out as f64 - probe_amount_in as f64) / probe_amount_in as f64) * 10_000.0
            } else {
                0.0
            };
            (Some(bid_out_ui), Some(bid_price), Some(roundtrip_bps))
        } else {
            (None, None, None)
        };

        tracing::info!(
            pool_id = %pool_id,
            probe_symbol,
            probe_amount_in_units = probe_amount_in,
            probe_amount_in_ui = ask_input_ui,
            token_mint = %token_mint,
            token_amount_out_units = ask_token_out,
            token_amount_out_ui = ask_token_out_ui,
            ask_price_probe_per_token = ask_price,
            bid_probe_out_ui = bid_out_ui,
            bid_price_probe_per_token = bid_price,
            roundtrip_bps = roundtrip_bps,
            "pumpswap probe quote"
        );
        true
    }

    async fn scan_and_execute_graph_arbs(&mut self, ctx: &RuntimeCtx<'_>) -> Result<()> {
        let scan_started = Instant::now();
        let mut exec_candidates: Vec<(StartMintPlan, RouteCandidate, CandidateEval)> = Vec::new();

        for plan in self.start_mint_plans.clone() {
            for amount in plan.probe_amounts_units.iter().copied() {
                let best = self.find_best_cycle_for_amount(&plan, &[plan.mint], amount);
                let Some((mut cand, eval)) = best else {
                    continue;
                };

                let route = cand.route_key();
                self.trace_route_estimates = self.trace_route_estimates.saturating_add(1);
                let estimate_key = format!("{}:{}", plan.mint, amount);
                let should_log = self
                    .last_estimate_by_key
                    .get(&estimate_key)
                    .map(|(prev_route, prev_net_profit)| {
                        prev_route != &route
                            || (eval.net_profit_units - *prev_net_profit).abs()
                                >= ESTIMATE_LOG_DELTA_UNITS
                    })
                    .unwrap_or(true);

                if should_log {
                    tracing::info!(
                        amount_in = amount,
                        exec_est_amount_out = eval.exec_est_amount_out,
                        gross_profit_units = eval.gross_profit_units,
                        estimated_cost_units = eval.estimated_cost_units,
                        net_profit_units = eval.net_profit_units,
                        min_required_net_units = plan.min_profit_units,
                        hops = cand.legs.len(),
                        start_mint = %cand.start_mint,
                        start_symbol = %plan.symbol,
                        start_decimals = plan.decimals,
                        dynamic_amount_selection = self.dynamic_amount_selection,
                        route_key = %route,
                        route_venues = %format_route_venues(&cand.legs),
                        route_pools = %format_route_pools(&cand.legs),
                        route_mints = %format_route_mints(&cand.legs),
                        "graph-arb estimate"
                    );
                    self.last_estimate_by_key
                        .insert(estimate_key, (route, eval.net_profit_units));
                }

                if self.dynamic_amount_selection || amount == plan.exec_amount_units {
                    cand.amount_out = eval.exec_est_amount_out;
                    exec_candidates.push((plan.clone(), cand, eval));
                }
            }
        }

        self.trace_positive_candidates = self.trace_positive_candidates.saturating_add(
            exec_candidates
                .iter()
                .filter(|(plan, _, eval)| eval.net_profit_units > i128::from(plan.min_profit_units))
                .count() as u64,
        );

        let Some((exec_plan, mut exec_candidate, mut exec_eval)) = exec_candidates
            .into_iter()
            .filter(|(plan, _, eval)| eval.net_profit_units > i128::from(plan.min_profit_units))
            .max_by(|(_, a_cand, a_eval), (_, b_cand, b_eval)| {
                route_net_profit_bps(a_cand.amount_in, a_eval.net_profit_units)
                    .cmp(&route_net_profit_bps(
                        b_cand.amount_in,
                        b_eval.net_profit_units,
                    ))
                    .then_with(|| a_eval.net_profit_units.cmp(&b_eval.net_profit_units))
            })
        else {
            self.trace_no_route_scans = self.trace_no_route_scans.saturating_add(1);
            return Ok(());
        };

        let key = exec_candidate.route_key();
        if self
            .last_route_exec
            .get(&key)
            .is_some_and(|t| t.elapsed() < self.route_cooldown)
        {
            return Ok(());
        }

        if scan_started.elapsed() > self.max_route_staleness {
            tracing::info!(
                route = %key,
                max_route_staleness_ms = self.max_route_staleness.as_millis(),
                "graph-arb skipped: route became stale before build"
            );
            return Ok(());
        }

        let requoted_eval = match self.evaluate_candidate(&exec_plan, &exec_candidate) {
            Ok(eval) => eval,
            Err(err) => {
                self.notify_execution_error(
                    ctx,
                    "pre_send_requote_failed",
                    &exec_plan,
                    &exec_candidate,
                    &key,
                    &format!("{err:#}"),
                )
                .await;
                return Ok(());
            }
        };
        let requote_drop_bps = drop_bps(
            exec_eval.exec_est_amount_out,
            requoted_eval.exec_est_amount_out,
        );
        if requote_drop_bps > self.requote_max_drop_bps {
            tracing::info!(
                route = %key,
                old_exec_est_amount_out = exec_eval.exec_est_amount_out,
                new_exec_est_amount_out = requoted_eval.exec_est_amount_out,
                requote_drop_bps,
                max_drop_bps = self.requote_max_drop_bps,
                "graph-arb skipped: requote moved too much"
            );
            return Ok(());
        }
        exec_eval = requoted_eval;
        exec_candidate.amount_out = exec_eval.exec_est_amount_out;

        if exec_eval.net_profit_units <= i128::from(exec_plan.min_profit_units) {
            tracing::info!(
                reason = "below_min_net_profit_after_requote",
                amount_in = exec_candidate.amount_in,
                exec_est_amount_out = exec_eval.exec_est_amount_out,
                gross_profit_units = exec_eval.gross_profit_units,
                estimated_cost_units = exec_eval.estimated_cost_units,
                net_profit_units = exec_eval.net_profit_units,
                min_required_net_units = exec_plan.min_profit_units,
                hops = exec_candidate.legs.len(),
                start_mint = %exec_candidate.start_mint,
                dynamic_amount_selection = self.dynamic_amount_selection,
                "graph-arb skipped"
            );
            return Ok(());
        }

        let is_dynamic_route = self.route_contains_dynamic_pool(&exec_candidate.legs);
        let is_sim_only_route = self.global_sim_only || (self.dynamic_sim_only && is_dynamic_route);

        let ixs = match self.build_route_instructions(ctx, &exec_candidate) {
            Ok(ixs) => ixs,
            Err(err) => {
                if is_sim_only_route {
                    self.notify_sim_found_failure(
                        ctx,
                        "build_route_instructions_failed",
                        &exec_plan,
                        &exec_candidate,
                        &key,
                        &format!("{err:#}"),
                    )
                    .await;
                } else {
                    self.notify_execution_error(
                        ctx,
                        "build_route_instructions_failed",
                        &exec_plan,
                        &exec_candidate,
                        &key,
                        &format!("{err:#}"),
                    )
                    .await;
                }
                return Ok(());
            }
        };
        if ixs.is_empty() {
            return Ok(());
        }
        let account_metas = unique_account_meta_count(&ixs);
        let account_meta_limit = if ctx.lookup_tables.is_empty() {
            LEGACY_ACCOUNT_META_SOFT_LIMIT
        } else {
            V0_ACCOUNT_META_SOFT_LIMIT
        };
        if account_metas > account_meta_limit {
            tracing::info!(
                route = %key,
                account_metas,
                soft_limit = account_meta_limit,
                uses_v0 = !ctx.lookup_tables.is_empty(),
                "graph-arb skipped: route too large by account-meta soft limit"
            );
            return Ok(());
        }

        if ctx.lookup_tables.is_empty() {
            let tx_raw_size = estimate_legacy_tx_size_with_budget(ctx.payer, &ixs)?;
            if tx_raw_size > LEGACY_TX_MAX_RAW_BYTES {
                tracing::info!(
                    route = %key,
                    amount_in = exec_candidate.amount_in,
                    tx_raw_size,
                    max_raw = LEGACY_TX_MAX_RAW_BYTES,
                    "graph-arb skipped: tx too large for legacy packet, use v0+ALT"
                );
                return Ok(());
            }
        }

        let start_ata =
            get_associated_token_address(&ctx.payer.pubkey(), &exec_candidate.start_mint);
        let pre_start_balance = match fetch_spl_token_ata_amount(
            ctx.rpc,
            start_ata,
            ctx.payer.pubkey(),
            exec_candidate.start_mint,
        )
        .await
        {
            Ok(amount) => amount,
            Err(err) => {
                self.notify_execution_error(
                    ctx,
                    "start_balance_read_failed",
                    &exec_plan,
                    &exec_candidate,
                    &key,
                    &format!("{err:#}"),
                )
                .await;
                return Ok(());
            }
        };

        if self.debug_missing_accounts {
            debug_missing_accounts(ctx.rpc, &ixs).await?;
        }

        self.trace_sim_attempts = self.trace_sim_attempts.saturating_add(1);
        let sim = match simulate_tx(
            ctx.rpc,
            ctx.payer,
            ixs.clone(),
            ctx.lookup_tables,
            &[start_ata],
        )
        .await
        {
            Ok(sim) => sim,
            Err(err) => {
                self.trace_sim_failures = self.trace_sim_failures.saturating_add(1);
                if is_sim_only_route {
                    self.notify_sim_found_failure(
                        ctx,
                        "simulate_rpc_error",
                        &exec_plan,
                        &exec_candidate,
                        &key,
                        &format!("{err:#}"),
                    )
                    .await;
                } else {
                    self.notify_execution_error(
                        ctx,
                        "simulate_rpc_error",
                        &exec_plan,
                        &exec_candidate,
                        &key,
                        &format!("{err:#}"),
                    )
                    .await;
                }
                self.last_route_exec.insert(key.clone(), Instant::now());
                return Ok(());
            }
        };
        dump_sim("GRAPH ROUTE SIM RESULT", &sim);

        if let Some(err) = sim.err {
            self.trace_sim_failures = self.trace_sim_failures.saturating_add(1);
            tracing::info!(
                route = %key,
                start_symbol = %exec_plan.symbol,
                ?err,
                "graph-arb sim failed"
            );
            if is_sim_only_route {
                self.notify_sim_found_failure(
                    ctx,
                    "simulation_failed",
                    &exec_plan,
                    &exec_candidate,
                    &key,
                    &format!("{err:?}"),
                )
                .await;
            } else {
                self.notify_execution_error(
                    ctx,
                    "simulation_failed",
                    &exec_plan,
                    &exec_candidate,
                    &key,
                    &format!("{err:?}"),
                )
                .await;
            }
            self.last_route_exec.insert(key.clone(), Instant::now());
            return Ok(());
        }

        let sim_post_start_balance = match simulated_token_amount_at(&sim, 0) {
            Ok(amount) => amount,
            Err(err) => {
                self.trace_sim_failures = self.trace_sim_failures.saturating_add(1);
                if is_sim_only_route {
                    self.notify_sim_found_failure(
                        ctx,
                        "simulation_balance_parse_failed",
                        &exec_plan,
                        &exec_candidate,
                        &key,
                        &format!("{err:#}"),
                    )
                    .await;
                } else {
                    self.notify_execution_error(
                        ctx,
                        "simulation_balance_parse_failed",
                        &exec_plan,
                        &exec_candidate,
                        &key,
                        &format!("{err:#}"),
                    )
                    .await;
                }
                return Ok(());
            }
        };
        let sim_balance_delta = i128::from(sim_post_start_balance) - i128::from(pre_start_balance);
        if sim_balance_delta <= 0 {
            self.trace_sim_failures = self.trace_sim_failures.saturating_add(1);
            tracing::info!(
                route = %key,
                start_symbol = %exec_plan.symbol,
                start_mint = %exec_candidate.start_mint,
                amount_in = exec_candidate.amount_in,
                pre_start_balance,
                sim_post_start_balance,
                sim_balance_delta,
                "graph-arb skipped: non-positive simulated start balance delta"
            );
            return Ok(());
        }

        if scan_started.elapsed() > self.max_route_staleness {
            tracing::info!(
                route = %key,
                max_route_staleness_ms = self.max_route_staleness.as_millis(),
                "graph-arb skipped: route became stale before send"
            );
            return Ok(());
        }

        if is_sim_only_route {
            self.trace_sim_success = self.trace_sim_success.saturating_add(1);
            self.notify_sim_found_success(
                ctx,
                &exec_plan,
                &exec_candidate,
                &exec_eval,
                &key,
                sim_balance_delta,
            )
            .await;
            self.last_route_exec.insert(key.clone(), Instant::now());
            tracing::info!(
                route = %key,
                start_symbol = %exec_plan.symbol,
                start_mint = %exec_candidate.start_mint,
                amount_in = exec_candidate.amount_in,
                sim_balance_delta,
                route_venues = %format_route_venues(&exec_candidate.legs),
                route_pools = %format_route_pools(&exec_candidate.legs),
                dynamic_route = is_dynamic_route,
                global_sim_only = self.global_sim_only,
                "graph-arb skipped: route SIM-only mode (execution disabled)"
            );
            return Ok(());
        }
        self.trace_sim_success = self.trace_sim_success.saturating_add(1);

        let msg = format!(
            "<b>ARB Executed</b>\n\
<b>Start</b>: {} (<code>{}</code>)\n\
<b>Hops</b>: {}\n\
<b>In</b>: {} {}\n\
<b>Est Out</b>: {} {}\n\
<b>Gross Profit</b>: {} {}\n\
<b>Est Cost</b>: {} {}\n\
<b>Est Net</b>: {} {}\n\
<b>Sim Delta</b>: {} {}\n\
<b>Route</b>:\n{}",
            escape_html(&exec_plan.symbol),
            exec_candidate.start_mint,
            exec_candidate.legs.len(),
            format_amount_units(exec_candidate.amount_in, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_amount_units(exec_eval.exec_est_amount_out, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_signed_units(exec_eval.gross_profit_units, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_amount_units(exec_eval.estimated_cost_units, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_signed_units(exec_eval.net_profit_units, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_signed_units(sim_balance_delta, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_route_legs_html(&exec_candidate.legs),
        );
        let sig = match send_tx_and_notify(
            ctx.rpc,
            ctx.payer,
            ixs,
            ctx.lookup_tables,
            ctx.tg_token,
            ctx.tg_chat_id,
            Some(msg),
        )
        .await
        {
            Ok(sig) => sig,
            Err(err) => {
                self.notify_execution_error(
                    ctx,
                    "send_transaction_failed",
                    &exec_plan,
                    &exec_candidate,
                    &key,
                    &format!("{err:#}"),
                )
                .await;
                return Ok(());
            }
        };

        match fetch_spl_token_ata_amount(
            ctx.rpc,
            start_ata,
            ctx.payer.pubkey(),
            exec_candidate.start_mint,
        )
        .await
        {
            Ok(post_start_balance) => {
                let realized_delta = i128::from(post_start_balance) - i128::from(pre_start_balance);
                tracing::info!(
                    %sig,
                    route = %key,
                    start_symbol = %exec_plan.symbol,
                    start_mint = %exec_candidate.start_mint,
                    pre_start_balance,
                    post_start_balance,
                    realized_delta,
                    "graph-arb realized start balance delta"
                );
                if realized_delta <= 0 {
                    self.notify_execution_error(
                        ctx,
                        "non_positive_realized_balance_delta",
                        &exec_plan,
                        &exec_candidate,
                        &key,
                        &format!(
                            "pre_start_balance={pre_start_balance} post_start_balance={post_start_balance} realized_delta={realized_delta}"
                        ),
                    )
                    .await;
                }
            }
            Err(err) => {
                self.notify_execution_error(
                    ctx,
                    "post_execution_balance_read_failed",
                    &exec_plan,
                    &exec_candidate,
                    &key,
                    &format!("{err:#}"),
                )
                .await;
            }
        }

        self.last_route_exec.insert(key.clone(), Instant::now());
        tracing::info!(
            %sig,
            route = %key,
            start_symbol = %exec_plan.symbol,
            start_mint = %exec_candidate.start_mint,
            amount_in = exec_candidate.amount_in,
            min_profit_units = exec_plan.min_profit_units,
            gross_profit_units = exec_eval.gross_profit_units,
            estimated_cost_units = exec_eval.estimated_cost_units,
            net_profit_units = exec_eval.net_profit_units,
            "GRAPH ARB SENT"
        );

        Ok(())
    }

    async fn notify_execution_error(
        &mut self,
        ctx: &RuntimeCtx<'_>,
        reason: &str,
        exec_plan: &StartMintPlan,
        exec_candidate: &RouteCandidate,
        route_key: &str,
        details: &str,
    ) {
        let (Some(token), Some(chat_id)) = (ctx.tg_token, ctx.tg_chat_id) else {
            return;
        };

        let throttle_key = format!("{reason}:{route_key}");
        if let Some(last) = self.last_error_notify.get(&throttle_key)
            && last.elapsed() < TG_ERROR_NOTIFY_COOLDOWN
        {
            return;
        }
        self.last_error_notify.insert(throttle_key, Instant::now());

        let mut trimmed_details = details.trim().to_string();
        if trimmed_details.len() > 800 {
            trimmed_details.truncate(800);
            trimmed_details.push_str("...");
        }

        let message = format!(
            "<b>ARB Execution Error</b>\n\
<b>Reason</b>: <code>{}</code>\n\
<b>Start</b>: {} (<code>{}</code>)\n\
<b>Hops</b>: {}\n\
<b>In</b>: {} {}\n\
<b>Est Out</b>: {} {}\n\
<b>Est Profit</b>: {} {}\n\
<b>Route</b>:\n{}\n\
<b>Details</b>:\n<pre>{}</pre>",
            escape_html(reason),
            escape_html(&exec_plan.symbol),
            exec_candidate.start_mint,
            exec_candidate.legs.len(),
            format_amount_units(exec_candidate.amount_in, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_amount_units(exec_candidate.amount_out, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_signed_units(exec_candidate.profit_units(), exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_route_legs_html(&exec_candidate.legs),
            escape_html(&trimmed_details),
        );

        if let Err(err) = tg_send(token, chat_id, &message).await {
            tracing::warn!(?err, "telegram error-notify failed");
        }
    }

    async fn notify_sim_found_failure(
        &mut self,
        ctx: &RuntimeCtx<'_>,
        reason: &str,
        exec_plan: &StartMintPlan,
        exec_candidate: &RouteCandidate,
        route_key: &str,
        details: &str,
    ) {
        let (Some(token), Some(chat_id)) = (ctx.tg_token, ctx.tg_chat_id) else {
            return;
        };

        let throttle_key = format!("sim_fail:{reason}:{route_key}");
        if let Some(last) = self.last_error_notify.get(&throttle_key)
            && last.elapsed() < TG_ERROR_NOTIFY_COOLDOWN
        {
            return;
        }
        self.last_error_notify.insert(throttle_key, Instant::now());

        let mut trimmed_details = details.trim().to_string();
        if trimmed_details.len() > 800 {
            trimmed_details.truncate(800);
            trimmed_details.push_str("...");
        }

        let message = format!(
            "<b>ARB Sim Failed</b>\n\
<b>Reason</b>: <code>{}</code>\n\
<b>Mode</b>: SIM-only\n\
<b>Start</b>: {} (<code>{}</code>)\n\
<b>Hops</b>: {}\n\
<b>In</b>: {} {}\n\
<b>Route</b>: {}\n\
<b>Pools</b>: {}\n\
<b>Details</b>:\n<pre>{}</pre>",
            escape_html(reason),
            escape_html(&exec_plan.symbol),
            exec_candidate.start_mint,
            exec_candidate.legs.len(),
            format_amount_units(exec_candidate.amount_in, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            escape_html(&format_route_venues(&exec_candidate.legs)),
            escape_html(&format_route_pools(&exec_candidate.legs)),
            escape_html(&trimmed_details),
        );

        if let Err(err) = tg_send(token, chat_id, &message).await {
            tracing::warn!(?err, "telegram sim-failure notify failed");
        }
    }

    async fn notify_sim_found_success(
        &mut self,
        ctx: &RuntimeCtx<'_>,
        exec_plan: &StartMintPlan,
        exec_candidate: &RouteCandidate,
        exec_eval: &CandidateEval,
        route_key: &str,
        sim_balance_delta: i128,
    ) {
        let (Some(token), Some(chat_id)) = (ctx.tg_token, ctx.tg_chat_id) else {
            return;
        };

        if let Some(last) = self.last_sim_found_notify.get(route_key)
            && last.elapsed() < TG_SIM_FOUND_NOTIFY_COOLDOWN
        {
            return;
        }
        self.last_sim_found_notify
            .insert(route_key.to_string(), Instant::now());

        let swap_lines = self
            .format_route_swaps_html(exec_candidate)
            .unwrap_or_else(|err| {
                format!(
                    "(route quote formatting failed: {})",
                    escape_html(&err.to_string())
                )
            });

        let message = format!(
            "<b>ARB Found (Sim)</b>\n\
<b>Mode</b>: SIM-only\n\
<b>Start</b>: {} (<code>{}</code>)\n\
<b>Hops</b>: {}\n\
<b>In</b>: {} {}\n\
<b>Est Out</b>: {} {}\n\
<b>Gross Profit</b>: {} {}\n\
<b>Est Cost</b>: {} {}\n\
<b>Est Net</b>: {} {}\n\
<b>Sim Delta</b>: {} {}\n\
<b>Route</b>: {}\n\
<b>Pools</b>: {}\n\n\
<b>Swaps</b>\n{}",
            escape_html(&exec_plan.symbol),
            exec_candidate.start_mint,
            exec_candidate.legs.len(),
            format_amount_units(exec_candidate.amount_in, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_amount_units(exec_eval.exec_est_amount_out, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_signed_units(exec_eval.gross_profit_units, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_amount_units(exec_eval.estimated_cost_units, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_signed_units(exec_eval.net_profit_units, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            format_signed_units(sim_balance_delta, exec_plan.decimals),
            escape_html(&exec_plan.symbol),
            escape_html(&format_route_venues(&exec_candidate.legs)),
            escape_html(&format_route_pools(&exec_candidate.legs)),
            swap_lines,
        );

        if let Err(err) = tg_send(token, chat_id, &message).await {
            tracing::warn!(?err, "telegram sim-found notify failed");
        }
    }

    fn route_contains_dynamic_pool(&self, legs: &[RouteLeg]) -> bool {
        legs.iter()
            .any(|leg| self.dynamic_pool_keys.contains(&(leg.kind, leg.pool_id)))
    }

    fn format_route_swaps_html(&self, cand: &RouteCandidate) -> Result<String> {
        let mut amount_in = cand.amount_in;
        let mut lines = Vec::with_capacity(cand.legs.len());
        for (idx, leg) in cand.legs.iter().enumerate() {
            let venue = self
                .venues
                .iter()
                .find(|v| v.pool_id == leg.pool_id && v.kind == leg.kind)
                .context("missing venue meta while formatting route swaps")?;
            let amount_out = self.quote_venue_exact_in(venue, leg.input_mint, amount_in)?;

            let in_sym = self.mint_symbol(leg.input_mint);
            let out_sym = self.mint_symbol(leg.output_mint);
            let in_amt = self.format_mint_amount(leg.input_mint, amount_in);
            let out_amt = self.format_mint_amount(leg.output_mint, amount_out);

            lines.push(format!(
                "{}. Swap <code>{}</code> {} → <code>{}</code> {} on <code>{:?}</code> (<code>{}</code>)",
                idx + 1,
                in_amt,
                escape_html(&in_sym),
                out_amt,
                escape_html(&out_sym),
                leg.kind,
                leg.pool_id
            ));

            if idx + 1 < cand.legs.len() {
                amount_in = u64::try_from(apply_bps_cut(amount_out.into(), self.route_haircut_bps))
                    .context("next amount_in overflow while formatting route swaps")?;
                if amount_in == 0 {
                    anyhow::bail!("next amount_in became zero while formatting route swaps");
                }
            }
        }
        Ok(lines.join("\n"))
    }

    fn format_mint_amount(&self, mint: Pubkey, units: u64) -> String {
        match self.mint_decimals(mint) {
            Some(dec) => format_amount_units(units, dec),
            None => units.to_string(),
        }
    }

    fn mint_symbol(&self, mint: Pubkey) -> String {
        if let Some(plan) = self.start_mint_plans.iter().find(|p| p.mint == mint) {
            return plan.symbol.clone();
        }
        if mint.to_string() == USDC_MINT_STR {
            return "USDC".to_string();
        }
        if mint.to_string() == USDT_MINT_STR {
            return "USDT".to_string();
        }
        if mint.to_string() == WSOL_MINT_STR {
            return "WSOL".to_string();
        }
        short_pubkey(mint)
    }

    fn mint_decimals(&self, mint: Pubkey) -> Option<u8> {
        if let Some(plan) = self.start_mint_plans.iter().find(|p| p.mint == mint) {
            return Some(plan.decimals);
        }
        for st in self.pumpswap_static.values() {
            if st.base_mint == mint {
                return Some(st.base_decimals);
            }
            if st.quote_mint == mint {
                return Some(st.quote_decimals);
            }
        }
        for st in self.meteora_damm_static.values() {
            if st.token_a_mint == mint {
                return Some(st.token_a_decimals);
            }
            if st.token_b_mint == mint {
                return Some(st.token_b_decimals);
            }
        }
        for st in self.raydium_cpmm_static.values() {
            if st.token_0_mint == mint {
                return Some(st.mint_0_decimals);
            }
            if st.token_1_mint == mint {
                return Some(st.mint_1_decimals);
            }
        }
        None
    }

    fn find_best_cycle_for_amount(
        &self,
        plan: &StartMintPlan,
        starts: &[Pubkey],
        amount_in: u64,
    ) -> Option<(RouteCandidate, CandidateEval)> {
        let mut best: Option<(RouteCandidate, CandidateEval)> = None;

        for &start in starts {
            let mut path: Vec<RouteLeg> = Vec::new();
            let mut used_pools: HashSet<Pubkey> = HashSet::new();
            self.dfs_cycles(
                plan,
                start,
                start,
                amount_in,
                amount_in,
                0,
                &mut path,
                &mut used_pools,
                &mut best,
            );
        }

        best
    }

    #[allow(clippy::too_many_arguments)]
    fn dfs_cycles(
        &self,
        plan: &StartMintPlan,
        start_mint: Pubkey,
        current_mint: Pubkey,
        start_amount: u64,
        current_amount: u64,
        depth: usize,
        path: &mut Vec<RouteLeg>,
        used_pools: &mut HashSet<Pubkey>,
        best: &mut Option<(RouteCandidate, CandidateEval)>,
    ) {
        if depth >= self.route_max_hops {
            return;
        }

        for venue in &self.venues {
            if used_pools.contains(&venue.pool_id) {
                continue;
            }

            let Some(output_mint) = other_mint(venue, current_mint) else {
                continue;
            };

            let Ok(out_amount) = self.quote_venue_exact_in(venue, current_mint, current_amount)
            else {
                continue;
            };
            if out_amount == 0 {
                continue;
            }

            let leg = RouteLeg {
                kind: venue.kind,
                pool_id: venue.pool_id,
                input_mint: current_mint,
                output_mint,
            };
            path.push(leg);
            used_pools.insert(venue.pool_id);

            let hops = depth + 1;
            if hops >= ROUTE_MIN_HOPS && output_mint == start_mint {
                let cand = RouteCandidate {
                    start_mint,
                    amount_in: start_amount,
                    amount_out: out_amount,
                    legs: path.clone(),
                };
                if let Ok(eval) = self.evaluate_candidate(plan, &cand) {
                    consider_best_evaluated(best, cand, eval);
                }
            }

            if hops < self.route_max_hops {
                self.dfs_cycles(
                    plan,
                    start_mint,
                    output_mint,
                    start_amount,
                    out_amount,
                    hops,
                    path,
                    used_pools,
                    best,
                );
            }

            used_pools.remove(&venue.pool_id);
            path.pop();
        }
    }

    fn quote_venue_exact_in(
        &self,
        venue: &VenueMeta,
        input_mint: Pubkey,
        amount_in: u64,
    ) -> Result<u64> {
        match venue.kind {
            VenueKind::RaydiumAmm => {
                let snap = self
                    .latest_amm
                    .get(&venue.pool_id)
                    .copied()
                    .context("missing latest_amm snapshot")?;
                let (coin_mint, pc_mint) = self
                    .amm_pair_mints
                    .get(&venue.pool_id)
                    .copied()
                    .context("missing amm pair mints")?;

                if input_mint == pc_mint {
                    let out = swap_exact_amount(
                        snap.pc_vault_amount,
                        snap.coin_vault_amount,
                        snap.fee_numerator,
                        snap.fee_denominator,
                        SwapDirection::PC2Coin,
                        amount_in,
                        true,
                    )?;
                    Ok(u64::try_from(out).context("amm quote out overflow")?)
                } else if input_mint == coin_mint {
                    let out = swap_exact_amount(
                        snap.pc_vault_amount,
                        snap.coin_vault_amount,
                        snap.fee_numerator,
                        snap.fee_denominator,
                        SwapDirection::Coin2PC,
                        amount_in,
                        true,
                    )?;
                    Ok(u64::try_from(out).context("amm quote out overflow")?)
                } else {
                    anyhow::bail!("input mint not in AMM pool");
                }
            }
            VenueKind::RaydiumCpmm => {
                let snap = self
                    .latest_raydium_cpmm
                    .get(&venue.pool_id)
                    .copied()
                    .context("missing latest_raydium_cpmm snapshot")?;
                let st = self
                    .raydium_cpmm_static
                    .get(&venue.pool_id)
                    .context("missing raydium cpmm static")?;
                Ok(raydium_cpmm_quote_exact_in(
                    st, &snap, input_mint, amount_in,
                )?)
            }
            VenueKind::RaydiumClmm | VenueKind::PancakeswapClmm => {
                let snap = self
                    .latest_clmm
                    .get(&venue.pool_id)
                    .context("missing latest_clmm snapshot")?;
                let user_ata_mint0 = get_associated_token_address(
                    &Pubkey::default(),
                    &Pubkey::new_from_array(snap.pool_state.token_mint_0.to_bytes()),
                );
                let user_ata_mint1 = get_associated_token_address(
                    &Pubkey::default(),
                    &Pubkey::new_from_array(snap.pool_state.token_mint_1.to_bytes()),
                );
                let route = clmm_route_from_input_mint(
                    &snap.pool_state,
                    input_mint,
                    user_ata_mint0,
                    user_ata_mint1,
                )?;
                let out = raydium_clmm_quote(snap, amount_in, route.zero_for_one)
                    .map_err(|e| anyhow::anyhow!("clmm quote failed: {e}"))?;
                Ok(out)
            }
            VenueKind::MeteoraDlmm => {
                let snap = self
                    .latest_meteora
                    .get(&venue.pool_id)
                    .context("missing latest_meteora snapshot")?;
                Ok(meteora_quote_exact_in(
                    snap,
                    venue.pool_id,
                    amount_in,
                    input_mint,
                )?)
            }
            VenueKind::MeteoraDamm => {
                let snap = self
                    .latest_meteora_damm
                    .get(&venue.pool_id)
                    .context("missing latest_meteora_damm snapshot")?;
                let st = self
                    .meteora_damm_static
                    .get(&venue.pool_id)
                    .context("missing meteora damm static")?;
                Ok(meteora_damm_quote_exact_in(
                    st, snap, input_mint, amount_in,
                )?)
            }
            VenueKind::OrcaWhirlpool => {
                let snap = self
                    .latest_orca
                    .get(&venue.pool_id)
                    .context("missing latest_orca snapshot")?;
                Ok(orca_quote_exact_in(snap, amount_in, input_mint)?)
            }
            VenueKind::PumpAmm => {
                let snap = self
                    .latest_pumpswap
                    .get(&venue.pool_id)
                    .context("missing latest_pumpswap snapshot")?;
                let st = self
                    .pumpswap_static
                    .get(&venue.pool_id)
                    .context("missing pumpswap static")?;
                let fee_cfg = self
                    .pumpswap_fee_config
                    .as_ref()
                    .context("missing pumpswap fee config")?;
                Ok(pumpswap_quote_exact_in(
                    st, snap, fee_cfg, input_mint, amount_in,
                )?)
            }
        }
    }

    fn build_route_instructions(
        &self,
        ctx: &RuntimeCtx<'_>,
        cand: &RouteCandidate,
    ) -> Result<Vec<Instruction>> {
        let mut ixs = self.build_route_user_ata_setup_instructions(ctx, cand)?;
        ixs.reserve(cand.legs.len());
        let mut amount_in = cand.amount_in;

        let amm_program = Pubkey::from_str(RAYDIUM_AMM_PROGRAM_STR)?;

        for (leg_idx, leg) in cand.legs.iter().enumerate() {
            let venue = self
                .venues
                .iter()
                .find(|v| v.pool_id == leg.pool_id && v.kind == leg.kind)
                .context("missing venue meta for leg")?;

            let quoted_out = self.quote_venue_exact_in(venue, leg.input_mint, amount_in)?;
            let min_out = u64::try_from(apply_bps_cut(quoted_out.into(), self.route_slippage_bps))
                .context("min_out overflow")?;
            let next_amount_in_after_haircut =
                u64::try_from(apply_bps_cut(quoted_out.into(), self.route_haircut_bps))
                    .context("next amount_in overflow")?;

            let user_in_ata = get_associated_token_address(&ctx.payer.pubkey(), &leg.input_mint);
            let user_out_ata = get_associated_token_address(&ctx.payer.pubkey(), &leg.output_mint);

            match leg.kind {
                VenueKind::RaydiumAmm => {
                    let info = self
                        .amm_info_by_id
                        .get(&leg.pool_id)
                        .context("missing AmmInfo")?;
                    let amm_auth = derive_amm_authority(&amm_program, info.nonce)?;

                    ixs.push(build_swap_base_in_v2_ix(
                        amm_program,
                        leg.pool_id,
                        amm_auth,
                        info.coin_vault,
                        info.pc_vault,
                        user_in_ata,
                        user_out_ata,
                        ctx.payer.pubkey(),
                        amount_in.into(),
                        min_out.into(),
                    ));
                }
                VenueKind::RaydiumCpmm => {
                    let st = self
                        .raydium_cpmm_static
                        .get(&leg.pool_id)
                        .context("missing RaydiumCpmmStatic")?;
                    ixs.push(build_raydium_cpmm_swap_base_input_ix(
                        ctx.payer.pubkey(),
                        leg.pool_id,
                        st,
                        leg.input_mint,
                        amount_in,
                        min_out,
                    )?);
                }
                VenueKind::RaydiumClmm | VenueKind::PancakeswapClmm => {
                    let snap = self
                        .latest_clmm
                        .get(&leg.pool_id)
                        .context("missing ClmmSnapshot")?;
                    let st = self
                        .clmm_static
                        .get(&leg.pool_id)
                        .context("missing ClmmStatic")?;

                    let user_ata_mint0 = get_associated_token_address(
                        &ctx.payer.pubkey(),
                        &Pubkey::new_from_array(snap.pool_state.token_mint_0.to_bytes()),
                    );
                    let user_ata_mint1 = get_associated_token_address(
                        &ctx.payer.pubkey(),
                        &Pubkey::new_from_array(snap.pool_state.token_mint_1.to_bytes()),
                    );
                    let route = clmm_route_from_input_mint(
                        &snap.pool_state,
                        leg.input_mint,
                        user_ata_mint0,
                        user_ata_mint1,
                    )?;

                    ixs.push(build_clmm_swap_v2_ix(
                        st.program_id,
                        ctx.payer.pubkey(),
                        st.amm_config,
                        leg.pool_id,
                        route.user_input_ata,
                        route.user_output_ata,
                        route.input_vault,
                        route.output_vault,
                        route.observation_key,
                        route.input_mint,
                        route.output_mint,
                        amount_in,
                        min_out,
                        0u128,
                        true,
                        Some(st.bitmap_pda),
                        &snap.tick_array_pubkeys,
                    ));
                }
                VenueKind::MeteoraDlmm => {
                    let snap = self
                        .latest_meteora
                        .get(&leg.pool_id)
                        .context("missing MeteoraDlmmSnapshot")?;
                    ixs.push(build_meteora_swap_exact_in_ix(
                        ctx.payer.pubkey(),
                        leg.pool_id,
                        snap,
                        user_in_ata,
                        user_out_ata,
                        leg.input_mint,
                        amount_in,
                        min_out,
                    )?);
                }
                VenueKind::MeteoraDamm => {
                    let st = self
                        .meteora_damm_static
                        .get(&leg.pool_id)
                        .context("missing MeteoraDammStatic")?;
                    ixs.push(build_meteora_damm_swap2_exact_in_ix(
                        ctx.payer.pubkey(),
                        leg.pool_id,
                        st,
                        leg.input_mint,
                        amount_in,
                        min_out,
                    )?);
                }
                VenueKind::OrcaWhirlpool => {
                    let snap = self
                        .latest_orca
                        .get(&leg.pool_id)
                        .context("missing OrcaWhirlpoolSnapshot")?;
                    ixs.push(build_orca_swap_exact_in_ix(
                        ctx.payer.pubkey(),
                        leg.pool_id,
                        snap,
                        leg.input_mint,
                        amount_in,
                        min_out,
                    )?);
                }
                VenueKind::PumpAmm => {
                    let st = self
                        .pumpswap_static
                        .get(&leg.pool_id)
                        .context("missing PumpPoolStatic")?;

                    let _pump_user_in_ata = get_associated_token_address_with_program_id(
                        &ctx.payer.pubkey(),
                        &leg.input_mint,
                        if leg.input_mint == st.base_mint {
                            &st.base_token_program
                        } else {
                            &st.quote_token_program
                        },
                    );
                    let _pump_user_out_ata = get_associated_token_address_with_program_id(
                        &ctx.payer.pubkey(),
                        &leg.output_mint,
                        if leg.output_mint == st.base_mint {
                            &st.base_token_program
                        } else {
                            &st.quote_token_program
                        },
                    );

                    if leg.input_mint == st.base_mint && leg.output_mint == st.quote_mint {
                        ixs.push(build_pumpswap_sell_ix(
                            ctx.payer.pubkey(),
                            leg.pool_id,
                            st,
                            amount_in,
                            min_out,
                        ));
                    } else if leg.input_mint == st.quote_mint && leg.output_mint == st.base_mint {
                        // PumpSwap "buy" is exact-out; for intermediate legs request the chained
                        // amount directly to keep downstream amounts deterministic.
                        let requested_out = if leg_idx + 1 == cand.legs.len() {
                            min_out
                        } else {
                            next_amount_in_after_haircut
                        };
                        if requested_out == 0 {
                            anyhow::bail!("pumpswap requested_out became zero");
                        }
                        ixs.push(build_pumpswap_buy_ix(
                            ctx.payer.pubkey(),
                            leg.pool_id,
                            st,
                            requested_out,
                            amount_in,
                        ));
                    } else {
                        anyhow::bail!("pumpswap leg mints do not match pool");
                    }
                }
            }

            amount_in = next_amount_in_after_haircut;
            if amount_in == 0 {
                anyhow::bail!("next amount_in became zero");
            }
        }

        Ok(ixs)
    }

    fn build_route_user_ata_setup_instructions(
        &self,
        ctx: &RuntimeCtx<'_>,
        cand: &RouteCandidate,
    ) -> Result<Vec<Instruction>> {
        let mut seen_atas = HashSet::new();
        let mut out = Vec::new();

        for leg in &cand.legs {
            for mint in [leg.input_mint, leg.output_mint] {
                let token_program =
                    self.token_program_for_route_mint(leg, mint)
                        .with_context(|| {
                            format!(
                                "missing token program for route mint {} on {:?} pool {}",
                                mint, leg.kind, leg.pool_id
                            )
                        })?;
                let ata = get_associated_token_address_with_program_id(
                    &ctx.payer.pubkey(),
                    &mint,
                    &token_program,
                );
                if !seen_atas.insert(ata) {
                    continue;
                }
                out.push(create_associated_token_account_idempotent(
                    &ctx.payer.pubkey(),
                    &ctx.payer.pubkey(),
                    &mint,
                    &token_program,
                ));
            }
        }

        Ok(out)
    }

    fn token_program_for_route_mint(&self, leg: &RouteLeg, mint: Pubkey) -> Option<Pubkey> {
        match leg.kind {
            VenueKind::MeteoraDamm => {
                let st = self.meteora_damm_static.get(&leg.pool_id)?;
                if mint == st.token_a_mint {
                    Some(st.token_a_program)
                } else if mint == st.token_b_mint {
                    Some(st.token_b_program)
                } else {
                    None
                }
            }
            VenueKind::PumpAmm => {
                let st = self.pumpswap_static.get(&leg.pool_id)?;
                if mint == st.base_mint {
                    Some(st.base_token_program)
                } else if mint == st.quote_mint {
                    Some(st.quote_token_program)
                } else {
                    None
                }
            }
            VenueKind::RaydiumCpmm => {
                let st = self.raydium_cpmm_static.get(&leg.pool_id)?;
                if mint == st.token_0_mint {
                    Some(st.token_0_program)
                } else if mint == st.token_1_mint {
                    Some(st.token_1_program)
                } else {
                    None
                }
            }
            VenueKind::OrcaWhirlpool => {
                let snap = self.latest_orca.get(&leg.pool_id)?;
                let mint_a = Pubkey::new_from_array(snap.whirlpool.token_mint_a.to_bytes());
                let mint_b = Pubkey::new_from_array(snap.whirlpool.token_mint_b.to_bytes());
                if mint == mint_a {
                    Some(snap.token_program_a)
                } else if mint == mint_b {
                    Some(snap.token_program_b)
                } else {
                    None
                }
            }
            // Current execution builders for these venues use SPL Token accounts.
            VenueKind::RaydiumAmm
            | VenueKind::RaydiumClmm
            | VenueKind::PancakeswapClmm
            | VenueKind::MeteoraDlmm => Some(spl_token::id()),
        }
    }

    fn evaluate_candidate(
        &self,
        plan: &StartMintPlan,
        cand: &RouteCandidate,
    ) -> Result<CandidateEval> {
        let exec_est_amount_out = self.estimate_exec_amount_out(cand)?;
        let gross_profit_units = i128::from(exec_est_amount_out) - i128::from(cand.amount_in);
        let safety_buffer_units = bps_units(cand.amount_in, self.net_safety_bps);
        let estimated_cost_units = plan
            .estimated_tx_cost_units
            .saturating_add(safety_buffer_units);
        let net_profit_units = gross_profit_units - i128::from(estimated_cost_units);

        Ok(CandidateEval {
            exec_est_amount_out,
            gross_profit_units,
            estimated_cost_units,
            net_profit_units,
        })
    }

    fn estimate_exec_amount_out(&self, cand: &RouteCandidate) -> Result<u64> {
        if cand.legs.is_empty() {
            anyhow::bail!("route has no legs");
        }

        let mut leg_amount_in = cand.amount_in;
        for (idx, leg) in cand.legs.iter().enumerate() {
            let venue = self
                .venues
                .iter()
                .find(|v| v.pool_id == leg.pool_id && v.kind == leg.kind)
                .context("missing venue meta for leg while evaluating candidate")?;

            let quoted_out = self.quote_venue_exact_in(venue, leg.input_mint, leg_amount_in)?;
            if idx + 1 == cand.legs.len() {
                return Ok(quoted_out);
            }

            leg_amount_in = u64::try_from(apply_bps_cut(quoted_out.into(), self.route_haircut_bps))
                .context("next amount_in overflow while evaluating candidate")?;
            if leg_amount_in == 0 {
                anyhow::bail!("next amount_in became zero while evaluating candidate");
            }
        }

        anyhow::bail!("failed to evaluate route output")
    }
}

impl RuntimeState {
    fn maybe_log_trace_summary(&mut self) {
        if self.last_trace_summary_at.elapsed() < self.trace_summary_interval {
            return;
        }
        tracing::info!(
            pool_updates = self.trace_pool_updates,
            scan_runs = self.trace_scan_runs,
            no_route_scans = self.trace_no_route_scans,
            route_estimates = self.trace_route_estimates,
            positive_candidates = self.trace_positive_candidates,
            sim_attempts = self.trace_sim_attempts,
            sim_success = self.trace_sim_success,
            sim_failures = self.trace_sim_failures,
            venues = self.venues.len(),
            watched_snapshots_pumpswap = self.latest_pumpswap.len(),
            watched_snapshots_damm = self.latest_meteora_damm.len(),
            watched_snapshots_dlmm = self.latest_meteora.len(),
            watched_snapshots_orca = self.latest_orca.len(),
            watched_snapshots_clmm = self.latest_clmm.len(),
            watched_snapshots_amm = self.latest_amm.len(),
            watched_snapshots_cpmm = self.latest_raydium_cpmm.len(),
            "arb runtime summary"
        );
        self.trace_pool_updates = 0;
        self.trace_scan_runs = 0;
        self.trace_no_route_scans = 0;
        self.trace_route_estimates = 0;
        self.trace_positive_candidates = 0;
        self.trace_sim_attempts = 0;
        self.trace_sim_success = 0;
        self.trace_sim_failures = 0;
        self.last_trace_summary_at = Instant::now();
    }
}

fn other_mint(venue: &VenueMeta, input: Pubkey) -> Option<Pubkey> {
    if input == venue.mint_a {
        Some(venue.mint_b)
    } else if input == venue.mint_b {
        Some(venue.mint_a)
    } else {
        None
    }
}

fn consider_best_evaluated(
    best: &mut Option<(RouteCandidate, CandidateEval)>,
    cand: RouteCandidate,
    eval: CandidateEval,
) {
    let replace = best
        .as_ref()
        .map(|(b_cand, b_eval)| {
            route_net_profit_bps(cand.amount_in, eval.net_profit_units)
                .cmp(&route_net_profit_bps(
                    b_cand.amount_in,
                    b_eval.net_profit_units,
                ))
                .then_with(|| eval.net_profit_units.cmp(&b_eval.net_profit_units))
                .then_with(|| cand.profit_units().cmp(&b_cand.profit_units()))
                .is_gt()
        })
        .unwrap_or(true);
    if replace {
        *best = Some((cand, eval));
    }
}

fn route_net_profit_bps(amount_in: u64, net_profit_units: i128) -> i128 {
    if amount_in == 0 {
        return i128::MIN;
    }
    net_profit_units.saturating_mul(10_000) / i128::from(amount_in)
}

fn drop_bps(old_amount: u64, new_amount: u64) -> u64 {
    if old_amount == 0 || new_amount >= old_amount {
        return 0;
    }
    let diff = old_amount.saturating_sub(new_amount);
    let bps = (u128::from(diff) * BPS_DENOMINATOR) / u128::from(old_amount);
    u64::try_from(bps).unwrap_or(u64::MAX)
}

fn bps_units(amount: u64, bps: u64) -> u64 {
    if amount == 0 || bps == 0 {
        return 0;
    }
    let units = (u128::from(amount) * u128::from(bps)) / BPS_DENOMINATOR;
    u64::try_from(units).unwrap_or(u64::MAX)
}

fn format_route_legs_html(legs: &[RouteLeg]) -> String {
    legs.iter()
        .enumerate()
        .map(|(i, leg)| {
            format!(
                "{}. <code>{:?}</code> <code>{}</code> <code>{}→{}</code>",
                i + 1,
                leg.kind,
                short_pubkey(leg.pool_id),
                short_pubkey(leg.input_mint),
                short_pubkey(leg.output_mint)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn format_route_venues(legs: &[RouteLeg]) -> String {
    legs.iter()
        .map(|leg| format!("{:?}", leg.kind))
        .collect::<Vec<_>>()
        .join(" -> ")
}

fn format_route_pools(legs: &[RouteLeg]) -> String {
    legs.iter()
        .map(|leg| leg.pool_id.to_string())
        .collect::<Vec<_>>()
        .join(" -> ")
}

fn format_route_mints(legs: &[RouteLeg]) -> String {
    let Some(first) = legs.first() else {
        return String::new();
    };
    let mut mints = vec![first.input_mint.to_string()];
    for leg in legs {
        mints.push(leg.output_mint.to_string());
    }
    mints.join(" -> ")
}

fn short_pubkey(pk: Pubkey) -> String {
    let s = pk.to_string();
    if s.len() <= 10 {
        return s;
    }
    format!("{}...{}", &s[..4], &s[s.len().saturating_sub(4)..])
}

fn format_amount_units(units: u64, decimals: u8) -> String {
    let decimals = usize::from(decimals);
    if decimals == 0 {
        return units.to_string();
    }

    let scale = 10u64.saturating_pow(u32::from(decimals as u8));
    let whole = units / scale;
    let frac = units % scale;
    if frac == 0 {
        return whole.to_string();
    }

    let mut frac_s = format!("{:0width$}", frac, width = decimals);
    while frac_s.ends_with('0') {
        frac_s.pop();
    }
    format!("{whole}.{frac_s}")
}

fn format_signed_units(units: i128, decimals: u8) -> String {
    if units >= 0 {
        format!("+{}", format_amount_units(units as u64, decimals))
    } else {
        format!("-{}", format_amount_units((-units) as u64, decimals))
    }
}

fn escape_html(input: &str) -> String {
    input
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn apply_bps_cut(amount: u128, bps: u64) -> u128 {
    amount.saturating_mul(BPS_DENOMINATOR - (bps as u128)) / BPS_DENOMINATOR
}

async fn fetch_spl_token_ata_amount(
    rpc: &RpcClient,
    ata: Pubkey,
    expected_owner: Pubkey,
    expected_mint: Pubkey,
) -> Result<u64> {
    let acc = rpc
        .get_account(&ata)
        .await
        .with_context(|| format!("failed to fetch token account {ata}"))?;

    if acc.owner != spl_token::ID {
        anyhow::bail!(
            "unsupported token account program {} for {ata} (expected {})",
            acc.owner,
            spl_token::ID
        );
    }

    let token_acc = SplTokenAccount::unpack(&acc.data)
        .with_context(|| format!("failed to decode SPL token account {ata}"))?;

    if token_acc.owner != expected_owner {
        anyhow::bail!(
            "token account owner mismatch for {ata}: actual={} expected={}",
            token_acc.owner,
            expected_owner
        );
    }
    if token_acc.mint != expected_mint {
        anyhow::bail!(
            "token account mint mismatch for {ata}: actual={} expected={}",
            token_acc.mint,
            expected_mint
        );
    }

    Ok(token_acc.amount)
}

fn unique_account_meta_count(ixs: &[Instruction]) -> usize {
    let mut set: HashSet<Pubkey> = HashSet::new();
    for ix in ixs {
        set.insert(ix.program_id);
        for meta in &ix.accounts {
            set.insert(meta.pubkey);
        }
    }
    set.len()
}

fn estimate_legacy_tx_size_with_budget(payer: &Keypair, ixs: &[Instruction]) -> Result<usize> {
    let mut all = Vec::with_capacity(ixs.len() + 2);
    all.push(ComputeBudgetInstruction::set_compute_unit_limit(800_000));
    all.push(ComputeBudgetInstruction::set_compute_unit_price(1));
    all.extend(ixs.iter().cloned());

    let msg = Message::new(&all, Some(&payer.pubkey()));
    let mut tx = Transaction::new_unsigned(msg);
    tx.try_sign(&[payer], Hash::new_unique())
        .map_err(|e| anyhow::anyhow!("failed signing tx for size estimation: {e}"))?;

    let sig_count = tx.signatures.len();
    let message_len = tx.message_data().len();
    Ok(shortvec_encoded_len(sig_count) + sig_count.saturating_mul(64) + message_len)
}

fn shortvec_encoded_len(mut value: usize) -> usize {
    let mut len = 0usize;
    loop {
        len = len.saturating_add(1);
        value >>= 7;
        if value == 0 {
            break;
        }
    }
    len
}

fn build_start_mint_plans(
    cfg_start_mints: Option<Vec<StartMintConfig>>,
) -> Result<Vec<StartMintPlan>> {
    let start_mints = cfg_start_mints.unwrap_or_else(default_start_mint_cfgs);
    let mut plans = Vec::new();
    let estimated_tx_cost_amount =
        parse_env_f64_non_negative("ARB_ESTIMATED_TX_COST", DEFAULT_STABLE_ESTIMATED_TX_COST);

    for cfg in start_mints {
        let mint = Pubkey::from_str(&cfg.mint)
            .with_context(|| format!("invalid start_mints.mint '{}'", cfg.mint))?;
        let symbol = cfg.symbol.unwrap_or_else(|| mint.to_string());
        let decimals = cfg.decimals;

        let exec_amount_units = amount_to_units(cfg.exec_amount, decimals, false)
            .with_context(|| format!("invalid exec_amount for start mint {}", mint))?;
        let min_profit_units = amount_to_units(cfg.min_profit, decimals, true)
            .with_context(|| format!("invalid min_profit for start mint {}", mint))?;
        let estimated_tx_cost_units = amount_to_units(estimated_tx_cost_amount, decimals, true)
            .with_context(|| format!("invalid ARB_ESTIMATED_TX_COST for start mint {}", mint))?;

        let mut probe_amounts_units = if let Some(probes) = cfg.probe_amounts {
            let mut out = Vec::new();
            for p in probes {
                let units = amount_to_units(p, decimals, false)
                    .with_context(|| format!("invalid probe amount for start mint {}", mint))?;
                out.push(units);
            }
            out
        } else {
            default_probe_units(exec_amount_units)
        };

        if !probe_amounts_units.contains(&exec_amount_units) {
            probe_amounts_units.push(exec_amount_units);
        }
        probe_amounts_units.sort_unstable();
        probe_amounts_units.dedup();
        probe_amounts_units.retain(|v| *v > 0);

        plans.push(StartMintPlan {
            mint,
            symbol,
            decimals,
            exec_amount_units,
            min_profit_units,
            estimated_tx_cost_units,
            probe_amounts_units,
        });
    }

    if plans.is_empty() {
        anyhow::bail!("no valid start mints configured");
    }

    Ok(plans)
}

fn default_start_mint_cfgs() -> Vec<StartMintConfig> {
    let default_exec = parse_env_f64("ARB_EXEC_STABLE", DEFAULT_STABLE_EXEC_AMOUNT);
    let default_min_profit = parse_env_f64("ARB_MIN_PROFIT_STABLE", DEFAULT_STABLE_MIN_PROFIT);
    let default_probe_amounts = Some(DEFAULT_STABLE_PROBE_AMOUNTS.to_vec());

    vec![
        StartMintConfig {
            mint: USDC_MINT_STR.to_string(),
            decimals: 6,
            exec_amount: default_exec,
            min_profit: default_min_profit,
            probe_amounts: default_probe_amounts.clone(),
            symbol: Some("USDC".to_string()),
        },
        StartMintConfig {
            mint: USDT_MINT_STR.to_string(),
            decimals: 6,
            exec_amount: default_exec,
            min_profit: default_min_profit,
            probe_amounts: default_probe_amounts,
            symbol: Some("USDT".to_string()),
        },
    ]
}

fn default_probe_units(exec_units: u64) -> Vec<u64> {
    let mut out = vec![
        exec_units.saturating_div(10),
        exec_units.saturating_div(5),
        exec_units.saturating_div(2),
        exec_units,
    ];
    out.retain(|v| *v > 0);
    out.sort_unstable();
    out.dedup();
    out
}

fn parse_env_u64(var: &str, default: u64) -> u64 {
    let Ok(raw) = std::env::var(var) else {
        return default;
    };

    match raw.parse::<u64>() {
        Ok(v) => v,
        Err(_) => {
            tracing::warn!(env = var, value = %raw, "invalid integer env override");
            default
        }
    }
}

fn parse_env_bps(var: &str, default: u64) -> u64 {
    let bps = parse_env_u64(var, default);
    if bps >= 10_000 {
        tracing::warn!(
            env = var,
            value = bps,
            "bps env override must be < 10000, using default"
        );
        default
    } else {
        bps
    }
}

fn parse_env_hops(var: &str, default: usize, min: usize, max: usize) -> usize {
    let raw = parse_env_u64(var, default as u64);
    let value = raw as usize;
    if value < min || value > max {
        tracing::warn!(
            env = var,
            value = raw,
            min,
            max,
            "invalid hop-count env override, using default"
        );
        default
    } else {
        value
    }
}

fn parse_env_f64(var: &str, default: f64) -> f64 {
    let Ok(raw) = std::env::var(var) else {
        return default;
    };
    match raw.parse::<f64>() {
        Ok(v) if v.is_finite() && v > 0.0 => v,
        _ => {
            tracing::warn!(env = var, value = %raw, "invalid numeric env override");
            default
        }
    }
}

fn parse_env_f64_non_negative(var: &str, default: f64) -> f64 {
    let Ok(raw) = std::env::var(var) else {
        return default;
    };
    match raw.parse::<f64>() {
        Ok(v) if v.is_finite() && v >= 0.0 => v,
        _ => {
            tracing::warn!(env = var, value = %raw, "invalid numeric env override");
            default
        }
    }
}

fn amount_to_units(amount: f64, decimals: u8, allow_zero: bool) -> Result<u64> {
    if !amount.is_finite() || (!allow_zero && amount <= 0.0) || (allow_zero && amount < 0.0) {
        anyhow::bail!("amount must be finite and non-negative");
    }
    let scale = 10f64.powi(i32::from(decimals));
    let units = (amount * scale).round();
    if !(0.0..=(u64::MAX as f64)).contains(&units) {
        anyhow::bail!("amount out of range");
    }
    Ok(units as u64)
}

fn parse_bool_env(var: &str, default: bool) -> bool {
    let Ok(raw) = std::env::var(var) else {
        return default;
    };

    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}
