# solana-arb-simulator

Public simulation-first Solana arbitrage engine showcasing multi-venue watcher architecture and graph-route evaluation.

## What this repo includes
- Watchers for all supported venues:
  - Raydium AMM
  - Raydium CPMM
  - Raydium CLMM
  - Meteora DLMM
  - Meteora DAMM
  - Orca Whirlpool
  - PumpSwap AMM
- Graph-style route search and quote estimation across watched pools.
- Simulation gate before execution decisions.
- Telegram notifications for sim success/failure.
- Dual pool source modes:
  - static pools from `cfg.toml`
  - dynamically discovered pools from `sol_pool_listener` sqlite DB

## Safety defaults
- `ARB_SIM_ONLY=true` by default in this repo.
- No live sends unless you explicitly disable sim-only.

## Run
1. `cp .env.example .env`
2. `cp cfg.example.toml cfg.toml`
3. `cargo run`

The binary reads `cfg.toml` from repo root.
RPC endpoints are loaded from `.env`:
- `RPC_URL`
- `WSS_URL`

## Pool source modes
1. Static cfg mode:
   - set `ARB_DYNAMIC_DB_ENABLED=false`
   - define pools under `[[pools]]` in `cfg.toml`
2. Dynamic listener DB mode:
   - set `ARB_DYNAMIC_DB_ENABLED=true`
   - set `ARB_LISTENER_DB_PATH` to the sqlite produced by your `sol_pool_listener`
   - keep `cfg.toml` pools minimal or empty

## Useful env vars
- `RPC_URL=https://...`
- `WSS_URL=wss://...`
- `ARB_SIM_ONLY=true|false`
- `ARB_DYNAMIC_SIM_ONLY=true|false`
- `ARB_DYNAMIC_DB_ENABLED=true|false`
- `ARB_LISTENER_DB_PATH=/path/to/pools.db`
- `ARB_DYNAMIC_DB_POLL_SECS=8`
- `RUST_LOG=info` (or `debug`)

## Quality checks
- `cargo fmt`
- `cargo check`
- `cargo test`
