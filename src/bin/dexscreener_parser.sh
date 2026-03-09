#!/usr/bin/env bash
set -euo pipefail

# Parse DexScreener HTML rows (href="/solana/<pair>") and generate cfg.toml-ready
# [[pools]] entries for cross-venue arbitrage candidates.
#
# Dependencies: bash, curl, jq, grep, sed, awk, xargs

SCRIPT_NAME="$(basename "$0")"

SOL_MINT="So11111111111111111111111111111111111111112"
USDC_MINT="EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"

# Defaults tuned for fast-moving Solana meme pairs.
MIN_LIQ_USD=20000
MIN_H1_VOLUME_USD=2000
MIN_M5_TXNS=3
MAX_TOKENS=60
MAX_POOLS_PER_TOKEN=16
MAX_TOTAL_POOLS=200
SEED_MAX_LINKS=120

# Comma-separated allow lists.
ALLOWED_QUOTE_MINTS="$SOL_MINT,$USDC_MINT"
ALLOWED_DEX_IDS="raydium,meteora,orca,pumpswap,pancakeswap"

API_BASE="${DEXSCREENER_API_BASE:-https://api.dexscreener.com}"
CURL_MAX_TIME="${DEXSCREENER_CURL_MAX_TIME:-12}"
FETCH_PARALLEL="${DEXSCREENER_FETCH_PARALLEL:-10}"

HTML_FILE=""
OUT_FILE=""
DELETE_HTML=0

usage() {
  cat <<USAGE
Usage:
  $SCRIPT_NAME --html <dexscreener_rows.html> [options]
  cat rows.html | $SCRIPT_NAME [options]

Options:
  --html <path>               Read HTML from file instead of stdin.
  --out <path>                Write output to file (default: stdout).
  --min-liq-usd <num>         Minimum pool liquidity USD. Default: $MIN_LIQ_USD
  --min-h1-volume-usd <num>   Minimum 1h volume USD. Default: $MIN_H1_VOLUME_USD
  --min-m5-txns <num>         Minimum m5 tx count (buys+sells). Default: $MIN_M5_TXNS
  --max-tokens <num>          Max unique token mints expanded. Default: $MAX_TOKENS
  --max-pools-per-token <n>   Max pools kept per token from token-pairs API. Default: $MAX_POOLS_PER_TOKEN
  --max-total-pools <num>     Final cap for rendered pools. Default: $MAX_TOTAL_POOLS
  --seed-max-links <num>      Max seed /solana/<pair> links to use from input HTML (0 = no cap). Default: $SEED_MAX_LINKS
  --allowed-quotes <csv>      Quote mint allow list. Default: SOL,USDC
  --allowed-dex-ids <csv>     Dex IDs allow list. Default: raydium,meteora,orca,pumpswap,pancakeswap
  --delete-html               Delete input --html file on exit.
  -h, --help                  Show this help.

Environment:
  DEXSCREENER_API_BASE        API base URL. Default: https://api.dexscreener.com
  DEXSCREENER_CURL_MAX_TIME   Per-request timeout sec. Default: 12
  DEXSCREENER_FETCH_PARALLEL  Parallel API fetch workers. Default: 10

Notes:
  - Output keeps only token/quote groups seen on >=2 venues.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --html)
      HTML_FILE="${2:-}"
      shift 2
      ;;
    --out)
      OUT_FILE="${2:-}"
      shift 2
      ;;
    --min-liq-usd)
      MIN_LIQ_USD="${2:-}"
      shift 2
      ;;
    --min-h1-volume-usd)
      MIN_H1_VOLUME_USD="${2:-}"
      shift 2
      ;;
    --min-m5-txns)
      MIN_M5_TXNS="${2:-}"
      shift 2
      ;;
    --max-tokens)
      MAX_TOKENS="${2:-}"
      shift 2
      ;;
    --max-pools-per-token)
      MAX_POOLS_PER_TOKEN="${2:-}"
      shift 2
      ;;
    --max-total-pools)
      MAX_TOTAL_POOLS="${2:-}"
      shift 2
      ;;
    --seed-max-links)
      SEED_MAX_LINKS="${2:-}"
      shift 2
      ;;
    --allowed-quotes)
      ALLOWED_QUOTE_MINTS="${2:-}"
      shift 2
      ;;
    --allowed-dex-ids)
      ALLOWED_DEX_IDS="${2:-}"
      shift 2
      ;;
    --delete-html)
      DELETE_HTML=1
      shift 1
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

is_number() {
  [[ "$1" =~ ^[0-9]+([.][0-9]+)?$ ]]
}

is_uint() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

for n in "$MIN_LIQ_USD" "$MIN_H1_VOLUME_USD" "$MIN_M5_TXNS"; do
  if ! is_number "$n"; then
    echo "numeric option required, got: $n" >&2
    exit 1
  fi
done

for n in "$MAX_TOKENS" "$MAX_POOLS_PER_TOKEN" "$MAX_TOTAL_POOLS" "$SEED_MAX_LINKS" "$CURL_MAX_TIME" "$FETCH_PARALLEL"; do
  if ! is_uint "$n"; then
    echo "integer option required, got: $n" >&2
    exit 1
  fi
done

TMP_DIR="$(mktemp -d)"
SEED_PAIRS_FILE="$TMP_DIR/seed_pairs.txt"
SEED_JSONL_FILE="$TMP_DIR/seed_pairs.jsonl"
TOKEN_MINTS_FILE="$TMP_DIR/token_mints.txt"
CANDIDATES_JSONL_FILE="$TMP_DIR/candidates.jsonl"
FINAL_FILE="$TMP_DIR/final.toml"
SEED_RAW_DIR="$TMP_DIR/seed_raw"
TOKEN_RAW_DIR="$TMP_DIR/token_raw"

cleanup() {
  rm -rf "$TMP_DIR"
  if [[ "$DELETE_HTML" -eq 1 && -n "$HTML_FILE" && -f "$HTML_FILE" ]]; then
    rm -f "$HTML_FILE" || true
  fi
}
trap cleanup EXIT

if [[ -n "$HTML_FILE" ]]; then
  if [[ ! -f "$HTML_FILE" ]]; then
    echo "HTML file not found: $HTML_FILE" >&2
    exit 1
  fi
  HTML_CONTENT="$(cat "$HTML_FILE")"
else
  if [[ -t 0 ]]; then
    echo "No --html file and no stdin provided." >&2
    usage >&2
    exit 1
  fi
  HTML_CONTENT="$(cat)"
fi

if [[ -z "${HTML_CONTENT//[[:space:]]/}" ]]; then
  echo "Input HTML is empty." >&2
  exit 1
fi

PAIR_LINKS="$(printf '%s' "$HTML_CONTENT" | grep -Eo 'href="/solana/[A-Za-z0-9]+"' || true)"

printf '%s\n' "$PAIR_LINKS" \
  | sed -E 's@href="/solana/@@; s@"$@@' \
  | awk -v max="$SEED_MAX_LINKS" '
      length($0) >= 32 && length($0) <= 64 {
        if (!seen[$0]++) {
          print $0;
          count++;
          if (max > 0 && count >= max) {
            exit;
          }
        }
      }
    ' > "$SEED_PAIRS_FILE"

SEED_COUNT="$(wc -l < "$SEED_PAIRS_FILE" | tr -d ' ')"
if [[ "$SEED_COUNT" -eq 0 ]]; then
  echo "No /solana/<pairAddress> links found in input." >&2
  exit 1
fi

echo "[info] seed Solana pair ids: $SEED_COUNT" >&2
echo "[info] fetch_parallel=$FETCH_PARALLEL curl_timeout=${CURL_MAX_TIME}s" >&2

mkdir -p "$SEED_RAW_DIR"

# Phase 1: fetch seed pair payloads in parallel.
cat "$SEED_PAIRS_FILE" | xargs -P "$FETCH_PARALLEL" -n 1 bash -c '
  api_base="$1"
  timeout_s="$2"
  out_dir="$3"
  pair_id="$4"
  curl -fsS --max-time "$timeout_s" "$api_base/latest/dex/pairs/solana/$pair_id" -o "$out_dir/$pair_id.json" 2>/dev/null || true
' _ "$API_BASE" "$CURL_MAX_TIME" "$SEED_RAW_DIR"

shopt -s nullglob
seed_files=("$SEED_RAW_DIR"/*.json)
if [[ "${#seed_files[@]}" -eq 0 ]]; then
  echo "Failed to resolve seed pairs via DexScreener API." >&2
  exit 1
fi

for f in "${seed_files[@]}"; do
  [[ -s "$f" ]] || continue
  jq -c '
    (if (.pair? | type) == "object" then [ .pair ] else (.pairs // []) end)
    | map(select(.chainId == "solana"))
    | .[]
    | {
        pairAddress,
        dexId,
        labels: (.labels // []),
        baseToken,
        quoteToken,
        liquidity,
        volume,
        txns
      }
  ' "$f" >> "$SEED_JSONL_FILE" 2>/dev/null || true
done

if [[ ! -s "$SEED_JSONL_FILE" ]]; then
  echo "Failed to parse seed pair payloads." >&2
  exit 1
fi

jq -r --arg quotes "$ALLOWED_QUOTE_MINTS" '
  def qset: ($quotes | split(",") | map(select(length > 0)));
  def is_quote($m): (qset | index($m)) != null;

  (if is_quote(.quoteToken.address // "") and (is_quote(.baseToken.address // "") | not)
   then .baseToken.address
   elif is_quote(.baseToken.address // "") and (is_quote(.quoteToken.address // "") | not)
   then .quoteToken.address
   else empty end)
' "$SEED_JSONL_FILE" | sort -u | head -n "$MAX_TOKENS" > "$TOKEN_MINTS_FILE"

TOKEN_COUNT="$(wc -l < "$TOKEN_MINTS_FILE" | tr -d ' ')"
if [[ "$TOKEN_COUNT" -eq 0 ]]; then
  echo "No non-quote token mints discovered from seed pairs." >&2
  exit 1
fi

echo "[info] unique token mints to expand: $TOKEN_COUNT" >&2

mkdir -p "$TOKEN_RAW_DIR"

# Phase 2: fetch token-pairs payloads in parallel.
cat "$TOKEN_MINTS_FILE" | xargs -P "$FETCH_PARALLEL" -n 1 bash -c '
  api_base="$1"
  timeout_s="$2"
  out_dir="$3"
  token="$4"
  curl -fsS --max-time "$timeout_s" "$api_base/token-pairs/v1/solana/$token" -o "$out_dir/$token.json" 2>/dev/null || true
' _ "$API_BASE" "$CURL_MAX_TIME" "$TOKEN_RAW_DIR"

token_files=("$TOKEN_RAW_DIR"/*.json)
if [[ "${#token_files[@]}" -eq 0 ]]; then
  echo "No token-pairs payloads fetched." >&2
  exit 1
fi

for f in "${token_files[@]}"; do
  [[ -s "$f" ]] || continue
  token_mint="$(basename "$f" .json)"

  jq -c \
    --arg token "$token_mint" \
    --arg quotes "$ALLOWED_QUOTE_MINTS" \
    --arg dex_ids "$ALLOWED_DEX_IDS" \
    --argjson min_liq "$MIN_LIQ_USD" \
    --argjson min_h1_vol "$MIN_H1_VOLUME_USD" \
    --argjson min_m5_txns "$MIN_M5_TXNS" \
    --argjson max_per_token "$MAX_POOLS_PER_TOKEN" \
    '
      def qset: ($quotes | split(",") | map(select(length > 0)));
      def dexset: ($dex_ids | split(",") | map(select(length > 0)));
      def is_quote($m): (qset | index($m)) != null;
      def dex_allowed($d): (dexset | index($d)) != null;

      def dex_kind:
        if .dexId == "raydium" and ((.labels // []) | index("CLMM")) != null then {dex:"raydium", kind:"clmm"}
        elif .dexId == "raydium" and ((.labels // []) | index("CPMM")) != null then {dex:"raydium", kind:"cpmm"}
        elif .dexId == "raydium" then {dex:"raydium", kind:"amm"}
        elif .dexId == "meteora" and ((.labels // []) | index("DLMM")) != null then {dex:"meteora", kind:"dlmm"}
        elif .dexId == "meteora" then {dex:"meteora", kind:"damm"}
        elif .dexId == "orca" then {dex:"orca", kind:"whirlpool"}
        elif .dexId == "pumpswap" then {dex:"pumpswap", kind:"amm"}
        elif .dexId == "pancakeswap" then {dex:"pancakeswap", kind:"clmm"}
        else empty end;

      def oriented:
        if (.baseToken.address // "") == $token and is_quote(.quoteToken.address // "") then {
          token_mint: (.baseToken.address // ""),
          token_symbol: (.baseToken.symbol // "UNK"),
          quote_mint: (.quoteToken.address // ""),
          quote_symbol: (.quoteToken.symbol // "UNK")
        }
        elif (.quoteToken.address // "") == $token and is_quote(.baseToken.address // "") then {
          token_mint: (.quoteToken.address // ""),
          token_symbol: (.quoteToken.symbol // "UNK"),
          quote_mint: (.baseToken.address // ""),
          quote_symbol: (.baseToken.symbol // "UNK")
        }
        else empty end;

      [ .[]
        | select(.chainId == "solana")
        | select(dex_allowed(.dexId // ""))
        | select((.liquidity.usd // 0) >= $min_liq)
        | select((.volume.h1 // 0) >= $min_h1_vol)
        | select((((.txns.m5.buys // 0) + (.txns.m5.sells // 0)) >= $min_m5_txns))
        | . as $p
        | (dex_kind) as $dk
        | (oriented) as $o
        | select($dk != null and $o != null)
        | {
            dex: $dk.dex,
            kind: $dk.kind,
            symbol: ($o.token_symbol + "_" + $o.quote_symbol),
            pool_id: $p.pairAddress,
            token_mint: $o.token_mint,
            quote_mint: $o.quote_mint,
            liquidity_usd: ($p.liquidity.usd // 0),
            h1_volume_usd: ($p.volume.h1 // 0),
            m5_txns: (($p.txns.m5.buys // 0) + ($p.txns.m5.sells // 0))
          }
      ]
      | sort_by(-.h1_volume_usd, -.liquidity_usd)
      | .[:$max_per_token]
      | .[]
    ' "$f" >> "$CANDIDATES_JSONL_FILE" 2>/dev/null || true
done

if [[ ! -s "$CANDIDATES_JSONL_FILE" ]]; then
  echo "No candidate pools passed filters." >&2
  exit 1
fi

jq -sr \
  --argjson max_total "$MAX_TOTAL_POOLS" \
  '
    map(select(.pool_id != null and (.pool_id | length > 0)))
    | unique_by(.pool_id)
    | group_by(.token_mint + "|" + .quote_mint)
    | map(
        . as $g
        | {
            token_mint: $g[0].token_mint,
            quote_mint: $g[0].quote_mint,
            symbol: $g[0].symbol,
            venues: ($g | map(.dex + ":" + .kind) | unique),
            total_liq: ($g | map(.liquidity_usd) | add),
            max_h1_vol: ($g | map(.h1_volume_usd) | max),
            pools: ($g | sort_by(-.h1_volume_usd, -.liquidity_usd))
          }
      )
    | map(select((.venues | length) >= 2))
    | sort_by(-.max_h1_vol, -.total_liq)
    | .[:$max_total]
    | .[]
    | (
        "# ---- " + .symbol
        + " | venues=" + ((.venues | length) | tostring)
        + " | token=" + .token_mint
        + " | quote=" + .quote_mint
      ),
      (
        .pools[]
        | "[[pools]]\n"
          + "dex = \"" + .dex + "\"\n"
          + "kind = \"" + .kind + "\"\n"
          + "symbol = \"" + .symbol + "\"\n"
          + "pool_id = \"" + .pool_id + "\"\n"
      ),
      ""
  ' "$CANDIDATES_JSONL_FILE" > "$FINAL_FILE"

if [[ ! -s "$FINAL_FILE" ]]; then
  echo "No cross-venue groups found after grouping." >&2
  exit 1
fi

if [[ -n "$OUT_FILE" ]]; then
  cp "$FINAL_FILE" "$OUT_FILE"
  echo "[ok] wrote cfg blocks to: $OUT_FILE" >&2
else
  cat "$FINAL_FILE"
fi
