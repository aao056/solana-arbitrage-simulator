#!/usr/bin/env bash
set -euo pipefail

URL='https://dexscreener.com/5m?rankBy=pairAge&order=asc&chainIds=solana&profile=1'
SCRIPT_DIR="$(cd -- "$(dirname -- "$0")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." >/dev/null 2>&1 && pwd)"
OUT="$REPO_ROOT/pools.generated.toml"
HTML="$(mktemp -t ds_5m)"

cleanup() {
  rm -f "$HTML" || true
}
trap cleanup EXIT

curl -fsSL --max-time 25 "$URL" -o "$HTML"

DEXSCREENER_FETCH_PARALLEL=24 \
DEXSCREENER_CURL_MAX_TIME=3 \
"$SCRIPT_DIR/dexscreener_parser.sh" \
  --html "$HTML" \
  --out "$OUT" \
  --seed-max-links 20 \
  --max-tokens 8 \
  --max-pools-per-token 6 \
  --max-total-pools 120 \
  --min-liq-usd 5000 \
  --min-h1-volume-usd 200 \
  --min-m5-txns 1 \
  --delete-html

POOL_COUNT="$(grep -c '^\[\[pools\]\]' "$OUT" || true)"
echo "$POOL_COUNT"

if [[ "$POOL_COUNT" -gt 0 ]]; then
  sed -n '1,80p' "$OUT"
else
  echo "No cross-venue pool groups matched current filters." >&2
fi
