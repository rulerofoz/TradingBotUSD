#!/bin/bash
# Run a backtest using the TradingBot Backtester. Safe-guarded with flock.
# Fully relative version: Works anywhere for any user!

# 1. Dynamically get the directory where THIS script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 2. Fix Lockfile location to use the local project folder (bypasses root permission bugs)
LOCKFILE="$SCRIPT_DIR/tradingbot-backtest.lock"
exec 200>"$LOCKFILE" || exit 1
flock -n 200 || exit 0

# CLI flags
OUTPUT_JSON=0
DRY_RUN=0
USAGE="Usage: $0 [--output-json|-j] [--dry-run|-n]"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output-json|-j)
      OUTPUT_JSON=1; shift ;;
    --dry-run|-n)
      DRY_RUN=1; shift ;;
    -h|--help)
      echo "$USAGE"; exit 0 ;;
    *)
      echo "Unknown argument: $1"; echo "$USAGE"; exit 1 ;;
  esac
done

# 3. Detect the Python interpreter automatically (.venv or venv)
if [ -f "$SCRIPT_DIR/.venv/bin/python" ]; then
    PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
elif [ -f "$SCRIPT_DIR/venv/bin/python" ]; then
    PYTHON_BIN="$SCRIPT_DIR/venv/bin/python"
else
    # Fallback to system python if no local venv folder is found
    PYTHON_BIN="python"
fi

TIMESTAMP=$(date +%Y%m%d_%H%M)
OUT="$SCRIPT_DIR/reports/backtest_${TIMESTAMP}.txt"
JSON_OUT="$SCRIPT_DIR/reports/backtest_${TIMESTAMP}.json"
JSONL_FILE="$SCRIPT_DIR/reports/backtest_results.jsonl"

mkdir -p "$(dirname "$OUT")"
mkdir -p "$(dirname "$JSON_OUT")"

echo "Backtest run at $(date -u +'%Y-%m-%dT%H:%M:%SZ')" > "$OUT"

export BACKTEST_DRY_RUN="$DRY_RUN"

# 4. Run Backtester using the dynamically discovered Python path and current directory
"$PYTHON_BIN" - <<'PY' >> "$OUT" 2>&1
import sys, toml, os
# Dynamically insert the current script directory into Python path
current_dir = os.getcwd()
sys.path.insert(0, current_dir)

from trading_bot import Backtester
from kraken_interface import KrakenAPI

cfg = toml.load(os.path.join(current_dir, 'config.toml'))
bt = Backtester(KrakenAPI('',''), cfg)
bt.run()
PY

chmod 644 "$OUT"

# Optional: produce machine-readable JSON + append to a cumulative JSONL
if [ "$OUTPUT_JSON" -eq 1 ]; then
  export REPORT_TXT="$OUT"
  "$PYTHON_BIN" - <<'PY' > "$JSON_OUT"
import os, re, json
path = os.environ.get('REPORT_TXT')
if not path:
    raise SystemExit(1)
with open(path, 'r') as f:
    txt = f.read()
d = {}
m = re.search(r"Total Return:\s*([0-9.+-]+)%", txt)
if m: d['total_return_pct'] = float(m.group(1))
m = re.search(r"Sharpe Ratio:\s*([0-9.+-]+)", txt)
if m: d['sharpe'] = float(m.group(1))
m = re.search(r"Sortino Ratio:\s*([0-9.+-]+)", txt)
if m: d['sortino'] = float(m.group(1))
m = re.search(r"Max Drawdown:\s*([0-9.+-]+)%", txt)
if m: d['max_drawdown_pct'] = float(m.group(1))
m = re.search(r"Total Trades:\s*([0-9]+)", txt)
if m: d['total_trades'] = int(m.group(1))
m = re.search(r"Win Rate:\s*([0-9.+-]+)%", txt)
if m: d['win_rate_pct'] = float(m.group(1))

from datetime import datetime
d['timestamp'] = datetime.utcnow().isoformat() + 'Z'
d['report_txt'] = path
print(json.dumps(d))
PY
  mkdir -p "$(dirname "$JSONL_FILE")"
  echo "$(cat "$JSON_OUT")" >> "$JSONL_FILE"
  chmod 644 "$JSON_OUT"
  chmod 644 "$JSONL_FILE"
fi

exit 0
