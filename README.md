# 🤖 TradingBotUSD

An automated, signal-driven algorithmic spot trading bot for [Kraken](https://www.kraken.com) — completely overhauled for **USD pairs**, designed to be lean, transparent, and safe to run with real money.

This repository is an optimized fork heavily adapted from the core multi-pair framework originally built by **Felix Helleckes** (`Felix-Helleckes/TradingBot:main`). Major props to Felix for the rock-solid base architecture!

> ⚠️ **This bot executes real trades.** Always start with a small amount, monitor logs closely, and utilize sandbox keys for initial testing. Never risk more than you can afford to lose.

---

## ✨ System Features

- **Multi-Pair Fiat Alignment:** Fully calibrated to trade major liquidity pairs against the US Dollar—tracking **XBTUSD, ETHUSD, SOLUSD, and XRPUSD** natively.
- **Dual Signal Engine:** Mean-reversion parsing (via Relative Strength Index) paired with trend breakout indicators (via Bollinger Bands).
- **Hardware Protection:** Time-throttled buffer flushes to disk every 5 minutes (`_save_interval_sec = 300.0`) using atomic file replacements to protect SD cards from excessive wear and sudden corruption.
- **Hardened API Data Isolation:** Configured with robust environment variables. Sensitive API keys and secret signatures remain strictly localized on your hardware.
- **Zero-Stall Account Reconciliation:** Reconstructs live crypto holdings, weighted average entry costs, and realized P&L directly from historical logs, ensuring data state survives a bot restart.
- **Layered Risk Management:** Includes an Average True Range (ATR) dynamic trailing stop, break-even protection adjustments, hard stop-losses, portfolio drawdown controls, trailing stops, trading hour windows, and asset-specific volume filters.
- **Adaptive Market Positioning:** Dynamically switches between `RISK_ON` and `RISK_OFF` macro regimes based on composite Bitcoin benchmark metrics, automatically adjusting allocation sizing on the fly.
- **Soft Momentum Filtering:** Employs advanced MACD line and histogram calculations to score momentum velocity, preventing the bot from buying into a rapid market crash.
- **Anchored Terminal Dashboard (TUI):** Overhauled multi-line terminal user interface that uses clear screen horizons (`\033[5A\r`) to overwrite metrics cleanly in place while letting system log events stream overhead naturally.

---

## 🚀 Quick Start

### 1. Clone and Install Dependencies
```bash
git clone [https://github.com/rulerofoz/TradingBotUSD.git](https://github.com/rulerofoz/TradingBotUSD.git)
cd TradingBotUSD
python -m venv .venv
source .venv/bin/activate  # On Windows use: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Set Up Private API Keys
Create a `.env` file in the root project folder to hold your Kraken API sandbox or live credentials securely:
```text
KRAKEN_API_KEY="your_api_public_key_here"
KRAKEN_API_SECRET="your_api_private_secret_here"
```
> 🔒 **Security Best Practice:** Ensure your Kraken API keys are explicitly restricted to **Trade** and **Query** permissions only. Never enable withdrawal access.

### 3. Configure Currency Run Parameters
Open `config.toml` to adjust your baseline tracking capital and target metrics to match the USD engine configuration:
```toml
[bot_settings]
base_currency = "USD"
trade_pairs = ["XBTUSD", "ETHUSD", "SOLUSD", "XRPUSD"]

[bot_settings.trade_amounts]
trade_amount_usd = 20.0       # Base USD allocated per trade order
target_balance_usd = 250.0    # Automated bot milestone profit target halt
```

### 4. Run the Trading Loop
```bash
python main.py --paper
```

---

## 🛡️ Operational Risk Controls

| Parameter Controls | System Default Setup | Functional Description |
|---|---|---|
| **Take-Profit Target** | 5.0% + fee margins | Minimum performance gain threshold required before closing longs. |
| **Hard Stop-Loss** | 2.0% | Absolute maximum downside protection limit permitted per position. |
| **Trailing Stop** | 1.5× ATR | Dynamic volatility tracker that ratchets upward exclusively as price climbs. |
| **Break-Even Stop** | Enabled (1.5% trigger) | Instantly moves your stop-loss level to entry price upon a 1.5% position gain. |
| **Portfolio Drawdown** | 10.0% circuit breaker | Total valuation monitor (Cash USD + Open Positions) that pauses buys on large drops. |
| **Loss Streak Guard** | 3 consecutive losses | Temporarily enforces an absolute cooling-off period to prevent over-trading wiggles. |
| **Bear Shield** | Enabled | Automatically liquidates open assets into cash USD if the BTC macro index breaks. |

---

## 📊 Live Terminal Dashboard

The bot paints a real-time, high-visibility, color-coded visual grid that locks cleanly at the bottom of your screen:

```text
=====================================================================================
 [Loop Tick #46]  Market Status: RISK_OFF/ACTIVE  |  Active Signals: BTC:BUY ETH:HOLD
 Balance: $225.04  (Started: $225.04)  |  Net Capital Flow: +$0.00/-$0.00
 Performance: Adj P&L: +$0.00  |  Total Profit: +$0.00  |  Executed Trades: 7
 Best Market Target: XBTUSD (BUY)
=====================================================================================
```

- **`Loop Tick`**: The live execution counter tracking stable data polling cycles without hyper-looping.
- **`Market Status & Signals`**: Active view of the systemic risk tier (`RISK_ON` vs `RISK_OFF`) beside exact asset direction vectors.
- **`Balance Ledger Matrix`**: Live liquidity trackers matching your current holdings against initial deposits and withdrawals.
- **`Best Market Target`**: Isolates the single highest-scoring momentum breakout across all watched assets.

---

## 🏗️ Project Architecture Layout

```text
TradingBotUSD/
├── main.py              # Application bootstrap, instance lockers, configuration loading
├── trading_bot.py       # Core Engine: Sizing logic, entry guards, risk layers, dashboard UI
├── analysis.py          # Math Engine: Indicators calculation (RSI, Bollinger Bands, MACD)
├── kraken_interface.py  # Network Layer: API handlers, rate limit backoffs, ledger short-circuits
├── utils.py             # Config processing, validation formatting utilities
├── config.toml          # Single source of truth settings file (pairs, sizing, metrics)
├── logs/                # Rotating runtime bot activity documentation files
├── reports/             # Structured historical CSV trade logs and journals
└── data/                # Persistent P&L initialization boundaries and cooldown cache mappings
```

---

## ⚖️ Disclaimer

This software is developed strictly for educational and empirical purposes. Trading cryptocurrencies involves substantial exposure to financial market volatility. Past simulated backtesting performance is never a definitive guarantee of live future returns. The authors assume no liability for real-world trading results or financial capital losses incurred during utilization.
