# 🤖 TradingBotUSD

An automated, signal-driven algorithmic spot trading bot for [Kraken](https://www.kraken.com) — completely overhauled for **USD pairs**, designed to be lean, transparent, and safe to run with real money.

> ⚠️ **This bot executes real trades.** Always start with a small amount, monitor logs closely, and utilize sandbox keys for initial testing. Never risk more than you can afford to lose.

---

## ✨ System Features

- **Multi-Pair Fiat Alignment:** Fully calibrated to trade major liquidity pairs against the US Dollar—tracking **XBTUSD, ETHUSD, SOLUSD, and XRPUSD** natively.
- **Dual Signal Engine:** Mean-reversion parsing (via Relative Strength Index) paired with trend breakout indicators (via Bollinger Bands).
- **Hardened API Data Isolation:** Configured with robust environment variables. Sensitive API keys and secret signatures remain strictly localized on your hardware and are completely shielded from the web.
- **Zero-Stall Account Reconciliation:** Reconstructs live crypto holdings, weighted average entry costs, and realized P&L directly from historical logs, ensuring data state survives a bot restart.
- **Surgical Private API Bypass:** Built-in short-circuit framework overrides restricted endpoints (like Ledger history queries) to prevent crashing, save API request bandwidth, and ensure error-free loops.
- **Layered Risk Management:** Includes an Average True Range (ATR) dynamic trailing stop, break-even protection adjustments, hard stop-losses, trading hour windows, and asset-specific volume filters.
- **Adaptive Market Positioning:** Dynamically switches between `RISK_ON` and `RISK_OFF` macro regimes based on composite Bitcoin benchmark metrics, automatically cutting allocation sizing by 50% during down-trending markets.
- **Sandbox Optimization:** Includes a temporary `return {}` short-circuit on the ledger query interface to prevent endpoint permission errors during sandbox key validation.
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
python main.py
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

## 📊 Live Console Telemetry

The runtime loops display a clean, single-line terminal readout updating every 60-second cycle:

```text
[42] BTC:HOLD ETH:HOLD SOL:BUY XRP:HOLD | RISK_ON/ACTIVE | Best: ETHUSD (HOLD) | Bal: $219.11 | Start: $219.11 | NetCF: +$0.00/-$0.00 | AdjPnL: $+0.00 | TotalPnL: $+0.00 | Trades: 6
```

- **`RISK_ON / RISK_OFF`**: Displays the active macro regime processing live volatility scaling.
- **`Bal / Start`**: Real-time evaluation of your spendable fiat liquidity directly from the exchange.
- **`AdjPnL / TotalPnL`**: Persistent cumulative dollar tracking across separate application sessions.

---

## 🏗️ Project Architecture Layout

```text
TradingBotUSD/
├── main.py              # Application bootstrap, instance lockers, configuration loading
├── trading_bot.py       # Core Engine: Sizing logic, entry guards, risk layers, order routing
├── analysis.py          # Math Engine: Indicators calculation (RSI, SMA, Bollinger Bands)
├── kraken_interface.py  # Network Layer: API handlers, rate limit backoffs, ledger short-circuits
├── utils.py             # Config processing, validation formatting utilities
├── .gitignore           # Hardened repository security rules (shields .env and local caches)
├── config.toml          # Single source of truth settings file (pairs, sizing, metrics)
├── logs/                # Rotating runtime bot activity documentation files
├── reports/             # Structured historical CSV trade logs and journals
└── data/                # Persistent P&L initialization boundaries and cooldown cache mappings
```

---

## ⚖️ Disclaimer

This software is developed strictly for educational and empirical purposes. Trading cryptocurrencies involves substantial exposure to financial market volatility. Past simulated backtesting performance is never a definitive guarantee of live future returns. The authors assume no liability for real-world trading results or financial capital losses incurred during utilization.
