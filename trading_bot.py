# Trading Bot Core Logic - Multi-Pair Analysis
"""
Kraken Trading Bot — Core Engine
This module is the heart of the trading bot.  It contains the ``TradingBot``
class that orchestrates the full trading lifecycle, plus a minimal ``Backtester``
helper for offline strategy validation.
"""

import json
import logging
import time
import os
from datetime import datetime, timezone
from pathlib import Path
from analysis import TechnicalAnalysis
from utils import load_config, pct_to_frac, apply_trade_costs, append_jsonl_locked, last_closed_trade_net_profit_pct

# Load .env if python-dotenv is available (graceful fallback otherwise)
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    _env_path = Path(__file__).parent / ".env"
    if _env_path.exists():
        for _line in _env_path.read_text().splitlines():
            if "=" in _line and not _line.startswith("#"):
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip())

from core import notifier as _notifier
try:
    from core.ws_feed import KrakenWSFeed as _KrakenWSFeed
    _WS_FEED_AVAILABLE = True
except ImportError:
    _WS_FEED_AVAILABLE = False

# NAS root — read from config [paths] nas_root, fallback to default mount point
def _resolve_nas_root(config: dict) -> Path:
    return Path(config.get('paths', {}).get('nas_root', '/mnt/fritz_nas/Volume/kraken'))
_TRADE_HISTORY_REFRESH_INTERVAL = 600  # seconds between Kraken API fetches (10 min)


def _sd_notify_watchdog() -> None:
    """Send WATCHDOG=1 ping to systemd via the NOTIFY_SOCKET."""
    import socket
    sock_path = os.environ.get("NOTIFY_SOCKET")
    if not sock_path:
        return
    try:
        addr = "\0" + sock_path[1:] if sock_path.startswith("@") else sock_path
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.sendto(b"WATCHDOG=1", addr)
    except Exception:
        pass


class TradingBot:
    def __init__(self, api_client, config):
        self.api_client = api_client
        self.config = config
        self.config_path = os.path.join(os.path.dirname(__file__), 'config.toml')
        self.logger = logging.getLogger(__name__)
        self.nas_root = _resolve_nas_root(config)

        self.analysis_tool = TechnicalAnalysis(rsi_period=14, sma_short=20, sma_long=50)

        # Dynamic fiat currency extraction
        self.base_currency = str(self.config.get('bot_settings', {}).get('base_currency', 'USD')).upper()
        # Kraken tracks standard USD under the 'ZUSD' legacy system ledger key
        self.kraken_fiat_key = 'ZUSD' if self.base_currency == 'USD' else 'ZEUR'

        # Signal engine mode: mean-reversion (reversion_bias) and/or trend/breakout (BB)
        self.enable_mr_signals = bool(self.config.get('risk_management', {}).get('enable_mean_reversion_signals', True))
        self.enable_trend_signals = bool(self.config.get('risk_management', {}).get('enable_trend_breakout_signals', True))
        self.mr_rsi_oversold = float(self.config.get('risk_management', {}).get('mr_rsi_oversold_threshold', 33.0))
        self.mr_rsi_overbought = float(self.config.get('risk_management', {}).get('mr_rsi_overbought_threshold', 67.0))
        self.analysis_tool.enable_mr_signals = self.enable_mr_signals
        self.analysis_tool.enable_trend_signals = self.enable_trend_signals
        self.analysis_tool.mr_rsi_buy = self.mr_rsi_oversold
        self.analysis_tool.mr_rsi_sell = self.mr_rsi_overbought

        self.trade_pairs = self.config['bot_settings'].get('trade_pairs', ['XBTUSD'])
        self.pair_signals = {}
        self.pair_prices = {}
        self.pair_scores = {}
        self.holdings = {}
        self.purchase_prices = {}
        self.peak_prices = {}
        self.position_qty = {}
        self.short_qty = {}
        self.short_entry_prices = {}
        self.realized_pnl = {}
        self.fees_paid = {}
        self.trade_metrics = {}
        self.closed_trade_pnls = []
        self.last_trade_at = {}
        self.entry_timestamps = {}
        self.last_global_trade_at = 0
        self._normalized_pair_logs_seen = set()
        self._last_empty_sell_log_at = {}
        self._load_cooldown_state()

        self.trade_count = 0
        self.consecutive_losses = 0
        self.trading_paused_until_ts = 0
        self.target_balance_fiat = self._get_target_balance()
        # stop info per pair (stop_price, type)
        self.stop_info = {}
        # journaling path
        self.journal_path = os.path.join(os.path.dirname(__file__), 'reports', 'trade_journal.csv')
        # structured JSONL trade log for observability
        self.json_journal_path = os.path.join(os.path.dirname(__file__), 'logs', 'trade_events.jsonl')
        os.makedirs(os.path.dirname(self.json_journal_path), exist_ok=True)
        # manual kill-switch file: if present, bot will pause buys
        self.kill_switch_path = os.path.join(os.path.dirname(__file__), 'PAUSE')
        self.take_profit_percent = self._get_take_profit_percent()
        self.stop_loss_percent = self._get_stop_loss_percent()
        self.max_open_positions = int(self.config.get('risk_management', {}).get('max_open_positions', 3))
        self.trade_cooldown_sec = int(self.config.get('risk_management', {}).get('trade_cooldown_seconds', 180))
        self.global_trade_cooldown_sec = int(self.config.get('risk_management', {}).get('global_trade_cooldown_seconds', 300))
        self.trailing_stop_percent = float(self.config.get('risk_management', {}).get('trailing_stop_percent', 1.5))
        self.min_buy_score = float(self.config.get('risk_management', {}).get('min_buy_score', 18.0))
        self.adaptive_tp_enabled = bool(self.config.get('risk_management', {}).get('adaptive_take_profit', True))
        self.max_tp_percent = float(self.config.get('risk_management', {}).get('max_take_profit_percent', 14.0))
        self.sell_fee_buffer_percent = float(self.config.get('risk_management', {}).get('sell_fee_buffer_percent', 0.0))

        self.fees_maker_percent = float(self.config.get('risk_management', {}).get('fees_maker_percent', 0.16))
        self.fees_taker_percent = float(self.config.get('risk_management', {}).get('fees_taker_percent', 0.26))
        try:
            self.fees_maker_frac = pct_to_frac(self.fees_maker_percent)
            self.fees_taker_frac = pct_to_frac(self.fees_taker_percent)
        except Exception:
            self.fees_maker_frac = 0.0
            self.fees_taker_frac = 0.0

        self.reentry_guard_pairs = [p.upper() for p in self.config.get('risk_management', {}).get('reentry_guard_pairs', ['VER'])]
        self.min_reentry_profit_pct = float(self.config.get('risk_management', {}).get('min_reentry_profit_pct', 5.0))
        self.min_net_sell_profit_pct = float(self.config.get('risk_management', {}).get('min_net_sell_profit_pct', 0.0))
        self.empty_sell_log_cooldown_sec = int(self.config.get('risk_management', {}).get('empty_sell_log_cooldown_seconds', 1800))

        # ATR stop config
        self.enable_atr_stop = bool(self.config.get('risk_management', {}).get('enable_atr_stop', False))
        self.atr_period = int(self.config.get('risk_management', {}).get('atr_period', 14))
        self.atr_multiplier = float(self.config.get('risk_management', {}).get('atr_multiplier', 1.5))
        self.atr_trail_multiplier = float(self.config.get('risk_management', {}).get('atr_trail_multiplier', 0.75))
        self.enable_atr_dynamic_tp = bool(self.config.get('risk_management', {}).get('enable_atr_dynamic_tp', False))
        self.atr_tp_multiplier = float(self.config.get('risk_management', {}).get('atr_tp_multiplier', 2.0))

        # Signal refresh and regime cache
        self.signal_refresh_interval = int(self.config.get('execution', {}).get('signal_refresh_interval_seconds', 300))
        self._last_signal_refresh_ts = 0
        self._regime_cache_ttl = int(self.config.get('execution', {}).get('regime_cache_ttl_seconds', 300))
        self._regime_cache = {'ts': 0, 'risk_on': True}

        self.enable_break_even = bool(self.config.get('risk_management', {}).get('enable_break_even', True))
        self.break_even_trigger_pct = float(self.config.get('risk_management', {}).get('break_even_trigger_percent', 1.5))
        self.enable_pyramiding = bool(self.config.get('risk_management', {}).get('enable_pyramiding', False))
        self.pyramiding_add_pct = float(self.config.get('risk_management', {}).get('pyramiding_add_pct', 0.5))
        self.enable_regime_filter = bool(self.config.get('risk_management', {}).get('enable_regime_filter', True))
        self.regime_benchmark_pair = str(self.config.get('risk_management', {}).get('regime_benchmark_pair', 'XXBTZUSD')).upper()
        self.regime_min_score = float(self.config.get('risk_management', {}).get('regime_min_score', -5.0))
        self.enable_hard_stop_loss = bool(self.config.get('risk_management', {}).get('enable_hard_stop_loss', True))
        self.hard_stop_loss_percent = float(self.config.get('risk_management', {}).get('hard_stop_loss_percent', 4.0))
        self.enable_mtf_regime_scoring = bool(self.config.get('risk_management', {}).get('enable_mtf_regime_scoring', True))
        self.mtf_regime_min_score = float(self.config.get('risk_management', {}).get('mtf_regime_min_score', -2.0))
        self.enable_time_stop = bool(self.config.get('risk_management', {}).get('enable_time_stop', True))
        self.time_stop_hours = int(self.config.get('risk_management', {}).get('time_stop_hours', 72))
        self.enable_daily_drawdown = bool(self.config.get('risk_management', {}).get('enable_daily_drawdown', True))
        self.daily_drawdown_percent = float(self.config.get('risk_management', {}).get('daily_loss_limit_percent', 3.0))
        self.risk_off_allocation_multiplier = float(self.config.get('risk_management', {}).get('risk_off_allocation_multiplier', 0.35))
        self.enable_volatility_targeting = bool(self.config.get('risk_management', {}).get('enable_volatility_targeting', True))
        self.target_volatility_pct = float(self.config.get('risk_management', {}).get('target_volatility_pct', 1.6))
        self.max_consecutive_losses = int(self.config.get('risk_management', {}).get('max_consecutive_losses', 3))
        self.pause_after_loss_streak_minutes = int(self.config.get('risk_management', {}).get('pause_after_loss_streak_minutes', 180))

        self.enable_live_shorts = bool(self.config.get('shorting', {}).get('enabled', False))
        self.short_leverage = str(self.config.get('shorting', {}).get('leverage', '2'))
        self.max_short_notional_fiat = float(self.config.get('shorting', {}).get(f'max_short_notional_{self.base_currency.lower()}', 50.0))
        self.short_take_profit_percent = float(self.config.get('shorting', {}).get('short_take_profit_percent', 2.5))
        self.short_stop_loss_percent = float(self.config.get('shorting', {}).get('short_stop_loss_percent', 3.0))

        # Fast scalp profile
        self.enable_fast_scalp = bool(self.config.get('profiles', {}).get('fast_scalp', {}).get('enabled', False))
        self.fast_scalp_require_flag = bool(self.config.get('profiles', {}).get('fast_scalp', {}).get('require_enable_flag', True))
        self.fast_scalp_time_stop_minutes = int(self.config.get('profiles', {}).get('fast_scalp', {}).get('time_stop_minutes', 30))
        self.fast_scalp_stop_loss_pct = float(self.config.get('profiles', {}).get('fast_scalp', {}).get('stop_loss_percent', 0.6))
        self.fast_scalp_take_profit_pct = float(self.config.get('profiles', {}).get('fast_scalp', {}).get('take_profit_percent', 1.2))

        # Core Intercept Override: Force base variables to scale if fast scalp profile is true
        if self.enable_fast_scalp:
            self.take_profit_percent = self.fast_scalp_take_profit_pct
            self.hard_stop_loss_percent = self.fast_scalp_stop_loss_pct
            self.stop_loss_percent = self.fast_scalp_stop_loss_pct

        self.start_time = datetime.now()
        self.last_config_reload = datetime.now()
        self.config_reload_interval = 300
        self.loop_interval_sec = int(self.config.get('bot_settings', {}).get('loop_interval_seconds', 60))
        self.daily_start_balance = None
        self.initial_balance_fiat = None
        self.start_timestamp = int(time.time())
        self.net_deposits_fiat = 0.0
        self.net_withdrawals_fiat = 0.0
        self._last_cashflow_refresh_ts = 0
        self.cashflow_refresh_interval_sec = int(self.config.get('reporting', {}).get('cashflow_refresh_seconds', 600))
        self.last_daily_reset_ts = int(time.time())

        self._ema_bullish = {}
        self._macd_1h_hist = {}
        self._macd_15m_hist = {}
        self._macd_15m_hist_prev = {}
        self._partial_exit_done = {}

        self.valid_pairs = self._fetch_valid_trade_pairs(self.trade_pairs)
        self.trade_pairs = self.valid_pairs if self.valid_pairs else []
        self._init_pair_state(self.trade_pairs)

        self.price_history_airbag = {p: [] for p in self.trade_pairs}
        self.airbag_drop_threshold = float(self.config.get('risk_management', {}).get('airbag_drop_threshold', 15.0))
        self.airbag_window_minutes = int(self.config.get('risk_management', {}).get('airbag_window_minutes', 10))

        self.enable_sentiment_guard = bool(self.config.get('risk_management', {}).get('enable_sentiment_guard', False))
        self.news_marquee_path = "/tmp/youtube_stream/news_marquee.txt"
        self.sentiment_pause_keywords = ["crash", "hack", "dump", "sec", "lawsuit", "regulation", "ban"]
        self.sentiment_active = False

        self.enable_trading_hours = bool(self.config.get('risk_management', {}).get('enable_trading_hours', True))
        self.trading_hours_start_utc = int(self.config.get('risk_management', {}).get('trading_hours_start_utc', 14))
        self.trading_hours_end_utc = int(self.config.get('risk_management', {}).get('trading_hours_end_utc', 22))

        self.enable_volume_filter = bool(self.config.get('risk_management', {}).get('enable_volume_filter', True))
        self.volume_filter_min_ratio = float(self.config.get('risk_management', {}).get('volume_filter_min_ratio', 0.5))
        self._volume_cache = {}

        bear_cfg = self.config.get('bear_shield', {})
        self.enable_bear_shield = bool(bear_cfg.get('enable_bear_shield', False))
        self.bear_ema_period = int(bear_cfg.get('bear_ema_period', 50))
        self.bear_confirm_candles = int(bear_cfg.get('bear_confirm_candles', 3))
        self.bear_benchmark_pair = str(bear_cfg.get('bear_benchmark_pair', 'XXBTZUSD')).upper()
        self.bear_log_interval_minutes = int(bear_cfg.get('bear_log_interval_minutes', 60))
        self._bear_mode_active = False
        self._bear_last_log_ts = 0

        self._trade_history_cache = {}
        self._trade_history_last_fetch = 0.0

        tech_cfg = self.config.get('technical', {})
        self.enable_ema_crossover_filter = bool(tech_cfg.get('enable_ema_crossover_filter', True))
        self.ema_fast_period = int(tech_cfg.get('ema_fast_period', 9))
        self.ema_slow_period = int(tech_cfg.get('ema_slow_period', 21))
        self.enable_mtf_macd_filter = bool(tech_cfg.get('enable_mtf_macd_filter', True))

        self.enable_partial_exit = bool(tech_cfg.get('enable_partial_exit', True))
        self.partial_exit_trigger_pct = float(tech_cfg.get('partial_exit_trigger_pct', 4.0))
        self.partial_exit_fraction = float(tech_cfg.get('partial_exit_fraction', 0.5))
        self.partial_exit_min_remaining_fiat = float(tech_cfg.get(f'partial_exit_min_remaining_{self.base_currency.lower()}', 5.0))

        self.ws_feed = None
        ws_cfg = self.config.get('websocket', {})
        if bool(ws_cfg.get('enable_ws_feed', True)) and _WS_FEED_AVAILABLE:
            try:
                self.ws_feed = _KrakenWSFeed(self.trade_pairs)
                self.ws_feed.start()
            except Exception as _e:
                self.logger.warning(f"WebSocket feed could not start: {_e} — falling back to REST polling")

    def _notify_pause(self, reason):
        try:
            import json, subprocess, datetime, os
            logp = os.path.join(os.path.dirname(__file__), 'logs', 'pause_events.log')
            os.makedirs(os.path.dirname(logp), exist_ok=True)
            entry = {
                'ts': datetime.datetime.utcnow().isoformat(),
                'reason': reason,
                'balance': float(self.get_fiat_balance()),
                'consecutive_losses': int(getattr(self,'consecutive_losses',0))
            }
            with open(logp,'a') as f:
                f.write(json.dumps(entry) + "\n")
            script = os.path.join(os.path.dirname(__file__), 'scripts', 'notify_pause.sh')
            if os.path.exists(script) and os.access(script, os.X_OK):
                try:
                    subprocess.Popen([script, reason], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except Exception as e:
                    self.logger.debug(f"notify_pause: could not run notifier script: {e}")
        except Exception as e:
            self.logger.warning(f"notify_pause: failed to write pause log: {e}")

    def _calc_ema(self, prices, period):
        if len(prices) < period:
            return None
        k = 2.0 / (period + 1)
        ema = prices[0]
        for p in prices[1:]:
            ema = p * k + ema * (1 - k)
        return ema

    def _is_bear_market(self):
        if not self.enable_bear_shield:
            return False
        try:
            ohlc = self.api_client.get_ohlc_data(self.bear_benchmark_pair, interval=240)  # 4h
            if not ohlc:
                return False
            key = [k for k in ohlc.keys() if k != 'last']
            if not key:
                return False
            rows = ohlc[key[0]]
            closes = [float(r[4]) for r in rows if r and len(r) >= 5]
            if len(closes) < self.bear_ema_period + self.bear_confirm_candles:
                return False

            ema = self._calc_ema(closes[:-self.bear_confirm_candles], self.bear_ema_period)
            if ema is None:
                return False

            last_n = closes[-self.bear_confirm_candles:]
            return all(c < ema for c in last_n)
        except Exception as e:
            self.logger.debug(f"Bear shield check failed (safe fallback to False): {e}")
            return False

    def _bear_shield_exit_all(self):
        sold_any = False
        for pair in list(self.trade_pairs):
            qty = self.holdings.get(pair, 0.0)
            min_vol = self._get_min_volume(pair)
            if qty >= min_vol:
                price = self.pair_prices.get(pair, 0.0)
                if price > 0:
                    self.logger.warning(
                        f"BEAR SHIELD: selling {qty:.6f} {pair} @ {price:.4f} {self.base_currency} to park in FIAT"
                    )
                    self.execute_sell_order(pair, price)
                    sold_any = True
        return sold_any

    def _update_airbag_history(self, pair, price):
        now = time.time()
        history = self.price_history_airbag.get(pair, [])
        history.append((now, price))
        cutoff = now - (self.airbag_window_minutes * 60)
        self.price_history_airbag[pair] = [h for h in history if h[0] >= cutoff]

    def _check_airbag_trigger(self, pair):
        history = self.price_history_airbag.get(pair, [])
        if len(history) < 2:
            return False
        peak_price = max(h[1] for h in history)
        current_price = history[-1][1]
        drop = ((peak_price - current_price) / peak_price) * 100.0
        if drop >= self.airbag_drop_threshold:
            self.logger.critical(f"AIRBAG TRIGGERED for {pair}: drop of {drop:.2f}% in {self.airbag_window_minutes}m")
            return True
        return False

    def _scan_news_sentiment(self):
        try:
            if not os.path.exists(self.news_marquee_path):
                return False
            import re, fcntl
            with open(self.news_marquee_path, 'r') as f:
                try:
                    fcntl.flock(f.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
                    content = f.read().lower()
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                except (OSError, BlockingIOError):
                    return self.sentiment_active
            found = [k for k in self.sentiment_pause_keywords if re.search(r'\b' + re.escape(k) + r'\b', content)]
            if found:
                if not self.sentiment_active:
                    self.logger.warning(f"SENTIMENT GUARD: Keywords found in news ({', '.join(found)}). Pausing Buys.")
                return True
            return False
        except Exception:
            return False

    def _init_pair_state(self, pairs):
        for pair in pairs:
            self.pair_signals.setdefault(pair, "HOLD")
            self.holdings.setdefault(pair, 0.0)
            self.purchase_prices.setdefault(pair, 0.0)
            self.peak_prices.setdefault(pair, 0.0)
            self.position_qty.setdefault(pair, 0.0)
            self.short_qty.setdefault(pair, 0.0)
            self.short_entry_prices.setdefault(pair, 0.0)
            self.realized_pnl.setdefault(pair, 0.0)
            self.fees_paid.setdefault(pair, 0.0)
            self.trade_metrics.setdefault(pair, {"closed": 0, "wins": 0, "losses": 0, "sum_pnl": 0.0})
            self.last_trade_at.setdefault(pair, 0)
            self.entry_timestamps.setdefault(pair, None)
            self._ema_bullish.setdefault(pair, None)
            self._macd_1h_hist.setdefault(pair, None)
            self._macd_15m_hist.setdefault(pair, None)
            self._macd_15m_hist_prev.setdefault(pair, None)
            self._partial_exit_done.setdefault(pair, False)

    def _get_target_balance(self):
        try:
            return self.config['bot_settings']['trade_amounts'].get(f'target_balance_{self.base_currency.lower()}', 1000.0)
        except Exception:
            return self.config['bot_settings'].get(f'target_balance_{self.base_currency.lower()}', 1000.0)

    def _get_take_profit_percent(self):
        try:
            return float(self.config['risk_management'].get('take_profit_percent', 5.0))
        except Exception:
            return 5.0

    def _get_stop_loss_percent(self):
        try:
            return float(self.config['risk_management'].get('stop_loss_percent', 2.0))
        except Exception:
            return 2.0

    def _get_trade_amount_fiat(self):
        try:
            return float(self.config['bot_settings']['trade_amounts'].get(f'trade_amount_{self.base_currency.lower()}', 30.0))
        except Exception:
            return 30.0

    def _get_dynamic_trade_amount_fiat(self, pair, available_fiat):
        base_amount = self._get_trade_amount_fiat()
        allocation_pct = float(self.config.get('risk_management', {}).get('allocation_per_trade_percent', 10.0))
        amount = available_fiat * (allocation_pct / 100.0)

        small_account_floor = float(self.config.get('risk_management', {}).get(f'small_account_fixed_trade_{self.base_currency.lower()}', 25.0))
        small_account_threshold = float(self.config.get('risk_management', {}).get(f'small_account_threshold_{self.base_currency.lower()}', 200.0))
        if available_fiat <= small_account_threshold:
            amount = min(amount, small_account_floor)

        # Optimization Integration: Volatility Targeting Sizing Engine
        atr = self._compute_atr(pair)
        current_price = self.pair_prices.get(pair, 0)

        if atr and current_price > 0:
            volatility_ratio = (atr / current_price) * 100.0
            target_vol_pct = float(self.config.get('risk_management', {}).get('target_volatility_pct', 1.6))
            vol_multiplier = (target_vol_pct / max(0.1, volatility_ratio))
            vol_multiplier = max(0.3, min(3.0, vol_multiplier))
            amount *= vol_multiplier
        else:
            # Timing Protection Fallback: Ensures your trade amounts never break
            # if the ATR indicator buffer is still warming up.
            amount *= 1.0

        amount *= self._allocation_multiplier()

        # Safe Minimum Floor: Ensures planned_fiat never drops below your auto sizing minimums
        min_auto_notional = float(self.config.get('risk_management', {}).get('min_auto_scale_notional', 1.0))
        final_amount = min(base_amount * 1.5, amount, available_fiat * 0.95)
        return max(min_auto_notional + 0.1, final_amount)

    def _is_mtf_trend_bullish(self, pair):
        try:
            now = time.time()
            if (now - self._regime_cache.get('ts', 0)) <= self._regime_cache_ttl:
                returns = self.analysis_tool.pair_price_history.get(pair)
                if returns:
                    closes = list(returns)
                    return self.analysis_tool.check_mtf_trend(closes)

            ohlc = self.api_client.get_ohlc_data(pair, interval=60) # 1h
            if not ohlc:
                return False
            data_key = next((k for k in ohlc if k != 'last'), None)
            if not data_key:
                return False
            closes = [float(row[4]) for row in ohlc[data_key]]
            return self.analysis_tool.check_mtf_trend(closes)
        except Exception as e:
            self.logger.error(f"MTF check failed for {pair}: {e}")
            return False

    def _get_min_volume(self, pair):
        try:
            min_volumes = self.config['bot_settings'].get('min_volumes', {})
            if pair in min_volumes:
                return float(min_volumes.get(pair, 0.0001))

            aliases = {
                'XBTUSD': 'XXBTZUSD', 'ETHUSD': 'XETHZUSD', 'XRPUSD': 'XXRPZUSD',
                'XXBTZUSD': 'XBTUSD', 'XETHZUSD': 'ETHUSD', 'XXRPZUSD': 'XRPUSD',
            }
            alt = aliases.get(pair)
            if alt and alt in min_volumes:
                return float(min_volumes.get(alt, 0.0001))

            return 0.0001
        except Exception:
            return 0.0001

    def _calculate_volume(self, pair, price, available_fiat=None):
        trade_amount_fiat = self._get_trade_amount_fiat()
        if available_fiat is not None:
            trade_amount_fiat = min(trade_amount_fiat, max(0.0, available_fiat))
        min_volume = self._get_min_volume(pair)
        if price <= 0:
            return 0.0
        calculated_volume = trade_amount_fiat / price
        return max(calculated_volume, min_volume)

    def _fetch_valid_trade_pairs(self, requested_pairs):
        assets = self.api_client.get_asset_pairs()
        if not assets:
            self.logger.warning("Could not fetch AssetPairs; using configured pairs unchanged")
            return requested_pairs

        valid_requested = []
        seen = set()

        pair_index = {}
        for key, meta in assets.items():
            alt = (meta.get('altname') or key or '').upper()
            ws = (meta.get('wsname') or '').upper()
            ws_noslash = ws.replace('/', '')
            key_u = (key or '').upper()
            for alias in [alt, ws, ws_noslash, key_u, alt.replace('/', '')]:
                if alias:
                    pair_index[alias] = alt

        for raw_pair in requested_pairs:
            pair = (raw_pair or '').upper()
            normalized = pair_index.get(pair) or pair_index.get(pair.replace('/', ''))
            if normalized:
                if normalized not in seen:
                    valid_requested.append(normalized)
                    seen.add(normalized)
                if pair != normalized:
                    normalization_key = f"{pair}->{normalized}"
                    if normalization_key not in self._normalized_pair_logs_seen:
                        self.logger.info(f"Pair normalized: {pair} -> {normalized}")
                        self._normalized_pair_logs_seen.add(normalization_key)
            else:
                self.logger.warning(f"Skipping unknown Kraken pair: {raw_pair}")
        self.kelly_fraction = self._calculate_kelly_fraction()

        if not valid_requested:
            self.logger.error("No valid trading pairs after Kraken validation")
        else:
            self.logger.info(f"Validated trading pairs: {valid_requested}")
        return valid_requested

    def reload_config(self):
        try:
            new_config = load_config(self.config_path)
            if not new_config:
                return False

            old_pairs = set(self.trade_pairs)
            self.config = new_config

            # Hot-reload currency tokens cleanly
            self.base_currency = str(self.config.get('bot_settings', {}).get('base_currency', 'USD')).upper()
            self.kraken_fiat_key = 'ZUSD' if self.base_currency == 'USD' else 'ZEUR'

            requested = self.config['bot_settings'].get('trade_pairs', ['XBTUSD'])
            self.trade_pairs = self._fetch_valid_trade_pairs(requested)
            new_pairs = set(self.trade_pairs)
            added_pairs = list(new_pairs - old_pairs)
            if added_pairs:
                self._init_pair_state(added_pairs)
            self._sync_account_state()

            self.target_balance_fiat = self._get_target_balance()
            self.take_profit_percent = self._get_take_profit_percent()
            self.stop_loss_percent = self._get_stop_loss_percent()
            self.max_open_positions = int(self.config.get('risk_management', {}).get('max_open_positions', self.max_open_positions))
            self.trade_cooldown_sec = int(self.config.get('risk_management', {}).get('trade_cooldown_seconds', self.trade_cooldown_sec))
            self.global_trade_cooldown_sec = int(self.config.get('risk_management', {}).get('global_trade_cooldown_seconds', self.global_trade_cooldown_sec))
            self.trailing_stop_percent = float(self.config.get('risk_management', {}).get('trailing_stop_percent', self.trailing_stop_percent))
            self.empty_sell_log_cooldown_sec = int(self.config.get('risk_management', {}).get('empty_sell_log_cooldown_seconds', self.empty_sell_log_cooldown_sec))
            self.enable_regime_filter = bool(self.config.get('risk_management', {}).get('enable_regime_filter', self.enable_regime_filter))
            self.regime_benchmark_pair = str(self.config.get('risk_management', {}).get('regime_benchmark_pair', self.regime_benchmark_pair)).upper()
            self.regime_min_score = float(self.config.get('risk_management', {}).get('regime_min_score', self.regime_min_score))
            self.enable_hard_stop_loss = bool(self.config.get('risk_management', {}).get('enable_hard_stop_loss', self.enable_hard_stop_loss))
            self.hard_stop_loss_percent = float(self.config.get('risk_management', {}).get('hard_stop_loss_percent', self.hard_stop_loss_percent))
            self.enable_mtf_regime_scoring = bool(self.config.get('risk_management', {}).get('enable_mtf_regime_scoring', self.enable_mtf_regime_scoring))
            self.mtf_regime_min_score = float(self.config.get('risk_management', {}).get('mtf_regime_min_score', self.mtf_regime_min_score))
            self.enable_time_stop = bool(self.config.get('risk_management', {}).get('enable_time_stop', self.enable_time_stop))
            self.time_stop_hours = int(self.config.get('risk_management', {}).get('time_stop_hours', self.time_stop_hours))
            self.enable_daily_drawdown = bool(self.config.get('risk_management', {}).get('enable_daily_drawdown', self.enable_daily_drawdown))
            self.daily_drawdown_percent = float(self.config.get('risk_management', {}).get('daily_loss_limit_percent', self.daily_drawdown_percent))
            self.risk_off_allocation_multiplier = float(self.config.get('risk_management', {}).get('risk_off_allocation_multiplier', self.risk_off_allocation_multiplier))
            self.enable_volatility_targeting = bool(self.config.get('risk_management', {}).get('enable_volatility_targeting', self.enable_volatility_targeting))
            self.target_volatility_pct = float(self.config.get('risk_management', {}).get('target_volatility_pct', self.target_volatility_pct))
            self.max_consecutive_losses = int(self.config.get('risk_management', {}).get('max_consecutive_losses', self.max_consecutive_losses))
            self.pause_after_loss_streak_minutes = int(self.config.get('risk_management', {}).get('pause_after_loss_streak_minutes', self.pause_after_loss_streak_minutes))
            self.sell_fee_buffer_percent = float(self.config.get('risk_management', {}).get('sell_fee_buffer_percent', self.sell_fee_buffer_percent))
            try:
                self.fees_maker_frac = pct_to_frac(float(self.config.get('risk_management', {}).get('fees_maker_percent', self.fees_maker_percent)))
                self.fees_taker_frac = pct_to_frac(float(self.config.get('risk_management', {}).get('fees_taker_percent', self.fees_taker_percent)))
            except Exception:
                self.fees_maker_frac = getattr(self, 'fees_maker_frac', 0.0)
                self.fees_taker_frac = getattr(self, 'fees_taker_frac', 0.0)
            self.enable_sentiment_guard = bool(self.config.get('risk_management', {}).get('enable_sentiment_guard', self.enable_sentiment_guard))
            self.enable_mr_signals = bool(self.config.get('risk_management', {}).get('enable_mean_reversion_signals', self.enable_mr_signals))
            self.enable_trend_signals = bool(self.config.get('risk_management', {}).get('enable_trend_breakout_signals', self.enable_trend_signals))
            self.mr_rsi_oversold = float(self.config.get('risk_management', {}).get('mr_rsi_oversold_threshold', self.mr_rsi_oversold))
            self.mr_rsi_overbought = float(self.config.get('risk_management', {}).get('mr_rsi_overbought_threshold', self.mr_rsi_overbought))
            self.analysis_tool.enable_mr_signals = self.enable_mr_signals
            self.analysis_tool.enable_trend_signals = self.enable_trend_signals
            self.analysis_tool.mr_rsi_buy = self.mr_rsi_oversold
            self.analysis_tool.mr_rsi_sell = self.mr_rsi_overbought
            self.enable_atr_stop = bool(self.config.get('risk_management', {}).get('enable_atr_stop', self.enable_atr_stop))
            self.atr_period = int(self.config.get('risk_management', {}).get('atr_period', self.atr_period))
            self.atr_multiplier = float(self.config.get('risk_management', {}).get('atr_multiplier', self.atr_multiplier))
            self.atr_trail_multiplier = float(self.config.get('risk_management', {}).get('atr_trail_multiplier', self.atr_trail_multiplier))
            self.enable_atr_dynamic_tp = bool(self.config.get('risk_management', {}).get('enable_atr_dynamic_tp', self.enable_atr_dynamic_tp))
            self.atr_tp_multiplier = float(self.config.get('risk_management', {}).get('atr_tp_multiplier', self.atr_tp_multiplier))
            self.enable_break_even = bool(self.config.get('risk_management', {}).get('enable_break_even', self.enable_break_even))
            self.break_even_trigger_pct = float(self.config.get('risk_management', {}).get('break_even_trigger_percent', self.break_even_trigger_pct))
            self.enable_pyramiding = bool(self.config.get('risk_management', {}).get('enable_pyramiding', self.enable_pyramiding))
            self.pyramiding_add_pct = float(self.config.get('risk_management', {}).get('pyramiding_add_pct', self.pyramiding_add_pct))

            if old_pairs != new_pairs:
                self.logger.info(f"CONFIG RELOAD: trade_pairs changed {sorted(old_pairs)} -> {sorted(new_pairs)}")

            bear_cfg = self.config.get('bear_shield', {})
            self.enable_bear_shield = bool(bear_cfg.get('enable_bear_shield', self.enable_bear_shield))
            self.bear_ema_period = int(bear_cfg.get('bear_ema_period', self.bear_ema_period))
            self.bear_confirm_candles = int(bear_cfg.get('bear_confirm_candles', self.bear_confirm_candles))
            self.bear_benchmark_pair = str(bear_cfg.get('bear_benchmark_pair', self.bear_benchmark_pair)).upper()
            self.bear_log_interval_minutes = int(bear_cfg.get('bear_log_interval_minutes', self.bear_log_interval_minutes))

            tech_cfg = self.config.get('technical', {})
            self.enable_ema_crossover_filter = bool(tech_cfg.get('enable_ema_crossover_filter', self.enable_ema_crossover_filter))
            self.ema_fast_period = int(tech_cfg.get('ema_fast_period', self.ema_fast_period))
            self.ema_slow_period = int(tech_cfg.get('ema_slow_period', self.ema_slow_period))
            self.enable_mtf_macd_filter = bool(tech_cfg.get('enable_mtf_macd_filter', self.enable_mtf_macd_filter))
            self.enable_partial_exit = bool(tech_cfg.get('enable_partial_exit', self.enable_partial_exit))
            self.partial_exit_trigger_pct = float(tech_cfg.get('partial_exit_trigger_pct', self.partial_exit_trigger_pct))
            self.partial_exit_fraction = float(tech_cfg.get('partial_exit_fraction', self.partial_exit_fraction))
            self.partial_exit_min_remaining_fiat = float(tech_cfg.get(f'partial_exit_min_remaining_{self.base_currency.lower()}', self.partial_exit_min_remaining_fiat))

            # Dynamic Config Overrides for profiles
            self.enable_fast_scalp = bool(self.config.get('profiles', {}).get('fast_scalp', {}).get('enabled', False))
            self.fast_scalp_time_stop_minutes = int(self.config.get('profiles', {}).get('fast_scalp', {}).get('time_stop_minutes', 30))
            self.fast_scalp_stop_loss_pct = float(self.config.get('profiles', {}).get('fast_scalp', {}).get('stop_loss_percent', 0.6))
            self.fast_scalp_take_profit_pct = float(self.config.get('profiles', {}).get('fast_scalp', {}).get('take_profit_percent', 1.2))

            self.last_config_reload = datetime.now()
            self.loop_interval_sec = int(self.config.get('bot_settings', {}).get('loop_interval_seconds', self.loop_interval_sec))
            return True
        except Exception as e:
            self.logger.error(f"Error reloading config: {e}")
            return False

    def get_fiat_balance(self):
        """Dynamic lookup using verified Kraken registry key (ZUSD vs ZEUR)"""
        try:
            balance = self.api_client.get_account_balance()
            if balance:
                return float(balance.get(self.kraken_fiat_key, 0))
            return 0.0
        except Exception as e:
            self.logger.error(f"Error getting {self.base_currency} balance: {e}")
            return 0.0

    def get_crypto_holdings(self):
        try:
            balance = self.api_client.get_account_balance()
            if not balance:
                return

            # Maps base ticker definitions dynamically
            pair_to_balance = {
                'XBTUSD': 'XXBT', 'XXBTZUSD': 'XXBT',
                'ETHUSD': 'XETH', 'XETHZUSD': 'XETH',
                'SOLUSD': 'SOL', 'XRPUSD': 'XXRP', 'XXRPZUSD': 'XXRP',
                'XBTEUR': 'XXBT', 'XXBTZEUR': 'XXBT',
                'ETHEUR': 'XETH', 'XETHZEUR': 'XETH',
                'SOLEUR': 'SOL', 'XXRPZEUR': 'XXRP', 'XRPEUR': 'XXRP'
            }
            for pair in self.trade_pairs:
                key = pair_to_balance.get(pair)
                if not key:
                    continue
                self.holdings[pair] = float(balance.get(key, 0))
        except Exception as e:
            self.logger.error(f"Error getting holdings: {e}")

    def _reconcile_open_orders(self):
        try:
            open_orders_result = self.api_client.get_open_orders()
            if not open_orders_result:
                return
            open_map = open_orders_result.get('open', open_orders_result) if isinstance(open_orders_result, dict) else {}
            if not open_map:
                return

            watched = set(self.trade_pairs)
            pair_aliases = {
                'XXBTZUSD': 'XBTUSD', 'XBTUSD': 'XBTUSD',
                'XETHZUSD': 'ETHUSD', 'ETHUSD': 'ETHUSD',
                'SOLUSD': 'SOLUSD', 'XXRPZUSD': 'XRPUSD', 'XRPUSD': 'XRPUSD',
                'XXBTZEUR': 'XBTEUR', 'XBTEUR': 'XBTEUR',
                'XETHZEUR': 'ETHEUR', 'ETHEUR': 'ETHEUR',
            }

            for txid, order in open_map.items():
                raw_pair = str(order.get('descr', {}).get('pair', '') or order.get('pair', '')).upper()
                norm_pair = pair_aliases.get(raw_pair, raw_pair)
                if norm_pair not in watched:
                    continue
                side = str(order.get('descr', {}).get('type', '') or '').lower()
                vol = float(order.get('vol', 0) or 0)
                local_holding = self.holdings.get(norm_pair, 0.0)
                local_short = self.short_qty.get(norm_pair, 0.0)

                if side == 'buy' and local_holding < self._get_min_volume(norm_pair):
                    self.logger.warning(
                        f"RECONCILE: Open BUY order {txid} ({vol:.6f} {norm_pair}) exists on Kraken "
                        f"but local holdings={local_holding:.8f}."
                    )
                elif side == 'sell' and local_short <= 0 and local_holding < self._get_min_volume(norm_pair):
                    self.logger.warning(
                        f"RECONCILE: Open SELL order {txid} ({vol:.6f} {norm_pair}) exists on Kraken "
                        f"but no local long/short position found."
                    )

            self.logger.info(f"Order reconciliation complete. {len(open_map)} open order(s) checked.")
        except Exception as e:
            self.logger.error(f"Order reconciliation failed: {e}", exc_info=True)

    def _sync_account_state(self, force_history: bool = False):
        if force_history:
            try:
                self.api_client.invalidate_balance_cache()
            except Exception:
                pass
        self.get_crypto_holdings()
        self.load_purchase_prices_from_history(force=force_history)

    def _place_live_order(self, pair, direction, volume, price=None, leverage=None, post_only=False, reduce_only=False):
        exec_cfg = self.config.get('execution', {}) if isinstance(self.config, dict) else {}
        use_fallback = bool(exec_cfg.get('enable_live_limit_fallback', True))
        timeout_sec = int(exec_cfg.get('limit_fallback_timeout_sec', 30))

        if use_fallback:
            return self.api_client.place_order_with_fallback(
                pair=pair, direction=direction, volume=volume, price=price,
                leverage=leverage, post_only=post_only, reduce_only=reduce_only, timeout_sec=timeout_sec,
            )

        return self.api_client.place_order(
            pair=pair, direction=direction, volume=volume, price=price,
            leverage=leverage, post_only=post_only, reduce_only=reduce_only,
        )

    def _get_open_orders_snapshot(self):
        try:
            open_orders_result = self.api_client.get_open_orders()
            if not open_orders_result:
                return {}

            open_map = open_orders_result.get('open', open_orders_result) if isinstance(open_orders_result, dict) else {}
            if not isinstance(open_map, dict) or not open_map:
                return {}

            pair_aliases = {
                'XXBTZUSD': 'XBTUSD', 'XBTUSD': 'XBTUSD',
                'XETHZUSD': 'ETHUSD', 'ETHUSD': 'ETHUSD',
                'SOLUSD': 'SOLUSD', 'XXRPZUSD': 'XRPUSD', 'XRPUSD': 'XRPUSD',
                'XXBTZEUR': 'XBTEUR', 'XBTEUR': 'XBTEUR',
                'XETHZEUR': 'ETHEUR', 'ETHEUR': 'ETHEUR',
            }

            normalized = {}
            for txid, order in open_map.items():
                descr = order.get('descr', {}) if isinstance(order, dict) else {}
                side = str(descr.get('type', '') or order.get('type', '') or '').lower()
                raw_pair = str(descr.get('pair', '') or order.get('pair', '') or '').upper()
                norm_pair = pair_aliases.get(raw_pair, raw_pair)
                try:
                    vol = float(order.get('vol', 0) or 0)
                    vol_exec = float(order.get('vol_exec', 0) or 0)
                    remaining_vol = max(0.0, vol - vol_exec)
                except Exception:
                    remaining_vol = 0.0

                price_raw = descr.get('price', None)
                if price_raw in (None, '', '0', 0):
                    price_raw = order.get('price', 0)
                try:
                    limit_price = float(price_raw or 0)
                except Exception:
                    limit_price = 0.0

                normalized[txid] = {
                    'pair': norm_pair,
                    'side': side,
                    'remaining_vol': remaining_vol,
                    'limit_price': limit_price,
                    'raw': order,
                }
            return normalized
        except Exception as e:
            self.logger.debug(f"Could not load open-order snapshot: {e}")
            return {}

    def _has_open_order(self, pair, side) -> bool:
        try:
            for _, meta in self._get_open_orders_snapshot().items():
                if meta.get('pair') == pair and meta.get('side') == side and float(meta.get('remaining_vol', 0.0)) > 0:
                    return True
            return False
        except Exception:
            return False

    def _estimate_open_buy_reserve_fiat(self) -> float:
        try:
            reserved_fiat = 0.0
            for _, meta in self._get_open_orders_snapshot().items():
                if meta.get('side') != 'buy':
                    continue
                remaining_vol = float(meta.get('remaining_vol', 0.0))
                limit_price = float(meta.get('limit_price', 0.0))
                if remaining_vol > 0 and limit_price > 0:
                    reserved_fiat += remaining_vol * limit_price
            return reserved_fiat
        except Exception as e:
            self.logger.debug(f"Could not estimate reserved BUY from open orders: {e}")
            return 0.0

    def _load_trade_history_from_nas(self, year: int) -> dict:
        path = self.nas_root / str(year) / 'trade_history' / f'trades_{year}.json'
        try:
            if path.exists():
                with open(path, 'r') as f:
                    data = json.load(f)
                self.logger.info(f"Loaded {len(data)} trades from NAS cache ({path.name})")
                return data
        except Exception as e:
            self.logger.warning(f"Could not load NAS trade history ({path}): {e}")
        return {}

    def _save_trade_history_to_nas(self, trades: dict, year: int) -> None:
        try:
            trade_history_dir = self.nas_root / str(year) / 'trade_history'
            trade_history_dir.mkdir(parents=True, exist_ok=True)
            path = trade_history_dir / f'trades_{year}.json'
            with open(path, 'w') as f:
                json.dump(trades, f, separators=(',', ':'))
            self.logger.debug(f"Saved {len(trades)} trades to NAS cache ({path.name})")
        except Exception as e:
            self.logger.warning(f"Could not save trade history to NAS ({e}) — NAS mounted?")

    def _refresh_trade_history_cache(self, force: bool = False) -> None:
        now = time.time()
        if not force and (now - self._trade_history_last_fetch) < _TRADE_HISTORY_REFRESH_INTERVAL:
            return

        year = datetime.now(tz=timezone.utc).year
        year_start_ts = int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp())

        if not self._trade_history_cache:
            self._trade_history_cache = self._load_trade_history_from_nas(year)

        if self._trade_history_cache:
            last_ts = max(float(t.get('time', 0)) for t in self._trade_history_cache.values())
            fetch_start = max(year_start_ts, int(last_ts))
        else:
            fetch_start = year_start_ts

        new_trades = self.api_client.get_trade_history(start=fetch_start, fetch_all=True)
        if new_trades:
            self._trade_history_cache.update(new_trades)
            self._save_trade_history_to_nas(self._trade_history_cache, year)

        self._trade_history_last_fetch = now

    def load_purchase_prices_from_history(self, force: bool = False):
        try:
            self._refresh_trade_history_cache(force=force)
            trades = self._trade_history_cache
            if not trades:
                return

            watched = set(self.trade_pairs)
            pair_aliases = {
                'XXBTZUSD': 'XBTUSD', 'XBTUSD': 'XBTUSD',
                'XETHZUSD': 'ETHUSD', 'ETHUSD': 'ETHUSD',
                'SOLUSD': 'SOLUSD', 'XXRPZUSD': 'XRPUSD', 'XRPUSD': 'XRPUSD',
                'XXBTZEUR': 'XBTEUR', 'XBTEUR': 'XBTEUR',
                'XETHZEUR': 'ETHEUR', 'ETHEUR': 'ETHEUR',
            }

            for pair in watched:
                self.position_qty[pair] = 0.0
                self.purchase_prices[pair] = 0.0
                self.realized_pnl[pair] = 0.0
                self.fees_paid[pair] = 0.0

            sorted_trades = sorted(trades.values(), key=lambda t: float(t.get('time', 0)))
            history_trade_count = 0

            for trade in sorted_trades:
                raw_pair = trade.get('pair', '')
                pair = pair_aliases.get(raw_pair, raw_pair)
                if pair not in watched:
                    continue

                ttype = trade.get('type', '').lower()
                vol = float(trade.get('vol', 0) or 0)
                cost = float(trade.get('cost', 0) or 0)  # quote currency balance
                fee = float(trade.get('fee', 0) or 0)
                if vol <= 0:
                    continue

                self.fees_paid[pair] += fee
                qty = self.position_qty.get(pair, 0.0)
                avg = self.purchase_prices.get(pair, 0.0)

                if ttype == 'buy':
                    history_trade_count += 1
                    total_cost = cost + fee
                    new_qty = qty + vol
                    if new_qty > 0:
                        new_avg = ((avg * qty) + total_cost) / new_qty
                    else:
                        new_avg = 0.0
                    self.position_qty[pair] = new_qty
                    self.purchase_prices[pair] = new_avg
                    self.peak_prices[pair] = max(self.peak_prices.get(pair, 0.0), new_avg)

                elif ttype == 'sell':
                    history_trade_count += 1
                    sell_qty = min(qty, vol)
                    proceeds_net = cost - fee
                    if sell_qty > 0 and avg > 0:
                        cost_basis = avg * sell_qty
                        self.realized_pnl[pair] += (proceeds_net - cost_basis)
                    remaining_qty = max(0.0, qty - sell_qty)
                    self.position_qty[pair] = remaining_qty
                    if remaining_qty <= self._get_min_volume(pair):
                        self.purchase_prices[pair] = 0.0
                        self.peak_prices[pair] = 0.0

            if history_trade_count > 0:
                self.trade_count = history_trade_count

            for pair in watched:
                live_qty = self.holdings.get(pair, 0.0)
                history_qty = self.position_qty.get(pair, 0.0)
                self.position_qty[pair] = live_qty
                min_vol = self._get_min_volume(pair)
                if live_qty < min_vol * 0.95:
                    self.purchase_prices[pair] = 0.0
                    self.peak_prices[pair] = 0.0
                    self.entry_timestamps[pair] = None
                elif self.purchase_prices.get(pair, 0.0) <= 0.0:
                    self.logger.warning(
                        f"Position {pair} exists ({live_qty:.8f}) but entry price unknown! "
                        f"Tracking safety active."
                    )
                    if self.entry_timestamps.get(pair) is None:
                        self.entry_timestamps[pair] = int(time.time())
                else:
                    if history_qty > live_qty * 1.10 and live_qty >= min_vol * 0.95:
                        recent_buy = next(
                            (t for t in reversed(sorted_trades)
                             if pair_aliases.get(t.get('pair', ''), t.get('pair', '')) == pair
                             and t.get('type', '').lower() == 'buy'),
                            None
                        )
                        if recent_buy:
                            rc = float(recent_buy.get('cost', 0))
                            rv = float(recent_buy.get('vol', 1)) or 1.0
                            rf = float(recent_buy.get('fee', 0))
                            corrected = (rc + rf) / rv
                            self.purchase_prices[pair] = corrected
                    if self.entry_timestamps.get(pair) is None:
                        self.entry_timestamps[pair] = int(time.time())

        except Exception as e:
            self.logger.error(f"Error loading last purchase prices: {e}")

    def _resolve_benchmark_history(self):
        bench = self.regime_benchmark_pair
        aliases = [bench, bench.replace('/', '')]
        if bench == 'XBTUSD' or bench == 'XXBTZUSD':
            aliases += ['XXBTZUSD', 'XBTUSD']
        for key in aliases:
            history = self.analysis_tool.pair_price_history.get(key)
            if history:
                return list(history)
        return []

    def _compute_mtf_regime_score(self):
        prices = self._resolve_benchmark_history()
        if len(prices) < 80:
            return None

        def _safe_rsi(window):
            val = self.analysis_tool.calculate_rsi(window)
            return 50.0 if val is None else float(val)

        rsi_fast = _safe_rsi(prices[-25:])
        rsi_mid = _safe_rsi(prices[-35:])
        rsi_slow = _safe_rsi(prices[-80:])

        sma10 = sum(prices[-10:]) / 10.0
        sma30 = sum(prices[-30:]) / 30.0
        sma70 = sum(prices[-70:]) / 70.0

        trend = (((sma10 - sma30) / sma30) * 100.0) * 0.9 + (((sma30 - sma70) / sma70) * 100.0) * 1.2
        momentum = ((rsi_fast - 50.0) * 0.4) + ((rsi_mid - 50.0) * 0.35) + ((rsi_slow - 50.0) * 0.25)

        recent = prices[-24:]
        mean = sum(recent) / len(recent)
        vol_pct = 0.0
        if mean > 0:
            variance = sum((p - mean) ** 2 for p in recent) / len(recent)
            vol_pct = ((variance ** 0.5) / mean) * 100.0
        vol_penalty = max(0.0, vol_pct - 2.2) * 1.5

        return trend + momentum - vol_penalty

    def _is_risk_on_regime(self):
        if not self.enable_regime_filter:
            return True

        if self.enable_mtf_regime_scoring:
            mtf_score = self._compute_mtf_regime_score()
            if mtf_score is not None:
                return mtf_score >= self.mtf_regime_min_score

        benchmark = self.regime_benchmark_pair
        score = float(self.pair_scores.get(benchmark, 0.0))
        return score >= self.regime_min_score

    def _benchmark_volatility_pct(self):
        bench = self.regime_benchmark_pair
        aliases = [bench, bench.replace('/', '')]
        if bench == 'XBTUSD' or bench == 'XXBTZUSD':
            aliases += ['XXBTZUSD', 'XBTUSD']

        try:
            history = None
            for key in aliases:
                history = self.analysis_tool.pair_price_history.get(key)
                if history and len(history) >= 20:
                    break
            if not history or len(history) < 20:
                return 0.0
            prices = list(history)[-20:]
            mean = sum(prices) / len(prices)
            if mean <= 0:
                return 0.0
            variance = sum((p - mean) ** 2 for p in prices) / len(prices)
            return ((variance ** 0.5) / mean) * 100.0
        except Exception:
            return 0.0

    def _allocation_multiplier(self):
        base = 1.0 if self._is_risk_on_regime() else self.risk_off_allocation_multiplier
        if not self.enable_volatility_targeting:
            return base
        vol = self._benchmark_volatility_pct()
        if vol <= 0:
            return base
        vol_scale = min(1.25, max(0.35, self.target_volatility_pct / vol))
        return max(0.2, min(1.25, base * vol_scale))

    def _is_trading_hours(self):
        if not self.enable_trading_hours:
            return True
        hour = datetime.now(timezone.utc).hour
        start = self.trading_hours_start_utc
        end = self.trading_hours_end_utc
        if start < end:
            return start <= hour < end
        return hour >= start or hour < end

    def _has_sufficient_volume(self, pair):
        if not self.enable_volume_filter:
            return True
        try:
            cached = self._volume_cache.get(pair)
            if cached and (time.time() - cached[0]) < 300:
                return cached[1] >= self.volume_filter_min_ratio

            ohlc = self.api_client.get_ohlc_data(pair, interval=15)
            if not ohlc:
                return False
            data_key = next((k for k in ohlc if k != 'last'), None)
            if not data_key:
                return False
            rows = ohlc[data_key]
            if len(rows) < 3:
                return False
            volumes = [float(row[6]) for row in rows]
            window = volumes[-20:] if len(volumes) >= 20 else volumes
            avg_vol = sum(window) / len(window)
            current_vol = volumes[-1]
            ratio = current_vol / avg_vol if avg_vol > 0 else 1.0
            self._volume_cache[pair] = (time.time(), ratio)
            if ratio < self.volume_filter_min_ratio:
                self.logger.info(
                    f"BUY skipped for {pair}: low volume (ratio {ratio:.2f})"
                )
                return False
            return True
        except Exception as e:
            self.logger.warning(f"Volume check failed for {pair}: {e}")
            return False

    def _is_temporarily_paused(self):
        try:
            if getattr(self, 'kill_switch_path', None) and os.path.exists(self.kill_switch_path):
                return True
        except Exception:
            pass
        return time.time() < getattr(self, 'trading_paused_until_ts', 0)

    def _available_fiat_for_buy(self):
        # Optimization Integration: Smart Preflight Cushion Reserves 1.5% for maker fees
        return max(0.0, self.get_fiat_balance() * 0.985)

    def _daily_drawdown_hit(self):
        if not getattr(self, 'enable_daily_drawdown', True):
            return False

        current = self.get_fiat_balance()
        if self.daily_start_balance is None:
            self.daily_start_balance = current
            return False
        if self.daily_start_balance <= 0:
            return False

        dd = ((self.daily_start_balance - current) / self.daily_start_balance) * 100
        abs_loss = max(0.0, self.daily_start_balance - current)
        min_abs_loss = float(self.config.get('risk_management', {}).get(f'daily_loss_min_{self.base_currency.lower()}', 0.0))

        if dd >= self.daily_drawdown_percent and abs_loss >= min_abs_loss:
            self.logger.warning(f"Daily drawdown limit hit: {dd:.2f}% (abs loss {abs_loss:.2f} {self.base_currency})")
            return True
        return False

    def _refresh_cashflows_from_ledger(self, force=False):
        now_ts = int(time.time())
        if not force and (now_ts - self._last_cashflow_refresh_ts) < self.cashflow_refresh_interval_sec:
            return

        try:
            ledgers = self.api_client.get_ledgers(asset=self.kraken_fiat_key, start=self.start_timestamp, fetch_all=True)
            if not ledgers:
                self._last_cashflow_refresh_ts = now_ts
                return

            deposits = 0.0
            withdrawals = 0.0
            for entry in ledgers.values():
                ltype = str(entry.get('type', '')).lower()
                try:
                    amount = abs(float(entry.get('amount', 0) or 0))
                except Exception:
                    amount = 0.0

                if amount <= 0:
                    continue

                if ltype == 'deposit':
                    deposits += amount
                elif ltype == 'withdrawal':
                    withdrawals += amount

            self.net_deposits_fiat = deposits
            self.net_withdrawals_fiat = withdrawals
            self._last_cashflow_refresh_ts = now_ts
        except Exception as e:
            self.logger.error(f"Error refreshing cashflows from ledger: {e}")

    def _adjusted_reference_balance(self):
        base = self.initial_balance_fiat if self.initial_balance_fiat is not None else (self.daily_start_balance or 0.0)
        return base + self.net_deposits_fiat - self.net_withdrawals_fiat

    def _adjusted_pnl_fiat(self, current_balance):
        return current_balance - self._adjusted_reference_balance()

    def _pnl_state_path(self) -> Path:
        return Path(__file__).parent / "data" / "pnl_state.json"

    def _cooldown_state_path(self) -> Path:
        return Path(__file__).parent / "data" / "cooldown_state.json"

    def _save_cooldown_state(self) -> None:
        try:
            state = {
                "last_global_trade_at": self.last_global_trade_at,
                "last_trade_at": self.last_trade_at,
            }
            path = self._cooldown_state_path()
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(state))
        except Exception as exc:
            self.logger.warning(f"Could not save cooldown state: {exc}")

    def _load_cooldown_state(self) -> None:
        path = self._cooldown_state_path()
        try:
            if not path.exists():
                return
            state = json.loads(path.read_text())
            self.last_global_trade_at = float(state.get("last_global_trade_at", 0))
            saved_pair_times = state.get("last_trade_at", {})
            for pair, ts in saved_pair_times.items():
                self.last_trade_at[pair] = float(ts)
        except Exception as exc:
            self.logger.warning(f"Could not load cooldown state: {exc}")

    def _load_cumulative_pnl_state(self, current_balance: float) -> None:
        path = self._pnl_state_path()
        try:
            if path.exists():
                state = json.loads(path.read_text())
                self.cumulative_start_fiat = float(state.get("start_fiat", current_balance))
                self.logger.info(
                    f"Loaded P&L baseline: ${self.cumulative_start_fiat:.2f} {self.base_currency}"
                )
            else:
                self.cumulative_start_fiat = current_balance
                state = {
                    "start_fiat": current_balance,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(json.dumps(state, indent=2))
                self.logger.info(f"Created P&L baseline: ${current_balance:.2f}")
        except Exception as exc:
            self.logger.warning(f"Could not load P&L state: {exc}")
            self.cumulative_start_fiat = current_balance

    def cumulative_pnl_fiat(self, current_balance: float) -> float:
        return current_balance - getattr(self, "cumulative_start_fiat", current_balance)

    def _count_open_positions(self) -> int:
        return sum(
            1 for pair in self.trade_pairs
            if self.holdings.get(pair, 0.0) >= self._get_min_volume(pair)
        )

    def _is_on_cooldown(self, pair):
        return (time.time() - self.last_trade_at.get(pair, 0)) < self.trade_cooldown_sec

    def _is_global_cooldown(self):
        return (time.time() - self.last_global_trade_at) < self.global_trade_cooldown_sec

    def _log_empty_sell_signal_throttled(self, pair):
        now_ts = time.time()
        last_ts = self._last_empty_sell_log_at.get(pair, 0)
        if (now_ts - last_ts) >= self.empty_sell_log_cooldown_sec:
            self.logger.info(f"SELL signal for {pair} but no holdings")
            self._last_empty_sell_log_at[pair] = now_ts

    def _profit_percent_from_entry(self, pair, current_price):
        entry = self.purchase_prices.get(pair, 0.0)
        if entry <= 0 or current_price <= 0:
            return None
        return ((current_price - entry) / entry) * 100.0

    def _last_closed_trade_net_profit_pct(self, pair):
        try:
            return last_closed_trade_net_profit_pct(self.json_journal_path, pair, self.fees_maker_percent, self.fees_taker_percent)
        except Exception:
            return None

    def _compute_atr(self, pair, period=None):
        try:
            p = period if period is not None else self.atr_period

            # Read straight from the pre-existing history buffers in RAM
            # Completely cuts out network dependencies and hidden API failure traps
            history = list(self.analysis_tool._get_price_history(pair))
            if not history or len(history) < 2:
                return None

            import numpy as _np
            prices = _np.array(history)
            tr = _np.abs(_np.diff(prices))

            if len(tr) < p:
                return float(_np.mean(tr)) if len(tr) > 0 else None
            return float(_np.mean(tr[-p:]))
        except Exception as e:
            self.logger.debug(f"ATR memory calculation fallback error: {e}")
            return None

    def _required_take_profit_percent(self, pair):
        base_tp = self.take_profit_percent
        if self.enable_atr_dynamic_tp:
            atr = self._compute_atr(pair)
            current_price = self.pair_prices.get(pair, 0)
            if atr and current_price > 0:
                atr_pct = (atr / current_price) * 100.0
                base_tp = max(base_tp, self.atr_tp_multiplier * atr_pct)

        if not self.adaptive_tp_enabled:
            fee_buffer = float(self.sell_fee_buffer_percent or 0.0)
            return min(self.max_tp_percent, base_tp + fee_buffer)

        score = abs(float(self.pair_scores.get(pair, 0.0)))
        bonus = 0.0
        if score > 20:
            bonus = min(4.0, (score - 20.0) / 30.0 * 4.0)

        fee_buffer = float(self.sell_fee_buffer_percent or 0.0)
        return min(self.max_tp_percent, base_tp + bonus + fee_buffer)

    def _can_sell_profit_target(self, pair, current_price):
        if self.enable_atr_stop and not self.enable_atr_dynamic_tp:
            return True
        slippage_pct = float(self.config.get('risk_management', {}).get('exit_slippage_buffer_pct', 0.3))
        conservative_exit_price = current_price * (1.0 - slippage_pct / 100.0)
        profit_pct = self._profit_percent_from_entry(pair, conservative_exit_price)
        if profit_pct is None:
            return False
        required_tp = self._required_take_profit_percent(pair)
        if profit_pct < required_tp:
            return False
        min_path = float(self.config.get('risk_management', {}).get('min_net_sell_profit_pct', self.min_net_sell_profit_pct))
        if min_path > 0:
            fees_total_frac = pct_to_frac(getattr(self, 'fees_maker_percent', 0.0)) + pct_to_frac(getattr(self, 'fees_taker_percent', 0.0))
            fees_total_pct = fees_total_frac * 100.0
            net_profit_pct = profit_pct - fees_total_pct
            return net_profit_pct >= min_path
        return True

    def _update_trade_metrics(self, pair, pnl_fiat):
        pnl_fiat = float(pnl_fiat)
        m = self.trade_metrics.setdefault(pair, {"closed": 0, "wins": 0, "losses": 0, "sum_pnl": 0.0})
        m["closed"] += 1
        m["sum_pnl"] += pnl_fiat
        self.closed_trade_pnls.append(pnl_fiat)
        if pnl_fiat >= 0:
            m["wins"] += 1
            self.consecutive_losses = 0
            if self.trading_paused_until_ts > time.time():
                self.trading_paused_until_ts = 0
        else:
            m["losses"] += 1
            self.consecutive_losses += 1
            if self.consecutive_losses >= self.max_consecutive_losses:
                pause_sec = self.pause_after_loss_streak_minutes * 60
                self.trading_paused_until_ts = max(self.trading_paused_until_ts, int(time.time()) + pause_sec)
                self.kelly_fraction = self._calculate_kelly_fraction()

    def _calculate_kelly_fraction(self):
        try:
            pnls = list(self.closed_trade_pnls)
            if len(pnls) < 10:
                return 0.1

            wins = [p for p in pnls if p > 0]
            losses = [abs(p) for p in pnls if p < 0]
            if not wins or not losses:
                return 0.1

            win_rate = len(wins) / len(pnls)
            avg_win = sum(wins) / len(wins)
            avg_loss = sum(losses) / len(losses)
            if avg_win <= 0 or avg_loss <= 0:
                return 0.1

            b = avg_win / avg_loss
            kelly = win_rate - ((1 - win_rate) / b)
            return max(0.01, min(0.5, kelly))
        except Exception:
            return 0.1

    def check_take_profit_or_stop_loss(self):
        # Establish base risk bounds from standard configurations
        base_tp = self.take_profit_percent
        base_sl = self.hard_stop_loss_percent if self.enable_hard_stop_loss else self.stop_loss_percent
        base_time_limit_sec = self.time_stop_hours * 3600

        # Fast Scalp Dynamic Intercept Override
        if getattr(self, 'enable_fast_scalp', False):
            base_tp = float(getattr(self, 'fast_scalp_take_profit_pct', 1.2))
            base_sl = float(getattr(self, 'fast_scalp_stop_loss_pct', 0.6))
            base_time_limit_sec = int(getattr(self, 'fast_scalp_time_stop_minutes', 30)) * 60

        for pair in self.trade_pairs:
            current_price = self.pair_prices.get(pair, 0)
            if current_price <= 0:
                continue

            holding = self.holdings.get(pair, 0)
            min_vol = self._get_min_volume(pair)
            if holding >= min_vol:
                prev_peak = self.peak_prices.get(pair, 0.0)
                self.peak_prices[pair] = max(prev_peak, current_price)

                change_percent = self._profit_percent_from_entry(pair, current_price)
                if change_percent is not None:
                    # 1. Dynamic Overridden Take Profit Check
                    if change_percent >= base_tp:
                        return pair, "TAKE_PROFIT", change_percent

                    # 2. Dynamic Overridden Hard Stop Loss Check
                    if change_percent <= -abs(base_sl):
                        return pair, "HARD_STOP", change_percent

                    # 3. Dynamic Overridden Time Stop Check
                    if self.enable_time_stop:
                        opened_at = self.entry_timestamps.get(pair)
                        if opened_at and (time.time() - opened_at) >= base_time_limit_sec:
                            return pair, "TIME_STOP", change_percent

                    # 4. Break-Even Safety Stop Ratchet
                    if self.enable_break_even and (change_even_trigger := getattr(self, 'break_even_trigger_pct', 1.5)):
                        if change_percent >= change_even_trigger:
                            entry_price = self.purchase_prices.get(pair, 0)
                            if entry_price > 0:
                                current_stop = self.stop_info.get(pair, {}).get('stop_price', 0)
                                if current_stop < entry_price:
                                    self.stop_info[pair] = {'stop_price': entry_price, 'type': 'BREAK_EVEN'}
                                    self.logger.info(f"RISK PROTECTION: Break-even stop ratcheted to entry for {pair}")

                    # 5. ATR Volatility Trailing Stop Tracker
                    if self.enable_atr_stop:
                        atr = self._compute_atr(pair)
                        if atr:
                            current_stop_info = self.stop_info.get(pair, {})
                            current_stop = current_stop_info.get('stop_price', 0)

                            if pair not in self.stop_info:
                                entry = self.purchase_prices.get(pair, current_price)
                                init_stop = max(0.0, entry - (atr * self.atr_multiplier))
                                self.stop_info[pair] = {'stop_price': init_stop, 'type': 'ATR'}
                                current_stop = init_stop

                            potential_stop = current_price - (atr * self.atr_trail_multiplier)
                            if potential_stop > current_stop:
                                self.stop_info[pair] = {'stop_price': potential_stop, 'type': 'ATR_TRAIL'}

                    stop_data = self.stop_info.get(pair, {})
                    s_price = stop_data.get('stop_price')
                    if s_price is not None and current_price <= s_price:
                        return pair, stop_data.get('type', 'STOP'), change_percent

                    if not self.enable_atr_stop and self.trailing_stop_percent > 0 and change_percent > 0:
                        drop_from_peak = ((self.peak_prices[pair] - current_price) / self.peak_prices[pair]) * 100.0
                        if drop_from_peak >= self.trailing_stop_percent:
                            return pair, "TRAILING_STOP", change_percent

            short_qty = self.short_qty.get(pair, 0.0)
            short_entry = self.short_entry_prices.get(pair, 0.0)
            if self.enable_live_shorts and short_qty > 0 and short_entry > 0:
                short_change_percent = ((short_entry - current_price) / short_entry) * 100.0
                if short_change_percent >= self.short_take_profit_percent:
                    return pair, "SHORT_TAKE_PROFIT", short_change_percent
                if short_change_percent <= -abs(self.short_stop_loss_percent):
                    return pair, "SHORT_HARD_STOP", short_change_percent
                if self.enable_time_stop:
                    opened_at = self.entry_timestamps.get(pair)
                    if opened_at and (time.time() - opened_at) >= base_time_limit_sec:
                        return pair, "SHORT_TIME_STOP", short_change_percent

        return None, None, None

    def _warmup_pair_history(self, pair):
        if self.nas_root:
            try:
                self.analysis_tool.seed_from_nas_ohlc(pair, self.nas_root)
                history = self.analysis_tool._get_price_history(pair)
                if len(history) >= self.analysis_tool.sma_long:
                    return
            except Exception as e:
                self.logger.warning(f"NAS warmup failed for {pair}: {e}")
        try:
            ohlc = self.api_client.get_ohlc_data(pair, interval=60)
            if not ohlc:
                return
            data_key = next((k for k in ohlc if k != 'last'), None)
            if not data_key:
                return
            closes = [float(row[4]) for row in ohlc[data_key]]
            self.analysis_tool.seed_from_ohlc(pair, closes)
        except Exception as e:
            self.logger.warning(f"OHLC warmup failed for {pair}: {e}")

    def _refresh_hourly_signals(self):
        for pair in self.trade_pairs:
            try:
                ohlc = self.api_client.get_ohlc_data(pair, interval=60)
                if not ohlc:
                    continue
                data_key = next((k for k in ohlc if k != 'last'), None)
                if not data_key:
                    continue
                series = ohlc[data_key]
                if not series:
                    continue
                closes_1h = [float(row[4]) for row in series]

                last_close = closes_1h[-1]
                signal, score = self.analysis_tool.generate_signal_with_score({pair: {'c': [last_close]}})
                self.pair_signals[pair] = signal
                self.pair_scores[pair] = score

                _, _, ema_bull = self.analysis_tool.calculate_ema_crossover(
                    closes_1h, fast=self.ema_fast_period, slow=self.ema_slow_period,
                )
                self._ema_bullish[pair] = ema_bull

                _, _, macd_h_1h = self.analysis_tool.calculate_macd(closes_1h)
                self._macd_1h_hist[pair] = macd_h_1h

                time.sleep(0.2)

                try:
                    ohlc_15 = self.api_client.get_ohlc_data(pair, interval=15)
                    if ohlc_15:
                        dk15 = next((k for k in ohlc_15 if k != 'last'), None)
                        if dk15 and ohlc_15[dk15]:
                            closes_15 = [float(row[4]) for row in ohlc_15[dk15]]
                            _, _, h15 = self.analysis_tool.calculate_macd(closes_15)
                            h15_prev = None
                            if len(closes_15) > 36:
                                _, _, h15_prev = self.analysis_tool.calculate_macd(closes_15[:-1])
                            self._macd_15m_hist[pair] = h15
                            self._macd_15m_hist_prev[pair] = h15_prev
                            time.sleep(0.2)
                except Exception:
                    pass

            except Exception as e:
                self.logger.debug(f"Hourly signal refresh error for {pair}: {e}")

    def _is_ema_trend_bullish(self, pair):
        if not self.enable_ema_crossover_filter:
            return True
        val = self._ema_bullish.get(pair)
        if val is None:
            return True
        return val

    def _is_mtf_macd_buy_aligned(self, pair):
        if not self.enable_mtf_macd_filter:
            return True
        h1h = self._macd_1h_hist.get(pair)
        h15m = self._macd_15m_hist.get(pair)
        if h1h is None or h15m is None:
            return True
        price = self.pair_prices.get(pair, 1.0) or 1.0
        h1h_pct = (h1h / price) * 100.0
        if h1h_pct < -0.05 and h15m < 0:
            return False
        return True

    def _execute_partial_exit(self, pair, price):
        try:
            full_volume = self.holdings.get(pair, 0.0)
            min_vol = self._get_min_volume(pair)
            if full_volume < min_vol:
                self._partial_exit_done[pair] = True
                return

            sell_volume = round(full_volume * self.partial_exit_fraction, 8)
            remaining_volume = full_volume - sell_volume

            if remaining_volume * price < self.partial_exit_min_remaining_fiat:
                self._partial_exit_done[pair] = True
                return
            if sell_volume < min_vol:
                self._partial_exit_done[pair] = True
                return

            avg_entry = self.purchase_prices.get(pair, 0.0)
            est_profit_pct = self._profit_percent_from_entry(pair, price)
            est_profit_fiat = (price - avg_entry) * sell_volume if avg_entry > 0 else 0.0
            pp_str = f"{est_profit_pct:.2f}%" if est_profit_pct is not None else "n/a"

            self.logger.info(
                f"PARTIAL EXIT ({self.partial_exit_fraction * 100:.0f}%): selling "
                f"{sell_volume:.6f} {pair} @ {price:.4f} {self.base_currency}  profit={pp_str}"
            )
            result = self._place_live_order(
                pair=pair, direction='sell', volume=sell_volume, price=price, post_only=True
            )
            if result:
                self._partial_exit_done[pair] = True
                self._sync_account_state(force_history=True)
                self.trade_count += 1
                now_ts = time.time()
                self.last_trade_at[pair] = now_ts
                self.last_global_trade_at = now_ts
                self._save_cooldown_state()
                self._update_trade_metrics(pair, est_profit_fiat)
                fill_price = None
                try:
                    if isinstance(result, dict) and 'fill_price' in result:
                        fill_price = float(result['fill_price'])
                except Exception:
                    pass
                self._journal_trade(
                    'PARTIAL_SELL', pair, sell_volume, price, est_profit_fiat, 'PARTIAL_EXIT',
                    extra={
                        'result': result,
                        'fraction': self.partial_exit_fraction,
                        'remaining_volume': remaining_volume,
                        'fill_price': fill_price,
                    },
                )
                print(
                    f"\n[PARTIAL SELL] {sell_volume:.6f} {pair} (~${sell_volume * price:.2f}) "
                    f"kept {remaining_volume:.6f} — Trade #{self.trade_count}"
                )
            else:
                self.logger.error(f"PARTIAL EXIT FAILED for {pair}")
        except Exception as e:
            self.logger.error(f"Error executing partial exit for {pair}: {e}", exc_info=True)

    def _update_regime_cache(self):
        try:
            now = time.time()
            bench = self.regime_benchmark_pair
            mtf = self._compute_mtf_regime_score()
            if mtf is None:
                try:
                    ohlc = self.api_client.get_ohlc_data(bench, interval=60)
                    if ohlc:
                        data_key = next((k for k in ohlc if k != 'last'), None)
                        if data_key:
                            closes = [float(r[4]) for r in ohlc[data_key]]
                            for c in closes[-self.analysis_tool.max_history:]:
                                self.analysis_tool._get_price_history(bench).append(c)
                            mtf = self._compute_mtf_regime_score()
                except Exception:
                    pass

            risk_on = True
            if mtf is not None:
                risk_on = mtf >= self.mtf_regime_min_score
            else:
                try:
                    score = float(self.pair_scores.get(bench, 0.0))
                    risk_on = score >= self.regime_min_score
                except Exception:
                    risk_on = True

            self._regime_cache = {'ts': now, 'risk_on': bool(risk_on)}
        except Exception:
            pass

    def analyze_all_pairs(self):
        best_pair = None
        best_signal = "HOLD"
        best_score = 0
        try:
            now = time.time()
            if (now - self._last_signal_refresh_ts) >= self.signal_refresh_interval:
                self._refresh_hourly_signals()
                self._last_signal_refresh_ts = now
        except Exception:
            pass

        for pair in self.trade_pairs:
            try:
                ws_price = None
                if self.ws_feed is not None:
                    try:
                        ws_price = self.ws_feed.get_price(pair)
                    except Exception:
                        ws_price = None

                if ws_price is not None:
                    current_price = ws_price
                    self.pair_prices[pair] = current_price
                else:
                    market_data = self.api_client.get_market_data(pair)
                    if market_data:
                        pair_key = list(market_data.keys())[0]
                        current_price = float(market_data[pair_key]['c'][0])
                        self.pair_prices[pair] = current_price
                    else:
                        current_price = self.pair_prices.get(pair, 0)

                if len(self.analysis_tool._get_price_history(pair)) < self.analysis_tool.sma_long:
                    self.analysis_tool._get_price_history(pair).clear()
                    self._warmup_pair_history(pair)

                self._update_airbag_history(pair, current_price)
                if self._check_airbag_trigger(pair):
                    if self.holdings.get(pair, 0) >= self._get_min_volume(pair):
                        self.execute_sell_order(pair, current_price, require_profit_target=False, reason="CRASH_AIRBAG")

                sig = self.pair_signals.get(pair)
                score = self.pair_scores.get(pair, 0)
                signal = sig if sig is not None else "HOLD"

                if signal in ["BUY", "SELL"] and abs(score) > abs(best_score):
                    best_pair = pair
                    best_signal = signal
                    best_score = score

                time.sleep(0.25)
            except Exception as e:
                self.logger.error(f"Error analyzing {pair}: {e}")

        return best_pair, best_signal, best_score

    def start_trading(self):
        self.logger.info("=" * 60)
        self.logger.info(f"TRADING BOT STARTED - MULTI-PAIR MODE ({self.base_currency})")
        self.logger.info(f"Watching: {', '.join(self.trade_pairs)}")
        self.logger.info(f"Target: ${self.target_balance_fiat:.2f} {self.base_currency}")
        self.logger.info("=" * 60)

        print("=" * 60)
        print(f"KRAKEN TRADING BOT - MULTI-PAIR MODE ({self.base_currency})")
        print(f"Watching {len(self.trade_pairs)} pairs: {', '.join(self.trade_pairs)}")
        print(f"Trade Amount: ${self._get_trade_amount_fiat():.2f} {self.base_currency} per trade")
        print(f"Target Balance: ${self.target_balance_fiat:.2f} {self.base_currency}")
        print("Press Ctrl+C to stop")
        print("=" * 60)

        initial_balance = self.get_fiat_balance()
        self.initial_balance_fiat = initial_balance
        self.peak_balance = initial_balance
        self.daily_start_balance = initial_balance
        self._load_cumulative_pnl_state(initial_balance)
        self._sync_account_state(force_history=True)
        self._reconcile_open_orders()
        self._refresh_cashflows_from_ledger(force=True)

        self.logger.info(f"Initial {self.base_currency} Balance: ${initial_balance:.2f}")
        self.logger.info(f"Take-Profit: {self.take_profit_percent}% | Stop-Loss: {self.stop_loss_percent}%")

        for pair in self.trade_pairs:
            qty = self.holdings.get(pair, 0.0)
            avg = self.purchase_prices.get(pair, 0.0)
            min_v = self._get_min_volume(pair)
            if qty >= min_v:
                self.logger.info(
                    f"Startup position: {pair} qty={qty:.8f} avg_entry=${avg:.4f}"
                )
            else:
                self.logger.info(f"Startup position: {pair} — no holdings (qty={qty:.8f})")

        try:
            iteration = 0
            while True:
                iteration += 1
                try:
                    current_balance = self.get_fiat_balance()

                    now = datetime.now()
                    last_reset = datetime.fromtimestamp(self.last_daily_reset_ts)
                    if now.day != last_reset.day or now.month != last_reset.month or now.year != last_reset.year:
                        self.daily_start_balance = current_balance
                        self.last_daily_reset_ts = int(time.time())
                        self.logger.info(f"Daily start balance reset to ${self.daily_start_balance:.2f}")
                    if current_balance >= self.target_balance_fiat:
                        print(f"\nTARGET REACHED! Balance: ${current_balance:.2f}")
                        break

                    best_pair, best_signal, best_score = self.analyze_all_pairs()
                    self._sync_account_state()

                    self.sentiment_active = self._scan_news_sentiment() if self.enable_sentiment_guard else False

                    risk_pair, risk_type, change = self.check_take_profit_or_stop_loss()
                    if risk_pair:
                        price = self.pair_prices.get(risk_pair, 0)
                        print(f"\n[{risk_type}] {risk_pair} at {change:.2f}%")
                        if str(risk_type).startswith("SHORT_"):
                            self.execute_close_short_order(risk_pair, price)
                        else:
                            _stop_types = {"ATR", "ATR_TRAIL", "HARD_STOP", "BREAK_EVEN",
                                           "TIME_STOP", "TRAILING_STOP"}
                            _require_tp = risk_type not in _stop_types
                            self.execute_sell_order(risk_pair, price,
                                                    require_profit_target=_require_tp,
                                                    reason=risk_type)

                    if self.enable_partial_exit:
                        for _pp_pair in list(self.trade_pairs):
                            if self._partial_exit_done.get(_pp_pair):
                                continue
                            _pp_qty = self.holdings.get(_pp_pair, 0.0)
                            if _pp_qty < self._get_min_volume(_pp_pair):
                                continue
                            _pp_price = self.pair_prices.get(_pp_pair, 0.0)
                            if _pp_price <= 0:
                                continue
                            _pp_pct = self._profit_percent_from_entry(_pp_pair, _pp_price)
                            if _pp_pct is not None and _pp_pct >= self.partial_exit_trigger_pct:
                                self._execute_partial_exit(_pp_pair, _pp_price)

                    self._refresh_cashflows_from_ledger()
                    adjusted_pnl = self._adjusted_pnl_fiat(current_balance)
                    try:
                        holdings_value = sum(
                            self.holdings.get(p, 0.0) * self.pair_prices.get(p, 0.0)
                            for p in self.trade_pairs
                        )
                        reserved_buy_fiat = self._estimate_open_buy_reserve_fiat()
                        portfolio_value = current_balance + holdings_value + reserved_buy_fiat
                    except Exception:
                        portfolio_value = current_balance
                    try:
                        self.peak_balance = max(getattr(self, 'peak_balance', portfolio_value), portfolio_value)
                        current_dd_pct = 0.0
                        if self.peak_balance > 0:
                            current_dd_pct = ((self.peak_balance - portfolio_value) / self.peak_balance) * 100.0
                            max_dd_cfg = float(self.config.get('risk_management', {}).get('max_drawdown_percent', 10.0))
                            if current_dd_pct >= max_dd_cfg:
                                pause_sec = int(self.pause_after_loss_streak_minutes * 60)
                                self.trading_paused_until_ts = max(self.trading_paused_until_ts, int(time.time()) + pause_sec)
                    except Exception:
                        current_dd_pct = 0.0

                    regime_state = "RISK_ON" if self._is_risk_on_regime() else "RISK_OFF"
                    pause_state = "PAUSED" if self._is_temporarily_paused() else "ACTIVE"

                    label_map = {
                        "XBTUSD": "BTC", "XXBTZUSD": "BTC",
                        "ETHUSD": "ETH", "XETHZUSD": "ETH",
                        "SOLUSD": "SOL", "XXRPZUSD": "XRP", "XRPUSD": "XRP"
                    }
                    pair_status = " ".join([
                        f"{label_map.get(p, p[:4])}:{self.pair_signals.get(p, '?')}" for p in self.trade_pairs
                    ])

                    if iteration > 1:
                        print("\033[5A\r", end="")

                    # Print the multi-line dashboard panel cleanly
                    total_pnl = self.cumulative_pnl_fiat(current_balance)
                    print(f"\033[1;36m" + "="*85 + "\033[0m")
                    print(f" [\033[1;33mLoop Tick #{iteration}\033[0m]  Market Status: \033[1;32m{regime_state}/{pause_state}\033[0m  |  Active Signals: {pair_status}")
                    print(f" Balance: \033[1;37m${current_balance:.2f}\033[0m  (Started: ${self.initial_balance_fiat:.2f})  |  Net Capital Flow: +${self.net_deposits_fiat:.2f}/-${self.net_withdrawals_fiat:.2f}")
                    print(f" Performance: Adj P&L: \033[1;32m${adjusted_pnl:+.2f}\033[0m  |  Total Profit: \033[1;32m${total_pnl:+.2f}\033[0m  |  Executed Trades: \033[1;35m{self.trade_count}\033[0m")
                    print(f" Best Market Target: \033[1;34m{best_pair or 'NONE'}\033[0m ({best_signal})")
                    print(f"\033[1;36m" + "="*85 + "\033[0m", end="", flush=True)

                    if self.enable_bear_shield:
                        bear_now = self._is_bear_market()
                        if bear_now and not self._bear_mode_active:
                            self._bear_mode_active = True
                            self._bear_shield_exit_all()
                        elif not bear_now and self._bear_mode_active:
                            self._bear_mode_active = False
                        elif bear_now:
                            now_ts = time.time()
                            if (now_ts - self._bear_last_log_ts) >= self.bear_log_interval_minutes * 60:
                                self._bear_last_log_ts = now_ts

                    if best_pair and best_signal != "HOLD" and not self._is_on_cooldown(best_pair) and not self._is_global_cooldown():
                        price = self.pair_prices.get(best_pair, 0)
                        if best_signal == "BUY":
                            score = float(self.pair_scores.get(best_pair, 0.0))

                            # Standard Timing Gatekeeper Flag
                            is_buy_approved = True

                            if self._is_temporarily_paused() or self._daily_drawdown_hit():
                                is_buy_approved = False
                            elif score < self.min_buy_score or self._count_open_positions() >= self.max_open_positions:
                                is_buy_approved = False
                            elif not self._is_trading_hours() or self._bear_mode_active or self.sentiment_active:
                                is_buy_approved = False
                            elif not self._is_mtf_trend_bullish(best_pair) or not self._is_ema_trend_bullish(best_pair):
                                is_buy_approved = False
                            elif not self._is_mtf_macd_buy_aligned(best_pair) or not self._has_sufficient_volume(best_pair):
                                is_buy_approved = False

                            try:
                                now = time.time()
                                if (now - self._regime_cache.get('ts', 0)) > self._regime_cache_ttl:
                                    self._update_regime_cache()
                                if self.enable_regime_filter and not bool(self._regime_cache.get('risk_on', True)):
                                    is_buy_approved = False
                            except Exception:
                                pass

                            # Execute order ONLY if all gatekeepers remain True
                            if is_buy_approved:
                                try:
                                    if any(g in (best_pair or '').upper() for g in self.reentry_guard_pairs):
                                        last_net = self._last_closed_trade_net_profit_pct(best_pair)
                                        if last_net is not None and last_net < self.min_reentry_profit_pct:
                                            is_buy_approved = False
                                except Exception:
                                    pass

                                if is_buy_approved:
                                    self.execute_buy_order(best_pair, price)

                        elif best_signal == "SELL":
                            min_vol = self._get_min_volume(best_pair)
                            if self.holdings.get(best_pair, 0) >= min_vol:
                                if self._can_sell_profit_target(best_pair, price):
                                    self.execute_sell_order(best_pair, price)
                                else:
                                    pass
                            elif self.enable_live_shorts and self.short_qty.get(best_pair, 0.0) <= 0:
                                score = float(self.pair_scores.get(best_pair, 0.0))
                                if (not self._is_risk_on_regime()) or score <= -self.min_buy_score:
                                    self.execute_open_short_order(best_pair, price)
                            elif self.enable_live_shorts and self.short_qty.get(best_pair, 0.0) > 0:
                                score = float(self.pair_scores.get(best_pair, 0.0))
                                if score >= self.min_buy_score:
                                    self.execute_close_short_order(best_pair, price)
                            else:
                                self._log_empty_sell_signal_throttled(best_pair)

                    time_since_reload = (datetime.now() - self.last_config_reload).total_seconds()
                    if time_since_reload >= self.config_reload_interval:
                        self.reload_config()

                except Exception as e:
                    self.logger.error(
                        f"Unhandled error in trading loop (iteration {iteration}): {e}", exc_info=True,
                    )

                _sd_notify_watchdog()
                time.sleep(self.loop_interval_sec)

        except KeyboardInterrupt:
            final_balance = self.get_fiat_balance()
            print(f"\nTrading bot stopped. Final Balance: ${final_balance:.2f}")

    def _journal_trade(self, ttype, pair, volume, price, pnl_fiat, reason, extra=None):
        try:
            import csv, os, datetime, json
            os.makedirs(os.path.dirname(self.journal_path), exist_ok=True)
            header = ['ts', 'type', 'pair', 'volume', 'price', 'pnl_fiat', 'reason', 'extra']
            exists = os.path.exists(self.journal_path)
            with open(self.journal_path, 'a', newline='') as fh:
                writer = csv.writer(fh)
                if not exists:
                    writer.writerow(header)
                row = [datetime.datetime.utcnow().isoformat(), ttype, pair, f"{volume:.8f}", f"{price:.6f}", f"{pnl_fiat:.6f}", reason, str(extra or '')]
                writer.writerow(row)
        except Exception as e:
            self.logger.error(f"Error writing trade journal CSV: {e}")

        try:
            os.makedirs(os.path.dirname(self.json_journal_path), exist_ok=True)
            j = {
                'ts': datetime.datetime.utcnow().isoformat(),
                'type': ttype,
                'pair': pair,
                'volume': float(volume),
                'price': float(price),
                'pnl_fiat': float(pnl_fiat),
                'reason': reason,
                'extra': extra or {},
                'balance_fiat': float(self.get_fiat_balance()),
                'consecutive_losses': int(self.consecutive_losses),
            }
            try:
                peak = float(getattr(self, 'peak_balance', j['balance_fiat']))
                if peak > 0:
                    j['current_drawdown_pct'] = round(((peak - j['balance_fiat']) / peak) * 100.0, 2)
            except Exception:
                pass

            ok = False
            try:
                ok = append_jsonl_locked(self.json_journal_path, j)
            except Exception:
                ok = False

            if not ok:
                try:
                    with open(self.json_journal_path, 'a', encoding='utf-8') as jf:
                        jf.write(json.dumps(j) + "\n")
                except Exception as e:
                    self.logger.error(f"Error writing JSON trade log fallback: {e}")
        except Exception as e:
            self.logger.error(f"Error writing JSON trade log: {e}")

    def execute_buy_order(self, pair, price):
        try:
            available_fiat = self._available_fiat_for_buy()
            min_trade_fiat = float(self.config.get('risk_management', {}).get('min_trade_usd', 10.0))
            planned_fiat = self._get_dynamic_trade_amount_fiat(pair, available_fiat)
            min_auto_notional = float(self.config.get('risk_management', {}).get('min_auto_scale_notional', 1.0))

            if planned_fiat < min_auto_notional:
                return
            if planned_fiat < min_trade_fiat:
                return

            volume = self._calculate_volume(pair, price, available_fiat=planned_fiat)

            is_valid_book = True

            try:
                exec_cfg = self.config.get('execution', {}) if isinstance(self.config, dict) else {}
                max_spread_pct = float(exec_cfg.get('max_spread_pct', 0.5))
                min_book_fill_ratio = float(exec_cfg.get('min_book_fill_ratio', 0.5))
                ob = self.api_client.get_order_book(pair, count=3)

                if not ob:
                    is_valid_book = False
                else:
                    data_key = next((k for k in ob if k != 'last'), None)
                    if not data_key:
                        is_valid_book = False
                    else:
                        asks = ob[data_key].get('asks', [])
                        bids = ob[data_key].get('bids', [])
                        if not asks or not bids:
                            is_valid_book = False
                        else:
                            best_ask = float(asks[0][0])
                            best_ask_vol = float(asks[0][1])
                            best_bid = float(bids[0][0])
                            mid = (best_ask + best_bid) / 2.0 if best_bid and best_ask else None

                            if mid is None:
                                is_valid_book = False
                            else:
                                spread_pct = ((best_ask - best_bid) / mid) * 100.0
                                planned_notional = planned_fiat
                                if spread_pct > max_spread_pct:
                                    self.logger.info(f"BUY skipped for {pair}: Spread too wide ({spread_pct:.2f}%)")
                                    is_valid_book = False
                                if (best_ask * best_ask_vol) < (planned_notional * min_book_fill_ratio):
                                    self.logger.info(f"BUY skipped for {pair}: Insufficient order book depth")
                                    is_valid_book = False
            except Exception as book_err:
                self.logger.debug(f"Order book guard error: {book_err}")
                is_valid_book = False

            if not is_valid_book:
                return

            self.logger.info(f"Placing BUY order (MAKER/POST-ONLY): {volume:.6f} {pair} at ${price:.2f}")
            result = self._place_live_order(pair=pair, direction='buy', volume=volume, price=price, post_only=True)
            if result:
                self.trade_count += 1
                now_ts = time.time()
                self.last_trade_at[pair] = now_ts
                self.last_global_trade_at = now_ts
                self._save_cooldown_state()
                self.peak_prices[pair] = max(self.peak_prices.get(pair, 0.0), price)
                if self.entry_timestamps.get(pair) is None:
                    self.entry_timestamps[pair] = int(time.time())
                self._partial_exit_done[pair] = False
                self._sync_account_state(force_history=True)

                if self.enable_atr_stop:
                    atr = self._compute_atr(pair)
                    if atr is not None:
                        init_stop = max(0.0, price - (atr * self.atr_multiplier))
                        self.stop_info[pair] = {'stop_price': init_stop, 'type': 'ATR'}

                fill_price = None
                try:
                    if isinstance(result, dict) and 'fill_price' in result:
                        fill_price = float(result.get('fill_price'))
                    elif isinstance(result, dict) and result.get('simulated'):
                        fill_price = float(result.get('fill_price')) if result.get('fill_price') else price
                except Exception:
                    fill_price = None
                self._journal_trade('BUY', pair, volume, price, 0.0, 'BUY_EXECUTED', extra={'result': result, 'expected_price': price, 'fill_price': fill_price})
                print(f"\n[BUY] {volume:.6f} {pair} (~${volume*price:.2f}) - Trade #{self.trade_count}")
            else:
                self.logger.error(f"BUY ORDER FAILED for {pair}")
        except Exception as e:
            self.logger.error(f"Error executing buy order: {e}", exc_info=True)

    def execute_sell_order(self, pair, price, require_profit_target=True, reason=None):
        try:
            volume = self.holdings.get(pair, 0)
            min_vol = self._get_min_volume(pair)
            if volume < min_vol:
                return

            if self._has_open_order(pair, 'sell'):
                return

            if require_profit_target and not self._can_sell_profit_target(pair, price):
                return

            avg_entry = self.purchase_prices.get(pair, 0.0)
            est_profit_pct = self._profit_percent_from_entry(pair, price)
            est_profit_fiat = (price - avg_entry) * volume if avg_entry > 0 else 0.0

            result = self._place_live_order(pair=pair, direction='sell', volume=volume, price=price, post_only=True)
            if result:
                self._sync_account_state(force_history=True)
                remaining_volume = self.holdings.get(pair, 0.0)
                if remaining_volume >= min_vol * 0.95 or self._has_open_order(pair, 'sell'):
                    return

                self.trade_count += 1
                now_ts = time.time()
                self.last_trade_at[pair] = now_ts
                self.last_global_trade_at = now_ts
                self._save_cooldown_state()
                self.purchase_prices[pair] = 0.0
                self.peak_prices[pair] = 0.0
                self.entry_timestamps[pair] = None
                self._partial_exit_done[pair] = False
                if pair in self.stop_info:
                    del self.stop_info[pair]
                self._update_trade_metrics(pair, est_profit_fiat)
                fill_price = None
                try:
                    if isinstance(result, dict) and 'fill_price' in result:
                        fill_price = float(result.get('fill_price'))
                    elif isinstance(result, dict) and result.get('simulated'):
                        fill_price = float(result.get('fill_price')) if result.get('fill_price') else price
                except Exception:
                    fill_price = None
                self._journal_trade('SELL', pair, volume, price, est_profit_fiat, reason or 'SELL_EXECUTED', extra={'result': result, 'expected_price': price, 'fill_price': fill_price})
                print(f"\n[SELL] {volume:.6f} {pair} (~${volume*price:.2f}) - Trade #{self.trade_count}")
            else:
                self.logger.error(f"SELL ORDER FAILED for {pair}")
        except Exception as e:
            self.logger.error(f"Error executing sell order: {e}", exc_info=True)

    def execute_open_short_order(self, pair, price):
        try:
            if not self.enable_live_shorts:
                return
            if self.short_qty.get(pair, 0.0) > 0:
                return

            notional = min(self.max_short_notional_fiat, self._get_dynamic_trade_amount_fiat(pair, self._available_fiat_for_buy()))
            if notional <= 0 or price <= 0:
                return
            volume = max(self._get_min_volume(pair), notional / price)
            result = self._place_live_order(pair=pair, direction='sell', volume=volume, leverage=self.short_leverage)
            if result:
                self.trade_count += 1
                now_ts = time.time()
                self.last_trade_at[pair] = now_ts
                self.last_global_trade_at = now_ts
                self._save_cooldown_state()
                self.short_qty[pair] = volume
                self.short_entry_prices[pair] = price
                self.entry_timestamps[pair] = int(now_ts)
                print(f"\n[SHORT OPEN] {volume:.6f} {pair} (~${notional:.2f}) - Trade #{self.trade_count}")
            else:
                self.logger.error(f"SHORT OPEN FAILED for {pair}")
        except Exception as e:
            self.logger.error(f"Error opening short order: {e}", exc_info=True)

    def execute_close_short_order(self, pair, price):
        try:
            qty = self.short_qty.get(pair, 0.0)
            entry = self.short_entry_prices.get(pair, 0.0)
            if qty <= 0 or entry <= 0:
                return
            pnl_fiat = (entry - price) * qty
            result = self._place_live_order(
                pair=pair, direction='buy', volume=qty, leverage=self.short_leverage, reduce_only=True,
            )
            if result:
                self.trade_count += 1
                now_ts = time.time()
                self.last_trade_at[pair] = now_ts
                self.last_global_trade_at = now_ts
                self._save_cooldown_state()
                self.short_qty[pair] = 0.0
                self.short_entry_prices[pair] = 0.0
                self.entry_timestamps[pair] = None
                self._update_trade_metrics(pair, pnl_fiat)
                print(f"\n[SHORT CLOSE] {qty:.6f} {pair} - Trade #{self.trade_count}")
            else:
                self.logger.error(f"SHORT CLOSE FAILED for {pair}")
        except Exception as e:
            self.logger.error(f"Error closing short order: {e}", exc_info=True)


class Backtester:
    def __init__(self, api_client, config):
        self.api_client = api_client
        self.config = config
        self.logger = logging.getLogger(__name__)

    def _simulate_fill_price_from_orderbook(self, pair, side, volume, fallback_price=None, depth_count=50):
        try:
            ob = self.api_client.get_order_book(pair, count=depth_count)
            if not ob:
                return fallback_price
            data_key = next((k for k in ob if k != 'last'), None)
            if not data_key:
                return fallback_price
            asks = ob[data_key].get('asks', [])
            bids = ob[data_key].get('bids', [])
            stack = asks if side == 'buy' else bids
            if not stack:
                return fallback_price
            remaining = float(volume)
            vwp_numer = 0.0
            vwp_denom = 0.0
            for level in stack:
                lvl_price = float(level[0])
                lvl_vol = float(level[1])
                take = min(remaining, lvl_vol)
                vwp_numer += take * lvl_price
                vwp_denom += take
                remaining -= take
                if remaining <= 1e-12:
                    break
            if vwp_denom <= 0:
                return fallback_price
            fill_price = vwp_numer / vwp_denom
            if remaining > 1e-12:
                worst_price = float(stack[-1][0])
                fill_price = (fill_price * (1 - 0.5 * (remaining / (remaining + vwp_denom))) + worst_price * (0.5 * (remaining / (remaining + vwp_denom))))
            return fill_price
        except Exception:
            return fallback_price

    def run(self):
        import numpy as np
        from datetime import datetime

        print("Backtesting mode activated.")
        pairs = self.config['bot_settings'].get('trade_pairs', ['XBTUSD'])
        bcfg = self.config.get('backtesting', {}) if isinstance(self.config, dict) else {}
        sd = bcfg.get('start_date')
        if sd:
            try:
                start_date = datetime.fromisoformat(str(sd))
            except Exception:
                try:
                    start_date = datetime.strptime(str(sd), '%Y-%m-%d')
                except Exception:
                    start_date = datetime(2024, 1, 1)
        else:
            start_date = datetime(2024, 1, 1)
        interval = int(bcfg.get('interval', 60))
        initial_balance = float(bcfg.get('initial_balance', 1000.0))

        rm = self.config.get('risk_management', {})
        fees_maker_frac = pct_to_frac(rm.get('fees_maker_percent', 0.16))
        fees_taker_frac = pct_to_frac(rm.get('fees_taker_percent', 0.26))
        exit_slippage_frac = pct_to_frac(rm.get('exit_slippage_buffer_pct', 0.35))
        min_net_sell = float(rm.get('min_net_sell_profit_pct', 0.0))
        min_reentry = float(rm.get('min_reentry_profit_pct', 0.0))
        reentry_pairs = [p.upper() for p in rm.get('reentry_guard_pairs', ['VER'])]

        ohlc_data = {}
        for pair in pairs:
            data = self.api_client.get_ohlc_data(pair, interval, int(start_date.timestamp()))
            if not data:
                continue
            if isinstance(data, dict) and pair in data:
                series = data.get(pair, [])
            elif isinstance(data, list):
                series = data
            else:
                series = list(data.values())[0] if isinstance(data, dict) and data else []

            if not series:
                continue
            ohlc_data[pair] = series

        if not ohlc_data:
            print("No data available for backtesting.")
            return

        balance = initial_balance
        positions = {pair: 0.0 for pair in pairs}
        entry_prices = {pair: 0.0 for pair in pairs}
        entry_costs = {pair: 0.0 for pair in pairs}
        last_closed_net = {pair: None for pair in pairs}
        pnls = []
        balances = [initial_balance]
        peak_balance = initial_balance

        analysis = TechnicalAnalysis()
        primary = None
        for p in pairs:
            if p in ohlc_data and ohlc_data[p]:
                primary = p
                break
        if primary is None:
            primary = next(iter(ohlc_data.keys()))
        series_len = len(ohlc_data[primary])

        for i in range(series_len):
            price = float(ohlc_data[primary][i][4])
            market_data = {primary: {'c': [price]}}
            signal, score = analysis.generate_signal_with_score(market_data)

            if signal == 'BUY' and positions[primary] == 0:
                try:
                    if any(g in primary.upper() for g in reentry_pairs) and last_closed_net.get(primary) is not None:
                        if last_closed_net[primary] < min_reentry:
                            continue
                except Exception:
                    pass

                volume = (balance * 0.10) / price if price > 0 else 0.0
                if volume <= 0:
                    continue
                fill_price = self._simulate_fill_price_from_orderbook(primary, 'buy', volume, fallback_price=price)
                try:
                    latency_sec = float(bcfg.get('latency_seconds', 5.0))
                    closes = [float(r[4]) for r in ohlc_data[primary][:i+1]]
                    if len(closes) >= 3:
                        rets = np.diff(closes) / np.array(closes[:-1])
                        per_sec_vol = np.std(rets) / max(1.0, np.sqrt(float(interval)))
                        latency_sigma = per_sec_vol * np.sqrt(latency_sec)
                    else:
                        latency_sigma = 0.0
                    if latency_sigma > 0 and fill_price is not None:
                        fill_price = float(fill_price) * (1.0 + float(np.random.normal(0.0, latency_sigma)))
                except Exception:
                    pass

                cost = volume * (fill_price if fill_price is not None else price)
                buy_fee = cost * fees_maker_frac
                positions[primary] = volume
                entry_prices[primary] = (fill_price if fill_price is not None else price)
                entry_costs[primary] = cost + buy_fee
                balance -= (cost + buy_fee)

            elif signal == 'SELL' and positions[primary] > 0:
                fill_price = self._simulate_fill_price_from_orderbook(primary, 'sell', positions[primary], fallback_price=price)
                try:
                    latency_sec = float(bcfg.get('latency_seconds', 5.0))
                    closes = [float(r[4]) for r in ohlc_data[primary][:i+1]]
                    if len(closes) >= 3:
                        rets = np.diff(closes) / np.array(closes[:-1])
                        per_sec_vol = np.std(rets) / max(1.0, np.sqrt(float(interval)))
                        latency_sigma = per_sec_vol * np.sqrt(latency_sec)
                    else:
                        latency_sigma = 0.0
                    if latency_sigma > 0 and fill_price is not None:
                        fill_price = float(fill_price) * (1.0 + float(np.random.normal(0.0, latency_sigma)))
                except Exception:
                    pass

                if fill_price is None:
                    sell_price_effective = price * (1.0 - exit_slippage_frac)
                else:
                    sell_price_effective = fill_price * (1.0 - 0.0)

                gross_pct = ((sell_price_effective - entry_prices[primary]) / entry_prices[primary]) * 100.0 if entry_prices[primary] > 0 else 0.0
                fees_total_pct = (fees_maker_frac + fees_taker_frac) * 100.0
                net_pct = gross_pct - fees_total_pct

                if min_net_sell > 0 and net_pct < min_net_sell:
                    pass
                else:
                    proceeds_gross = positions[primary] * sell_price_effective
                    sell_fee = proceeds_gross * fees_taker_frac
                    proceeds_net = proceeds_gross - sell_fee
                    balance += proceeds_net
                    pnl = proceeds_net - entry_costs[primary]
                    pnls.append(pnl)
                    last_closed_net[primary] = net_pct
                    positions[primary] = 0.0
                    entry_prices[primary] = 0.0
                    entry_costs[primary] = 0.0

            current_balance = balance + sum(positions[p] * price for p in positions)
            balances.append(current_balance)
            peak_balance = max(peak_balance, current_balance)

        try:
            returns = np.diff(balances) / balances[:-1]
            total_return = (balances[-1] - initial_balance) / initial_balance
            sharpe = np.mean(returns) / np.std(returns) if np.std(returns) > 0 else 0
            downside_returns = returns[returns < 0]
            sortino = np.mean(returns) / np.std(downside_returns) if len(downside_returns) > 0 else 0
        except Exception:
            total_return = (balances[-1] - initial_balance) / initial_balance
            sharpe = 0
            sortino = 0

        print(f"Total Return: {total_return:.2%}")
        print(f"Sharpe Ratio: {sharpe:.2f}")
        print(f"Sortino Ratio: {sortino:.2f}")
        try:
            max_drawdown = max((max(balances[:i+1]) - balances[i]) / max(balances[:i+1]) for i in range(1, len(balances)))
        except Exception:
            max_drawdown = 0.0
        print(f"Max Drawdown: {max_drawdown:.2%}")
        print(f"Total Trades: {len(pnls)}")
        print(f"Win Rate: {sum(1 for p in pnls if p > 0) / len(pnls):.2%}" if pnls else "Win Rate: N/A")
