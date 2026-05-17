# Technical Analysis Module for Trading Signals
"""
Technical Analysis Module
=========================
Provides ``TechnicalAnalysis`` — the signal engine used by ``TradingBot``.
"""

import logging
import numpy as np
import os
import json
from collections import deque


class TechnicalAnalysis:
    """
    Technical analysis tool for generating trading signals based on market data.
    Supports multi-pair analysis with separate price history per pair.
    """

    def __init__(self, rsi_period=14, sma_short=20, sma_long=50, min_volatility_pct=0.15):
        self.rsi_period = rsi_period
        self.sma_short = sma_short
        self.sma_long = sma_long
        self.min_volatility_pct = min_volatility_pct
        self.logger = logging.getLogger(__name__)
        self.pair_price_history = {}
        self.max_history = 200  # 200 ticks → SMA50 uses 50/200
        self.buffer_path = os.path.join(os.path.dirname(__file__), 'data', 'history_buffer.json')

        # Signal engine mode flags
        self.enable_mr_signals = True
        self.enable_trend_signals = True
        self.mr_rsi_buy = 33.0
        self.mr_rsi_sell = 67.0

        self._last_save_ts = 0.0
        self._save_interval_sec = 300.0
        self._load_history()

    def _get_price_history(self, pair):
        if pair not in self.pair_price_history:
            self.pair_price_history[pair] = deque(maxlen=self.max_history)
        return self.pair_price_history[pair]

    def _load_history(self):
        try:
            if os.path.exists(self.buffer_path):
                with open(self.buffer_path, 'r') as f:
                    data = json.load(f)
                for pair, prices in data.items():
                    self.pair_price_history[pair] = deque(prices, maxlen=self.max_history)
                self.logger.info(f"Loaded price history for {len(data)} pairs from buffer")
        except Exception as e:
            self.logger.error(f"Error loading price history buffer: {e}")

    def _save_history(self, force: bool = False):
        import time as _time
        now = _time.time()
        if not force and (now - self._last_save_ts) < self._save_interval_sec:
            return
        try:
            import tempfile
            os.makedirs(os.path.dirname(self.buffer_path), exist_ok=True)
            data = {pair: list(prices) for pair, prices in self.pair_price_history.items()}
            dir_path = os.path.dirname(self.buffer_path)
            fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix='.tmp')
            try:
                with os.fdopen(fd, 'w') as f:
                    json.dump(data, f)
                os.replace(tmp_path, self.buffer_path)
            except Exception:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise
            self._last_save_ts = now
        except Exception as e:
            self.logger.error(f"Error saving price history buffer: {e}")

    def seed_from_nas_ohlc(self, pair, nas_root):
        """Seed price history from NAS 5-minute OHLC CSV files.
        Dynamically resolves folder names for ANY coin (Blue-chip, Alt, or Memecoin).
        """
        import csv
        from pathlib import Path
        import datetime

        history = self._get_price_history(pair)
        if len(history) >= self.max_history:
            return  # buffer already full

        # 1. Clean the pair name to uppercase standard
        p_clean = pair.upper().replace('/', '')

        # 2. Check strict legacy overrides first
        legacy_map = {
            'XBTUSD': 'XXBTZUSD', 'XXBTZUSD': 'XXBTZUSD',
            'XBTEUR': 'XXBTZEUR', 'XXBTZEUR': 'XXBTZEUR',
            'ETHUSD': 'XETHZUSD', 'XETHZUSD': 'XETHZUSD',
            'ETHEUR': 'XETHZEUR', 'XETHZEUR': 'XETHZEUR',
            'XRPUSD': 'XXRPZUSD', 'XXRPZUSD': 'XXRPZUSD',
            'XRPEUR': 'XXRPZEUR', 'XXRPZEUR': 'XXRPZEUR',
        }

        if p_clean in legacy_map:
            folder = legacy_map[p_clean]
        else:
            # 3. Dynamic strip for all modern alts/memecoins (FWOGUSD -> FWOG)
            folder = p_clean
            for suffix in ['USD', 'USDT', 'EUR', 'ZUSD', 'ZEUR']:
                if folder.endswith(suffix) and folder != suffix:
                    folder = folder[:-len(suffix)]
                    break

        year = datetime.datetime.utcnow().year

        # Try the stripped asset folder first (e.g., .../2026/FWOG/ohlc_5m.csv)
        csv_path = Path(nas_root) / str(year) / folder / 'ohlc_5m.csv'

        # Fallback: Check if the folder was saved with the full name (e.g., .../2026/FWOGUSD/ohlc_5m.csv)
        if not csv_path.exists():
            csv_path = Path(nas_root) / str(year) / p_clean / 'ohlc_5m.csv'

        if not csv_path.exists():
            return

        try:
            closes = []
            with open(csv_path, newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        closes.append(float(row['close']))
                    except (KeyError, ValueError):
                        continue
            if not closes:
                return

            needed = self.max_history - len(history)
            nas_closes = closes[-needed:] if needed < len(closes) else closes
            existing = list(history)
            history.clear()
            for c in nas_closes:
                history.append(c)
            for c in existing:
                history.append(c)
            self._save_history(force=True)
            self.logger.info(f"[NAS seed] {pair}: prepended {len(nas_closes)} closes → buffer={len(history)}/{self.max_history}")
        except Exception as e:
            self.logger.warning(f"[NAS seed] {pair}: failed to seed from {csv_path}: {e}")

    def seed_from_ohlc(self, pair, closes):
        """Pre-populate price history from OHLC candle closes."""
        history = self._get_price_history(pair)
        if len(history) >= self.sma_long:
            return
        for c in closes[-self.max_history:]:
            history.append(float(c))
        self._save_history(force=True)
        self.logger.info(f"[OHLC seed] {pair}: seeded {len(history)} closes from 15m candles")

    def calculate_ema_crossover(self, prices, fast=9, slow=21):
        if len(prices) < slow + 1:
            return None, None, None
        arr = [float(p) for p in prices]

        def _ema(data, period):
            k = 2.0 / (period + 1)
            result = [data[0]]
            for p in data[1:]:
                result.append(p * k + result[-1] * (1 - k))
            return result

        fast_arr = _ema(arr, fast)
        slow_arr = _ema(arr, slow)
        return fast_arr[-1], slow_arr[-1], fast_arr[-1] > slow_arr[-1]

    def calculate_macd(self, prices, fast=12, slow=26, signal_period=9):
        min_len = slow + signal_period
        if len(prices) < min_len:
            return None, None, None
        arr = np.array(prices, dtype=float)

        def _ema(a, period):
            k = 2.0 / (period + 1)
            out = np.empty(len(a))
            out[0] = a[0]
            for i in range(1, len(a)):
                out[i] = a[i] * k + out[i - 1] * (1 - k)
            return out

        ema_fast = _ema(arr, fast)
        ema_slow = _ema(arr, slow)
        macd_line = ema_fast - ema_slow
        signal_line = _ema(macd_line, signal_period)
        histogram = macd_line - signal_line
        return float(macd_line[-1]), float(signal_line[-1]), float(histogram[-1])

    def calculate_rsi(self, prices):
        if len(prices) < self.rsi_period + 1:
            return None
        prices = np.array(prices)
        deltas = np.diff(prices)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains[-self.rsi_period:])
        avg_loss = np.mean(losses[-self.rsi_period:])
        if avg_loss == 0:
            return 100 if avg_gain > 0 else 0
        rs = avg_gain / avg_loss
        return 100 - (100 / (1 + rs))

    def calculate_atr(self, pair, period=20):
        prices = self._get_price_history(pair)
        sampled = list(prices)[::4]
        if len(sampled) < period + 1:
            return None
        tr = [abs(sampled[i] - sampled[i-1]) for i in range(1, len(sampled))]
        return np.mean(tr[-period:])

    def check_mtf_trend(self, prices, short_p=20, long_p=50):
        if len(prices) < long_p:
            return True
        sma_short = np.mean(prices[-short_p:])
        sma_long = np.mean(prices[-long_p:])
        return sma_short > sma_long

    def generate_signal(self, market_data):
        signal, _ = self.generate_signal_with_score(market_data)
        return signal

    def generate_signal_with_score(self, market_data):
        try:
            if not market_data:
                return "HOLD", 0

            pair_key = list(market_data.keys())[0]
            pair_data = market_data[pair_key]
            if 'c' not in pair_data:
                return "HOLD", 0

            close_price = float(pair_data['c'][0])
            price_history = self._get_price_history(pair_key)
            price_history.append(close_price)
            self._save_history()

            if len(price_history) < self.sma_long:
                return "HOLD", 0

            prices = np.array(list(price_history))

            sma20 = np.mean(prices[-20:])
            std20 = np.std(prices[-20:])
            sma50 = np.mean(prices[-50:])

            upper_bb = sma20 + (2.0 * std20)
            lower_bb = sma20 - (2.0 * std20)

            current_price = prices[-1]
            signal = "HOLD"
            score = 0.0

            rsi_confirm = self.calculate_rsi(list(price_history)[-20:]) if len(price_history) >= 20 else None
            rsi_full = self.calculate_rsi(list(price_history)) if len(price_history) >= self.rsi_period + 1 else None
            sma_ratio = (sma20 - sma50) / sma50 if sma50 > 0 else 0.0

            # Mean-reversion path
            if self.enable_mr_signals and rsi_full is not None:
                rsi_s = 0.0
                buy_t = getattr(self, 'mr_rsi_buy', 33)
                sell_t = getattr(self, 'mr_rsi_sell', 67)
                if rsi_full <= buy_t:
                    rsi_s = (buy_t - rsi_full) / max(buy_t, 1) * 50
                elif rsi_full >= sell_t:
                    rsi_s = -((rsi_full - sell_t) / max(100 - sell_t, 1) * 50)
                sma_s = max(-50.0, min(50.0, sma_ratio * 100 * 10))
                mr_score = rsi_s + sma_s
                if rsi_full <= self.mr_rsi_buy and sma_ratio > -0.01:
                    signal = "BUY"
                    score = mr_score
                elif rsi_full >= self.mr_rsi_sell and sma_ratio < 0.01:
                    signal = "SELL"
                    score = mr_score

            # Trend/breakout path
            if self.enable_trend_signals:
                if current_price > upper_bb:
                    if current_price > sma50 and (rsi_confirm is None or rsi_confirm >= 55):
                        trend_score = min(50.0, 25.0 + (((current_price - upper_bb) / upper_bb) * 100 * 50.0))
                        if trend_score > score:
                            signal = "BUY"
                            score = trend_score
                    elif current_price > sma50 and score == 0.0:
                        score = 8.0
                elif current_price < lower_bb:
                    if current_price < sma50 and (rsi_confirm is None or rsi_confirm <= 45):
                        trend_score = max(-50.0, -25.0 - (((lower_bb - current_price) / lower_bb) * 100 * 50.0))
                        if trend_score < score:
                            signal = "SELL"
                            score = trend_score
                    elif current_price < sma50 and score == 0.0:
                        score = -8.0

            score = max(-50.0, min(50.0, score))

            atr = None
            willr = None
            try:
                tr = np.abs(np.diff(prices))
                atr = float(np.mean(tr[-14:])) if len(tr) >= 14 else None
            except Exception:
                atr = None

            try:
                window = 14
                if len(prices) >= window:
                    high_w = np.max(prices[-window:])
                    low_w = np.min(prices[-window:])
                    willr = (high_w - current_price) / (high_w - low_w) * -100 if (high_w - low_w) != 0 else None
            except Exception:
                willr = None

            if atr is not None and willr is not None:
                if current_price > upper_bb and willr < -20:
                    score += min(8.0, (atr / max(1e-6, sma20)) * 100.0)
                if current_price < lower_bb and willr > -80:
                    score -= min(8.0, (atr / max(1e-6, sma20)) * 100.0)

            # MACD momentum filter
            try:
                if len(prices) >= 35:
                    macd_val, macd_sig, macd_hist = self.calculate_macd(list(prices))
                    if macd_val is not None and macd_hist is not None:
                        _, _, hist_prev = self.calculate_macd(list(prices[:-1])) if len(prices) > 35 else (None, None, None)
                        hist_rising = (hist_prev is not None) and (macd_hist > hist_prev)

                        if signal == "BUY":
                            if macd_hist < 0:
                                penalty = min(12.0, abs(macd_hist) / max(1e-6, abs(current_price)) * 100.0 * 500)
                                score -= penalty
                            elif hist_rising and macd_hist > 0:
                                boost = min(6.0, macd_hist / max(1e-6, abs(current_price)) * 100.0 * 300)
                                score += boost

                        elif signal == "SELL":
                            if macd_hist > 0:
                                penalty = min(12.0, abs(macd_hist) / max(1e-6, abs(current_price)) * 100.0 * 500)
                                score += penalty
                            elif not hist_rising and macd_hist < 0:
                                boost = min(6.0, abs(macd_hist) / max(1e-6, abs(current_price)) * 100.0 * 300)
                                score -= boost
            except Exception:
                pass

            score = max(-50.0, min(50.0, score))
            return signal, score

        except Exception as e:
            self.logger.error(f"Error generating signal: {e}")
            return "HOLD", 0
