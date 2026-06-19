"""Bollinger Bands Mean Reversion signal generator pentru live trading.

LONG entry: prev close sub bb_lower, cur close cross-up >= bb_lower, RSI < oversold+10.
SHORT entry: prev close peste bb_upper, cur close cross-down <= bb_upper, RSI > overbought-10.
SL: fix sl_pct.  TP: entry * (1 ± sl_pct * tp_rr).  Time exit: bars_held >= max_bars_in_trade.

API mirror IchimokuSignal:
    sig = BBMeanReversionSignal(pair_cfg)
    sig.warm_up(df_with_history)
    decision = sig.evaluate(has_position, entry_price, bars_held)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

import numpy as np
import pandas as pd

from strategies.ichimoku_signal import SignalDecision  # reuse common dataclass


@dataclass
class BBMRConfig:
    """Subset relevant pentru live signal — copiat din PairConfig."""
    symbol: str
    timeframe: str = "4h"
    bb_length: int = 26
    bb_std: float = 3.0
    rsi_length: int = 14
    rsi_oversold: float = 20.0
    rsi_overbought: float = 80.0
    sl_pct: float = 0.06
    tp_rr: float = 1.75
    max_bars_in_trade: int = 40
    taker_fee: float = 0.00055

    @property
    def min_history_bars(self) -> int:
        return max(self.bb_length, self.rsi_length) + 10


# ============================================================================
# INDICATORI vectorizati
# ============================================================================

def _sma(arr: np.ndarray, length: int) -> np.ndarray:
    n = len(arr); res = np.full(n, np.nan)
    for i in range(length - 1, n):
        win = arr[i - length + 1: i + 1]
        if not np.any(np.isnan(win)):
            res[i] = np.mean(win)
    return res


def _stdev(arr: np.ndarray, length: int) -> np.ndarray:
    n = len(arr); res = np.full(n, np.nan)
    for i in range(length - 1, n):
        win = arr[i - length + 1: i + 1]
        if not np.any(np.isnan(win)):
            res[i] = np.std(win, ddof=0)
    return res


def _rsi_pine(arr: np.ndarray, length: int) -> np.ndarray:
    n = len(arr); res = np.full(n, np.nan)
    if n < length + 1:
        return res
    diff = np.diff(arr, prepend=arr[0])
    gain = np.where(diff > 0, diff, 0)
    loss = np.where(diff < 0, -diff, 0)
    avg_gain = np.full(n, np.nan); avg_loss = np.full(n, np.nan)
    avg_gain[length] = np.mean(gain[1:length + 1])
    avg_loss[length] = np.mean(loss[1:length + 1])
    for i in range(length + 1, n):
        avg_gain[i] = (avg_gain[i - 1] * (length - 1) + gain[i]) / length
        avg_loss[i] = (avg_loss[i - 1] * (length - 1) + loss[i]) / length
    with np.errstate(divide='ignore', invalid='ignore'):
        rs = avg_gain / avg_loss
        res = 100 - 100 / (1 + rs)
    return res


@dataclass
class BBMRCache:
    bb_mid: np.ndarray
    bb_upper: np.ndarray
    bb_lower: np.ndarray
    rsi: np.ndarray


def precompute_indicators(df: pd.DataFrame, cfg: BBMRConfig) -> BBMRCache:
    close = df["close"].to_numpy()
    mid = _sma(close, cfg.bb_length)
    sd = _stdev(close, cfg.bb_length)
    return BBMRCache(
        bb_mid=mid,
        bb_upper=mid + sd * cfg.bb_std,
        bb_lower=mid - sd * cfg.bb_std,
        rsi=_rsi_pine(close, cfg.rsi_length),
    )


# ============================================================================
# LIVE SIGNAL CLASS
# ============================================================================

class BBMeanReversionSignal:
    """Live BB MR signal generator — mirror al IchimokuSignal."""

    def __init__(self, cfg: BBMRConfig) -> None:
        self.cfg = cfg
        self.df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        self.cache: BBMRCache | None = None

    def warm_up(self, df: pd.DataFrame) -> None:
        keep = max(500, self.cfg.min_history_bars + 50)
        self.df = df.tail(keep).copy()
        self.recompute_indicators()

    def update_buffer(self, bar: dict) -> None:
        ts = pd.Timestamp(bar["ts_ms"], unit="ms", tz="UTC")
        new_row = pd.DataFrame([{
            "open": bar["open"], "high": bar["high"], "low": bar["low"],
            "close": bar["close"], "volume": bar.get("volume", 0.0),
        }], index=[ts])
        if not self.df.empty and self.df.index[-1] == ts:
            self.df.iloc[-1] = new_row.iloc[0]
        else:
            self.df = pd.concat([self.df, new_row])
        keep = max(500, self.cfg.min_history_bars + 50)
        if len(self.df) > keep:
            self.df = self.df.tail(keep).copy()

    def recompute_indicators(self) -> None:
        if len(self.df) < self.cfg.min_history_bars:
            self.cache = None
            return
        self.cache = precompute_indicators(self.df, self.cfg)

    def _last_idx_valid(self) -> bool:
        if self.cache is None:
            return False
        i = len(self.df) - 1
        if i < self.cfg.min_history_bars:
            return False
        c = self.cache
        return not any(np.isnan(x) for x in
                       [c.bb_mid[i], c.bb_upper[i], c.bb_lower[i], c.rsi[i]])

    def evaluate(self, has_position: Optional[Literal["long", "short"]],
                 entry_price: float = 0.0, bars_held: int = 0) -> SignalDecision:
        """Decizie pe bara curenta confirmed.

        bars_held: bare scurse de la entry — necesar pentru time-exit BB MR.
        """
        if not self._last_idx_valid():
            return SignalDecision("HOLD", 0.0, "indicators_not_ready")
        i = len(self.df) - 1
        c = self.cache
        cur = self.df.iloc[i]
        close = float(cur["close"]); high = float(cur["high"]); low = float(cur["low"])
        bb_lower_now = c.bb_lower[i]; bb_upper_now = c.bb_upper[i]
        rsi_v = c.rsi[i]
        prev = self.df.iloc[i - 1]
        prev_close = float(prev["close"])
        bb_lower_prev = c.bb_lower[i - 1]; bb_upper_prev = c.bb_upper[i - 1]

        # Strat 2 — Guard entry_price > 0 inainte de orice calcul SL/TP.
        # Defense-in-depth: state corupt cu entry=0 ar produce sl=0 → SHORT
        # high>=0 trigger imediat fiecare bara. Refuzam evaluarea.
        if has_position is not None and (entry_price is None or entry_price <= 0):
            return SignalDecision("HOLD", close, "invalid_entry_price")

        # EXIT
        if has_position == "long":
            sl_price = entry_price * (1 - self.cfg.sl_pct)
            tp_price = entry_price * (1 + self.cfg.sl_pct * self.cfg.tp_rr)
            if low <= sl_price:
                return SignalDecision("SL_LONG", sl_price, f"sl_{self.cfg.sl_pct*100:.1f}pct_hit")
            if high >= tp_price:
                return SignalDecision("TP_LONG", tp_price, f"tp_{self.cfg.tp_rr:.2f}R_hit")
            if bars_held >= self.cfg.max_bars_in_trade:
                return SignalDecision("CLOSE_LONG", close, "max_bars_time_exit")
            return SignalDecision("HOLD", close, "in_long")

        if has_position == "short":
            sl_price = entry_price * (1 + self.cfg.sl_pct)
            tp_price = entry_price * (1 - self.cfg.sl_pct * self.cfg.tp_rr)
            if high >= sl_price:
                return SignalDecision("SL_SHORT", sl_price, f"sl_{self.cfg.sl_pct*100:.1f}pct_hit")
            if low <= tp_price:
                return SignalDecision("TP_SHORT", tp_price, f"tp_{self.cfg.tp_rr:.2f}R_hit")
            if bars_held >= self.cfg.max_bars_in_trade:
                return SignalDecision("CLOSE_SHORT", close, "max_bars_time_exit")
            return SignalDecision("HOLD", close, "in_short")

        # ENTRY
        cross_up_lower = (prev_close < bb_lower_prev and close >= bb_lower_now)
        cross_dn_upper = (prev_close > bb_upper_prev and close <= bb_upper_now)
        long_sig = cross_up_lower and rsi_v < (self.cfg.rsi_oversold + 10)
        short_sig = cross_dn_upper and rsi_v > (self.cfg.rsi_overbought - 10)
        if long_sig:
            return SignalDecision("OPEN_LONG", close, "bb_lower_reversal")
        if short_sig:
            return SignalDecision("OPEN_SHORT", close, "bb_upper_reversal")
        return SignalDecision("HOLD", close, "no_signal")
