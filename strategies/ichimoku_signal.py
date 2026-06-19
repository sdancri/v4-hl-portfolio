"""Hull + Ichimoku signal generator pentru live trading.

Per-pair config (Hull length, Kijun period, SnkB period, TP, sizing).
Indicatori vectorizati pe rolling buffer (~400 bare); recomputed la fiecare
bara confirmata.

API:
    sig = IchimokuSignal(pair_cfg)
    sig.warm_up(df_400)          # bare istorice 4h
    decision = sig.on_bar(bar)   # 'OPEN_LONG'/'OPEN_SHORT'/'CLOSE_LONG'/...
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional

import numpy as np
import pandas as pd


# ============================================================================
# CONFIG (per pereche)
# ============================================================================

@dataclass
class PairStrategyConfig:
    symbol: str
    timeframe: str = "4h"
    # Hull MA
    hull_length: int = 8
    # Ichimoku
    tenkan_periods: int = 9
    kijun_periods: int = 48
    senkou_b_periods: int = 40
    displacement: int = 24
    # Sizing
    risk_pct_per_trade: float = 0.05
    sl_initial_pct: float = 0.05
    tp_pct: Optional[float] = None
    # Smart filters
    max_hull_spread_pct: float = 2.0
    max_close_kijun_dist_pct: float = 6.0
    # Fees
    taker_fee: float = 0.00055

    @property
    def min_history_bars(self) -> int:
        return max(self.hull_length, self.kijun_periods,
                   self.senkou_b_periods, self.displacement) + 5


# ============================================================================
# INDICATORI vectorizati
# ============================================================================

def _wma(arr: np.ndarray, length: int) -> np.ndarray:
    n = len(arr)
    res = np.full(n, np.nan)
    if length <= 0 or n < length:
        return res
    weights = np.arange(1, length + 1, dtype=float)
    weight_sum = weights.sum()
    for i in range(length - 1, n):
        window = arr[i - length + 1: i + 1]
        if not np.any(np.isnan(window)):
            res[i] = np.dot(window, weights) / weight_sum
    return res


def hull_double(close: np.ndarray, length: int) -> tuple[np.ndarray, np.ndarray]:
    """Double Hull MA — n1 (curent) si n2 (lag 1)."""
    half_len = max(1, length // 2)
    sqn = max(1, int(np.sqrt(length)))
    n2ma = 2 * _wma(close, half_len)
    nma = _wma(close, length)
    n1 = _wma(n2ma - nma, sqn)
    close_lag = np.roll(close, 1)
    close_lag[0] = close[0]
    n2ma1 = 2 * _wma(close_lag, half_len)
    nma1 = _wma(close_lag, length)
    n2 = _wma(n2ma1 - nma1, sqn)
    return n1, n2


def _donchian_avg(high: np.ndarray, low: np.ndarray, length: int) -> np.ndarray:
    n = len(high)
    res = np.full(n, np.nan)
    for i in range(length - 1, n):
        hh = np.max(high[i - length + 1: i + 1])
        ll = np.min(low[i - length + 1: i + 1])
        res[i] = (hh + ll) / 2
    return res


@dataclass
class IndicatorCache:
    n1: np.ndarray
    n2: np.ndarray
    tenkan: np.ndarray
    kijun: np.ndarray
    senkou_h: np.ndarray
    senkou_l: np.ndarray
    chikou: np.ndarray


def precompute_indicators(df: pd.DataFrame, cfg: PairStrategyConfig) -> IndicatorCache:
    high = df["high"].to_numpy()
    low = df["low"].to_numpy()
    close = df["close"].to_numpy()
    n = len(close)

    n1, n2 = hull_double(close, cfg.hull_length)
    tenkan = _donchian_avg(high, low, cfg.tenkan_periods)
    kijun = _donchian_avg(high, low, cfg.kijun_periods)
    senkou_a = (tenkan + kijun) / 2
    senkou_b = _donchian_avg(high, low, cfg.senkou_b_periods)

    shift = cfg.displacement - 1
    senkou_h = np.full(n, np.nan)
    senkou_l = np.full(n, np.nan)
    chikou = np.full(n, np.nan)
    for i in range(shift, n):
        sa, sb = senkou_a[i - shift], senkou_b[i - shift]
        if not np.isnan(sa) and not np.isnan(sb):
            senkou_h[i] = max(sa, sb)
            senkou_l[i] = min(sa, sb)
        chikou[i] = close[i - shift]

    return IndicatorCache(n1=n1, n2=n2, tenkan=tenkan, kijun=kijun,
                          senkou_h=senkou_h, senkou_l=senkou_l, chikou=chikou)


# ============================================================================
# SIGNAL CHECKS (Pine logic)
# ============================================================================

def _long_entry(close, n1, n2, tk, kj, sh, ch) -> bool:
    return (n1 > n2 and close > n2 and close > ch and close > sh
            and (tk >= kj or close > kj))


def _short_entry(close, n1, n2, tk, kj, sl_, ch) -> bool:
    return (n1 < n2 and close < n2 and close < ch and close < sl_
            and (tk <= kj or close < kj))


def _close_long(close, n1, n2, tk, kj, sh, ch) -> bool:
    return (n1 < n2 and (close < n2 or tk < kj or close < tk
                         or close < kj or close < sh or close < ch))


def _close_short(close, n1, n2, tk, kj, sl_, ch) -> bool:
    return (n1 > n2 and (close > n2 or tk > kj or close > tk
                         or close > kj or close > sl_ or close > ch))


def passes_filters(close: float, n1: float, n2: float, kijun: float,
                   cfg: PairStrategyConfig) -> tuple[bool, str]:
    if close <= 0:
        return False, "invalid_price"
    spread = abs(n1 - n2) / close * 100
    if spread > cfg.max_hull_spread_pct:
        return False, f"hull_spread {spread:.2f}% > {cfg.max_hull_spread_pct}%"
    dist = abs(close - kijun) / close * 100
    if dist > cfg.max_close_kijun_dist_pct:
        return False, f"kijun_dist {dist:.2f}% > {cfg.max_close_kijun_dist_pct}%"
    return True, "ok"


# ============================================================================
# DECISION (intoarce strategy.py decision pe bara confirmed)
# ============================================================================

@dataclass
class SignalDecision:
    action: Literal["HOLD", "OPEN_LONG", "OPEN_SHORT", "CLOSE_LONG", "CLOSE_SHORT", "TP_LONG", "TP_SHORT", "SL_LONG", "SL_SHORT"]
    price: float
    reason: str = ""


# ============================================================================
# LIVE SIGNAL CLASS
# ============================================================================

class IchimokuSignal:
    """Signal generator + position state pentru o pereche.

    Setup:
        sig = IchimokuSignal(pair_cfg)
        sig.warm_up(df_with_400_bars)

    Per bar:
        sig.update_buffer(bar_dict)            # adauga bara confirmed
        sig.recompute_indicators()             # recalcul rolling
        decision = sig.evaluate_for_position(position_dict_or_None)
    """

    def __init__(self, cfg: PairStrategyConfig) -> None:
        self.cfg = cfg
        self.df = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        self.cache: IndicatorCache | None = None

    def warm_up(self, df: pd.DataFrame) -> None:
        """Initializeaza buffer-ul cu istoricul (>= min_history_bars bare)."""
        keep = max(500, self.cfg.min_history_bars + 50)
        self.df = df.tail(keep).copy()
        self.recompute_indicators()

    def update_buffer(self, bar: dict) -> None:
        """Adauga bara confirmed in buffer (idempotent: dedup pe ts)."""
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
                       [c.n1[i], c.n2[i], c.tenkan[i], c.kijun[i],
                        c.senkou_h[i], c.senkou_l[i], c.chikou[i]])

    def evaluate(self, has_position: Optional[Literal["long", "short"]],
                 entry_price: float = 0.0) -> SignalDecision:
        """Returneaza decizia pe bara curenta confirmed.

        has_position: None | 'long' | 'short' — daca exista pozitie deschisa
        entry_price: pentru calcul TP target intra-bar (daca tp_pct setat)
        """
        if not self._last_idx_valid():
            return SignalDecision("HOLD", 0.0, "indicators_not_ready")
        i = len(self.df) - 1
        c = self.cache
        cur = self.df.iloc[i]
        close = float(cur["close"]); high = float(cur["high"]); low = float(cur["low"])
        n1 = c.n1[i]; n2 = c.n2[i]
        tk = c.tenkan[i]; kj = c.kijun[i]
        sh = c.senkou_h[i]; sl_ = c.senkou_l[i]; ch = c.chikou[i]

        # Strat 2 — Guard entry_price > 0 inainte de orice calcul SL/TP.
        # Defense-in-depth: daca cineva alimenteaza state cu entry=0 (state
        # corupt, fork modificat al on_resume), evitam fake SL trigger pe
        # SHORT (sl=0, high>=0 → trigger imediat fiecare bara) si comportament
        # nedeterminat pe LONG.
        if has_position is not None and (entry_price is None or entry_price <= 0):
            return SignalDecision("HOLD", close, "invalid_entry_price")

        # EXIT
        if has_position == "long":
            sl_price = entry_price * (1 - self.cfg.sl_initial_pct)
            if low <= sl_price:
                return SignalDecision("SL_LONG", sl_price, "sl_5pct_hit")
            if self.cfg.tp_pct is not None:
                tp_price = entry_price * (1 + self.cfg.tp_pct)
                if high >= tp_price:
                    return SignalDecision("TP_LONG", tp_price, f"tp_{self.cfg.tp_pct*100:.0f}pct_hit")
            if _close_long(close, n1, n2, tk, kj, sh, ch):
                return SignalDecision("CLOSE_LONG", close, "hull_ichimoku_close_long")
            return SignalDecision("HOLD", close, "in_long")

        if has_position == "short":
            sl_price = entry_price * (1 + self.cfg.sl_initial_pct)
            if high >= sl_price:
                return SignalDecision("SL_SHORT", sl_price, "sl_5pct_hit")
            if self.cfg.tp_pct is not None:
                tp_price = entry_price * (1 - self.cfg.tp_pct)
                if low <= tp_price:
                    return SignalDecision("TP_SHORT", tp_price, f"tp_{self.cfg.tp_pct*100:.0f}pct_hit")
            if _close_short(close, n1, n2, tk, kj, sl_, ch):
                return SignalDecision("CLOSE_SHORT", close, "hull_ichimoku_close_short")
            return SignalDecision("HOLD", close, "in_short")

        # ENTRY
        ls = _long_entry(close, n1, n2, tk, kj, sh, ch)
        ss = _short_entry(close, n1, n2, tk, kj, sl_, ch)
        if ls or ss:
            ok, why = passes_filters(close, n1, n2, kj, self.cfg)
            if not ok:
                return SignalDecision("HOLD", close, f"filter_blocked: {why}")
            if ls:
                return SignalDecision("OPEN_LONG", close, "hull_ichimoku_long")
            return SignalDecision("OPEN_SHORT", close, "hull_ichimoku_short")
        return SignalDecision("HOLD", close, "no_signal")
