"""Config loader pentru V4 bot — multi-strategy (BB MR + Hull+Ichimoku).

Schema:
- per-pair: strategy ('hi' | 'bb_mr'), pe baza careia se folosesc fieldurile relevante
- shared equity pool (compound): equity = pool_total + cumulative PnL
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Optional

import yaml


@dataclass
class PortfolioConfig:
    """Setari globale portfolio (subaccount)."""
    name: str
    pool_total: float                   # capital initial pe subaccount (USDT)
    leverage: int = 15                  # default per-pair fallback
    leverage_max: int = 12              # SAFETY cap pentru cap_usd calculation
    cap_pct_of_max: float = 0.95
    taker_fee: float = 0.00055
    slippage_bps: float = 0.0


@dataclass
class PairConfig:
    """Per-pair: strategy-agnostic + per-strategy params."""
    symbol: str
    timeframe: str = "4h"
    enabled: bool = True
    # Strategy selector: 'hi' = Hull+Ichimoku, 'bb_mr' = BollingerBands Mean Reversion
    strategy: Literal["hi", "bb_mr"] = "hi"

    # === Hull+Ichimoku params (strategy='hi') ===
    hull_length: int = 8
    tenkan_periods: int = 9
    kijun_periods: int = 48
    senkou_b_periods: int = 40
    displacement: int = 24
    max_hull_spread_pct: float = 2.0
    max_close_kijun_dist_pct: float = 6.0

    # === BB Mean Reversion params (strategy='bb_mr') ===
    bb_length: int = 26
    bb_std: float = 3.0
    rsi_length: int = 14
    rsi_oversold: float = 20.0
    rsi_overbought: float = 80.0
    tp_rr: float = 1.75                 # TP la SL_dist * tp_rr (BB MR)
    max_bars_in_trade: int = 40         # Time exit (BB MR)

    # === Sizing (comun ambelor strategii) ===
    risk_pct_per_trade: float = 0.10    # % din shared equity / trade
    sl_initial_pct: float = 0.05        # SL fix HI
    sl_pct: float = 0.06                # SL fix BB MR (poate diferi de HI)
    tp_pct: Optional[float] = None      # TP fix optional pt HI (ex 0.12 = 12%)

    # Per-pair leverage (Bybit isolated). Override pe portfolio.leverage.
    leverage: Optional[int] = None

    @property
    def effective_sl_pct(self) -> float:
        """SL effective per strategy (folosit la sizing si SL price calc)."""
        return self.sl_pct if self.strategy == "bb_mr" else self.sl_initial_pct


@dataclass
class OperationalConfig:
    max_concurrent_positions: int = 3
    max_consecutive_api_errors: int = 5
    heartbeat_interval_seconds: int = 60
    save_state_interval_seconds: int = 300
    state_dir: Path = Path("./state")
    log_dir: Path = Path("./logs")


@dataclass
class AppConfig:
    portfolio: PortfolioConfig
    pairs: list[PairConfig]
    operational: OperationalConfig
    raw: dict[str, Any] = field(default_factory=dict)

    def leverage_for(self, pair_cfg: "PairConfig") -> int:
        """Leverage efectiv pentru o pereche: pair override sau portfolio default."""
        return pair_cfg.leverage if pair_cfg.leverage is not None else self.portfolio.leverage


def load_config(path: str | Path = "config/config.yaml") -> AppConfig:
    p = Path(path)
    with p.open() as f:
        raw = yaml.safe_load(f)

    pf = raw["portfolio"]
    portfolio = PortfolioConfig(
        name=pf["name"],
        pool_total=float(pf["pool_total"]),
        leverage=int(pf.get("leverage", 15)),
        leverage_max=int(pf.get("leverage_max", 12)),
        cap_pct_of_max=float(pf.get("cap_pct_of_max", 0.95)),
        taker_fee=float(pf.get("taker_fee", 0.00055)),
        slippage_bps=float(pf.get("slippage_bps", 0.0)),
    )

    pairs = []
    for pc in raw["pairs"]:
        strat = pc.get("strategy", "hi")
        if strat not in ("hi", "bb_mr"):
            raise ValueError(f"Invalid strategy '{strat}' for {pc.get('symbol')} — use 'hi' or 'bb_mr'")
        pairs.append(PairConfig(
            symbol=pc["symbol"],
            timeframe=pc.get("timeframe", "4h"),
            enabled=bool(pc.get("enabled", True)),
            strategy=strat,
            # HI
            hull_length=int(pc.get("hull_length", 8)),
            tenkan_periods=int(pc.get("tenkan_periods", 9)),
            kijun_periods=int(pc.get("kijun_periods", 48)),
            senkou_b_periods=int(pc.get("senkou_b_periods", 40)),
            displacement=int(pc.get("displacement", 24)),
            max_hull_spread_pct=float(pc.get("max_hull_spread_pct", 2.0)),
            max_close_kijun_dist_pct=float(pc.get("max_close_kijun_dist_pct", 6.0)),
            # BB MR
            bb_length=int(pc.get("bb_length", 26)),
            bb_std=float(pc.get("bb_std", 3.0)),
            rsi_length=int(pc.get("rsi_length", 14)),
            rsi_oversold=float(pc.get("rsi_oversold", 20.0)),
            rsi_overbought=float(pc.get("rsi_overbought", 80.0)),
            tp_rr=float(pc.get("tp_rr", 1.75)),
            max_bars_in_trade=int(pc.get("max_bars_in_trade", 40)),
            # Sizing
            risk_pct_per_trade=float(pc.get("risk_pct_per_trade", 0.10)),
            sl_initial_pct=float(pc.get("sl_initial_pct", 0.05)),
            sl_pct=float(pc.get("sl_pct", 0.06)),
            tp_pct=pc.get("tp_pct"),
            leverage=(int(pc["leverage"]) if pc.get("leverage") is not None else None),
        ))

    op = raw.get("operational", {})
    operational = OperationalConfig(
        max_concurrent_positions=int(op.get("max_concurrent_positions", 3)),
        max_consecutive_api_errors=int(op.get("max_consecutive_api_errors", 5)),
        heartbeat_interval_seconds=int(op.get("heartbeat_interval_seconds", 60)),
        save_state_interval_seconds=int(op.get("save_state_interval_seconds", 300)),
        state_dir=Path(op.get("state_dir", "./state")),
        log_dir=Path(op.get("log_dir", "./logs")),
    )

    return AppConfig(portfolio=portfolio, pairs=pairs, operational=operational, raw=raw)
