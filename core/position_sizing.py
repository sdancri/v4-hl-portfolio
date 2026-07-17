"""Position sizing pentru Ichimoku2.

Formula (model BP core/position_sizing.py qty_by_risk — cap prin MIN, NU skip):
    risk_usd = risk_pct_per_trade × shared_equity   (ex 7% × $100 = $7)
    pos_usd  = risk_usd / sl_initial_pct            (ex $7 / 0.04 = $175)
    cap_usd  = cap_pct_of_max × balance_broker × leverage_max
               (ex 0.95 × $100 × 12 = $1,140)
    pos_usd_final = min(pos_usd, cap_usd)           (BP: "notional_capped")

DOUA leverage diferite, scopuri DIFERITE:
  - per-pair leverage (ex 12×): folosit DOAR la set_leverage API call pe Bybit.
  - leverage_max (ex 12×): SAFETY cap intern. cap_usd foloseste leverage_max,
    nu per-pair, ca limita notional.

Cand pos_usd > cap_usd: pozitia se CAPEAZA (intra mai mica, nu se sare peste
trade), la fel ca BP. risk_usd se recalculeaza proportional pe pos_usd_final
(model BP sizing_snapshot "actual_risk") — Telegram/DB reflecta riscul REAL
al pozitiei intrate, nu cel teoretic dorit. `capped=True` marcheaza cazul
pentru logging/Telegram (caller poate afisa nota).
"""
from __future__ import annotations

from dataclasses import dataclass

from core.config import PairConfig, PortfolioConfig


@dataclass
class SizingResult:
    risk_usd: float
    pos_usd: float
    margin_needed: float
    cap_usd: float
    leverage: int
    capped: bool = False


def compute_position_size(
    pair_cfg: PairConfig,
    shared_equity: float,
    balance_broker: float,
    portfolio_cfg: PortfolioConfig,
    leverage: int | None = None,
) -> SizingResult:
    """Sizing per-pair folosind shared_equity (compound) cu cap pe balance real Bybit.

    Args:
        pair_cfg:        config pereche (risk_pct, sl_pct).
        shared_equity:   equity local (compound) — folosit pentru risk_usd.
        balance_broker:  USDT real din contul Bybit — folosit pentru cap_usd.
                         Daca caller-ul nu poate trage balance, paseaza shared_equity.
        portfolio_cfg:   pool config (cap_pct_of_max, leverage_max).
        leverage:        per-pair leverage (default: portfolio_cfg.leverage).

    Returneaza SizingResult intotdeauna — pos_usd e deja capat daca a fost
    cazul (caller verifica .capped doar pt logging, NU pt skip). Un pos_usd
    ramas prea mic (sub min_qty exchange) se prinde downstream, dupa
    round_qty_down — nu aici (mirror BP: fara concept de skip in sizing).
    """
    # SL pct effective per strategy: BB MR foloseste sl_pct, HI foloseste sl_initial_pct.
    sl_pct = pair_cfg.effective_sl_pct
    if sl_pct <= 0:
        raise ValueError(f"effective_sl_pct must be > 0 (strategy={pair_cfg.strategy}), got {sl_pct}")

    eff_leverage = leverage if leverage is not None else portfolio_cfg.leverage
    risk_usd = pair_cfg.risk_pct_per_trade * shared_equity
    pos_usd = risk_usd / sl_pct
    cap_usd = portfolio_cfg.cap_pct_of_max * balance_broker * portfolio_cfg.leverage_max

    capped = pos_usd > cap_usd
    if capped:
        pos_usd = max(0.0, cap_usd)
        risk_usd = pos_usd * sl_pct   # risc EFECTIV pe pozitia capata (model BP actual_risk)

    return SizingResult(
        risk_usd=risk_usd, pos_usd=pos_usd,
        margin_needed=pos_usd / eff_leverage,
        cap_usd=cap_usd, leverage=eff_leverage,
        capped=capped,
    )


def compute_qty(pos_usd: float, entry_price: float, step_size: float) -> float:
    """Round qty DOWN la step size al instrumentului."""
    if step_size <= 0:
        return pos_usd / entry_price
    qty_raw = pos_usd / entry_price
    return (qty_raw // step_size) * step_size
