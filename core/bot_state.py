"""
bot_state.py — State management pentru Ichimoku2
=================================================

Tine state-ul botului:
  - shared_equity: equity portofoliu (compound — dupa fiecare trade closed,
    shared_equity += pnl REAL Bybit)
  - positions: dict[symbol -> LivePosition | None] pt pozitiile DESCHISE
  - trades: lista de TradeRecord-uri inchise (pt chart panel + persistenta)
  - equity_curve: snapshot dupa fiecare trade
  - indicators: serii de overlay pe chart (Hull, Tenkan, Kijun, etc.)

Persistenta:
  - DATA_DIR env (gol = no persist; "/data" = persist la /data/bot_state.json)
  - RESET_TOKEN env (schimbarea valorii forteaza wipe la urmatorul start)

Equity contract:
  shared_equity NU se interogheaza din Bybit — local compute:
      shared_equity = initial + sum(trade.pnl_real for trade in closed)
  Sync cu Bybit balance se face DOAR la INIT (set initial = balance) si dupa
  fiecare close (audit ±3% drift, alerta Telegram daca diverge).
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field, asdict
from dataclasses import fields as _dc_fields
from datetime import datetime, timezone
from typing import Optional


ACCOUNT_SIZE = float(os.getenv("ACCOUNT_SIZE", "100.0"))
DATA_DIR = os.getenv("DATA_DIR", "")
RESET_TOKEN = os.getenv("RESET_TOKEN", "")


class ReconciliationError(Exception):
    """State desync intre local si Bybit — necesita HALT + manual review."""
    pass


@dataclass
class LivePosition:
    """Pozitie deschisa pe Bybit. State activ — devine TradeRecord la close."""
    symbol: str
    side: str                # "Buy" / "Sell" (Bybit native)
    direction: str           # "LONG" / "SHORT" (UI display)
    qty: float
    entry_price: float
    sl_price: float
    tp_price: Optional[float]
    leverage: int
    pos_usd: float           # nominal $ (qty * entry_price)
    risk_usd: float          # SL distance $
    opened_ts_ms: int        # entry timestamp UTC ms
    order_id: Optional[str] = None  # entry order ID (pt fetch_pnl)
    strategy: str = "hi"     # "hi" | "bb_mr" — folosit la dispatch + tg label
    bars_held: int = 0       # bare confirmed scurse de la entry (BB MR time-exit)
    sl_armed: bool = True    # True daca set_position_sl a reusit (Bybit-side SL atomic).
                              # False → fallback software: SL_LONG/SHORT signal din
                              # strategy escaleaza la close_position (vezi main.py).
    adopt_ts_ms: Optional[int] = None  # Set DOAR la resume (adopție din Bybit).
                                       # Folosit in fetch_pnl_for_trade ca entry_ts
                                       # in loc de opened_ts_ms istoric → window PnL
                                       # corect [adopt-60s, now+5min] FĂRĂ piramidari
                                       # vechi (închise ÎNAINTE de adopt) contaminate.
                                       # opened_ts_ms rămâne createdMs Bybit pt chart.

    def to_persist(self) -> dict:
        """Serializare pt state.json — pozitia activa supravietuieste restartului
        (opened_ts_ms REAL → time-exit corect; HL nu expune created_ms la adopt)."""
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "LivePosition":
        valid = {f.name for f in _dc_fields(cls)}
        return cls(**{k: v for k, v in d.items() if k in valid})


@dataclass
class TradeRecord:
    """Trade inchis — PnL REAL de pe Bybit closed-pnl endpoint."""
    id: int
    symbol: str
    direction: str           # "LONG" / "SHORT"
    entry_ts_ms: int
    entry_price: float
    sl_price: float
    tp_price: Optional[float]
    qty: float
    exit_ts_ms: int
    exit_price: float        # ACTUAL avg_exit Bybit
    exit_price_target: float # SL/TP/SIGNAL price targeted
    exit_reason: str         # "BYBIT_SL" / "BYBIT_TP" / "SIGNAL" / "EXTERNAL"
    pnl: float               # USDT real (incl fees)
    fees: float = 0.0
    extra: dict = field(default_factory=dict)  # Audit trail flexibil per trade.
                                                # Ex la adopt: {adopted: True,
                                                # bybit_created_ms, adopt_ts_ms}.

    @property
    def slippage(self) -> float:
        if self.exit_price_target <= 0 or self.exit_price <= 0:
            return 0.0
        if self.direction == "LONG":
            return self.exit_price_target - self.exit_price
        return self.exit_price - self.exit_price_target

    def to_dict(self) -> dict:
        """Format pentru chart_template.py & JSON API."""
        return {
            "id": self.id,
            "symbol": self.symbol,
            "direction": self.direction,
            "side": "L" if self.direction == "LONG" else "S",
            "entry_ms": self.entry_ts_ms,
            "entry_price": round(self.entry_price, 6),
            "sl": round(self.sl_price, 6),
            "tp": round(self.tp_price, 6) if self.tp_price else 0,
            "qty": round(self.qty, 6),
            "size_usdt": round(self.qty * self.entry_price, 2),
            "exit_ms": self.exit_ts_ms,
            "exit_price": round(self.exit_price, 6),
            "exit_price_target": round(self.exit_price_target, 6),
            "slippage": round(self.slippage, 6),
            "exit_reason": self.exit_reason,
            "pnl": round(self.pnl, 4),
            "fees": round(self.fees, 4),
            "extra": self.extra,
        }

    def to_persist(self) -> dict:
        return {
            "id": self.id, "symbol": self.symbol, "direction": self.direction,
            "entry_ts_ms": self.entry_ts_ms, "entry_price": self.entry_price,
            "sl_price": self.sl_price, "tp_price": self.tp_price,
            "qty": self.qty, "exit_ts_ms": self.exit_ts_ms,
            "exit_price": self.exit_price,
            "exit_price_target": self.exit_price_target,
            "exit_reason": self.exit_reason, "pnl": self.pnl, "fees": self.fees,
            "extra": self.extra,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TradeRecord":
        exit_price = d["exit_price"]
        return cls(
            id=d["id"], symbol=d["symbol"], direction=d["direction"],
            entry_ts_ms=d["entry_ts_ms"], entry_price=d["entry_price"],
            sl_price=d["sl_price"], tp_price=d.get("tp_price"),
            qty=d["qty"], exit_ts_ms=d["exit_ts_ms"], exit_price=exit_price,
            exit_price_target=d.get("exit_price_target", exit_price),
            exit_reason=d["exit_reason"], pnl=d["pnl"], fees=d.get("fees", 0.0),
            extra=d.get("extra", {}) or {},
        )


class BotState:
    """
    State global Ichimoku2.

    shared_equity: pornit la initial_account, creste/scade DOAR cu pnl real:
        shared_equity += trade.pnl    (dupa close, fees inclus)

    positions[symbol]: LivePosition | None (single-position-per-symbol)
    """

    def __init__(self, account_size: float = ACCOUNT_SIZE) -> None:
        self.initial_account: float = account_size
        self.shared_equity: float = account_size
        self.positions: dict[str, Optional[LivePosition]] = {}
        self.trades: list[TradeRecord] = []
        self.equity_curve: list[dict] = []
        self.first_candle_ts: dict[str, Optional[int]] = {}  # per-symbol
        self.start_utc: datetime = datetime.now(timezone.utc)

        # Indicatori overlay pe chart, organizati per simbol:
        # indicators[symbol][indicator_name] = list[{time, value}]
        self.indicators: dict[str, dict[str, list[dict]]] = {}
        self.indicator_meta: dict[str, dict] = {}

        self._lock = threading.Lock()

        first_ts = int(self.start_utc.timestamp())
        self.equity_curve.append({"time": first_ts, "value": round(self.shared_equity, 4)})

    # ----------------------------------------------------------------
    # Positions
    # ----------------------------------------------------------------
    def set_position(self, symbol: str, pos: Optional[LivePosition]) -> None:
        self.positions[symbol] = pos

    def get_position(self, symbol: str) -> Optional[LivePosition]:
        return self.positions.get(symbol)

    def has_position(self, symbol: str) -> bool:
        return self.positions.get(symbol) is not None

    def n_open_positions(self) -> int:
        return sum(1 for p in self.positions.values() if p is not None)

    # ----------------------------------------------------------------
    # Closed trades — pnl real Bybit, NU mutam shared_equity local
    # ----------------------------------------------------------------
    def record_closed_trade(self, trade: TradeRecord) -> None:
        """
        Inregistreaza trade inchis cu pnl real (deja tras de pe Bybit).

        IMPORTANT: shared_equity NU se muta local prin += trade.pnl.
        Caller-ul (main.py) face sync_equity(reason='CLOSE_<sym>') imediat
        dupa, care OVERWRITE shared_equity = balance real Bybit. Asta e
        single source of truth pentru equity (modelul Ichimoku, nu compound
        local ca boilerplate).
        """
        trade.id = len(self.trades) + 1
        self.trades.append(trade)
        # equity_curve point e adaugat de sync_equity (care e apelat dupa)
        # Free position slot
        self.positions[trade.symbol] = None
        print(f"  [STATE] Trade #{trade.id} {trade.symbol} {trade.direction} "
              f"PnL=${trade.pnl:+,.2f}  (equity update via sync_equity)")

    # ----------------------------------------------------------------
    # Indicators (overlay chart)
    # ----------------------------------------------------------------
    def register_indicator(self, name: str, color: str = "#ffd700",
                           line_width: int = 1, line_style: int = 0) -> None:
        self.indicator_meta[name] = {
            "color": color, "lineWidth": line_width, "lineStyle": line_style,
        }

    def add_indicator_point(self, symbol: str, name: str,
                            ts_s: int, value: float) -> None:
        if symbol not in self.indicators:
            self.indicators[symbol] = {}
        if name not in self.indicators[symbol]:
            self.indicators[symbol][name] = []
        self.indicators[symbol][name].append({
            "time": int(ts_s), "value": round(float(value), 8),
        })
        if len(self.indicators[symbol][name]) > 20000:
            self.indicators[symbol][name].pop(0)

    # ----------------------------------------------------------------
    # First-candle tracking (chart shows only candles >= this ts)
    # ----------------------------------------------------------------
    def mark_first_candle(self, symbol: str, ts_s: int) -> None:
        if self.first_candle_ts.get(symbol) is None:
            self.first_candle_ts[symbol] = ts_s

    # ----------------------------------------------------------------
    # Summary / chart payload
    # ----------------------------------------------------------------
    def summary(self) -> dict:
        n = len(self.trades)
        wins = sum(1 for t in self.trades if t.pnl > 0)
        pnl_total = self.shared_equity - self.initial_account
        ret_pct = (pnl_total / self.initial_account * 100) if self.initial_account else 0.0
        return {
            "initial_account": round(self.initial_account, 2),
            "account": round(self.shared_equity, 2),
            "pnl_total": round(pnl_total, 2),
            "return_pct": round(ret_pct, 2),
            "n_trades": n,
            "n_wins": wins,
            "n_losses": n - wins,
            "win_rate": round(wins / n * 100, 2) if n else 0.0,
            "n_open_positions": self.n_open_positions(),
            "start_utc": self.start_utc.isoformat(),
            "uptime_sec": int((datetime.now(timezone.utc) - self.start_utc).total_seconds()),
        }

    def init_payload(self) -> dict:
        return {
            "trades": [t.to_dict() for t in self.trades],
            "equity": self.equity_curve,
            "indicators": self.indicators,
            "indicator_meta": self.indicator_meta,
            "summary": self.summary(),
            "first_ts": self.first_candle_ts,
            "bot_name": os.getenv("BOT_NAME", "ichimoku2"),
            "strategy": os.getenv("STRATEGY_NAME", "Hull+Ichimoku 4h"),
            "timezone": os.getenv("CHART_TZ", "Europe/Bucharest"),
        }

    # ----------------------------------------------------------------
    # Persistenta
    # ----------------------------------------------------------------
    def _state_path(self) -> Optional[str]:
        if not DATA_DIR:
            return None
        try:
            os.makedirs(DATA_DIR, exist_ok=True)
        except OSError as e:
            print(f"  [STATE] WARN: nu pot crea {DATA_DIR}: {e}")
            return None
        return os.path.join(DATA_DIR, "bot_state.json")

    def save(self) -> None:
        path = self._state_path()
        if not path:
            return
        with self._lock:
            data = {
                "initial_account": self.initial_account,
                "shared_equity": self.shared_equity,
                "trades": [t.to_persist() for t in self.trades],
                "equity_curve": list(self.equity_curve),
                "first_candle_ts": self.first_candle_ts,
                "start_utc": self.start_utc.isoformat(),
                "indicators": self.indicators,
                "indicator_meta": self.indicator_meta,
                "reset_token": RESET_TOKEN,
                # Pozitii active persistate → opened_ts_ms REAL supravietuieste
                # restartului (time-exit BB MR corect; HL adopt n-are created_ms).
                "positions": {s: p.to_persist()
                              for s, p in self.positions.items() if p},
            }
        try:
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            os.replace(tmp, path)
        except Exception as e:
            print(f"  [STATE] save error: {e}")

    def load(self) -> None:
        path = self._state_path()
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            print(f"  [STATE] load error: {e}")
            return

        stored_token = data.get("reset_token", "")
        if RESET_TOKEN and stored_token != RESET_TOKEN:
            print(f"  [STATE] RESET_TOKEN changed ({stored_token!r} -> "
                  f"{RESET_TOKEN!r}) — wiping state")
            self.save()
            return

        self.initial_account = data.get("initial_account", self.initial_account)
        self.shared_equity = data.get("shared_equity", self.initial_account)
        self.trades = [TradeRecord.from_dict(t) for t in data.get("trades", [])]
        self.equity_curve = data.get("equity_curve", []) or self.equity_curve
        self.first_candle_ts = data.get("first_candle_ts", {}) or {}
        self.indicators = data.get("indicators", {}) or {}
        self.indicator_meta = data.get("indicator_meta", {}) or {}
        # Pozitii active persistate (opened_ts_ms real). Resume-ul le reconciliaza
        # cu exchange-ul inainte de a le folosi (vezi main.py adopt).
        self.positions = {s: LivePosition.from_dict(d)
                          for s, d in (data.get("positions") or {}).items()}
        try:
            self.start_utc = datetime.fromisoformat(data["start_utc"])
        except (KeyError, ValueError):
            pass
        print(f"  [STATE] loaded: equity=${self.shared_equity:,.2f}  "
              f"trades={len(self.trades)}")
