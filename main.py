"""
main.py — V4 portfolio entry point (multi-symbol, multi-strategy bot)
=====================================================================

Strategii suportate per pereche (selectate prin pair_cfg.strategy):
  - 'hi':    Hull+Ichimoku Cloud (TIA, NEAR 4h)
  - 'bb_mr': Bollinger Bands Mean Reversion (BTC 4h)

Orchestreaza:
  - Bootstrap: load config, set leverage per pair, warmup 400 bare,
    INIT equity sync din Bybit balance, instantieaza signal per strategy.
  - Public WS klines (interval 240=4h) pentru toate pair-urile enabled
  - Pe fiecare confirmed bar: defense-in-depth external close check,
    update buffer signal, evaluate strategy, dispatch decision
  - Order pipeline: open (market entry + setTradingStop SL+TP atomic),
    close (market opus + reduceOnly + side mapping Buy/Sell native)
  - SL/TP calculation branched per strategy:
    * HI: SL la sl_initial_pct, TP optional la tp_pct (signal exit dominant)
    * BB MR: SL la sl_pct, TP fix la entry × (1 ± sl_pct × tp_rr)
  - bars_held counter incrementat per pereche pe bara confirmed (BB MR time-exit)
  - Trade closed pipeline: fetch_pnl_for_trade real Bybit, record_closed_trade,
    equity sync post-close, Telegram notification
  - Private WS: position events (detect SL/TP/EXTERNAL trigger)
  - Heartbeat: equity sync la (next_bar_close - 60s)
  - FastAPI: chart endpoints + WebSocket broadcast

Env vars (esential):
    CONFIG_FILE        — path la config_v4.yaml
    HL_MAIN_ADDRESS    — wallet address Hyperliquid (account principal)
    HL_AGENT_PRIVATE_KEY — cheia agent generata via scripts/gen_agent.py
                          (EXPIRA la 180 zile → monitor!)
    HL_BASE_URL        — empty = mainnet defaults; testnet:
                          https://api.hyperliquid-testnet.xyz
    HL_WS_URL          — empty = mainnet defaults; testnet:
                          wss://api.hyperliquid-testnet.xyz/ws
    BOT_NAME           — "v4"
    TELEGRAM_TOKEN     — opt
    TELEGRAM_CHAT_ID   — opt
    CHART_PORT         — default 8104
    CHART_TZ           — default Europe/Bucharest
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, FileResponse
from fastapi.staticfiles import StaticFiles

from core import bot_control as bc
from core import exchange_api as ex

# Optional: bot_reporter pt integrare cu dashboard agregat (SQLite shared).
# Fail-safe: import esuat / DB inaccesibil → reporter dezactivat, bot continua.
try:
    from bot_reporter import BotReporter  # type: ignore
    _BOT_REPORTER_AVAILABLE = True
except Exception as _bre:
    BotReporter = None  # type: ignore
    _BOT_REPORTER_AVAILABLE = False
    print(f"  [REPORTER] bot_reporter import failed ({_bre!r}) — disabled")
from core import hl_ws_runner
from core import telegram_bot as tg
from core import no_lookahead as nl
from core.bot_state import BotState, LivePosition, TradeRecord, ReconciliationError
from core.config import AppConfig, load_config
from core.monitoring import (
    SHUTDOWN_SIGNAL,
    install_asyncio_exception_handler,
    install_signal_handlers,
    memory_monitor,
)
from core.position_sizing import compute_position_size, compute_qty
from strategies.bb_mr_signal import BBMeanReversionSignal, BBMRConfig
from strategies.ichimoku_signal import IchimokuSignal, PairStrategyConfig

# Type alias pentru orice signal generator (dispatch by strategy)
SignalGen = IchimokuSignal | BBMeanReversionSignal


# ============================================================================
# Last-resort crash handler — Telegram alert pe exceptii Python necapturate pe
# main-thread. Complementar lui install_asyncio_exception_handler (care prinde
# task-urile async). NU prinde SIGKILL/OOM/segfault (pt astea: memory_monitor
# pre-OOM + docker inspect OOMKilled).
# ============================================================================
def _crash_excepthook(exc_type, exc_value, tb):
    import traceback as _tb
    err_text = ''.join(_tb.format_exception(exc_type, exc_value, tb))
    print(f"[CRASH] uncaught {exc_type.__name__}: {exc_value}", file=sys.stderr)
    print(err_text, file=sys.stderr)
    # TG sincron via httpx — event loop poate fi deja mort, NU folosim asyncio.
    try:
        import httpx as _httpx
        import html as _html
        token = os.getenv("TELEGRAM_TOKEN", "")
        chat = os.getenv("TELEGRAM_CHAT_ID", "")
        if token and chat:
            name = _html.escape(os.getenv("BOT_NAME", "V4"))
            tb_short = _html.escape(err_text[-1500:])
            text = (f"🤖 <b>[{name}]</b>\n<b>BOT CRASHED 💥</b>\n"
                    f"<code>{exc_type.__name__}: "
                    f"{_html.escape(str(exc_value))[:200]}</code>\n<pre>{tb_short}</pre>")
            _httpx.post(f"https://api.telegram.org/bot{token}/sendMessage",
                        json={"chat_id": chat, "text": text, "parse_mode": "HTML"},
                        timeout=5)
    except Exception:
        pass
    sys.__excepthook__(exc_type, exc_value, tb)


sys.excepthook = _crash_excepthook


def _strategy_label(strategy: str) -> str:
    """Pretty label pentru logs / Telegram."""
    return {"hi": "Hull+Ichimoku", "bb_mr": "BB Mean Reversion"}.get(strategy, strategy)


# ============================================================================
# Globals
# ============================================================================
_args = argparse.ArgumentParser()
_args.add_argument("--config", default=os.getenv("CONFIG_FILE", "config/config_v4_hl.yaml"))
CLI = _args.parse_args()

CONFIG: AppConfig = load_config(CLI.config)
BOT_NAME = os.getenv("BOT_NAME", CONFIG.portfolio.name)
# Versiune raportata in dashboard bot_events (linie verticala pe equity chart
# la launch/restart). Setata in stack la deploy (ex BOT_VERSION="v1.3 fix SL"
# sau git short SHA). Gol → eveniment fara label.
BOT_VERSION = os.getenv("BOT_VERSION", "")
CHART_PORT = int(os.getenv("CHART_PORT", "8104"))
# Port host pentru link Telegram (Docker port mapping host:container poate diferi
# de portul intern). Default = CHART_PORT cand nu e mapping.
CHART_HOST_PORT = int(os.getenv("CHART_HOST_PORT", str(CHART_PORT)))
CHART_TZ = os.getenv("CHART_TZ", "Europe/Bucharest")

# Static files for chart
ROOT = Path(__file__).resolve().parent
STATIC = str(ROOT / "static")
LOG_DIR = ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Per-symbol state
_state = BotState(account_size=CONFIG.portfolio.pool_total)
_signals: dict[str, SignalGen] = {}  # HI sau BB MR — dispatch dupa pair_cfg.strategy
_pair_cfgs: dict[str, any] = {}  # PairConfig from config.yaml (has leverage, sl, etc.)
_candles: dict[str, list] = {}  # ring buffer per symbol pt chart [ts_s, o, h, l, c]
_last_synced_ts: dict[str, int] = {}  # last confirmed bar ts (seconds) per symbol
_sync_done: dict[str, bool] = {}  # per-symbol: gap-fill rulat pe sesiunea WS curenta
_clients: set[WebSocket] = set()
_halted: dict[str, bool] = {}  # per-symbol HALT flag (after ReconciliationError)
_reporters: dict[str, "BotReporter"] = {}  # per-symbol reporter (dashboard SQLite)
_last_prices: dict[str, float] = {}  # per-symbol last close (orice tick WS).
# Folosit de _periodic_heartbeat pt uPnL fresh (heartbeat_loop pe bara confirmed
# e prea rar pe TF 4h → dashboard considera bot 'dead' intre bare).

# Map order_id (string) -> entry context, used to fetch_pnl after close
# Filled at open, consumed at close.
_open_orders_meta: dict[str, dict] = {}

# Reconciliation: dupa close-ul strategiei, verificam ca pozitia s-a inchis
# pe Bybit. INVARIANT: bot-ul are ownership exclusiv pe perechile lui — daca
# alt bot tranzactioneaza acelasi simbol, get_position e contaminat si
# reconcilierea trigereaza halt-uri false (sau, mai grav, chase_close inchide
# pozitia altui bot).
_RECONCILE_QTY_EPS = 1e-9       # toleranta float pe comparatii de qty
_RECONCILE_RETRIES = 3          # iteratii pt stop-not-triggered branch
_RECONCILE_RETRY_SLEEP = 1.0    # secunde intre check-uri (~3s total wait)

# Per-symbol close lock — previne race intre close_position (signal exit din
# public WS task) si close_pipeline_external (private WS position event +
# defense-in-depth check_external_close). Ambele pot fi observate aproape
# simultan cand Bybit fileaza SL/TP atomic in timp ce strategia genereaza
# CLOSE_LONG/SHORT pe aceeasi bara — fara lock + dedup, acelasi trade s-ar
# inregistra de doua ori in _state.trades.
_close_locks: dict[str, asyncio.Lock] = {}


def _get_close_lock(symbol: str) -> asyncio.Lock:
    """Creeaza lazy un Lock per simbol."""
    lock = _close_locks.get(symbol)
    if lock is None:
        lock = asyncio.Lock()
        _close_locks[symbol] = lock
    return lock


_TF_INTERVAL = "240"  # 4h fix (toate perechile)
_TF_MS = 4 * 60 * 60 * 1000


# ============================================================================
# Helpers
# ============================================================================

def log_event(event: str, **fields) -> None:
    """JSONL log to logs/<bot>.jsonl"""
    rec = {"ts": int(time.time() * 1000), "bot": BOT_NAME, "event": event, **fields}
    try:
        with open(LOG_DIR / f"{BOT_NAME}.jsonl", "a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    except Exception as e:
        print(f"  [LOG] write failed: {e}")


async def broadcast(payload: dict) -> None:
    """Send to all connected chart WS clients. Drop disconnected."""
    if not _clients:
        return
    msg = json.dumps(payload)
    dead = []
    for ws in _clients:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _clients.discard(ws)


def _push_candle_ring(symbol: str, bar: dict) -> None:
    """Update/append bara in _candles ring (sursa pt chart via /api/init).
    Mirror V4 Bybit WS handler (main.py:1346): acelasi ts → update last
    (intra-bar tick), ts nou → append (cap 5000). Pe HL asta lipsea din
    handler-ele candle → ring gol → chart fara bare/scala."""
    ts_s = bar["ts_ms"] // 1000
    row = [ts_s, float(bar["open"]), float(bar["high"]),
           float(bar["low"]), float(bar["close"])]
    ring = _candles.setdefault(symbol, [])
    if ring and ring[-1][0] == ts_s:
        ring[-1] = row
    else:
        ring.append(row)
        if len(ring) > 5000:
            ring.pop(0)


def _direction_to_side(direction: str) -> str:
    """LONG -> Buy, SHORT -> Sell (Bybit native capitalized)."""
    return "Buy" if direction == "LONG" else "Sell"


def _close_side(direction: str) -> str:
    """LONG closes with Sell, SHORT closes with Buy."""
    return "Sell" if direction == "LONG" else "Buy"


def _next_bar_close_ms(now_ms: int) -> int:
    """Next 4h bar close ts (ms)."""
    return ((now_ms // _TF_MS) + 1) * _TF_MS


def _estimate_pnl_fallback(direction: str, entry_price: float, qty: float,
                            exit_price: float) -> tuple[float, float]:
    """FALLBACK PnL cand Bybit closed-pnl nu e indexat (n_fills=0).

    Estimare locala: gross dupa directie - fees taker pe ambele picioare.
    NU prinde slippage-ul real al exit-ului (folosim exit_price = target sau
    avg_exit daca exista), dar e ordinul de marime corect vs $0 fals.
    Fara asta, TradeRecord ar fi inregistrat PERMANENT cu PnL=0 (equity nu se
    misca, Telegram/chart/dashboard arata $0 pt un trade real).

    Returneaza (pnl_est, fees_est). Apelat doar daca n_fills == 0.
    """
    sign_dir = 1 if direction == "LONG" else -1
    gross = (exit_price - entry_price) * qty * sign_dir
    taker_rate = float(os.getenv("TAKER_FEE_RATE", "0.00055"))
    fees_est = (entry_price + exit_price) * qty * taker_rate
    return round(gross - fees_est, 4), round(fees_est, 4)


def _build_trade_extra(pos: LivePosition) -> dict:
    """Audit trail per trade — populat in TradeRecord.extra la close.

    Caz: pozitie adoptata la resume → marcam {adopted:True, bybit_created_ms,
    adopt_ts_ms}. Caz normal (trade deschis de bot) → {}.
    """
    extra: dict = {}
    if pos.adopt_ts_ms is not None:
        extra["adopted"] = True
        extra["bybit_created_ms"] = pos.opened_ts_ms
        extra["adopt_ts_ms"] = pos.adopt_ts_ms
    return extra


# ============================================================================
# Equity sync (INIT / CLOSE / HEARTBEAT)
# ============================================================================

async def sync_equity(reason: str = "MANUAL") -> None:
    """
    Bot Ichimoku citeste balance DIRECT de pe Bybit — single source of truth.
    NU tine equity local prin compound (account += pnl) ca boilerplate.

    La fiecare sync (INIT/CLOSE/HEARTBEAT):
      - Pull balance real Bybit
      - OVERWRITE _state.shared_equity = balance
      - Append punct in equity_curve (pt chart)
      - Anomaly detect: daca delta vs ultimul sync e neasteptat (>3%
        fara trade inchis recent), alerta Telegram.
    """
    bal = await ex.get_balance()
    if bal is None:
        print(f"  [EQUITY-SYNC {reason}] FAILED — Bybit balance None")
        return

    prev = _state.shared_equity
    _state.shared_equity = bal

    if reason == "INIT":
        _state.initial_account = bal
        _state.equity_curve.clear()
        _state.equity_curve.append({
            "time": int(time.time()), "value": round(bal, 4),
        })
        print(f"  [EQUITY-SYNC INIT] account = ${bal:,.2f} (Bybit balance)")
        return

    # Append equity point — chart shows actual Bybit balance over time
    _state.equity_curve.append({
        "time": int(time.time()), "value": round(bal, 4),
    })
    if len(_state.equity_curve) > 50000:
        _state.equity_curve.pop(0)

    delta_pct = abs(bal - prev) / prev * 100 if prev > 0 else 0
    print(f"  [EQUITY-SYNC {reason}] prev=${prev:,.2f}  bybit=${bal:,.2f}  "
          f"delta={delta_pct:.2f}%")
    log_event("equity_sync", reason=reason, prev=prev, bybit=bal,
              delta_pct=delta_pct)


# ============================================================================
# Order pipeline
# ============================================================================

async def open_position(symbol: str, direction: str, close_price: float,
                         bar_ts_ms: Optional[int] = None) -> None:
    """
    Deschide pozitie market + set leverage + setTradingStop (SL+TP atomic).

    Direction: 'LONG' / 'SHORT'. Bybit side: 'Buy' / 'Sell'.
    bar_ts_ms: open-ul barei la care s-a executat (chart-ul afiseaza linii
    aliniate la bara, nu la wall-clock).

    Respecta is_paused() (set via /api/pause endpoint din dashboard) — daca
    e True, NU deschide noi pozitii. Pozitiile EXISTENTE raman protejate de
    SL/TP atomic (NU le inchidem la pauza — `stop` face asta explicit).
    """
    if bc.is_paused():
        print(f"  [OPEN {symbol}] BOT PAUZAT — skip entry signal {direction}")
        return
    pair_cfg = _pair_cfgs[symbol]
    side = _direction_to_side(direction)

    # Sizing — fetch fresh Bybit balance pentru cap (fallback la shared_equity)
    balance = await ex.get_balance()
    if balance is None:
        balance = _state.shared_equity
    sizing = compute_position_size(
        pair_cfg, _state.shared_equity, balance,
        CONFIG.portfolio, leverage=pair_cfg.leverage,
    )
    if sizing.skip:
        print(f"  [OPEN {symbol}] SKIP: {sizing.skip_reason}")
        log_event("entry_skipped", symbol=symbol, reason=sizing.skip_reason)
        return

    info = await ex.get_market_info(symbol)
    qty_raw = sizing.pos_usd / close_price
    qty = ex.round_qty_down(qty_raw, info["qty_step"])
    if qty < info["min_qty"]:
        print(f"  [OPEN {symbol}] qty {qty} < min {info['min_qty']} — skip")
        return

    # Leverage (idempotent — Bybit returneaza ok daca deja setat)
    await ex.set_leverage(symbol, pair_cfg.leverage)

    # Maker entry cu fallback Market pe remainder (pattern 80/20).
    # ~80-90% din economia de fee fata de chase complet — taker 0.055% → ~0.02% blended.
    # Pe TF 4h, semnalul vine pe close-ul barei → urmatoarea bara nu va fugi 5%
    # in 5s, deci fill rate maker e ridicat. Timeout 5s + fallback "market".
    entry_result = await ex.maker_entry_or_market(
        symbol, side, qty,
        timeout_sec=5, fallback="market", reduce_only=False,
    )
    # HL adapter intoarce "rejected" (NU "failed" ca Bybit) pe esec total.
    if entry_result["result"] == "rejected":
        print(f"  [OPEN {symbol}] maker_entry_or_market REJECTED")
        await tg.send_critical(f"OPEN FAILED — {symbol}",
                               "maker_entry_or_market returned rejected / 0 fill",
                               symbol=symbol)
        return

    # Pretul real de fill: pe maker pur, avg_price din ordin; pe mixed/taker,
    # fetch din get_order_status / fetch_pnl_for_trade ulterior.
    await asyncio.sleep(0.5)
    real_fill_price = float(entry_result.get("avg_price") or 0) or close_price
    fill_kind = entry_result["result"]
    if fill_kind in ("mixed", "market"):
        # Avg-ul ponderat real vine cand fetch_pnl_for_trade trage closed-pnl;
        # pana atunci folosim signal price. Pt SL/TP calc, e suficient.
        real_fill_price = close_price
    # Filled qty REAL — pe partial fill (maker timeout + market remainder esuat,
    # rezultat "mixed"/"skipped"), filled_qty < qty CERUTA. Tracking-ul corect
    # asigura ca SL/close/uPnL opereaza pe cantitatea care chiar exista pe Bybit.
    filled_qty = float(entry_result.get("filled_qty") or 0) or qty
    if filled_qty <= 0:
        print(f"  [OPEN {symbol}] filled_qty=0 → skip (nimic deschis pe Bybit)")
        return
    if filled_qty < qty:
        print(f"  [OPEN {symbol}] PARTIAL FILL: requested={qty} filled={filled_qty}")
    slippage_bps = ((real_fill_price - close_price) / close_price * 10000
                    if real_fill_price > 0 else 0)
    print(f"  [OPEN {symbol}] {fill_kind:6s} signal={close_price:.6f} "
          f"fill={real_fill_price:.6f} slippage={slippage_bps:+.1f}bps")
    order_id = ""  # informational only — maker_entry_or_market nu returneaza id unic
    # De aici incolo, folosim filled_qty (NU qty cerut) pt LivePosition + SL + Telegram.
    qty = filled_qty

    # SL/TP din fill real — branched per strategy:
    #   - 'hi': SL la sl_initial_pct, TP optional la tp_pct (signal exit dominant)
    #   - 'bb_mr': SL la sl_pct, TP fix la entry × (1 ± sl_pct × tp_rr)
    sl_pct_use = pair_cfg.effective_sl_pct
    sl_price = (real_fill_price * (1 - sl_pct_use) if direction == "LONG"
                else real_fill_price * (1 + sl_pct_use))
    tp_price: Optional[float] = None
    if pair_cfg.strategy == "bb_mr":
        tp_d = pair_cfg.sl_pct * pair_cfg.tp_rr
        tp_price = (real_fill_price * (1 + tp_d) if direction == "LONG"
                    else real_fill_price * (1 - tp_d))
    elif pair_cfg.tp_pct and pair_cfg.tp_pct > 0:
        tp_price = (real_fill_price * (1 + pair_cfg.tp_pct) if direction == "LONG"
                    else real_fill_price * (1 - pair_cfg.tp_pct))

    # Telegram INTRARE — trimis INAINTE de set_position_sl. set_position_sl
    # emite intern HALT "SL/TP NESETAT" pe esec dupa ~7s retry; daca am trimite
    # INTRARE dupa, pe timeline-ul Telegram HALT-ul ar aparea PRIMUL (confuz —
    # pare ca botul deschide intentionat fara protectie). Ordinea corecta:
    # intai "am intrat", apoi (daca pica SL) "ruleaza fara protectie".
    # now_ms fixat aici = momentul fill-ului (nu +7s daca SL retry esueaza),
    # reutilizat ca opened_ts_ms.
    now_ms = int(time.time() * 1000)
    slip_line = ""
    if abs(real_fill_price - close_price) > 0:
        slip_bps = (real_fill_price - close_price) / close_price * 10000
        slip_line = (f"  <i>(signal <code>{ex.smart_price(close_price)}</code>, "
                     f"slip <code>{slip_bps:+.1f}bps</code>)</i>")
    fill_emoji = {"maker": "🟢", "mixed": "🟡", "taker": "🔴"}.get(fill_kind, "⚪")
    tp_section = (f"🎯 <b>Take Profit:</b> <code>{ex.smart_price(tp_price)}</code>  "
                 f"<i>(Market atomic)</i>\n\n" if tp_price else "")
    await tg.send(
        f"🚀 INTRARE: {direction} {tg.dir_emoji(direction)}",
        f"<b>Fill:</b>     {fill_emoji} <code>{fill_kind}</code>\n"
        f"<b>Entry:</b>    <code>{ex.smart_price(real_fill_price)}</code>{slip_line}  "
        f"(<code>{tg.fmt_time(now_ms)}</code>)\n"
        f"<b>Notional:</b> <code>${sizing.pos_usd:,.2f}</code>\n"
        f"<b>Risk:</b>     <code>${sizing.risk_usd:,.2f}</code>  "
        f"(<code>{pair_cfg.risk_pct_per_trade*100:.0f}%</code> × "
        f"<code>${_state.shared_equity:,.2f}</code>)\n"
        f"\n"
        f"{tp_section}"
        f"🛑 <b>Stop Loss:</b>   <code>{ex.smart_price(sl_price)}</code>  "
        f"(<code>{sl_pct_use*100:.1f}%</code>, Market)\n"
        f"\n"
        f"<b>Qty:</b> <code>{qty}</code>",
        symbol=symbol,
    )

    # SL + TP atomic server-side Market (varianta C — robust, simplu).
    # SL: siguranta executiei pe gap. TP: deterministic la trigger (fara
    # spike-through pe alts subtiri). Cost: ~3.5 bps fata de TP-Limit maker —
    # irelevant pe target R mare; beneficiu: exec garantata independent
    # de bot/WS uptime.
    # Pattern armed_set_sl (sync cu BP): Layer 1 = ex.set_position_sl quick
    # (race-aware, silent). Pe esec → Layer 2 background retry pana la
    # _SL_RETRY_TIMEOUT (flip sl_armed=True la succes, HALT o SINGURA data la
    # timeout). NU abandonam trade-ul; pana la armare ramane fallback software
    # (SL_LONG/SHORT pe bara confirmed → close_position).
    # Guard geometrie SL (validate_sl din BP). In V4 sl_price = fill×(1∓sl_pct)
    # e corect prin constructie, dar prindem bug-uri viitoare: SL pe partea
    # gresita sau <=0 → Bybit l-ar trigerui INSTANT la primul tick (pierdere
    # silentioasa + Telegram "SL/TP" misleading). NU armam un SL invalid.
    sl_geometry_bad = (
        sl_price <= 0
        or (direction == "LONG" and sl_price >= real_fill_price)
        or (direction == "SHORT" and sl_price <= real_fill_price)
    )
    if sl_geometry_bad:
        print(f"  [OPEN {symbol}] SL GEOMETRY INVALID: dir={direction} "
              f"entry={real_fill_price} sl={sl_price} — NU armez (fallback software)")
        await tg.send_critical(
            "SL geometrie INVALIDĂ",
            f"<b>Direcție:</b> {direction}  "
            f"<b>Entry:</b> <code>{ex.smart_price(real_fill_price)}</code>  "
            f"<b>SL:</b> <code>{sl_price}</code>\n"
            f"SL pe partea greșită / ≤0 — NU îl armez pe Bybit (ar închide "
            f"instant). Poziția rulează pe fallback software. Verifică strategia.",
            symbol=symbol,
        )
        sl_ok = False
    else:
        sl_ok = await _arm_sl(symbol, sl_price, tp_price)
    if not sl_ok:
        print(f"  [OPEN {symbol}] SL initial NEsetat — Layer 2 background retry "
              f"activ (max {_SL_RETRY_TIMEOUT}s); fallback software pana atunci.")

    pos = LivePosition(
        symbol=symbol, side=side, direction=direction,
        qty=qty, entry_price=real_fill_price,
        sl_price=sl_price, tp_price=tp_price,
        leverage=pair_cfg.leverage,
        pos_usd=sizing.pos_usd, risk_usd=sizing.risk_usd,
        opened_ts_ms=now_ms, order_id=order_id, sl_armed=sl_ok,
        strategy=pair_cfg.strategy, bars_held=0,
    )
    _state.set_position(symbol, pos)
    log_event("entry", symbol=symbol, direction=direction,
              signal_price=close_price, fill_price=real_fill_price,
              qty=qty, sl=sl_price, tp=tp_price, order_id=order_id)

    # Broadcast position_open (chart shows live entry/SL/TP lines + arrow marker)
    # entry_ms pt chart = bar open time (Lightweight Charts pozitioneaza la bar
    # open). Wall-clock now_ms ramane in pos.opened_ts_ms pt fetch_pnl.
    chart_entry_ms = bar_ts_ms if bar_ts_ms is not None else now_ms
    await broadcast({
        "type": "position_open", "symbol": symbol,
        "direction": direction, "entry": real_fill_price,
        "sl": sl_price, "tp": tp_price,
        "qty": qty, "risk_usd": sizing.risk_usd,
        "entry_ms": chart_entry_ms,
    })

    # (Telegram INTRARE deja trimis mai sus, INAINTE de set_position_sl —
    # vezi nota despre ordinea pe timeline vs HALT "SL NESETAT".)


# ============================================================================
# armed_set_sl pattern (sync cu BP) — Layer 1 quick + Layer 2 background retry
# ============================================================================
# Layer 1: _arm_sl face UN singur attempt (fail-fast, silent). Pe succes, SL e
#   armat Bybit-side instant. Pe esec → spawneaza Layer 2 si returneaza imediat
#   (non-blocking → open_position nu blocheaza ~7s, INTRARE iese instant).
# Layer 2: _sl_retry_loop ruleaza in background cu backoff lung. La succes:
#   flip pos.sl_armed=True (opreste fallback software). Trimite UN SINGUR
#   Telegram critical doar la timeout total — nu per-attempt (zero spam).
# Pana la armare ramane fallback software: SL_LONG/SHORT pe bara confirmed →
#   close_position (vezi on_confirmed_bar).
_SL_RETRY_BACKOFF = [5, 10, 20, 30, 60]                        # secunde (Layer 2)
_SL_RETRY_TIMEOUT = int(os.getenv("SL_RETRY_TIMEOUT", "120"))  # total wait Layer 2


async def _arm_sl(symbol: str, sl_price: float,
                  tp_price: Optional[float]) -> bool:
    """Layer 1 SL arming. Single attempt fail-fast (silent); pe esec spawneaza
    Layer 2 background retry si returneaza imediat (non-blocking). Return True =
    SL armat Bybit-side instant; False = in curs de retry background."""
    ok = await ex.set_position_sl(symbol, sl_price, tp_price, is_initial=True,
                                  max_retries=1, send_tg_on_fail=False)
    if not ok:
        asyncio.create_task(_sl_retry_loop(symbol, sl_price, tp_price))
    return ok


async def _sl_retry_loop(symbol: str, sl_price: float,
                         tp_price: Optional[float]) -> None:
    """Layer 2 background retry (NU se cheama direct — folosit de _arm_sl).
    Backoff _SL_RETRY_BACKOFF pana la _SL_RETRY_TIMEOUT. Abandoneaza daca pozitia
    s-a inchis sau a fost deja armata intre timp. La succes: flip sl_armed +
    Telegram info. La timeout total: UN singur Telegram critical (software
    fallback — V4 nu foloseste panic close)."""
    def _needs_arming() -> bool:
        # Pozitia inca deschisa SI inca nearmata (nu inchisa/reopen/armata altfel).
        p = _state.get_position(symbol)
        return p is not None and not p.sl_armed

    elapsed = 0
    for delay in _SL_RETRY_BACKOFF:
        if elapsed >= _SL_RETRY_TIMEOUT:
            break
        await asyncio.sleep(delay)
        elapsed += delay
        if not _needs_arming():
            return  # inchisa/armata intre timp — abandonam silentios
        ok = await ex.set_position_sl(symbol, sl_price, tp_price, is_initial=True,
                                      max_retries=1, send_tg_on_fail=False)
        if ok:
            pos = _state.get_position(symbol)
            if pos is not None:
                pos.sl_armed = True
                _state.save()
            print(f"  [{symbol}] SL armed dupa {elapsed}s retry (Layer 2)")
            try:
                await tg.send(
                    "✅ SL setat cu întârziere",
                    f"🛑 <b>set_position_sl</b> a reușit după <code>{elapsed}s</code> "
                    f"de reîncercări.\nPoziția e protejată acum și Bybit-side.",
                    symbol=symbol,
                )
            except Exception:
                pass
            return

    # Timeout total — fallback software in-code (SL_LONG/SHORT pe bara confirmed).
    if not _needs_arming():
        return
    print(f"  [{symbol}] SL retry timeout {elapsed}s — fallback software in-code")
    try:
        sl_str = ex.smart_price(sl_price)
        tp_line = (f"🎯 <b>TP:</b> <code>{ex.smart_price(tp_price)}</code>\n"
                   if tp_price is not None else "")
        await tg.send_critical(
            "SL/TP NESETAT" if tp_price is not None else "SL NESETAT",
            f"<b>set_position_sl A EȘUAT</b> după <code>{elapsed}s</code> de reîncercări\n"
            f"🛑 <b>SL:</b> <code>{sl_str}</code>\n"
            f"{tp_line}"
            f"<b>Poziția rulează FĂRĂ protecție Bybit-side.</b>\n"
            f"Fallback software: SL_LONG/SHORT pe bară confirmed → close_position. "
            f"Reconcilierea la primul close va force chase_close dacă e cazul.",
            symbol=symbol,
        )
    except Exception as e:
        print(f"  [{symbol}] SL timeout tg.send_critical failed: {e!r}")


async def _assert_closed(symbol: str, qty_local: float,
                          reason_label: str) -> None:
    """Verifica ca pozitia e inchisa dupa chase_close. Raise pe esec.

    chase_close are 20 incercari + fallback market, dar nu garanteaza
    inchiderea (Bybit down complet, ordinele respinse). Fara aceasta verificare,
    am inregistra trade-ul ca "inchis" cu pozitia inca deschisa.
    """
    bybit_pos = await ex.get_position(symbol)
    qty_after = float(bybit_pos.get("size", 0)) if bybit_pos else 0.0
    if qty_after <= _RECONCILE_QTY_EPS:
        return
    msg = (f"chase_close ({reason_label}) NU a inchis pozitia pe {symbol}: "
           f"qty_after={qty_after}, qty_local={qty_local}")
    print(f"  [RECONCILE {symbol}] HALT: {msg}")
    await tg.send_critical(
        f"{symbol} chase_close esuat ({reason_label})",
        f"<b>Qty locală:</b> {qty_local}\n"
        f"<b>Qty Bybit după închidere:</b> {qty_after}\n"
        f"chase_close nu a putut inchide pozitia (Bybit down sau order respins). "
        f"Bot oprit pe acest simbol. Verifică manual și restart.",
        symbol=symbol,
    )
    raise ReconciliationError(msg)


async def _reconcile_close(symbol: str, direction: str,
                            qty_local: float, exit_reason: str) -> str:
    """
    Confirma cu Bybit ca pozitia s-a inchis si rezolva discrepantele.
    Returneaza exit_reason-ul final (eventual cu sufix _PARTIAL / _FORCED).
    Ridica ReconciliationError pe ramura qty_real > qty_local sau daca
    chase_close esueaza sa inchida pozitia.

    Ramuri (qty_real = pozitie reala pe Bybit, qty_local = qty bot):
      qty_real == 0            -> Bybit a inchis clean, exit_reason neschimbat
      0 < qty_real < qty_local -> fill partial, chase_close pe rest, sufix _PARTIAL
      qty_real ≈ qty_local     -> stop nu s-a triggerit; retry; daca persista, force
      qty_real > qty_local     -> anomalie; HALT, raise ReconciliationError
    """
    # STRICT — pe API fail NU concludem fals "clean close" cu PnL=0.
    # Multi-attempt re-confirm; daca toate esueaza → assume open, force chase.
    qty_real_opt = await ex.get_position_qty_strict(symbol)
    if qty_real_opt is None:
        print(f"  [RECONCILE {symbol}] get_position API fail — re-confirm multi-attempt")
        confirmed = await ex.confirm_position_closed(symbol, attempts=3, delay=1.0)
        if confirmed is None:
            print(f"  [RECONCILE {symbol}] confirm all attempts FAILED — "
                  f"assume open, force chase_close")
            await ex.chase_close(symbol, direction)
            forced_reason = f"{exit_reason}_FORCED"
            await _assert_closed(symbol, qty_local, forced_reason)
            return forced_reason
        if confirmed:
            return exit_reason
        # confirmed False (inca activa) — citim qty pt ramurile de mai jos.
        bybit_pos = await ex.get_position(symbol)
        qty_real = float(bybit_pos.get("size", 0)) if bybit_pos else 0.0
    else:
        qty_real = qty_real_opt

    # Ramura 1: clean close (cazul comun)
    if qty_real <= _RECONCILE_QTY_EPS:
        return exit_reason

    # Ramura 4: anomalie — qty pe Bybit mai mare decat ce stim local.
    # NU inchidem automat — am putea inchide hedge manual / pozitia altui bot
    # / o piramidare necontorizata. Halt + alert + raise.
    if qty_real > qty_local + _RECONCILE_QTY_EPS:
        msg = (f"qty_real={qty_real} > qty_local={qty_local} pe {symbol} "
               f"(exit_reason={exit_reason}). Cauze posibile: piramidare "
               f"necontorizata, pozitie reziduala dintr-un run anterior, sau "
               f"interventie manuala. Bot HALTED pe simbol.")
        print(f"  [RECONCILE {symbol}] HALT: {msg}")
        await tg.send_critical(
            f"{symbol} reconciliere {exit_reason}",
            f"<b>Qty locală:</b> {qty_local}\n"
            f"<b>Qty Bybit:</b> {qty_real}\n"
            f"<b>Exit reason:</b> {exit_reason}\n"
            f"Bot oprit pe acest simbol. Verifică manual și restart.",
            symbol=symbol,
        )
        raise ReconciliationError(msg)

    # Ramura 2: fill partial (0 < qty_real < qty_local) — inchide restul
    if qty_real < qty_local - _RECONCILE_QTY_EPS:
        partial_reason = f"{exit_reason}_PARTIAL"
        print(f"  [RECONCILE {symbol}] partial: real={qty_real} < local={qty_local} "
              f"(reason={exit_reason}) — chase_close pe rest")
        await ex.chase_close(symbol, direction)
        await _assert_closed(symbol, qty_local, partial_reason)
        return partial_reason

    # Ramura 3: qty_real ≈ qty_local — stop-ul nu s-a triggerit pe Bybit.
    # Asteptam putin (poate e doar latenta), apoi forcam inchidere.
    for attempt in range(_RECONCILE_RETRIES):
        await asyncio.sleep(_RECONCILE_RETRY_SLEEP)
        # Strict: pe API fail in retry, assume still open (continue) — NU
        # concludem fals ca s-a triggerit.
        qty_real_opt = await ex.get_position_qty_strict(symbol)
        if qty_real_opt is None:
            print(f"  [RECONCILE {symbol}] retry {attempt+1}: API fail — "
                  f"assume still open, continuing")
            continue
        if qty_real_opt <= _RECONCILE_QTY_EPS:
            print(f"  [RECONCILE {symbol}] {exit_reason} a triggerit dupa retry "
                  f"#{attempt + 1}")
            return exit_reason

    forced_reason = f"{exit_reason}_FORCED"
    print(f"  [RECONCILE {symbol}] {exit_reason} NU s-a triggerit dupa "
          f"{_RECONCILE_RETRIES} retries — chase_close fortat")
    await ex.chase_close(symbol, direction)
    await _assert_closed(symbol, qty_local, forced_reason)
    return forced_reason


async def close_position(symbol: str, exit_reason: str,
                          target_price: float) -> None:
    """
    Inchide pozitia DESCHISA pe Bybit (market opus, reduceOnly).

    Pipeline:
      1. Place market reduce-only (side mapping LONG->Sell, SHORT->Buy)
         Fallback chase_close daca place_market esueaza.
      2. _reconcile_close — confirma qty_real=0 pe Bybit. 4 ramuri:
         clean / partial (chase rest, _PARTIAL) / not-triggered (retry+force,
         _FORCED) / qty desync (HALT raise). Raise pe anomalie sau chase failure.
      3. Wait for settle (1.5s)
      4. fetch_pnl_for_trade (real Bybit PnL incl fees)
      5. record_closed_trade in state
      6. Telegram notification cu icon corect
      7. Equity sync post-close

    Apelat din: signal flip (CLOSE_LONG/CLOSE_SHORT). NU din private WS sau
    check_external_close — acelea folosesc close_pipeline_external (qty=0
    deja confirmat, reconciliere redundanta).

    Concurrent safety: lock + dedup pe entry_ts_ms previn double-record cand
    Bybit fileaza SL/TP atomic in paralel (private WS task triggereaza si
    close_pipeline_external pentru acelasi trade).
    """
    async with _get_close_lock(symbol):
        pos = _state.get_position(symbol)
        if pos is None:
            # Inchis deja de un alt coroutine (private WS / defense-in-depth).
            print(f"  [CLOSE {symbol}] no position — skip (already closed by other coroutine)")
            return
        # Dedup explicit: daca un trade cu acelasi entry_ts deja inregistrat.
        for existing in reversed(_state.trades):
            if (existing.symbol == symbol
                    and existing.entry_ts_ms == pos.opened_ts_ms):
                print(f"  [CLOSE {symbol}] dedup: trade entry_ts={pos.opened_ts_ms} "
                      f"deja inregistrat (id={existing.id}) — skip duplicate")
                return
        await _close_position_locked(symbol, exit_reason, target_price, pos)


async def _close_position_locked(symbol: str, exit_reason: str,
                                  target_price: float, pos) -> None:
    """Body close_position, executat sub _close_locks[symbol]."""
    side = _close_side(pos.direction)
    try:
        order_id = await ex.place_market(symbol, side, pos.qty, reduce_only=True)
        if not order_id:
            # Fallback: chase_close (PostOnly maker chase + Market fallback la sfarsit)
            print(f"  [CLOSE {symbol}] place_market FAILED — chase_close")
            ok = await ex.chase_close(symbol, pos.direction)
            if not ok:
                await tg.send_critical(
                    f"CLOSE FAILED — {symbol}",
                    "place_market + chase_close au eșuat — verifică manual",
                    symbol=symbol,
                )
                log_event("close_failed", symbol=symbol, reason=exit_reason)
                return
    except Exception as e:
        # Network glitch / Bybit error — incercam chase_close inainte sa cedam
        print(f"  [CLOSE {symbol}] place_market raised: {e!r} — chase_close")
        try:
            ok = await ex.chase_close(symbol, pos.direction)
            if not ok:
                raise RuntimeError("chase_close esuat")
        except Exception as e2:
            await tg.send_critical(
                f"CLOSE FAILED — {symbol}",
                f"<code>place_market: {e!r}</code>\n<code>chase_close: {e2!r}</code>",
                symbol=symbol,
            )
            log_event("close_failed", symbol=symbol, reason=exit_reason,
                      error=f"{e!r} | {e2!r}")
            return

    # HL: SL/TP sunt trigger orders STANDALONE (nu position-attached ca Bybit) →
    # NU se auto-sterg cand pozitia se inchide. Pe path-ul normal (place_market
    # reduce-only) ar ramane orfane si ar declansa pe o pozitie viitoare. Cancel
    # explicit, best-effort (chase_close le sterge deja pe fallback; re-cancel e
    # idempotent). Pe Bybit asta nu era necesar — adaptare HL, nu port Bybit.
    try:
        await ex.cancel_all_stops(symbol)
    except Exception as e:
        print(f"  [CLOSE {symbol}] cancel_all_stops best-effort failed: {e!r}")

    # Reconcile cu Bybit (4 ramuri). Pe anomalie raise → halt simbol in caller.
    try:
        final_exit_reason = await _reconcile_close(
            symbol, pos.direction, pos.qty, exit_reason)
    except ReconciliationError as e:
        print(f"  [{symbol}] HALT pe ReconciliationError: {e}")
        _halted[symbol] = True
        log_event("reconcile_halt", symbol=symbol, reason=exit_reason, error=str(e))
        return

    # Allow Bybit to settle the close + index closed-pnl
    await asyncio.sleep(1.5)

    now_ms = int(time.time() * 1000)
    # PnL window: adopt_ts_ms (daca pozitia a fost adoptata la resume) ALTFEL
    # opened_ts_ms. Eviti contaminare cu piramidari/scale-out vechi inchise
    # INAINTE de adopt-time pe trade-urile preluate la restart.
    pnl_entry_ts = pos.adopt_ts_ms if pos.adopt_ts_ms is not None else pos.opened_ts_ms
    pnl_data = await ex.fetch_pnl_for_trade(symbol, pnl_entry_ts, now_ms)
    avg_exit = pnl_data.get("avg_exit") or target_price
    pnl_real = pnl_data.get("pnl", 0.0)
    fees_real = pnl_data.get("fees", 0.0)

    # FALLBACK PnL: cand closed-pnl nu e indexat dupa ~19s retry (sau API down),
    # n_fills=0 → pnl_data["pnl"]=0. Fara fallback trade-ul ar avea PnL=0
    # PERMANENT in TradeRecord. Estimam local din directie + qty + entry/exit.
    pnl_estimated = pnl_data.get("n_fills", 0) == 0
    if pnl_estimated:
        pnl_real, fees_real = _estimate_pnl_fallback(
            pos.direction, pos.entry_price, pos.qty, avg_exit)
        print(f"  [PNL {symbol}] closed-pnl neindexat → estimare locala: "
              f"pnl_est={pnl_real:+.4f} fees_est={fees_real:.4f}")

    extra = _build_trade_extra(pos)
    if pnl_estimated:
        extra["pnl_estimated"] = True

    trade = TradeRecord(
        id=0,  # set in record_closed_trade
        symbol=symbol, direction=pos.direction,
        entry_ts_ms=pos.opened_ts_ms, entry_price=pos.entry_price,
        sl_price=pos.sl_price, tp_price=pos.tp_price, qty=pos.qty,
        exit_ts_ms=now_ms, exit_price=avg_exit,
        exit_price_target=target_price, exit_reason=final_exit_reason,
        pnl=pnl_real, fees=fees_real,
        extra=extra,
    )
    _state.record_closed_trade(trade)
    _state.save()
    log_event("trade_closed", **trade.to_dict())
    # Dashboard: record trade closed (best-effort, exceptia nu blocheaza).
    reporter = _reporters.get(symbol)
    if reporter is not None:
        try:
            # pnl_pct = % din initial_account (consistent cu BP — referential
            # equity, NU notional). Dashboard agreghaza pnl_pct across boti
            # asumand acelasi numitor.
            init_acc = _state.initial_account
            pnl_pct = ((trade.pnl / init_acc * 100) if init_acc else None)
            # side: lowercase "long"/"short" (conventie dashboard, aceeasi ca
            # open_side din heartbeat). Try/except TypeError = backwards-compat
            # cu versiuni vechi bot_reporter fara `side` param.
            side = (trade.direction.lower()
                    if trade.direction in ("LONG", "SHORT") else None)
            try:
                reporter.record_trade(pnl=trade.pnl, pnl_pct=pnl_pct,
                                       exit_reason=trade.exit_reason,
                                       side=side)
            except TypeError:
                reporter.record_trade(pnl=trade.pnl, pnl_pct=pnl_pct,
                                       exit_reason=trade.exit_reason)
        except Exception as e:
            print(f"  [REPORTER {symbol}] record_trade failed: {e}")

    # Telegram — icon titlu: ⚠️ pe EXTERNAL/reconcile (defense-in-depth, verifica).
    # Altfel 💰 win / 🩸 loss (pnl_emoji) pt scan rapid in stream-ul Telegram.
    is_reconcile_path = final_exit_reason.endswith(("_PARTIAL", "_FORCED"))
    if final_exit_reason.startswith("EXTERNAL") or is_reconcile_path:
        sign = "⚠️"
    else:
        sign = tg.pnl_emoji(pnl_real)

    ret_pct = ((_state.shared_equity - _state.initial_account)
               / _state.initial_account * 100) if _state.initial_account else 0
    # PnL label: cand fallback estimat (closed-pnl neindexat), aratam clar
    # ca nu e PnL Bybit real → user sti ca cifra e aproximativa.
    pnl_label = ("estimat — closed-pnl neindexat" if pnl_estimated
                 else "Bybit real, fees incluse")
    # Best-effort tg.send + broadcast — daca pica, trade-ul ramane corect
    # inregistrat in _state (pipeline-ul nu cade pe notification failure).
    try:
        await tg.send(
            f"{sign} TRADE ÎNCHIS — {tg.dir_emoji(pos.direction)} {pos.direction}",
            f"<b>Entry:</b> <code>{ex.smart_price(pos.entry_price)}</code>  "
            f"(<code>{tg.fmt_time(pos.opened_ts_ms)}</code>)\n"
            f"<b>Exit:</b>  <code>{ex.smart_price(avg_exit)}</code>  "
            f"(<code>{final_exit_reason}</code>)  (<code>{tg.fmt_time(now_ms)}</code>)\n"
            f"{tg.pnl_emoji(pnl_real)} <b>PnL:</b> <code>${pnl_real:+,.2f}</code>  "
            f"({pnl_label})\n"
            f"📊 <b>Account:</b> <code>${_state.shared_equity:,.2f}</code>  |  "
            f"<b>Return:</b> <code>{ret_pct:+.2f}%</code>",
            symbol=symbol,
        )
    except Exception as e:
        print(f"  [CLOSE {symbol}] tg.send failed (best-effort): {e!r}")

    # Broadcast to chart (trade_closed include equity_point pt update curve)
    try:
        eq_point = (_state.equity_curve[-1] if _state.equity_curve else None)
        await broadcast({"type": "trade_closed", "symbol": symbol,
                         "trade": trade.to_dict(), "summary": _state.summary(),
                         "equity_point": eq_point})
    except Exception as e:
        print(f"  [CLOSE {symbol}] broadcast failed (best-effort): {e!r}")

    # Post-close equity sync
    await sync_equity(reason=f"CLOSE_{symbol}")


# ============================================================================
# Defense-in-depth: detect external close
# ============================================================================

async def check_external_close(symbol: str) -> bool:
    """
    Pe fiecare confirmed bar, daca local has_position dar Bybit qty=0,
    sintetizam EXTERNAL close. Returneaza True daca s-a sintetizat.
    """
    pos = _state.get_position(symbol)
    if pos is None:
        return False

    # Strict read — None pe API fail (NU 0.0). Un GET ratat (timeout, rate
    # limit, 5xx, NAT reset la 00:00 UTC) NU trebuie interpretat ca qty=0 →
    # close fals al unui trade sanatos.
    qty_real = await ex.get_position_qty_strict(symbol)
    if qty_real is None:
        print(f"  [EXTERNAL-CHECK {symbol}] API fail — skip (retry next bar)")
        return False

    if qty_real > _RECONCILE_QTY_EPS:
        # Position still exists — check no anomaly
        if qty_real > pos.qty + _RECONCILE_QTY_EPS:
            msg = f"qty_real={qty_real} > qty_local={pos.qty}"
            print(f"  [{symbol}] RECONCILE HALT: {msg}")
            await tg.send_critical(
                f"{symbol} qty desync",
                f"<b>Local:</b> {pos.qty}\n<b>Bybit:</b> {qty_real}\n"
                f"Bot HALTED pe simbol. Verifică manual.",
                symbol=symbol,
            )
            _halted[symbol] = True
            raise ReconciliationError(msg)
        return False

    # qty=0 dintr-un singur read — CONFIRMA multi-attempt inainte de a sintetiza
    # EXTERNAL (care e ireversibil: record_closed_trade). Pe API unreachable sau
    # qty>0 la re-check → skip, retry pe bara urmatoare.
    confirmed = await ex.confirm_position_closed(symbol)
    if confirmed is None:
        print(f"  [{symbol}] external_close: confirm API unreachable — skip, retry next bar")
        return False
    if not confirmed:
        print(f"  [{symbol}] external_close: confirm=False (inca activa/race) — skip")
        return False
    # Re-check local: alt handler (private WS) ar fi putut inchide intre timp.
    if _state.get_position(symbol) is None:
        return False

    # qty=0 confirmat multi-attempt — sintetizam EXTERNAL.
    print(f"  [{symbol}] DESYNC: local in_trade=True, Bybit qty=0 (confirmat) → EXTERNAL")
    last_close = _signals[symbol].df.iloc[-1]["close"] if len(_signals[symbol].df) else pos.entry_price
    await close_pipeline_external(symbol, exit_reason="EXTERNAL",
                                   target_price=float(last_close))
    return True


async def close_pipeline_external(symbol: str, exit_reason: str,
                                   target_price: float) -> None:
    """
    Variant a close_position pt cazul cand pozitia NU mai e pe Bybit (deja
    inchisa extern). Skip place_market, doar fetch PnL + record + notify.

    Concurrent safety: lock + dedup pe entry_ts_ms (acelasi mecanism ca
    close_position) — apelat din private WS si din check_external_close
    (defense-in-depth public WS); ambele pot observa qty=0 simultan.
    """
    async with _get_close_lock(symbol):
        pos = _state.get_position(symbol)
        if pos is None:
            print(f"  [CLOSE-EXT {symbol}] no position — skip (already closed)")
            return
        for existing in reversed(_state.trades):
            if (existing.symbol == symbol
                    and existing.entry_ts_ms == pos.opened_ts_ms):
                print(f"  [CLOSE-EXT {symbol}] dedup: trade entry_ts="
                      f"{pos.opened_ts_ms} deja inregistrat (id={existing.id}) — skip")
                return
        await _close_pipeline_external_locked(symbol, exit_reason, target_price, pos)


async def _close_pipeline_external_locked(symbol: str, exit_reason: str,
                                            target_price: float, pos) -> None:
    """Body close_pipeline_external, executat sub _close_locks[symbol]."""
    await asyncio.sleep(1.5)  # let closed-pnl index
    now_ms = int(time.time() * 1000)
    # PnL window: adopt_ts_ms (daca pozitia a fost adoptata la resume) ALTFEL
    # opened_ts_ms. Eviti contaminare cu piramidari/scale-out vechi inchise
    # INAINTE de adopt-time pe trade-urile preluate la restart.
    pnl_entry_ts = pos.adopt_ts_ms if pos.adopt_ts_ms is not None else pos.opened_ts_ms
    pnl_data = await ex.fetch_pnl_for_trade(symbol, pnl_entry_ts, now_ms)
    avg_exit = pnl_data.get("avg_exit") or target_price
    pnl_real = pnl_data.get("pnl", 0.0)
    fees_real = pnl_data.get("fees", 0.0)

    # FALLBACK PnL — vezi nota in _close_position_locked.
    pnl_estimated = pnl_data.get("n_fills", 0) == 0
    if pnl_estimated:
        pnl_real, fees_real = _estimate_pnl_fallback(
            pos.direction, pos.entry_price, pos.qty, avg_exit)
        print(f"  [PNL-EXT {symbol}] closed-pnl neindexat → estimare locala: "
              f"pnl_est={pnl_real:+.4f} fees_est={fees_real:.4f}")

    extra = _build_trade_extra(pos)
    if pnl_estimated:
        extra["pnl_estimated"] = True

    trade = TradeRecord(
        id=0, symbol=symbol, direction=pos.direction,
        entry_ts_ms=pos.opened_ts_ms, entry_price=pos.entry_price,
        sl_price=pos.sl_price, tp_price=pos.tp_price, qty=pos.qty,
        exit_ts_ms=now_ms, exit_price=avg_exit,
        exit_price_target=target_price, exit_reason=exit_reason,
        pnl=pnl_real, fees=fees_real,
        extra=extra,
    )
    _state.record_closed_trade(trade)
    _state.save()
    log_event("trade_closed", **trade.to_dict())
    # Dashboard: record trade closed (best-effort, exceptia nu blocheaza).
    reporter = _reporters.get(symbol)
    if reporter is not None:
        try:
            # pnl_pct = % din initial_account (consistent cu BP — referential
            # equity, NU notional). Dashboard agreghaza pnl_pct across boti
            # asumand acelasi numitor.
            init_acc = _state.initial_account
            pnl_pct = ((trade.pnl / init_acc * 100) if init_acc else None)
            # side: lowercase "long"/"short" (conventie dashboard, aceeasi ca
            # open_side din heartbeat). Try/except TypeError = backwards-compat
            # cu versiuni vechi bot_reporter fara `side` param.
            side = (trade.direction.lower()
                    if trade.direction in ("LONG", "SHORT") else None)
            try:
                reporter.record_trade(pnl=trade.pnl, pnl_pct=pnl_pct,
                                       exit_reason=trade.exit_reason,
                                       side=side)
            except TypeError:
                reporter.record_trade(pnl=trade.pnl, pnl_pct=pnl_pct,
                                       exit_reason=trade.exit_reason)
        except Exception as e:
            print(f"  [REPORTER {symbol}] record_trade failed: {e}")

    # Icon ⚠️ pe EXTERNAL: defense-in-depth a sintetizat close-ul (Bybit a
    # inchis fara stiinta strategiei — verifica de ce). Altfel pnl_emoji.
    sign = "⚠️" if exit_reason == "EXTERNAL" else tg.pnl_emoji(pnl_real)
    ret_pct = ((_state.shared_equity - _state.initial_account)
               / _state.initial_account * 100) if _state.initial_account else 0
    # Best-effort: tg + broadcast nu blocheaza state cleanup.
    try:
        await tg.send(
            f"{sign} TRADE ÎNCHIS — {tg.dir_emoji(pos.direction)} {pos.direction}",
            f"<b>Entry:</b> <code>{ex.smart_price(pos.entry_price)}</code>  "
            f"(<code>{tg.fmt_time(pos.opened_ts_ms)}</code>)\n"
            f"<b>Exit:</b>  <code>{ex.smart_price(avg_exit)}</code>  "
            f"(<code>{exit_reason}</code>)  (<code>{tg.fmt_time(now_ms)}</code>)\n"
            f"{tg.pnl_emoji(pnl_real)} <b>PnL:</b> <code>${pnl_real:+,.2f}</code>  "
            f"({'estimat — closed-pnl neindexat' if pnl_estimated else 'Bybit real, fees incluse'})\n"
            f"📊 <b>Account:</b> <code>${_state.shared_equity:,.2f}</code>  |  "
            f"<b>Return:</b> <code>{ret_pct:+.2f}%</code>",
            symbol=symbol,
        )
    except Exception as e:
        print(f"  [CLOSE-EXT {symbol}] tg.send failed (best-effort): {e!r}")
    try:
        eq_point = (_state.equity_curve[-1] if _state.equity_curve else None)
        await broadcast({"type": "trade_closed", "symbol": symbol,
                         "trade": trade.to_dict(), "summary": _state.summary(),
                         "equity_point": eq_point})
    except Exception as e:
        print(f"  [CLOSE-EXT {symbol}] broadcast failed (best-effort): {e!r}")
    await sync_equity(reason=f"CLOSE_{symbol}")


# ============================================================================
# Strategy evaluation (per confirmed bar)
# ============================================================================

async def on_confirmed_bar(symbol: str, bar: dict) -> None:
    """Apelat dupa fiecare bara confirmed (4h close)."""
    if _halted.get(symbol):
        return

    sig = _signals[symbol]
    sig.update_buffer(bar)
    sig.recompute_indicators()

    # Defense-in-depth: check external close before evaluating new signal
    try:
        if await check_external_close(symbol):
            return  # handled, don't evaluate this bar
    except ReconciliationError:
        return  # halted

    # ORDINE BROADCAST: candle + indicators FIRST, apoi strategy decisions.
    # Asta garanteaza ca chart-ul are bara curenta in CANDLES inainte sa
    # primeasca position_open → showLiveLines vede lastCandle.time = entry bar.
    ts_s = bar["ts_ms"] // 1000
    await broadcast({
        "type": "candle", "symbol": symbol,
        "candle": {"time": ts_s,
                   "open": bar["open"], "high": bar["high"],
                   "low": bar["low"], "close": bar["close"]},
    })

    # Update chart indicators (last bar) + broadcast — branched per strategy
    df = sig.df
    cache = sig.cache
    if cache is not None and len(df) > 0:
        i = len(df) - 1
        if isinstance(sig, BBMeanReversionSignal):
            indicator_arrs = [("bb_mid", cache.bb_mid), ("bb_upper", cache.bb_upper),
                              ("bb_lower", cache.bb_lower)]
        else:
            indicator_arrs = [("hull_n1", cache.n1), ("tenkan", cache.tenkan),
                              ("kijun", cache.kijun), ("senkou_a", cache.senkou_h),
                              ("senkou_b", cache.senkou_l)]
        for name, arr in indicator_arrs:
            v = arr[i]
            if not pd.isna(v):
                fv = float(v)
                _state.add_indicator_point(symbol, name, ts_s, fv)
                await broadcast({"type": "indicator", "symbol": symbol,
                                 "name": name, "time": ts_s, "value": fv})

    # Acum evalueaza strategia + dispatch
    pos = _state.get_position(symbol)
    pos_dir = pos.direction.lower() if pos else None
    entry_px = pos.entry_price if pos else 0.0

    if isinstance(sig, BBMeanReversionSignal):
        # bars_held DERIVAT din opened_ts_ms (nr bare intre bara-entry si bara
        # curenta), NU contor in-memory. Contorul se pierdea la restart →
        # time-exit ratat. Derivarea e restart-proof (opened_ts_ms persistat +
        # reconciliat la adopt) SI numara barele scurse cat botul a fost jos
        # (ca Pine). Floor opened_ts la bar boundary pt count exact.
        if pos is not None and pos.opened_ts_ms:
            iv_ms = nl.interval_ms(_TF_INTERVAL)
            entry_bar = nl.current_bar_open_ms(pos.opened_ts_ms, _TF_INTERVAL)
            # +1: opened_ts_ms = wall-clock (~close-ul barei-semnal S = inceputul
            # barei S+1), deci entry_bar = S+1, cu o bara in urma ancorei Pine
            # (bara-semnal S). Fara +1, time-exit iese cu 1 bara (4h) mai tarziu
            # decat Pine. Cu +1 → paritate exacta: Pine bars_held=k la S+k, la fel bot.
            bars_held = max(0, int((bar["ts_ms"] - entry_bar) / iv_ms)) + 1
            pos.bars_held = bars_held      # sync field pt UI/persist
        else:
            bars_held = 0
        decision = sig.evaluate(has_position=pos_dir, entry_price=entry_px,
                                 bars_held=bars_held)
    else:
        decision = sig.evaluate(has_position=pos_dir, entry_price=entry_px)
    print(f"  [{symbol}] decision={decision.action} reason={decision.reason}")
    log_event("decision", symbol=symbol, action=decision.action,
              reason=decision.reason, price=decision.price)

    action = decision.action
    if action == "OPEN_LONG":
        await open_position(symbol, "LONG", decision.price,
                             bar_ts_ms=bar["ts_ms"])
    elif action == "OPEN_SHORT":
        await open_position(symbol, "SHORT", decision.price,
                             bar_ts_ms=bar["ts_ms"])
    elif action in ("CLOSE_LONG", "CLOSE_SHORT"):
        await close_position(symbol, "SIGNAL", decision.price)
    elif action in ("SL_LONG", "SL_SHORT", "TP_LONG", "TP_SHORT"):
        # Normal: SL/TP atomic Bybit triggereaza intra-bar si private WS
        # detecteaza close-ul. Strategia evalueaza per bara si returneaza
        # acelasi signal — il ignoram (pozitia e deja inchisa).
        # SOFTWARE FALLBACK: cand pos.sl_armed=False (set_position_sl a
        # esuat la open), Bybit nu va trigerui niciodata SL/TP. In acest
        # caz folosim signal-ul strategiei ca SL/TP software — escaleaza
        # la close_position cu reason "{action}_SOFTWARE".
        if pos is not None and not pos.sl_armed:
            sw_reason = f"{action}_SOFTWARE"
            print(f"  [{symbol}] software fallback: {action} (Bybit SL nu e armed) "
                  f"→ close_position({sw_reason})")
            await close_position(symbol, sw_reason, decision.price)


# ============================================================================
# Public WS — klines consumer
# ============================================================================

async def _fill_ws_gap(symbol: str, first_ws_ts_s: int) -> None:
    """Backfill bare confirmed ratate intre ultima bara sincronizata si prima
    bara primita pe (re)conectarea WS curenta. HL streameaza candle DOAR de la
    subscribe inainte — barele inchise in timpul unui disconnect NU sunt
    retrimise → fara backfill, semnalul de pe bara ratata nu e evaluat
    (entry/exit ratat → desync fata de Pine). Le aducem prin REST si le rulam
    in ordine prin on_confirmed_bar (acelasi pipeline ca un tick WS confirmed)."""
    last_synced = _last_synced_ts.get(symbol)
    if last_synced is None:
        return
    interval_s = nl.interval_ms(_TF_INTERVAL) // 1000
    next_expected = last_synced + interval_s
    if first_ws_ts_s <= next_expected:
        return  # continuitate normala, niciun gap
    print(f"  [SYNC {symbol}] GAP: have {last_synced}, first WS bar "
          f"{first_ws_ts_s} (expected {next_expected}) — fetch REST")
    try:
        raw = await ex.get_kline(symbol, _TF_INTERVAL,
                                 start=(last_synced + 1) * 1000,
                                 end=first_ws_ts_s * 1000, limit=1000)
    except Exception as e:
        print(f"  [SYNC {symbol}] gap fetch failed: {e!r}")
        return
    n = 0
    for row in reversed(raw):  # get_kline returneaza DESC → procesam ASC (cronologic)
        ts_s = int(row[0]) // 1000
        if ts_s <= last_synced or ts_s >= first_ws_ts_s:
            continue  # capete excluse (last_synced procesat; first_ws vine via WS)
        bar = {"ts_ms": int(row[0]), "open": float(row[1]), "high": float(row[2]),
               "low": float(row[3]), "close": float(row[4]),
               "volume": float(row[5]), "confirmed": True}
        ring = _candles.setdefault(symbol, [])
        if not (ring and ring[-1][0] == ts_s):
            ring.append([ts_s, bar["open"], bar["high"], bar["low"], bar["close"]])
            if len(ring) > 5000:
                ring.pop(0)
        _last_synced_ts[symbol] = ts_s
        _state.mark_first_candle(symbol, ts_s)
        try:
            await on_confirmed_bar(symbol, bar)
        except Exception:
            print(f"  [{symbol}] gap-fill on_confirmed_bar CRASHED:\n"
                  f"{traceback.format_exc()}")
        n += 1
    print(f"  [SYNC {symbol}] gap filled: {n} bare "
          f"(last_synced → {_last_synced_ts.get(symbol)})")


# ============================================================================
# Private WS handlers
# ============================================================================

async def on_position_event(event: dict) -> None:
    """
    Detect Bybit-side close (SL/TP atomic trigger) sau external close.

    Eveniment cu size=0 dupa ce local has_position → trigger close pipeline.
    Distinctia BYBIT_SL vs BYBIT_TP se face prin avgPrice proximity to
    sl_price vs tp_price.
    """
    symbol = event.get("symbol", "")
    size = float(event.get("size", 0) or 0)
    avg = float(event.get("avgPrice", 0) or 0)

    if symbol not in _signals:
        return  # not our pair

    pos = _state.get_position(symbol)
    if size > _RECONCILE_QTY_EPS:
        # Pozitia inca exista — log doar
        return

    if pos is None:
        return  # already closed locally

    # Determine reason based on price proximity (sl vs tp)
    if pos.tp_price and abs(avg - pos.tp_price) / pos.tp_price < 0.005:
        reason = "BYBIT_TP"
    elif abs(avg - pos.sl_price) / pos.sl_price < 0.005:
        reason = "BYBIT_SL"
    else:
        reason = "EXTERNAL"

    print(f"  [{symbol}] private WS detected close: avg={avg} → {reason}")
    await close_pipeline_external(symbol, reason, target_price=avg or pos.entry_price)


async def on_order_event(event: dict) -> None:
    sym = event.get("symbol", "")
    if sym and sym not in _signals:
        return  # not our pair — drop instant (defensive: subaccount V4 e dedicat
                # dar daca cineva adauga manual alt simbol, NU logam zgomot)
    status = event.get("orderStatus", "?")
    if status in ("Filled", "Cancelled", "Rejected"):
        print(f"  [ORDER {sym}] {status}  id={event.get('orderId', '?')[:8]}")


async def on_execution_event(event: dict) -> None:
    sym = event.get("symbol", "")
    if sym and sym not in _signals:
        return  # not our pair
    # silent — fills sunt trate prin position events


# ============================================================================
# Heartbeat — equity sync la (next_bar_close - 60s)
# ============================================================================

def _send_reporter_heartbeat() -> None:
    """Heartbeat per simbol la dashboard. Best-effort: exceptii nu blocheaza.
    Apelat din heartbeat_loop (pe bara) + _periodic_heartbeat (30s)."""
    if not _reporters:
        return
    paused = bc.is_paused()
    equity = _state.shared_equity
    for sym, reporter in _reporters.items():
        try:
            if _halted.get(sym):
                status = "error"
            elif paused:
                status = "paused"
            else:
                status = "running"
            pos = _state.get_position(sym)
            open_side = None
            open_entry = None
            open_pnl = None
            if pos is not None:
                open_side = pos.direction.lower()
                open_entry = pos.entry_price
                # uPnL fresh: _last_prices[sym] update pe orice WS tick
                # (max 1-2s stale). Fallback la signal buffer (max 4h stale).
                last_price = _last_prices.get(sym)
                if last_price is None:
                    sig = _signals.get(sym)
                    if sig is not None and len(sig.df):
                        last_price = float(sig.df.iloc[-1]["close"])
                if last_price is not None and pos.entry_price and pos.qty:
                    sign_d = 1 if pos.direction == "LONG" else -1
                    open_pnl = (last_price - pos.entry_price) * pos.qty * sign_d
            reporter.heartbeat(
                status=status, equity=equity,
                open_side=open_side, open_entry=open_entry,
                open_pnl=open_pnl,
            )
        except Exception as e:
            print(f"  [REPORTER {sym}] heartbeat failed: {e}")


async def heartbeat_loop() -> None:
    while True:
        now_ms = int(time.time() * 1000)
        next_close = _next_bar_close_ms(now_ms)
        sleep_s = max(1, (next_close - 60_000 - now_ms) / 1000)
        await asyncio.sleep(sleep_s)
        try:
            await sync_equity(reason="HEARTBEAT")
        except Exception as e:
            print(f"  [HEARTBEAT] sync failed: {e}")
        _send_reporter_heartbeat()


async def periodic_reporter_heartbeat() -> None:
    """Heartbeat periodic la BOT_REPORTER_HEARTBEAT_SEC (default 30s) independent
    de bare confirmed.

    Motivatie: dashboard considera bot 'alive' daca last_heartbeat < threshold
    (tipic 5-10min). Pe TF mari (4h V4) heartbeat-ul pe-bara e prea rar →
    bot apare 'dead' intre bare. Acest task scrie heartbeat suplimentar cu
    interval scurt ca bot-ul sa apara mereu live in UI.

    Disable: BOT_REPORTER_HEARTBEAT_SEC=0. Fail-safe: exceptii logged, NU rup
    trading (helper-ul intern deja captureaza per-symbol).
    """
    interval = int(os.getenv("BOT_REPORTER_HEARTBEAT_SEC", "30"))
    if interval <= 0:
        print("  [REPORTER] periodic heartbeat DISABLED "
              "(BOT_REPORTER_HEARTBEAT_SEC=0)")
        return
    print(f"  [REPORTER] periodic heartbeat: every {interval}s")
    while True:
        await asyncio.sleep(interval)
        _send_reporter_heartbeat()


# ============================================================================
# Agent expiration check + watchdog (pattern BP-HL):
#   - BOOT check: o singura data la bootstrap. Daca agent NOT FOUND sau
#     EXPIRED → tg.send_critical + RuntimeError (boot fail). Daca <14d ramase
#     → tg.send warning + continua boot. Altfel doar print.
#   - WATCHDOG task: zilnic. Daca EXPIRED → tg.send_critical. Daca <14d ramase
#     → tg.send warning (mesaj ZILNIC pana se schimba cheia — visibility maxima).
#
# Env:
#   AGENT_EXPIRY_WARN_DAYS = 14         (prag warning)
#   AGENT_EXPIRY_CHECK_INTERVAL_SEC = 86400  (interval watchdog, 1 zi)
# ============================================================================

AGENT_EXPIRY_WARN_DAYS = int(os.getenv("AGENT_EXPIRY_WARN_DAYS", "14"))
AGENT_EXPIRY_CHECK_INTERVAL_SEC = int(os.getenv("AGENT_EXPIRY_CHECK_INTERVAL_SEC", "86400"))


async def _agent_expiry_boot_check() -> None:
    """One-time check la bootstrap (pattern BP-HL main.py:684-711):
      1. HL_AGENT_PRIVATE_KEY missing → critical + RuntimeError
      2. agent NOT FOUND in extraAgents → critical + RuntimeError
      3. days < 0 (EXPIRED) → critical + RuntimeError
      4. days < AGENT_EXPIRY_WARN_DAYS → tg.send warning + continue
      5. else → print log + continue
    """
    if not os.getenv("HL_AGENT_PRIVATE_KEY", "").strip():
        msg = ("<b>HL_AGENT_PRIVATE_KEY</b> not set in env.\n"
               "Genereaza agent via scripts/gen_agent.py + seteaza in compose.")
        try:
            await tg.send_critical(f"{BOT_NAME} agent key MISSING", msg)
        except Exception:
            pass
        raise RuntimeError("HL_AGENT_PRIVATE_KEY missing")
    try:
        valid_until = await ex.fetch_agent_expiration_ms()
    except Exception as e:
        print(f"  [BOOT] agent expiry check failed (continuing): "
              f"{type(e).__name__}: {e}")
        return
    agent_addr = getattr(ex, "HL_AGENT_ADDRESS", "") or "<unknown>"
    if valid_until is None:
        msg = (f"Agent <code>{agent_addr}</code> NOT FOUND in main wallet's "
               f"extraAgents on HL. Verifica autorizare la "
               f"app.hyperliquid.xyz/API.")
        try:
            await tg.send_critical(f"{BOT_NAME} agent unauthorized", msg)
        except Exception:
            pass
        raise RuntimeError("agent not authorized on HL")
    days_left = (valid_until - int(time.time() * 1000)) / 86_400_000.0
    if days_left < 0:
        try:
            await tg.send_critical(
                f"{BOT_NAME} agent EXPIRED",
                f"Cheia agent <code>{agent_addr}</code> expirata de "
                f"<b><code>{abs(days_left):.1f}</code></b> zile.\n"
                f"Bot-ul NU poate trada — regenereaza cheia + restart."
            )
        except Exception:
            pass
        raise RuntimeError(f"agent expired {abs(days_left):.1f}d ago")
    elif days_left < AGENT_EXPIRY_WARN_DAYS:
        try:
            await tg.send(
                f"⚠️ {BOT_NAME} agent expira curand",
                f"Cheia agent expira in <b><code>{days_left:.1f}</code></b> zile.\n"
                f"Regenereaza inainte sa pierzi accesul."
            )
        except Exception:
            pass
    print(f"  [BOOT] HL agent {agent_addr} valid {days_left:.1f}d")


async def agent_expiration_monitor() -> None:
    """Watchdog zilnic agent expiry. Pattern BP-HL: mesaj zilnic sub 14d (NO
    DEDUP — visibility maxima pana se regenereaza cheia). Fail-safe: exceptii
    logged, NU rup trading."""
    while True:
        try:
            await asyncio.sleep(AGENT_EXPIRY_CHECK_INTERVAL_SEC)
            valid_until = await ex.fetch_agent_expiration_ms()
            if valid_until is None:
                continue
            days_left = (valid_until - int(time.time() * 1000)) / 86_400_000.0
            if days_left < 0:
                await tg.send_critical(
                    f"{BOT_NAME} agent EXPIRED",
                    f"Cheia agent expirata de <b><code>{abs(days_left):.1f}</code></b> zile.\n"
                    f"Bot-ul NU mai poate trada. Regenereaza + restart."
                )
            elif days_left < AGENT_EXPIRY_WARN_DAYS:
                await tg.send(
                    f"⚠️ {BOT_NAME} agent expira in {days_left:.1f} zile",
                    "Regenereaza cheia + restart bot."
                )
        except Exception as e:
            print(f"  [AGENT-EXP] check failed: {type(e).__name__}: {e}")


# ============================================================================
# Bootstrap
# ============================================================================

async def bootstrap() -> None:
    pairs_summary = [(p.symbol, p.strategy) for p in CONFIG.pairs if p.enabled]
    print(f"\n{'='*70}")
    print(f"  V4 PORTFOLIO [{BOT_NAME}] starting")
    print(f"  Strategies: multi (BB MR + Hull+Ichimoku)")
    print(f"  Pairs:    {pairs_summary}")
    print(f"  Chart:    http://0.0.0.0:{CHART_PORT}/  (TZ: {CHART_TZ})")
    print(f"{'='*70}\n")

    # HL agent expiration boot check (pattern BP-HL):
    # raise RuntimeError daca EXPIRED → boot fail visibility maxima.
    # <14 zile → Telegram warning + continua. Vezi _agent_expiry_watchdog
    # task pentru reminder ZILNIC pana se regenereaza cheia.
    await _agent_expiry_boot_check()

    # Preload HL universe metadata (asset_id, sz/px precision, max leverage)
    # INAINTE de set_leverage/sizing/order. Fara el, _meta() ridica "Instrument
    # not in cache". Sanity-check: ridica daca vreun simbol enabled lipseste din
    # universe HL.
    await ex.preload_instruments([p.symbol for p in CONFIG.pairs if p.enabled])

    # Restore state if persisted
    await asyncio.to_thread(_state.load)

    # Colectam pozitiile adoptate la resume; Telegram POZITIE GASITA se trimite
    # DUPA "BOT PORNIT" (UX: user vede intai ca bot-ul s-a pornit, apoi pozitiile).
    _resume_announce: list[tuple] = []
    # Pozitii persistate dar inchise EXTERN cat botul era jos (exchange flat la
    # boot). Le inregistram (PnL din fills + Telegram + DB) DUPA init reporter,
    # NU le stergem silentios. Vezi procesarea de dupa "BOT PORNIT".
    _offline_closed: list[str] = []

    for pair_cfg in CONFIG.pairs:
        if not pair_cfg.enabled:
            continue
        sym = pair_cfg.symbol
        sig: SignalGen
        if pair_cfg.strategy == "bb_mr":
            bb_cfg = BBMRConfig(
                symbol=sym, timeframe=pair_cfg.timeframe,
                bb_length=pair_cfg.bb_length, bb_std=pair_cfg.bb_std,
                rsi_length=pair_cfg.rsi_length,
                rsi_oversold=pair_cfg.rsi_oversold,
                rsi_overbought=pair_cfg.rsi_overbought,
                sl_pct=pair_cfg.sl_pct, tp_rr=pair_cfg.tp_rr,
                max_bars_in_trade=pair_cfg.max_bars_in_trade,
                taker_fee=CONFIG.portfolio.taker_fee,
            )
            sig = BBMeanReversionSignal(bb_cfg)
        else:  # 'hi' (default)
            ssc = PairStrategyConfig(
                symbol=sym, timeframe=pair_cfg.timeframe,
                hull_length=pair_cfg.hull_length,
                tenkan_periods=pair_cfg.tenkan_periods,
                kijun_periods=pair_cfg.kijun_periods,
                senkou_b_periods=pair_cfg.senkou_b_periods,
                displacement=pair_cfg.displacement,
                risk_pct_per_trade=pair_cfg.risk_pct_per_trade,
                sl_initial_pct=pair_cfg.sl_initial_pct,
                tp_pct=pair_cfg.tp_pct,
                max_hull_spread_pct=pair_cfg.max_hull_spread_pct,
                max_close_kijun_dist_pct=pair_cfg.max_close_kijun_dist_pct,
                taker_fee=CONFIG.portfolio.taker_fee,
            )
            sig = IchimokuSignal(ssc)
        print(f"  [{sym}] strategy={_strategy_label(pair_cfg.strategy)}")
        _signals[sym] = sig
        _pair_cfgs[sym] = pair_cfg
        # NU clobberam _state.positions[sym] aici — daca _state.load() a
        # restaurat un LivePosition pentru acest simbol, vrem sa-l pastram
        # si sa-l reconciliem cu Bybit mai jos. _state.get_position() returneaza
        # oricum None pe missing key (no need pt explicit set).

        # Set leverage
        await ex.set_leverage(sym, pair_cfg.leverage)

        # Warmup 400 bars cu retry 4x backoff (0, 2, 5, 10s) — Bybit poate
        # avea hicup-uri tranzient pe market/kline endpoint la boot. Daca toate
        # cele 4 incercari esueaza, marcam halt pe simbol + Telegram critical
        # si continuam (restul simbolurilor ruleaza normal).
        bars: list = []
        for attempt, delay in enumerate([0, 2, 5, 10]):
            if delay > 0:
                await asyncio.sleep(delay)
            try:
                bars = await ex.get_kline(sym, _TF_INTERVAL, limit=400)
                bars = nl.filter_closed_bars(bars, _TF_INTERVAL)
                if bars:
                    # get_kline întoarce DESC (cel mai nou primul). Indicatorii
                    # (BB/RSI/Ichimoku) + ancora de sync cer ASC (cronologic).
                    # Fără reverse, seria e flipată → indicatori garbage și
                    # _last_synced_ts ancorat pe bara cea mai VECHE (~66 zile in
                    # urma) → gap-fill masiv pe primul tick WS. BP-HL face la fel
                    # in load_history (reversed(raw)).
                    bars = list(reversed(bars))
                    if attempt > 0:
                        print(f"  [{sym}] warmup OK dupa retry #{attempt}")
                    break
            except Exception as e:
                print(f"  [{sym}] warmup attempt {attempt+1}/4 raised: {e!r}")
            print(f"  [{sym}] warmup attempt {attempt+1}/4 returned 0 bars")
        if bars:
            df = pd.DataFrame(bars, columns=["ts_ms", "open", "high", "low",
                                              "close", "volume", "turnover"])
            # get_kline da str-uri → forteaza numeric inainte de indicatori
            # (altfel np.isnan/SMA crapa pe dtype=object).
            df["ts_ms"] = df["ts_ms"].astype("int64")
            df.index = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
            df = df[["open", "high", "low", "close", "volume"]].astype(float)
            sig.warm_up(df)
            _last_synced_ts[sym] = int(bars[-1][0]) // 1000  # bars ASC → cel mai nou
            # Seed chart ring din warmup → chart-ul arata istoricul imediat la
            # load (pe 4h altfel ar fi gol pana la primul WS confirmed bar, ore).
            _candles[sym] = [[int(b[0]) // 1000, float(b[1]), float(b[2]),
                              float(b[3]), float(b[4])] for b in bars]
            print(f"  [{sym}] warmup {len(bars)} bars  last={df.index[-1]}")
        else:
            print(f"  [{sym}] FATAL: warmup esuat dupa 4 retry-uri — halt simbol")
            _halted[sym] = True
            try:
                await tg.send_critical(
                    f"{sym} warmup eșuat",
                    f"<b>get_kline</b> returnează 0 bare după 4 reîncercări "
                    f"(~17s total).\n"
                    f"<b>Stare bot:</b> simbolul e HALTAT — restul perechilor "
                    f"rulează normal.\nVerifică connectivitate Bybit și redeploy.",
                    symbol=sym,
                )
            except Exception:
                pass
            continue  # skip restul setup-ului pt acest simbol

        # Boot-time resume: V4 NU persista LivePosition in bot_state.json (doar
        # trades + equity). Single source of truth pt pozitii active = Bybit.
        # Scanam Bybit, dar Telegram POZITIE GASITA se trimite DUPA "BOT PORNIT"
        # (vezi mai jos) — colectam aici in _resume_announce.
        bybit_pos = await ex.fetch_open_position(sym)
        if bybit_pos is None and _state.get_position(sym) is not None:
            # Exchange FLAT dar state persistat avea pozitie → inchisa EXTERN
            # (manual/SL/TP) cat botul era jos. NU stergem silentios (s-ar pierde
            # trade-ul din Telegram + dashboard DB). O pastram in _state si o
            # inregistram via close_pipeline_external DUPA init reporter (PnL real
            # din fills). Colectam aici, procesam mai jos.
            print(f"  [{sym}] resume: exchange flat + pozitie persistata → "
                  f"inchisa extern cat botul era jos; inregistrez close dupa init reporter.")
            _offline_closed.append(sym)
            continue  # nu rula adopt-ul (bybit_pos e None oricum)
        if bybit_pos is not None:
            entry_px = bybit_pos["entry_price"]
            qty_real = bybit_pos["qty"]
            dir_real = bybit_pos["direction"]
            sl_real  = bybit_pos["sl_price"]
            tp_real  = bybit_pos["tp_price"]
            has_bybit_sl = sl_real is not None and sl_real > 0

            # STRAT 1 — Refuz adopt daca Bybit NU are SL setat.
            # Scenariu suspect: cineva a deschis manual SAU bot anterior a
            # esuat la set_position_sl + reconcilierile au omis. NU adoptam
            # local — alerta CRITICA, user decide (close manual sau add SL pe
            # Bybit App, apoi restart). Strategia continua in stadiul "no
            # position" si poate genera trade nou pe semnal — risc dublu-pozitie
            # pe care user trebuie sa-l gestioneze manual.
            if not has_bybit_sl:
                print(f"  [{sym}] resume: REFUZ ADOPT — Bybit qty={qty_real} "
                      f"FARA SL setat (suspect: manual sau bot fail)")
                try:
                    await tg.send_critical(
                        "POZIȚIE FĂRĂ SL — refuz adoptie",
                        f"<b>Pe Bybit:</b> {tg.dir_emoji(dir_real)} {dir_real}  "
                        f"<b>Qty:</b> <code>{qty_real}</code>  "
                        f"<b>Entry:</b> <code>{ex.smart_price(entry_px)}</code>\n"
                        f"<b>SL Bybit:</b> <code>NESETAT</code>\n"
                        f"\n"
                        f"<b>Acțiune:</b>\n"
                        f"  1. Setează SL manual pe Bybit App (recomandat: "
                        f"entry × (1 ± <code>{pair_cfg.effective_sl_pct*100:.1f}%</code>)),\n"
                        f"  2. SAU închide poziția manual,\n"
                        f"  3. Apoi redeploy bot.\n"
                        f"\n"
                        f"<b>Stare bot:</b> NU adoptă local. Strategia poate "
                        f"genera trade nou pe semnal — risc dublă-poziție.",
                        symbol=sym,
                    )
                except Exception as e:
                    print(f"  [{sym}] resume tg.send_critical failed: {e!r}")
                # NU adoptam — continuam la urmatorul simbol
                continue

            # Bybit are SL → adoptie normala
            pos_usd = qty_real * entry_px
            risk_usd = pos_usd * pair_cfg.effective_sl_pct
            # opened_ts_ms = createdMs Bybit (chart entry line afisata la
            # momentul real al deschiderii). adopt_ts_ms = now (folosit pentru
            # fetch_pnl_for_trade window la close → exclude piramidari vechi
            # inchise INAINTE de adopt).
            opened_ts = bybit_pos["created_ms"] or int(time.time() * 1000)
            # Restart-proof time-exit: daca state persistat are o pozitie care
            # MATCH-uieste exchange-ul (aceeasi directie + qty ~egal), pastram
            # opened_ts_ms REAL din state. Altfel (HL n-are created_ms) opened_ts
            # = now → time-exit s-ar reseta la fiecare restart. Mismatch → stale,
            # folosim now.
            _persist = _state.get_position(sym)
            if (_persist is not None and _persist.direction == dir_real
                    and abs(_persist.qty - qty_real) <= max(qty_real * 0.05, 1e-9)):
                opened_ts = _persist.opened_ts_ms
                print(f"  [{sym}] resume: opened_ts din state persistat "
                      f"({opened_ts}) → time-exit restart-proof")
            adopt_ts = int(time.time() * 1000)
            resumed = LivePosition(
                symbol=sym,
                side=("Buy" if dir_real == "LONG" else "Sell"),
                direction=dir_real,
                qty=qty_real,
                entry_price=entry_px,
                sl_price=sl_real,
                tp_price=tp_real,
                leverage=pair_cfg.leverage,
                pos_usd=pos_usd,
                risk_usd=risk_usd,
                opened_ts_ms=opened_ts,
                order_id="",
                strategy=pair_cfg.strategy,
                bars_held=0,
                sl_armed=True,
                adopt_ts_ms=adopt_ts,
            )
            _state.set_position(sym, resumed)
            print(f"  [{sym}] resume: pos adoptata ({dir_real} qty={qty_real} "
                  f"entry={entry_px} sl={sl_real})")
            # Stocam datele pentru Telegram dupa "BOT PORNIT"
            _resume_announce.append((sym, pair_cfg, resumed))

    # INIT equity sync
    await sync_equity(reason="INIT")

    # Init bot_reporter per pereche (dashboard agregat). Fail-safe: orice eroare
    # → reporter dezactivat pe acel simbol, restul continua. Disable explicit
    # via BOT_REPORTER_DB="" in env.
    db_path = os.getenv("BOT_REPORTER_DB", "/dashboard/state.db")
    # control_url scris in state.db ca dashboard sa descopere automat unde
    # POST /api/pause|resume|stop. Format: schema://container:port (fara /api/).
    # NULL → dashboard NU afiseaza butoane control pt botul asta.
    control_url = os.getenv("BOT_CONTROL_URL", "") or None
    if db_path and _BOT_REPORTER_AVAILABLE:
        for pair_cfg in CONFIG.pairs:
            if not pair_cfg.enabled or _halted.get(pair_cfg.symbol):
                continue
            sym = pair_cfg.symbol
            try:
                _reporters[sym] = BotReporter(
                    bot_id=f"{BOT_NAME}_{sym}",
                    bot_name=BOT_NAME,
                    symbol=sym,
                    timeframe=pair_cfg.timeframe,
                    db_path=db_path,
                    control_url=control_url,
                )
                print(f"  [{sym}] reporter init OK (db={db_path} "
                      f"control_url={control_url})")
            except Exception as e:
                print(f"  [{sym}] reporter init FAILED ({type(e).__name__}: {e})"
                      f" — disabled pe simbol")

    # Offline-close drain: pozitii persistate dar inchise EXTERN cat botul era
    # jos (colectate in bucla de resume la _offline_closed). Le procesam ABIA
    # AICI, DUPA init reporter — close_pipeline_external scrie in DB via
    # _reporters.get(sym), care era None in bucla de sus (init dupa warmup).
    # Fara acest drain, close-ul amanat s-ar pierde: trade neinregistrat (fara
    # PnL/DB/Telegram) SI pozitie fantoma ramasa in state (record_closed_trade
    # n-ar rula → nu s-ar curata). target_price = fallback (fills dau avg_exit
    # real); ultimul close din warmup, altfel entry.
    for sym in _offline_closed:
        if _state.get_position(sym) is None:
            continue  # deja procesata (dedup/race)
        sig = _signals.get(sym)
        last_close = (float(sig.df.iloc[-1]["close"])
                      if sig is not None and len(sig.df)
                      else _state.get_position(sym).entry_price)
        try:
            await close_pipeline_external(sym, exit_reason="EXTERNAL",
                                          target_price=last_close)
            print(f"  [{sym}] offline-close inregistrat (PnL fills + DB + Telegram)")
        except Exception as e:
            print(f"  [{sym}] offline-close FAILED: {e!r} — pozitia ramane in state")

    # Strategy register indicators (chart overlay meta)
    # HI overlays
    _state.register_indicator("hull_n1", color="#ff8c00", line_width=2)
    _state.register_indicator("tenkan", color="#3498db", line_width=1)
    _state.register_indicator("kijun", color="#e74c3c", line_width=1)
    _state.register_indicator("senkou_a", color="#2ecc71", line_width=1)
    _state.register_indicator("senkou_b", color="#c0392b", line_width=1)
    # BB MR overlays
    _state.register_indicator("bb_mid", color="#9b59b6", line_width=1)
    _state.register_indicator("bb_upper", color="#34495e", line_width=1)
    _state.register_indicator("bb_lower", color="#34495e", line_width=1)

    # Telegram BOT_STARTED
    pairs_label = ", ".join(
        f"{p.symbol} [{_strategy_label(p.strategy)}]"
        for p in CONFIG.pairs if p.enabled
    )
    # INITIAL vs RESTART — daca restart_at ~ start_utc (< 60s) e primul boot;
    # altfel bot-ul a fost repornit si afisam ambele timestamp-uri (cand a
    # pornit prima oara + cand s-a repornit acum).
    restart_at = datetime.now(timezone.utc)
    is_first_boot = abs((restart_at - _state.start_utc).total_seconds()) < 60
    time_lines = (
        f"⏰ <b>Pornit la:</b>     <code>{tg.fmt_time(restart_at)}</code>\n"
        if is_first_boot else
        f"⏰ <b>Pornit initial:</b> <code>{tg.fmt_time(_state.start_utc)}</code>\n"
        f"🔄 <b>Restart la:</b>     <code>{tg.fmt_time(restart_at)}</code>\n"
    )
    await tg.send(
        "BOT PORNIT ✅ (multi-pair)",
        f"🧠 <b>Strategies:</b>   <code>multi (BB MR + Hull+Ichimoku)</code>\n"
        f"🪙 <b>Pairs:</b>        {pairs_label}\n"
        f"📊 <b>Account init:</b> <code>${_state.initial_account:,.2f}</code>\n"
        f"{time_lines}"
        f"🌐 <b>Chart:</b>        port <code>{CHART_HOST_PORT}</code>",
    )

    # Eveniment dashboard (linie verticala pe equity chart):
    # launch la primul boot, restart pe redeploy-uri ulterioare. Label =
    # BOT_VERSION env. bot_events e cheiat pe bot_name (NU bot_id/symbol),
    # deci emite UN SINGUR eveniment per bot — folosim primul reporter
    # disponibil. Fail-safe: exceptii nu rup boot-ul.
    if _reporters:
        first_reporter = next(iter(_reporters.values()))
        rec = getattr(first_reporter, "record_event", None)
        if callable(rec):
            try:
                rec("launch" if is_first_boot else "restart", BOT_VERSION)
            except Exception as e:
                print(f"  [REPORTER] record_event(launch/restart) failed: "
                      f"{type(e).__name__}: {e}")

    # Telegram POZITIE GASITA — dupa BOT PORNIT (ordine UX). Per pozitie
    # adoptata din Bybit, anunta user-ul ca bot-ul a preluat.
    for sym, _pair_cfg, pos in _resume_announce:
        sl_str = f"<code>{ex.smart_price(pos.sl_price)}</code>" if pos.sl_price else "—"
        tp_str = f"<code>{ex.smart_price(pos.tp_price)}</code>" if pos.tp_price else "—"
        try:
            await tg.send(
                "♻️ POZIȚIE GĂSITĂ",
                f"<b>Direcție:</b> {tg.dir_emoji(pos.direction)} {pos.direction}  "
                f"<b>Qty:</b> <code>{pos.qty}</code>\n"
                f"<b>Entry:</b> <code>{ex.smart_price(pos.entry_price)}</code>\n"
                f"<b>SL:</b> {sl_str}  <b>TP:</b> {tp_str}\n"
                f"<b>Risk:</b> <code>${pos.risk_usd:.2f}</code>\n"
                f"Trailing continuă pe SL existent.",
                symbol=sym,
            )
        except Exception as e:
            print(f"  [{sym}] resume tg.send failed (best-effort): {e!r}")


# ============================================================================
# FastAPI app + lifespan
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Install asyncio exception handler INAINTE de await-uri (loop e activ aici).
    # Task-uri create_task care arunca fara await sunt logged cu traceback in
    # loc de "Task exception was never retrieved" silent la GC.
    install_asyncio_exception_handler()

    await bootstrap()
    # Spawn background tasks (memory_monitor inclusiv — pre-OOM Telegram alert
    # cand RSS > MEM_MON_RSS_ALERT_MB; SIGKILL/OOM NU invoca excepthook,
    # singura fereastra de notification e PRE-kill via monitor).
    # HL WS multi-pair: ONE socket per coin pt candle + ONE socket pt events.
    # Wrapper-uri pt paritate cu V4 Bybit:
    #   1. on_confirmed_bar gap-fill: la reconnect WS, bare ratate aduse via REST
    #   2. on_unconfirmed_bar: tracking _last_prices pt uPnL fresh in periodic
    #      heartbeat (max 1-2s stale vs 4h).
    _TF_S = _TF_MS // 1000

    async def _on_confirmed_bar_hl(sym: str, bar: dict) -> None:
        # Gap-fill: daca avem un last_synced anterior si bara curenta vine cu
        # gap (> _TF_S + 60s margin), aducem barele lipsa via REST inainte de
        # a procesa cea curenta. _fill_ws_gap reuseaza pipeline-ul on_confirmed_bar.
        ts_s = bar["ts_ms"] // 1000
        last = _last_synced_ts.get(sym, 0)
        if last and ts_s > last + _TF_S + 60:
            try:
                await _fill_ws_gap(sym, ts_s)
            except Exception as e:
                print(f"  [{sym}] gap-fill failed: {e!r}")
        # Append bara confirmed in ring (chart). on_confirmed_bar broadcastuieste
        # candle separat; aici doar persistam in _candles pt /api/init la reload.
        _push_candle_ring(sym, bar)
        # Dedup + AVANSEAZA ancora (paritate V4 Bybit main.py:1357-1361). _fill_ws_gap
        # poate fi deja avansat _last_synced_ts → re-citim. FARA asta, ancora ramane
        # inghetata la valoarea de warmup → gap-fill REST + re-evaluare strategie pe
        # FIECARE bara (decizii duplicate, posibil entry/close spurios).
        if ts_s <= _last_synced_ts.get(sym, 0):
            return  # bara deja procesata (re-emisie HL sau adusa de gap-fill)
        _last_synced_ts[sym] = ts_s
        _state.mark_first_candle(sym, ts_s)
        # Forward la pipeline normal
        await on_confirmed_bar(sym, bar)

    async def _on_unconfirmed_bar_hl(sym: str, bar: dict) -> None:
        # Gap-fill la PRIMUL tick al sesiunii WS per simbol (paritate V4 Bybit
        # main.py:1343 _sync_done) → barele inchise in timpul unui disconnect
        # sunt recuperate imediat, nu abia la urmatorul close confirmed (~4h).
        ts_s = bar["ts_ms"] // 1000
        if not _sync_done.get(sym):
            _sync_done[sym] = True
            try:
                await _fill_ws_gap(sym, ts_s)
            except Exception as e:
                print(f"  [{sym}] first-tick gap-fill failed: {e!r}")
        # Tracking _last_prices pe ORICE tick → uPnL dashboard fresh in
        # periodic_reporter_heartbeat (paritate cu V4 Bybit public_ws_loop).
        _last_prices[sym] = float(bar["close"])
        # Update bara curenta in ring + broadcast intra-bar → chart live
        # (paritate V4 Bybit main.py:1372). NU evaluam strategia pe tick.
        _push_candle_ring(sym, bar)
        await broadcast({
            "type": "candle", "symbol": sym,
            "candle": {"time": ts_s,
                       "open": float(bar["open"]), "high": float(bar["high"]),
                       "low": float(bar["low"]), "close": float(bar["close"])},
        })

    enabled_symbols = [p.symbol for p in CONFIG.pairs if p.enabled]

    async def _on_user_event_hl_adapter(event: dict) -> None:
        """Adapter HL userEvents → V4 on_position_event Bybit-shape.
        HL ws_hl trimite {"kind": "fills"|"funding"|..., "data": <payload>}.
        V4 on_position_event asteapta {"symbol": ..., "size": ..., "avgPrice": ...}.
        Pe fill, query Bybit-style: get_position_qty(symbol) pt size curent.
        """
        kind = event.get("kind", "")
        if kind != "fills":
            # alte tipuri (funding, liq) — doar log
            print(f"  [HL USEREV] {kind}: {str(event.get('data'))[:200]}")
            return
        # Pe fill: ws_hl _handle_user_events filtreaza fills per coin (modificare
        # V4_HL vs BP-HL upstream — fix triple-fire pe multi-pair). Asadar toate
        # fills primite pe acest socket sunt pt coin-ul socket-ului. coins_seen
        # dedup pe acelasi update (acelasi simbol in 2 fills consecutive in
        # acelasi payload — rare, dar posibil pe scale-out partial).
        fills = event.get("data") or []
        if not isinstance(fills, list):
            return
        coins_seen: set[str] = set()
        for fill in fills:
            try:
                coin = (fill.get("coin") or "").upper()
                if not coin or coin in coins_seen:
                    continue
                coins_seen.add(coin)
                if coin not in _signals:
                    continue  # not our pair
                qty_abs = await ex.get_position_qty(coin)
                avg_px = float(fill.get("px", 0) or 0)
                await on_position_event({
                    "symbol": coin,
                    "size": str(qty_abs),
                    "avgPrice": avg_px,
                    "raw": fill,
                })
            except Exception as e:
                print(f"  [HL USEREV] fill adapter failed: {type(e).__name__}: {e}")

    tasks = [
        asyncio.create_task(hl_ws_runner.public_ws_loop_hl(
            symbols=enabled_symbols,
            interval_str=_TF_INTERVAL,
            on_confirmed_bar=_on_confirmed_bar_hl,
            on_unconfirmed_bar=_on_unconfirmed_bar_hl,
        )),
        asyncio.create_task(hl_ws_runner.private_ws_loop_hl(
            symbols=enabled_symbols,
            on_order_update=on_order_event,
            on_user_event=_on_user_event_hl_adapter,
        )),
        asyncio.create_task(heartbeat_loop()),
        asyncio.create_task(memory_monitor(BOT_NAME, tg_alert=tg.send_critical)),
        asyncio.create_task(periodic_reporter_heartbeat()),
        asyncio.create_task(agent_expiration_monitor()),
    ]
    try:
        yield
    finally:
        # INTERPRETARE EXIT CODES post-mortem (docker inspect ... OOMKilled):
        #   137 + OOMKilled=true   → OOM killer (SIGKILL, lifespan NU ajunge aici)
        #   143 + SIGTERM in log   → docker stop / restart policy
        #   130 + SIGINT in log    → Ctrl-C manual
        #   0   + graceful         → lifespan return curat
        # Pentru OOM (SIGKILL) NU ajungem aici — alerta vine din memory_monitor.
        print(f"  [SHUTDOWN] lifespan finally start  "
              f"signal={SHUTDOWN_SIGNAL['name']}", flush=True)
        for t in tasks:
            t.cancel()
        try:
            ret_pct = ((_state.shared_equity - _state.initial_account)
                       / _state.initial_account * 100) if _state.initial_account else 0
            await tg.send(
                "BOT OPRIT 🛑",
                f"🧠 <b>Strategies:</b> <code>multi (BB MR + Hull+Ichimoku)</code>\n"
                f"📡 <b>Signal:</b>     <code>{SHUTDOWN_SIGNAL['name'] or 'graceful'}</code>\n"
                f"⏰ <b>Oprit la:</b>   <code>{tg.fmt_time(datetime.now(timezone.utc))}</code>\n"
                f"📊 <b>Account:</b>    <code>${_state.shared_equity:,.2f}</code>  |  "
                f"<b>Return:</b> <code>{ret_pct:+.2f}%</code>\n"
                f"🔢 <b>Trades:</b>     <code>{len(_state.trades)}</code>",
            )
        except Exception as e:
            print(f"  [SHUTDOWN] tg.send failed: {e}")
        # Eveniment dashboard "shutdown" (linie verticala pe equity chart).
        # Label = signal name sau "clean" cand lifespan returneaza natural.
        # Fail-safe: exception nu blocheaza shutdown.
        if _reporters:
            first_reporter = next(iter(_reporters.values()))
            rec = getattr(first_reporter, "record_event", None)
            if callable(rec):
                try:
                    rec("shutdown", SHUTDOWN_SIGNAL["name"] or "clean")
                except Exception as e:
                    print(f"  [REPORTER] record_event(shutdown) failed: "
                          f"{type(e).__name__}: {e}")


# Signal handlers: SIGTERM (docker stop), SIGINT (Ctrl-C), SIGHUP. SIGKILL
# (OOM, kill -9) NU poate fi interceptat; vezi memory_monitor pre-OOM alert si
# `docker inspect ... OOMKilled` post-mortem.
install_signal_handlers()


app = FastAPI(lifespan=lifespan, title=f"{BOT_NAME} chart")

if Path(STATIC).exists():
    app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.get("/")
async def root():
    chart = Path(STATIC) / "chart_live.html"
    if chart.exists():
        return FileResponse(str(chart))
    return JSONResponse({"bot": BOT_NAME, "status": "running",
                         "note": "chart_live.html not yet created"})


@app.get("/api/init")
async def api_init():
    payload = _state.init_payload()
    # Active positions snapshot per simbol (chart afiseaza live lines la load)
    active = {}
    for sym, pos in _state.positions.items():
        if pos is None:
            continue
        active[sym] = {
            "direction": pos.direction, "entry": pos.entry_price,
            "sl": pos.sl_price, "tp": pos.tp_price,
            "qty": pos.qty, "risk_usd": pos.risk_usd,
            "entry_ms": pos.opened_ts_ms,
        }
    payload.update({
        "candles": _candles,
        "timeframe": "4h",
        "pairs": [p.symbol for p in CONFIG.pairs if p.enabled],
        "active_positions": active,
    })
    return JSONResponse(payload)


@app.get("/api/status")
async def api_status():
    return {
        "bot_name": BOT_NAME,
        "pairs": [p.symbol for p in CONFIG.pairs if p.enabled],
        "candles_total": {s: len(c) for s, c in _candles.items()},
        "connected_clients": len(_clients),
        "trading_paused": bc.is_paused(),
        "summary": _state.summary(),
        "halted": list(_halted.keys()) if _halted else [],
    }


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    _clients.add(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _clients.discard(ws)


# ============================================================================
# Control endpoints — dashboard ecosystem (vezi core/bot_control.py)
# ============================================================================

@app.post("/api/pause")
async def api_pause(token: str = ""):
    ok, err = bc.check_token(token)
    if not ok:
        return JSONResponse({"error": err}, status_code=403)
    bc.set_paused(True)
    try:
        await tg.send_warning(
            "Bot pauzat via dashboard",
            "Pozitiile EXISTENTE raman protejate de SL/TP atomic Bybit-side. "
            "Nu se mai deschid pozitii noi pana la /api/resume.",
        )
    except Exception:
        pass
    return {"status": "paused", "trading_paused": True}


@app.post("/api/resume")
async def api_resume(token: str = ""):
    ok, err = bc.check_token(token)
    if not ok:
        return JSONResponse({"error": err}, status_code=403)
    bc.set_paused(False)
    try:
        await tg.send_info(
            "Bot reluat via dashboard",
            "Strategiile pot deschide pozitii noi pe semnal.",
        )
    except Exception:
        pass
    return {"status": "running", "trading_paused": False}


@app.post("/api/stop")
async def api_stop(token: str = ""):
    """Stop = pauza + market-close TOATE pozitiile active."""
    ok, err = bc.check_token(token)
    if not ok:
        return JSONResponse({"error": err}, status_code=403)
    bc.set_paused(True)
    closed: list = []
    failed: list = []
    for sym in list(_state.positions.keys()):
        pos = _state.get_position(sym)
        if pos is None:
            continue
        try:
            await close_position(sym, "DASHBOARD_STOP", pos.entry_price)
            closed.append(sym)
        except Exception as e:
            print(f"  [STOP {sym}] close failed: {e!r}")
            failed.append(sym)
    try:
        await tg.send_critical(
            "Bot OPRIT via dashboard",
            f"<b>Inchise:</b> {', '.join(closed) if closed else '—'}\n"
            f"<b>Esuate:</b> {', '.join(failed) if failed else '—'}\n"
            f"<b>Stare:</b> PAUZAT (use /api/resume pt restart trading).",
        )
    except Exception:
        pass
    return {"status": "stopped", "closed": closed, "failed": failed}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=CHART_PORT, log_level="info")
