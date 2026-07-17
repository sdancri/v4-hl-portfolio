"""
smoke_test.py — Verificari sanity rapide pe V4-HL (BB-MR + Hull+Ichimoku, HL).

Ruleaza: python scripts/smoke_test.py

Testeaza:
  1. Toate modulele importeaza fara erori
  2. Config-ul V4-HL (config_v4_hl.yaml) incarca corect
  3. Strategy: PairStrategyConfig + Signal instantiaza
  4. position_sizing calculeaza corect
  5. exchange_api functii exista
  6. bot_state operatii (LivePosition, TradeRecord, equity update)
  7. no_lookahead filter
  8. telegram_bot fmt_time
  9. hl_ws_runner
  10. main.py (full import)

NU face apeluri reale catre HL. Verifica doar structura codului.
"""
from __future__ import annotations

import asyncio
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd


PASS = 0
FAIL = 0
errors = []


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  ✓ {name}")
    else:
        FAIL += 1
        print(f"  ✗ {name}  — {detail}")
        errors.append((name, detail))


def section(title: str) -> None:
    print(f"\n{'─' * 60}\n  {title}\n{'─' * 60}")


# ============================================================================
# 1. Imports
# ============================================================================
section("1. Imports")

try:
    from core import exchange_api as ex
    check("core.exchange_api", True)
except Exception as e:
    check("core.exchange_api", False, repr(e))

try:
    from core import rate_limiter as rl
    check("core.rate_limiter", True)
except Exception as e:
    check("core.rate_limiter", False, repr(e))

try:
    from core import no_lookahead as nl
    check("core.no_lookahead", True)
except Exception as e:
    check("core.no_lookahead", False, repr(e))

try:
    from core import bot_state
    from core.bot_state import BotState, LivePosition, TradeRecord, ReconciliationError
    check("core.bot_state", True)
except Exception as e:
    check("core.bot_state", False, repr(e))

try:
    from core import hl_ws_runner
    check("core.hl_ws_runner", True)
except Exception as e:
    check("core.hl_ws_runner", False, repr(e))

try:
    from core import telegram_bot as tg
    check("core.telegram_bot", True)
except Exception as e:
    check("core.telegram_bot", False, repr(e))

try:
    from core.config import AppConfig, PairConfig, load_config
    check("core.config", True)
except Exception as e:
    check("core.config", False, repr(e))

try:
    from core.position_sizing import compute_position_size, compute_qty
    check("core.position_sizing", True)
except Exception as e:
    check("core.position_sizing", False, repr(e))

try:
    from strategies.ichimoku_signal import IchimokuSignal, PairStrategyConfig, SignalDecision
    check("strategies.ichimoku_signal", True)
except Exception as e:
    check("strategies.ichimoku_signal", False, repr(e))

# ============================================================================
# 2. Config loading
# ============================================================================
section("2. Config loading (config_v4_hl.yaml)")

try:
    cfg1 = load_config(str(ROOT / "config" / "config_v4_hl.yaml"))
    enabled1 = [p.symbol for p in cfg1.pairs if p.enabled]
    check("config_v4_hl.yaml loads", True)
    check("V4-HL has BTC+TIA+NEAR", set(enabled1) == {"BTC", "TIA", "NEAR"},
          detail=f"got {enabled1}")
    check("V4-HL leverage_max=5", cfg1.portfolio.leverage_max == 5,
          detail=f"got {cfg1.portfolio.leverage_max}")
    check("V4-HL pool_total=100", cfg1.portfolio.pool_total == 100.0)
except Exception as e:
    check("V4-HL config", False, repr(e))

# ============================================================================
# 3. Strategy: PairStrategyConfig + IchimokuSignal
# ============================================================================
section("3. Strategy")

try:
    pcfg = PairStrategyConfig(
        symbol="MNTUSDT", timeframe="4h",
        hull_length=10, tenkan_periods=9, kijun_periods=48,
        senkou_b_periods=52, displacement=24,
        risk_pct_per_trade=0.07, sl_initial_pct=0.04, tp_pct=0.05,
    )
    check("PairStrategyConfig MNT", pcfg.symbol == "MNTUSDT")
    check("min_history_bars >= 76",
          pcfg.min_history_bars >= max(10, 48, 52, 24) + 1,
          detail=f"got {pcfg.min_history_bars}")
except Exception as e:
    check("PairStrategyConfig", False, repr(e))

try:
    sig = IchimokuSignal(pcfg)
    check("IchimokuSignal init", sig.cfg.symbol == "MNTUSDT")

    # Warm-up cu 200 bare random (placeholder — fara semnale reale)
    n = 200
    rng = np.random.default_rng(42)
    prices = 1.0 + np.cumsum(rng.normal(0, 0.005, n))
    df = pd.DataFrame({
        "open": prices, "high": prices * 1.01, "low": prices * 0.99,
        "close": prices, "volume": 100.0,
    }, index=pd.date_range("2026-01-01", periods=n, freq="4h", tz="UTC"))

    sig.warm_up(df)
    check("warm_up no crash", True)
    check("indicators ready",
          sig._last_idx_valid(),
          detail=f"cache={sig.cache is not None}")

    # Evaluate (no position)
    decision = sig.evaluate(has_position=None, entry_price=0.0)
    check("evaluate returns SignalDecision",
          decision.action in {"OPEN_LONG", "OPEN_SHORT", "HOLD"},
          detail=f"got {decision.action}")

    # Update buffer + re-eval
    sig.update_buffer({
        "ts_ms": int(df.index[-1].timestamp() * 1000) + 4 * 3600 * 1000,
        "open": prices[-1], "high": prices[-1] * 1.01,
        "low": prices[-1] * 0.99, "close": prices[-1] * 1.005, "volume": 100.0,
    })
    sig.recompute_indicators()
    check("update_buffer + recompute", sig._last_idx_valid())
except Exception as e:
    check("IchimokuSignal flow", False, repr(e))

# ============================================================================
# 4. Position sizing
# ============================================================================
section("4. Position sizing")

try:
    pair_cfg = cfg1.pairs[0]  # BTC (bb_mr)
    sizing = compute_position_size(pair_cfg, shared_equity=100.0,
                                    balance_broker=100.0,
                                    portfolio_cfg=cfg1.portfolio,
                                    leverage=pair_cfg.leverage)
    # effective_sl_pct (nu sl_initial_pct direct) — BB MR foloseste sl_pct,
    # HI foloseste sl_initial_pct (vezi compute_position_size).
    expected_pos_usd = (100.0 * pair_cfg.risk_pct_per_trade) / pair_cfg.effective_sl_pct
    cap_usd = cfg1.portfolio.cap_pct_of_max * 100.0 * cfg1.portfolio.leverage_max
    # pos_usd should be at most expected_pos_usd; if > cap_usd, sizing.skip=True
    if sizing.skip:
        check("position_sizing skip flag", sizing.pos_usd > cap_usd,
              detail=f"skip={sizing.skip_reason}")
    else:
        check("compute_position_size pos_usd",
              abs(sizing.pos_usd - expected_pos_usd) < 0.01,
              detail=f"got {sizing.pos_usd:.2f} expected {expected_pos_usd:.2f}")
    check("risk_usd = 10%",
          abs(sizing.risk_usd - 10.0) < 0.01,
          detail=f"got {sizing.risk_usd:.2f}")
    check("cap_usd = $475 (0.95*100*5)",
          abs(sizing.cap_usd - 475.0) < 0.01,
          detail=f"got {sizing.cap_usd:.2f}")
except Exception as e:
    check("position_sizing", False, repr(e))

# ============================================================================
# 5. Exchange API surface (without real calls)
# ============================================================================
section("5. exchange_api surface")

required = [
    "get_kline", "get_market_info", "get_balance", "get_balance_usdc",
    "get_available_margin_usdc",
    "get_position", "fetch_open_position", "place_market",
    "place_limit_postonly", "cancel_order", "cancel_all_orders",
    "cancel_all_stops", "get_order_status", "set_leverage", "set_position_sl",
    "fetch_pnl_for_trade", "preload_instruments",
    "_info", "_sign_and_post", "round_qty_down",
]
missing = [f for f in required if not hasattr(ex, f)]
check(f"{len(required)} functii core ({len(required)} required)",
      len(missing) == 0,
      detail=f"missing: {missing}")

# Test _coin (normalizare symbol -> HL coin name, deterministic, fara retea)
check("_coin('BTC') == 'BTC'", ex._coin("BTC") == "BTC")
check("_coin('BTCUSDC') == 'BTC'", ex._coin("BTCUSDC") == "BTC")
check("_coin('ETHUSDC.P') == 'ETH'", ex._coin("ETHUSDC.P") == "ETH")

# Test _next_nonce (monoton crescator, deterministic, fara retea)
try:
    n1 = ex._next_nonce()
    n2 = ex._next_nonce()
    check("_next_nonce monoton crescator", n2 > n1, detail=f"n1={n1} n2={n2}")
except Exception as e:
    check("_next_nonce", False, repr(e))

# round_qty_down
check("round_qty_down(0.567, 0.01) == 0.56",
      abs(ex.round_qty_down(0.567, 0.01) - 0.56) < 1e-9)
check("round_qty_down(123.45, 1.0) == 123.0",
      abs(ex.round_qty_down(123.45, 1.0) - 123.0) < 1e-9)
check("round_qty_down(0.0, anything) == 0.0",
      ex.round_qty_down(0.0, 0.01) == 0.0)

# ============================================================================
# 6. BotState
# ============================================================================
section("6. BotState")

try:
    state = BotState(account_size=1000.0)
    check("init equity = 1000", state.shared_equity == 1000.0)
    check("init n_open=0", state.n_open_positions() == 0)

    pos = LivePosition(
        symbol="MNT", side="Buy", direction="LONG",
        qty=100.0, entry_price=1.0, sl_price=0.96, tp_price=1.05,
        leverage=12, pos_usd=100.0, risk_usd=4.0, opened_ts_ms=1000,
    )
    state.set_position("MNT", pos)
    check("set_position", state.n_open_positions() == 1)
    check("get_position", state.get_position("MNT") is pos)

    trade = TradeRecord(
        id=0, symbol="MNT", direction="LONG",
        entry_ts_ms=1000, entry_price=1.0,
        sl_price=0.96, tp_price=1.05, qty=100.0,
        exit_ts_ms=2000, exit_price=1.05, exit_price_target=1.05,
        exit_reason="HL_TP", pnl=4.95, fees=0.05,
    )
    state.record_closed_trade(trade)
    # Model compound local (conform BP-HL): record_closed_trade MUTA
    # shared_equity local (+= trade.pnl). Nu mai exista sync_equity() care sa
    # re-citeasca balanta live de pe HL.
    check("record_closed_trade actualizeaza shared_equity local (compound)",
          state.shared_equity == 1004.95,
          detail=f"got {state.shared_equity} (expected 1004.95 = 1000 + pnl 4.95)")
    check("position cleared after close", state.n_open_positions() == 0)
    check("trade in history", len(state.trades) == 1)

    # Slippage calc
    check("trade.slippage = 0", trade.slippage == 0.0)

    # to_dict / to_persist round-trip
    persisted = trade.to_persist()
    rebuilt = TradeRecord.from_dict(persisted)
    check("TradeRecord persist round-trip",
          rebuilt.pnl == trade.pnl and rebuilt.symbol == trade.symbol)

    # Summary
    summary = state.summary()
    check("summary has return_pct",
          "return_pct" in summary and summary["n_trades"] == 1)
except Exception as e:
    check("BotState flow", False, repr(e))

# Regression BP-HL 6ee1463: save() TREBUIE sa fie tot sub lock (build+write+
# replace), altfel doua save() concurente (record_closed_trade/heartbeat/etc,
# fiecare prin asyncio.to_thread → thread-uri diferite) se calca pe tmp-ul cu
# nume fix → "save error" (ENOENT) si/sau stale-overwrite silentios pe disk.
# Reproducere identica cu BP: 40 threads concurente → 0 erori, JSON final
# valid, zero tmp orfan (pe codul VECHI: 15-19 erori pe reproducerea BP).
try:
    import os as _os
    import json as _json
    import shutil as _shutil
    import threading as _threading
    import core.bot_state as _bs

    # DATA_DIR e constanta MODUL-LEVEL (citita o data la import, NU live din
    # os.environ) — setarea os.environ["DATA_DIR"] dupa import n-ar avea NICIUN
    # efect (capcana cunoscuta). Trebuie mutat direct atributul modulului.
    _race_dir = "/tmp/smoke_save_race_test"
    _shutil.rmtree(_race_dir, ignore_errors=True)
    _os.makedirs(_race_dir, exist_ok=True)
    _prev_data_dir = _bs.DATA_DIR
    _bs.DATA_DIR = _race_dir

    race_state = BotState(account_size=100.0)
    save_errors = []
    _orig_print = print

    def _capture_print(*a, **kw):
        msg = " ".join(str(x) for x in a)
        if "save error" in msg:
            save_errors.append(msg)
        _orig_print(*a, **kw)

    import builtins as _builtins
    _builtins.print = _capture_print
    try:
        def _worker(i):
            race_state.shared_equity = 100.0 + i
            race_state.save()

        threads = [_threading.Thread(target=_worker, args=(i,)) for i in range(40)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        _builtins.print = _orig_print

    _tmp_path = race_state._state_path() + ".tmp"
    _final_path = race_state._state_path()
    check("save() concurent (40x) — zero erori", len(save_errors) == 0,
          detail=f"got {len(save_errors)} save error(s)")
    check("save() concurent — zero tmp orfan", not _os.path.exists(_tmp_path))
    with open(_final_path, encoding="utf-8") as f:
        _final_data = _json.load(f)
    check("save() concurent — JSON final valid", "shared_equity" in _final_data)

    _bs.DATA_DIR = _prev_data_dir
    _shutil.rmtree(_race_dir, ignore_errors=True)
except Exception as e:
    check("save() concurrent race", False, repr(e))

# ============================================================================
# 7. no_lookahead
# ============================================================================
section("7. no_lookahead")

try:
    check("tf_to_interval 4h", nl.tf_to_interval("4h") == "240")
    check("tf_to_interval 1h", nl.tf_to_interval("1h") == "60")

    # Filter: 2 bare 4h, ultima e bara curenta
    now_ms = 1_000_000_000_000
    cutoff = (now_ms // 14_400_000) * 14_400_000
    prev_bar = cutoff - 14_400_000
    bars = [[prev_bar, 1, 1, 1, 1, 1, 1], [cutoff, 2, 2, 2, 2, 2, 2]]
    out = nl.filter_closed_bars(bars, "240", now_ms=now_ms)
    check("filter_closed_bars excludes current",
          len(out) == 1 and out[0][0] == prev_bar)
except Exception as e:
    check("no_lookahead", False, repr(e))

# ============================================================================
# 8. telegram_bot
# ============================================================================
section("8. telegram_bot")

try:
    formatted = tg.fmt_time(1700000000)  # 14 nov 2023 22:13 UTC
    check("fmt_time produces string",
          isinstance(formatted, str) and len(formatted) > 10,
          detail=f"got {formatted!r}")
    # send-uri reale skip — nu avem TOKEN setat
    check("send is async callable", callable(tg.send))
    check("send_critical is async callable", callable(tg.send_critical))
except Exception as e:
    check("telegram_bot", False, repr(e))

# ============================================================================
# 9. HL WS runner
# ============================================================================
section("9. hl_ws_runner")

try:
    check("public_ws_loop_hl exists",
          hasattr(hl_ws_runner, "public_ws_loop_hl"))
    check("private_ws_loop_hl exists",
          hasattr(hl_ws_runner, "private_ws_loop_hl"))
except Exception as e:
    check("hl_ws_runner", False, repr(e))

# ============================================================================
# 10. main.py (full import — bootstrap nu ruleaza pana FastAPI start)
# ============================================================================
section("10. main.py")

try:
    import os
    os.environ["CONFIG_FILE"] = str(ROOT / "config" / "config_v4_hl.yaml")
    # Reimport main daca a fost incarcat anterior
    if "main" in sys.modules:
        del sys.modules["main"]
    import main
    check("main.py imports", True)
    check("CONFIG.pairs loaded",
          len([p for p in main.CONFIG.pairs if p.enabled]) == 3)
    check("FastAPI app constructed",
          main.app is not None and main.app.title.startswith("v4"))

    # Regression BP-HL / BP 8f404ca: _halted[sym]=True TREBUIE sa
    # scurt-circuiteze on_confirmed_bar INAINTE de orice acces la
    # _signals[sym] — altfel un simbol halted la refuz-adopt ar STIVUI un
    # trade nou pe next bar peste pozitia neprotejata. _signals nu e populat
    # fara bootstrap() → daca halt-check-ul nu opreste PRIMUL, ar arunca
    # KeyError (proof-of-reach).
    main._halted["BTC"] = True
    try:
        asyncio.run(main.on_confirmed_bar("BTC", {"ts": 0, "close": 1.0}))
        check("on_confirmed_bar respecta _halted (nu atinge _signals)", True)
    except KeyError:
        check("on_confirmed_bar respecta _halted (nu atinge _signals)", False,
              detail="a trecut de halt-check pana la _signals[sym] — regresie 8f404ca")
    finally:
        del main._halted["BTC"]

    # Regression incident live 2026-07-11 (TIAUSDT stuck open): dedup-hit in
    # _dedup_and_record_trade TREBUIE sa curete _state.positions[symbol],
    # altfel pozitia ramane FANTOMA permanent (model BP main_multi.py:
    # record_closed_trade e dedup-safe, caller curata neconditionat).
    main._state.set_position("BTC", LivePosition(
        symbol="BTC", side="Buy", direction="LONG",
        qty=1.0, entry_price=100.0, sl_price=96.0, tp_price=105.0,
        leverage=10, pos_usd=100.0, risk_usd=4.0, opened_ts_ms=123456,
    ))
    main._state.trades.append(TradeRecord(
        id=999, symbol="BTC", direction="LONG",
        entry_ts_ms=123456, entry_price=100.0, sl_price=96.0, tp_price=105.0,
        qty=1.0, exit_ts_ms=124000, exit_price=105.0, exit_price_target=105.0,
        exit_reason="TP", pnl=5.0, fees=0.05,
    ))
    try:
        dup_trade = TradeRecord(
            id=0, symbol="BTC", direction="LONG",
            entry_ts_ms=123456, entry_price=100.0, sl_price=96.0, tp_price=105.0,
            qty=1.0, exit_ts_ms=999999, exit_price=101.0, exit_price_target=101.0,
            exit_reason="AUTO", pnl=1.0, fees=0.01,
        )
        recorded = main._dedup_and_record_trade(dup_trade)
        check("_dedup_and_record_trade returneaza False pe dedup",
              recorded is False)
        check("_dedup_and_record_trade curata pozitia stale pe dedup-hit",
              main._state.get_position("BTC") is None)
    finally:
        main._state.trades.pop()  # scoate trade-ul de test (id=999)
except Exception as e:
    check("main.py", False, repr(e))

# ============================================================================
# Summary
# ============================================================================
print(f"\n{'═' * 60}")
print(f"  RESULTS: {PASS} passed  /  {FAIL} failed")
print(f"{'═' * 60}")
if FAIL:
    print("\nFailed tests:")
    for name, detail in errors:
        print(f"  • {name}: {detail}")
    sys.exit(1)
print("\n✓ All smoke tests passed.")
