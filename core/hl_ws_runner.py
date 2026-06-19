"""hl_ws_runner.py — multi-pair WS pattern pe Hyperliquid pentru V4_HL.

Spawneaza ONE HLWebSocket per coin enabled + ONE socket events
(orderUpdates + userEvents on HL_MAIN_ADDRESS wallet).

Pattern compat cu V4 Bybit main.py:
  - public_ws_loop_hl(symbols, on_confirmed_bar, on_unconfirmed_bar) — multi-coin
  - private_ws_loop_hl(on_position_event) — orderUpdates + userEvents

Diferenta vs V4 Bybit:
  - HL trimite candle pe channel "candle", separat per coin
  - HL n-are dedicated position channel; folosim userEvents (fills) +
    confirm cu get_position_qty pt size=0 detection
"""
from __future__ import annotations

import asyncio
import os
import traceback
from typing import Awaitable, Callable, Optional

from core import exchange_api as ex
from core.ws_hl import HLWebSocket


async def public_ws_loop_hl(
    symbols: list[str],
    interval_str: str,
    on_confirmed_bar: Callable[[str, dict], Awaitable[None]],
    on_unconfirmed_bar: Optional[Callable[[str, dict], Awaitable[None]]] = None,
) -> None:
    """Spawneaza HLWebSocket per coin in `symbols`. Fiecare task ruleaza pana
    la cancel (lifespan finally).

    interval_str: BP-style ("240" pentru 4h). Conversia la HL native facuta
    via ex._to_hl_interval.

    on_confirmed_bar(sym, bar_dict): apelat la bara closed.
    on_unconfirmed_bar(sym, bar_dict): apelat la tick intra-bar (optional).
    """
    hl_interval = ex._to_hl_interval(interval_str)
    print(f"  [HL-WS] starting {len(symbols)} candle sockets "
          f"(interval BP={interval_str} → HL={hl_interval})")

    async def _make_task(coin: str) -> None:
        async def _on_candle_cb(candle: dict) -> None:
            """ws_hl normalized candle → V4 bar dict shape.
            candle: {ts (sec), open, high, low, close, volume, confirmed}.
            V4 bar shape: {ts_ms, open, high, low, close, volume, confirmed}.
            """
            bar = {
                "ts_ms": int(candle["ts"]) * 1000,
                "open": candle["open"],
                "high": candle["high"],
                "low": candle["low"],
                "close": candle["close"],
                "volume": candle.get("volume", 0.0),
                "confirmed": candle.get("confirmed", False),
            }
            try:
                if bar["confirmed"]:
                    await on_confirmed_bar(coin, bar)
                elif on_unconfirmed_bar is not None:
                    await on_unconfirmed_bar(coin, bar)
            except Exception:
                print(f"  [{coin}] HL candle handler CRASHED:\n"
                      f"{traceback.format_exc()}")

        ws_client = HLWebSocket(
            on_candle=_on_candle_cb,
            on_order_update=None,
            on_user_event=None,
            coin=coin,
        )
        ws_client.subscribe_candle(coin, hl_interval)
        print(f"  [HL-WS] subscribe candle {coin}/{hl_interval}")
        await ws_client.run()  # bucla interna cu reconnect + watchdog

    # Spawneaza cate un task per coin si asteapta toate
    tasks = [asyncio.create_task(_make_task(s)) for s in symbols]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        raise


async def private_ws_loop_hl(
    on_order_update: Optional[Callable[[dict], Awaitable[None]]] = None,
    on_user_event: Optional[Callable[[dict], Awaitable[None]]] = None,
) -> None:
    """Subscribe orderUpdates + userEvents pe HL_MAIN_ADDRESS. Single socket
    multiplexat. HL nu are dedicated 'position' channel — detectia close
    se face din userEvents (fills) + verificare get_position_qty=0.
    """
    user = os.getenv("HL_MAIN_ADDRESS", "")
    if not user:
        print("  [HL-WS] HL_MAIN_ADDRESS not set → events socket DISABLED "
              "(detectie close va folosi defense-in-depth check_external_close)")
        return

    ws_client = HLWebSocket(
        on_candle=None,
        on_order_update=on_order_update,
        on_user_event=on_user_event,
        coin="BTC",  # placeholder — normalizer-ul candle nu se foloseste aici
    )
    ws_client.subscribe_order_updates(user)
    ws_client.subscribe_user_events(user)
    print(f"  [HL-WS] subscribe orderUpdates + userEvents pe {user}")
    await ws_client.run()
