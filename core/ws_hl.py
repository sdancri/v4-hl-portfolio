"""
Hyperliquid WebSocket layer — inlocuieste public WS (kline) + private_ws
(order/exec/position) din BP-ul Bybit. Un singur socket HL multiplexeaza
candle + orderUpdates + userEvents.

HL WS quirks vs Bybit:
  - Un singur endpoint pt tot (public + user events) — wss://api.hyperliquid.xyz/ws
  - Fara auth pe public/userEvents (info-style read-only)
  - Subscriptions via JSON: {"method":"subscribe","subscription":{...}}
  - Heartbeat: HL trimite ping auto; noi raspundem cu pong (websockets lib face automat)
  - Reconnect: pe socket close, retry cu backoff

Subscription types relevante:
  - candle:        {"type":"candle","coin":"ETH","interval":"30m"}
  - userEvents:    {"type":"userEvents","user":"0x..."}  (fills, funding, liq)
  - orderUpdates:  {"type":"orderUpdates","user":"0x..."} (order state changes)

Message shapes (incoming):
  - {"channel":"candle","data":{"t":startMs,"T":endMs,"i":"30m","s":"ETH","o":"...","h":"...","l":"...","c":"...","v":"...","n":N}}
  - {"channel":"orderUpdates","data":[{"order":{coin,side,limitPx,sz,oid,...},"status":"...","statusTimestamp":...}]}
  - {"channel":"userEvents","data":{...}}    (mai multe subtipuri)

Candle "confirmed" detection: HL nu trimite explicit "is final" pe candle.
Aproximam: cand `t` (open time) se schimba intre 2 update-uri consecutive,
candle-ul anterior s-a inchis (confirmed=True pe ultimul update al lui).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import time
from typing import Awaitable, Callable, Optional

import websockets


HL_WS_URL = os.getenv("HL_WS_URL", "wss://api.hyperliquid.xyz/ws")

# Tipuri normalizate pt callbacks (pastram compat cu BP strategy shape)
OnCandle      = Callable[[dict], Awaitable[None]]   # {ts, open, high, low, close, volume, confirmed}
OnOrderUpdate = Callable[[dict], Awaitable[None]]   # {symbol, orderId, status, ...}
OnUserEvent   = Callable[[dict], Awaitable[None]]   # {kind: 'fill'|'funding'|..., data: ...}


# ---------------------------------------------------------------------------
# Candle normalizer — tracks last open_ts pt confirmed detection
# ---------------------------------------------------------------------------

class CandleNormalizer:
    """Convert HL raw candle msg la format compat BP (ts_s, OHLCV, confirmed)."""

    def __init__(self, coin: str) -> None:
        self.coin = coin.upper()
        self._last_open_ts_ms: Optional[int] = None
        self._last_candle: Optional[dict] = None    # confirmed=False, ultimul snapshot

    def normalize(self, raw: dict) -> list[dict]:
        """
        Returneaza LISTA de candle-uri (in ordine cronologica):
          - 0 elemente: mesaj nu pt coin-ul nostru / invalid
          - 1 element : update normal pe bara curenta (confirmed=False)
                        sau primul tick din sesiune
          - 2 elemente: tranzitie de bara — [bara_veche confirmed=True,
                        bara_noua confirmed=False]

        Caller (strategie BP-style) primeste fiecare candle si decide:
        confirmed=False -> tick update (check SL/TP h/l)
        confirmed=True  -> bara inchisa, evalueaza signal entry/exit
        """
        try:
            if (raw.get("s") or "").upper() != self.coin:
                return []
            ts_ms = int(raw["t"])
            current = {
                "ts":        ts_ms // 1000,
                "open":      float(raw["o"]),
                "high":      float(raw["h"]),
                "low":       float(raw["l"]),
                "close":     float(raw["c"]),
                "volume":    float(raw["v"]),
                "confirmed": False,
            }
        except (KeyError, ValueError, TypeError) as e:
            print(f"[HL WS] bad candle msg: {e!r}  raw={raw}")
            return []

        # Cazul 1: primul mesaj din sesiune
        if self._last_open_ts_ms is None:
            self._last_open_ts_ms = ts_ms
            self._last_candle = current
            return [current]

        # Cazul 2: update al barei curente (acelasi ts)
        if ts_ms == self._last_open_ts_ms:
            self._last_candle = current
            return [current]

        # Cazul 3: bara noua — bara anterioara inchisa
        out: list[dict] = []
        if self._last_candle is not None:
            confirmed_old = dict(self._last_candle)
            confirmed_old["confirmed"] = True
            out.append(confirmed_old)
        out.append(current)
        self._last_open_ts_ms = ts_ms
        self._last_candle = current
        return out


# ---------------------------------------------------------------------------
# Order/User event normalizer — minimum viable pt strategy
# ---------------------------------------------------------------------------

def normalize_order_update(raw_entry: dict, our_coin: str) -> Optional[dict]:
    """
    HL orderUpdates entry:
      {"order": {"coin":"ETH","side":"B"|"A","limitPx":"...","sz":"...","oid":int,"cloid":...},
       "status": "open"|"filled"|"canceled"|"triggered"|"rejected"|"marginCanceled"|...,
       "statusTimestamp": ms}

    Output normalizat (compat BP shape):
      {"symbol":"ETH", "orderId":<int>, "orderStatus":"<status>",
       "cumExecQty":<sz or partial>, "leavesQty":<remaining>, "avgPrice":<limit>,
       "side":"Buy"|"Sell", "rejectReason":<str if rejected>}
    """
    order = raw_entry.get("order") or {}
    coin = (order.get("coin") or "").upper()
    if coin != our_coin.upper():
        return None
    side_b = order.get("side", "")
    side = "Buy" if side_b in ("B", "b") else "Sell"
    return {
        "symbol":       coin,
        "orderId":      int(order.get("oid", 0)),
        "orderStatus":  raw_entry.get("status", ""),
        "cumExecQty":   float(order.get("totalSz", 0) or order.get("sz", 0)),
        "leavesQty":    float(order.get("leavesSz", 0) or 0),
        "avgPrice":     float(order.get("limitPx", 0) or 0),
        "side":         side,
        "rejectReason": raw_entry.get("rejectReason") or "",
        "raw":          raw_entry,
    }


# ---------------------------------------------------------------------------
# WS connection w/ subscribe + reconnect loop
# ---------------------------------------------------------------------------

class HLWebSocket:
    """
    Single-connection HL WebSocket cu subscribe + reconnect loop.

    Usage:
        ws = HLWebSocket(
            on_candle=async_handler_a,
            on_order_update=async_handler_b,
        )
        ws.subscribe_candle("ETH", "30m")
        ws.subscribe_order_updates(main_wallet_addr)
        await ws.run()    # bloqueaza pana cand opresti via stop()
    """

    # Reconnect backoff (s): [1, 2, 4, 8, 16, 30, 30, 30...]
    _RECONNECT_BACKOFF = [1, 2, 4, 8, 16, 30]
    # Watchdog: daca nu primesc niciun mesaj N secunde -> reconnect (zombie).
    # Configurabil via WS_ZOMBIE_TIMEOUT (mirror BP private_ws); default 90s
    # HL-specific (BP Bybit default 60 — compose-ul seteaza oricum explicit).
    _STALE_TIMEOUT_S = int(os.getenv("WS_ZOMBIE_TIMEOUT", "90"))

    def __init__(self,
                 url: Optional[str] = None,
                 on_candle: Optional[OnCandle] = None,
                 on_order_update: Optional[OnOrderUpdate] = None,
                 on_user_event: Optional[OnUserEvent] = None,
                 coin: str = "ETH") -> None:
        self.url = url or HL_WS_URL
        self.coin = coin.upper()
        self._on_candle = on_candle
        self._on_order_update = on_order_update
        self._on_user_event = on_user_event
        self._normalizer = CandleNormalizer(self.coin)

        # Subscriptions queued — re-sent la fiecare reconnect
        self._subscriptions: list[dict] = []

        self._stop_event = asyncio.Event()
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._last_msg_ts: float = 0.0

    # ----- Public API ------------------------------------------------------

    def subscribe_candle(self, coin: str, interval: str) -> None:
        self._subscriptions.append({
            "type":     "candle",
            "coin":     coin.upper(),
            "interval": interval,
        })

    def subscribe_order_updates(self, user_address: str) -> None:
        self._subscriptions.append({
            "type": "orderUpdates",
            "user": user_address.lower(),
        })

    def subscribe_user_events(self, user_address: str) -> None:
        self._subscriptions.append({
            "type": "userEvents",
            "user": user_address.lower(),
        })

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        """Connect + subscribe + dispatch messages cu reconnect loop."""
        attempt = 0
        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_size=10 * 1024 * 1024,    # 10MB pt orderUpdates batches mari
                ) as ws:
                    self._ws = ws
                    attempt = 0
                    self._last_msg_ts = time.monotonic()
                    print(f"[HL WS] connected to {self.url}")

                    # Re-subscribe la tot
                    for sub in self._subscriptions:
                        msg = {"method": "subscribe", "subscription": sub}
                        await ws.send(json.dumps(msg))
                        print(f"[HL WS] subscribed: {sub}")

                    # Pornesc watchdog in parallel
                    watchdog_task = asyncio.create_task(self._watchdog(ws))

                    try:
                        async for raw_msg in ws:
                            self._last_msg_ts = time.monotonic()
                            try:
                                msg = json.loads(raw_msg)
                            except json.JSONDecodeError:
                                continue
                            await self._dispatch(msg)
                    finally:
                        watchdog_task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await watchdog_task

            except (websockets.ConnectionClosed,
                    websockets.InvalidStatusCode,
                    OSError, asyncio.TimeoutError) as e:
                if self._stop_event.is_set():
                    break
                delay = self._RECONNECT_BACKOFF[min(attempt, len(self._RECONNECT_BACKOFF) - 1)]
                attempt += 1
                print(f"[HL WS] disconnected ({type(e).__name__}: {e}), "
                      f"reconnect in {delay}s (attempt {attempt})")
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
                    break
                except asyncio.TimeoutError:
                    continue
            except Exception as e:
                print(f"[HL WS] unexpected error: {e!r}")
                await asyncio.sleep(5)

        print("[HL WS] stopped")

    # ----- Internal --------------------------------------------------------

    async def _watchdog(self, ws) -> None:
        """Daca _last_msg_ts e prea vechi, inchide socket-ul (force reconnect)."""
        while True:
            await asyncio.sleep(15)
            if (time.monotonic() - self._last_msg_ts) > self._STALE_TIMEOUT_S:
                print(f"[HL WS] stale (no msg {self._STALE_TIMEOUT_S}s) — force close")
                with contextlib.suppress(Exception):
                    await ws.close()
                return

    async def _dispatch(self, msg: dict) -> None:
        channel = msg.get("channel", "")
        data = msg.get("data")

        # Handler errors IZOLATE (mirror BP private_ws): o exceptie dintr-un
        # callback NU trebuie sa omoare conexiunea WS si sa forteze reconnect
        # — loop-ul de mesaje continua, eroarea e logata cu traceback.
        try:
            if channel == "candle":
                await self._handle_candle(data)
            elif channel == "orderUpdates":
                await self._handle_order_updates(data)
            elif channel == "userEvents":
                await self._handle_user_events(data)
            elif channel == "subscriptionResponse":
                print(f"[HL WS] subscribed OK: {data}")
            elif channel == "error":
                print(f"[HL WS] server error: {data}")
            # ignore "pong" si alte heartbeat
        except Exception:
            import traceback
            print(f"[HL WS] {channel} handler error:\n{traceback.format_exc()}")

    async def _handle_candle(self, data: dict) -> None:
        if self._on_candle is None:
            return
        candles = self._normalizer.normalize(data)
        # Lista cu 1 (update normal / first tick) sau 2 (tranzitie:
        # vechi confirmed + nou in-progress). Pasam in ordine.
        for c in candles:
            await self._on_candle(c)

    async def _handle_order_updates(self, data) -> None:
        if self._on_order_update is None:
            return
        if not isinstance(data, list):
            return
        for entry in data:
            norm = normalize_order_update(entry, self.coin)
            if norm is not None:
                await self._on_order_update(norm)

    async def _handle_user_events(self, data) -> None:
        if self._on_user_event is None:
            return
        # HL userEvents este un dict cu chei variabile (fills, funding, liquidation).
        # V4_HL FIX (vs BP-HL identic upstream): filtreaza fills per coin (mirror
        # la _handle_order_updates). Fara filtru, in multi-pair cele N sockets
        # primesc TOATE fills-urile wallet-ului → adapter ruleaza N× pe fiecare
        # fill (triple-fire pe 3 perechi). Cu filter, fiecare socket vede DOAR
        # fills-urile propriului coin. Funding/liquidation alte tipuri sunt
        # NORM-wallet level, pasate ca atare (low-volume, fara duplicare risc).
        if not isinstance(data, dict):
            return
        for kind, payload in data.items():
            if kind == "fills" and isinstance(payload, list):
                payload = [f for f in payload
                           if (f.get("coin") or "").upper() == self.coin]
                if not payload:
                    continue
            await self._on_user_event({"kind": kind, "data": payload})
