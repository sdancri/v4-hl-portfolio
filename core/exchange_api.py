"""
Hyperliquid exchange layer — drop-in replacement pt core/exchange_api.py (Bybit).

Aceleasi semnaturi publice ca adaptorul Bybit din BP, ca strategiile +
main.py sa ramana neschimbate (vorbesc DOAR prin `ex.*`). Migrezi un bot de
pe Bybit pe HL schimband acest fisier (+ ws_hl + env), nu logica strategiei.

Phase A — connectivity (read-only):
  - get_balance_usdc(): cross-margin USDC din clearinghouseState.marginSummary
  - get_kline(symbol, interval, limit): candles via info `candleSnapshot`
  - get_position_qty(symbol): pozitia curenta (signed: + LONG / - SHORT / 0)
  - fetch_open_position(symbol): dict normalizat compat BP strategy
  - fetch_agent_expiration_ms(main, agent): validUntil al agent-ului aprobat

Phase B — trading (signed via agent EIP-712):
  - preload_instruments(symbols): cache universe metadata (sz/px precision)
  - smart_price(p, symbol): round la HL tick rules (5 sig figs)
  - smart_qty(q, symbol): round la sz decimals
  - sizing_snapshot(symbol, ...): notional/qty/risk compute
  - place_market_order(symbol, side, qty): aggressive limit IOC
  - set_position_sl(symbol, sl_price, direction): trigger reduce-only
  - cancel_all_orders(symbol): cleanup trigger orders ramase
  - fetch_open_position(symbol): ENHANCED cu SL/TP din openOrders
  - fetch_pnl_for_trade(symbol, entry_ts_ms, exit_ts_ms): din userFills

API surface mirror Bybit exchange_api.py ca strategy code-ul sa fie portabil.

Hyperliquid quirks vs Bybit (relevante aici):
  - Endpoint: TOATE info-calls sunt POST /info cu body {"type":..., ...}
  - Auth: NU pt info endpoint (read-only public). Doar exchange endpoint
    cere semnatura EIP-712 cu cheia agent.
  - Symbol: "ETH" (perp coin), NU "ETHUSDC".
  - Interval format: string "1m"/"5m"/"15m"/"30m"/"1h"/"4h"/"1d".
  - Settlement: USDC (toate balanteele si PnL).
  - Position direction: szi (signed size) — pozitiv = LONG, negativ = SHORT.
"""
from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Optional

import httpx
from eth_account import Account


# ---------------------------------------------------------------------------
# Config + HTTP client
# ---------------------------------------------------------------------------

HL_BASE_URL = (os.getenv("HL_BASE_URL") or "https://api.hyperliquid.xyz").rstrip("/")
# Reteaua pt semnare EIP-712 — chain ID difera mainnet vs testnet. Detectia
# DOAR prin substring "testnet" in URL e fragila: un URL custom/proxy/vanity
# fara "testnet" dar care pointeaza spre testnet → semnatura pe chain gresit →
# TOATE ordinele respinse. De-aia HL_NETWORK explicit (mainnet|testnet) are
# prioritate; heuristica pe URL ramane doar fallback cand env-ul nu e setat.
# (model BP-HL — regresie fata de referinta, portat acum)
_hl_net = os.getenv("HL_NETWORK", "").strip().lower()
if _hl_net in ("mainnet", "main"):
    HL_IS_MAINNET = True
elif _hl_net in ("testnet", "test"):
    HL_IS_MAINNET = False
else:
    HL_IS_MAINNET = "testnet" not in HL_BASE_URL
HL_MAIN_ADDRESS = (os.getenv("HL_MAIN_ADDRESS") or "").strip().lower()
HL_AGENT_PRIVATE_KEY = (os.getenv("HL_AGENT_PRIVATE_KEY") or "").strip()
HL_AGENT_ADDRESS_EXPECTED = (os.getenv("HL_AGENT_ADDRESS") or "").strip().lower()

# Derive agent address din PK la import time. Daca lipseste PK, se trateaza
# in functiile care semneaza — info endpoint NU cere semnatura.
_agent_acct: Optional[Account] = None
HL_AGENT_ADDRESS: Optional[str] = None
if HL_AGENT_PRIVATE_KEY:
    try:
        _agent_acct = Account.from_key(HL_AGENT_PRIVATE_KEY)
        HL_AGENT_ADDRESS = _agent_acct.address.lower()
        if HL_AGENT_ADDRESS_EXPECTED and HL_AGENT_ADDRESS != HL_AGENT_ADDRESS_EXPECTED:
            raise RuntimeError(
                f"HL_AGENT_ADDRESS mismatch: PK derives "
                f"{HL_AGENT_ADDRESS} but env says {HL_AGENT_ADDRESS_EXPECTED}. "
                f"Check whether the PK matches the agent you approved on HL UI."
            )
    except Exception as e:
        # Re-raise daca e mismatch (security), dar log doar la import problem
        if "mismatch" in str(e).lower():
            raise
        print(f"[HL] WARN: cannot derive agent address from PK: {e}")

# httpx async client refolosit. Timeout: 10s connect, 30s read (HL e rapid
# in mod normal dar info endpoint poate fi lent pe userFills/openOrders).
_http: Optional[httpx.AsyncClient] = None


def _client() -> httpx.AsyncClient:
    global _http
    if _http is None:
        _http = httpx.AsyncClient(
            base_url=HL_BASE_URL,
            timeout=httpx.Timeout(connect=10.0, read=30.0, write=10.0, pool=10.0),
            headers={"Content-Type": "application/json"},
        )
    return _http


async def close_client() -> None:
    global _http
    if _http is not None:
        await _http.aclose()
        _http = None


# ---------------------------------------------------------------------------
# Symbol normalization
# ---------------------------------------------------------------------------

def _coin(symbol: str) -> str:
    """
    Normalizeaza symbol pt HL: "ETH" / "ETHUSDC" / "ETHUSDC.P" -> "ETH".
    HL foloseste DOAR coin name pt perp (USDC e implicit).
    """
    s = (symbol or "").upper().strip()
    for suffix in (".P", "USDC", "USDT", "USD"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


# ---------------------------------------------------------------------------
# Low-level: POST /info
# ---------------------------------------------------------------------------

async def _info(body: dict) -> dict | list:
    """
    POST {HL_BASE_URL}/info cu body dict. Returneaza JSON parsat.
    Info endpoint NU cere semnatura (public read-only).
    Rate-limited via core.rate_limiter (protectie contra HL 429 IP throttle).
    """
    import core.rate_limiter as rl
    await rl.wait_token()
    cli = _client()
    r = await cli.post("/info", json=body)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Read-only API (Phase A)
# ---------------------------------------------------------------------------

async def get_balance_usdc(user: Optional[str] = None) -> float:
    """
    Returneaza TOTAL USDC tradabil pe perp pt main wallet.

    BUGFIX 2026-07-08: formula veche (perp_value + spot.total) DUBLA colateralul
    cand exista o pozitie perp deschisa. Pe HL Unified, USDC din spot e folosit
    AUTOMAT ca margin pt perps — `spot.hold` reflecta exact suma blocata acolo,
    care e ACEEASI suma deja continuta in `perp accountValue` (margin lockuit +
    uPnL). Suma naiva (perp + spot.total) numara acel colateral de 2 ori →
    echitate umflata cu ~valoarea marginii ori de cate ori exista pozitie
    deschisa (incident 2026-07-08: $92 raportat, real ~$64 — verificat: spot.hold
    ≈ perp.accountValue aproape exact, cu pozitie NEAR deschisa).

    Fix: total = perp_value + spot_FREE (spot.total - spot.hold), NU spot.total
    intreg. perp_value deja include margin lockuit + uPnL (profit SAU pierdere);
    adaugam doar cash-ul liber din spot (nefolosit ca margin). Cand nu exista
    pozitie deschisa, hold=0 → formula se reduce la perp_value(~0) + spot.total,
    identic cu inainte (no-op pe cazul comun).

    Pe conturi Manual/Cross-separate (spot USDC NU disponibil pt perp),
    `clearinghouseState.accountValue` singur ar fi raspunsul corect — pe acelea
    hold ramane 0 (spot neatins de perp), deci formula de mai jos se comporta
    identic (spot_free = spot.total).
    """
    addr = (user or HL_MAIN_ADDRESS or "").lower()
    if not addr:
        raise RuntimeError("HL_MAIN_ADDRESS not set")
    perp_state = await _info({"type": "clearinghouseState", "user": addr})
    perp_value = float(perp_state["marginSummary"]["accountValue"])
    # spot poate sa nu existe in toate cazurile — failsafe la 0
    spot_usdc = 0.0
    try:
        spot_state = await _info({"type": "spotClearinghouseState", "user": addr})
        for bal in (spot_state or {}).get("balances", []):
            if (bal.get("coin") or "").upper() == "USDC":
                spot_total = float(bal.get("total", 0))
                spot_hold = float(bal.get("hold", 0) or 0)
                # max(0, ...) — clamp defensiv: daca hold > total tranzitoriu
                # (rounding/settlement-timing edge case), evita delta negativ
                # care ar subestima balanta (model BP-HL).
                spot_usdc = max(0.0, spot_total - spot_hold)   # doar cash LIBER
                break
    except Exception:
        pass
    return perp_value + spot_usdc


async def get_balance_spot_usdc(user: Optional[str] = None) -> float:
    """
    Returneaza balanta SPOT USDC (separate de perps).

    Pe HL trebuie transfer intern din spot in perps inainte sa poti trada.
    Folosit la diagnosticare ("ai bani in spot, nu in perps").
    """
    addr = (user or HL_MAIN_ADDRESS or "").lower()
    if not addr:
        raise RuntimeError("HL_MAIN_ADDRESS not set")
    try:
        state = await _info({"type": "spotClearinghouseState", "user": addr})
    except Exception as e:
        print(f"[HL] spotClearinghouseState failed: {e}")
        return 0.0
    if not isinstance(state, dict):
        return 0.0
    for bal in state.get("balances", []):
        if (bal.get("coin") or "").upper() == "USDC":
            try:
                return float(bal.get("total", 0))
            except Exception:
                pass
    return 0.0


async def get_kline(symbol: str, interval: str = "30",
                    limit: int = 300,
                    start: Optional[int] = None,
                    end: Optional[int] = None) -> list[list]:
    """
    Returneaza candles inchise pt `symbol` la `interval`.

    `interval` accepta AMBELE formate:
      - BP/Bybit: "1","3","5","15","30","60","240","D" (minute ca string)
      - HL native: "1m","5m","15m","30m","1h","4h","1d"
    Translatam intern la HL native (e ce cere candleSnapshot).

    `start`/`end` (ms epoch, optionale) — fereastra explicita pt gap-fill
    (REST backfill dupa disconnect WS). Daca lipsesc, derivam fereastra din
    `limit` (ultimele ~limit bare pana acum). _fill_ws_gap le foloseste.

    HL format candleSnapshot returneaza list de dict cu t (start ms),
    o, h, l, c, v, n. Convertim la format compat BP: list de list
    [ts_ms_str, open, high, low, close, volume, turnover] DESC order.
    """
    coin = _coin(symbol)
    hl_interval = _to_hl_interval(interval)
    now_ms = int(time.time() * 1000)
    interval_ms = _interval_to_ms(hl_interval)
    end_ms = int(end) if end is not None else now_ms
    start_ms = int(start) if start is not None else end_ms - (limit + 5) * interval_ms

    raw = await _info({
        "type": "candleSnapshot",
        "req": {
            "coin":      coin,
            "interval":  hl_interval,
            "startTime": start_ms,
            "endTime":   end_ms,
        },
    })
    # raw e list cu camp T (open time ms), c, h, l, o, v. ASC order.
    # Output compatibil cu BP Bybit get_kline: list[list[str]] DESC order.
    out: list[list] = []
    for c in raw:
        # turnover (col 6) = quote volume. HL candleSnapshot nu-l da; aproximam
        # base_vol * close ca echivalent Bybit, ca outputul sa aiba 7 coloane
        # (consumatorii mirror din V4 Bybit cer [ts,o,h,l,c,v,turnover]).
        try:
            turnover = float(c["v"]) * float(c["c"])
        except (KeyError, ValueError, TypeError):
            turnover = 0.0
        out.append([
            str(c["t"]),         # open time ms (matched Bybit row[0])
            str(c["o"]),
            str(c["h"]),
            str(c["l"]),
            str(c["c"]),
            str(c["v"]),
            str(turnover),       # col 6 — Bybit-compat (vezi nota de mai sus)
        ])
    # BP Bybit returneaza DESC, vom face la fel
    out.reverse()
    return out[:limit]


def _interval_to_ms(interval: str) -> int:
    """Convert HL interval string ('30m', '1h', '4h', '1d') la ms."""
    s = interval.strip().lower()
    if s.endswith("m"):
        return int(s[:-1]) * 60_000
    if s.endswith("h"):
        return int(s[:-1]) * 3_600_000
    if s.endswith("d"):
        return int(s[:-1]) * 86_400_000
    raise ValueError(f"Unknown HL interval: {interval}")


# BP/Bybit interval format ("30","60","240","D") -> HL format ("30m","1h","4h","1d")
_BP_TO_HL_INTERVAL = {
    "1":   "1m",
    "3":   "3m",
    "5":   "5m",
    "15":  "15m",
    "30":  "30m",
    "60":  "1h",
    "120": "2h",
    "240": "4h",
    "480": "8h",
    "720": "12h",
    "D":   "1d",
    "W":   "1w",
}


def _to_hl_interval(interval: str) -> str:
    """
    Translateaza BP/Bybit format ('30','D') la HL native ('30m','1d').

    BP-stand-alone alpha ('D','W') -> table lookup (D -> 1d).
    Strings cu prefix numeric + suffix alpha ('30m','4h','1d') -> deja HL.
    """
    s = interval.strip()
    if not s:
        raise ValueError("empty interval")
    # Daca strict alpha (ex 'D','W'), foloseste table lookup (NU returna lower!)
    if s.isalpha():
        hl = _BP_TO_HL_INTERVAL.get(s) or _BP_TO_HL_INTERVAL.get(s.upper())
        if hl is None:
            raise ValueError(f"Cannot translate BP interval {interval!r} to HL format")
        return hl
    # Daca ultima char e litera SI restul e numeric, e deja format HL
    if s[-1].isalpha() and s[-1].lower() in ("m", "h", "d", "w") and s[:-1].isdigit():
        return s.lower()
    # Altfel, lookup table (numeric BP format)
    hl = _BP_TO_HL_INTERVAL.get(s)
    if hl is None:
        raise ValueError(f"Cannot translate BP interval {interval!r} to HL format")
    return hl


async def get_position_qty(symbol: str,
                           user: Optional[str] = None) -> float:
    """
    Returneaza qty ABSOLUTE (>=0) — convention BP/Bybit-compatible.

    NU returna signed! BP `base_strategy.maybe_synthesize_external_close`
    verifica `qty_real < eps` ca "pozitia s-a inchis". Pe SHORT, daca
    am returna signed (-x), abs(qty_real < eps) ar fi mereu False — dar
    daca returnam signed direct, `-x < eps` = True → fals pozitiv
    "EXTERNAL close" → bot cancel SL si raport $0 PnL pe pozitie deschisa.
    Bug INCIDENT 2026-06-02 12:30 UTC.
    """
    pos = await fetch_open_position(symbol, user)
    if not pos:
        return 0.0
    return float(pos["qty"])    # absolute, always >=0


async def get_position(symbol: str,
                       user: Optional[str] = None) -> Optional[dict]:
    """V4 Bybit-compat: dict pozitie cu cheia "size" (qty ABSOLUTE) sau None
    daca flat. main.py (_reconcile_close/_assert_closed) face
    float(pos.get("size", 0)). BP-HL n-are functia asta — delta V4 (altfel
    AttributeError pe ramurile de reconciliere)."""
    pos = await fetch_open_position(symbol, user)
    if not pos:
        return None
    return {**pos, "size": float(pos["qty"])}


async def get_position_qty_strict(symbol: str,
                                   user: Optional[str] = None) -> Optional[float]:
    """
    Strict variant: returneaza None pe API fail (in loc de 0.0).
    BP-compat semantics — caller decide "unknown" vs "absent".
    """
    addr = (user or HL_MAIN_ADDRESS or "").lower()
    coin = _coin(symbol)
    try:
        state = await _info({"type": "clearinghouseState", "user": addr})
    except Exception:
        return None
    try:
        for ap in state.get("assetPositions", []):
            pos = ap.get("position", {})
            if pos.get("coin") != coin:
                continue
            szi = float(pos.get("szi", 0))
            return abs(szi)
        return 0.0  # API ok, symbol absent = legitimate 0
    except Exception:
        return None


async def confirm_position_closed(symbol:   str,
                                  attempts: int = 3,
                                  delay:    float = 1.5) -> Optional[bool]:
    """
    Multi-attempt confirm pozitie inchisa pe HL (BP-compat).
    True = toate attempts succes + qty=0 (close confirmed)
    False = cel putin 1 attempt a returnat qty>0 (still open)
    None = cel putin 1 API fail (unknown)
    """
    for i in range(attempts):
        if i > 0:
            await asyncio.sleep(delay)
        qty = await get_position_qty_strict(symbol)
        if qty is None:
            return None
        if qty > 1e-9:
            return False
    return True


async def cancel_all_stops(symbol: str) -> int:
    """Alias for cancel_all_orders — BP convention."""
    return await cancel_all_orders(symbol)


async def chase_close(symbol: str, direction: str,
                      max_attempts: int = 20,
                      interval_sec: float = 3.0) -> bool:
    """
    HL chase_close — simplified vs Bybit (HL n-are post-only maker chase semnificativ
    diferit de aggressive limit IOC). Cancel toate trigger orders + loop market
    reduce-only pana cand qty=0.

    Returneaza True daca pozitia e confirmata inchisa (qty<=eps), False daca
    toate incercarile s-au epuizat cu pozitia inca deschisa. V4 main.py
    (_close_position_locked) face `if not ok:` → contract bool OBLIGATORIU
    (BP-HL intoarce None — delta V4).
    """
    await cancel_all_stops(symbol)
    close_side = "Sell" if direction == "LONG" else "Buy"

    for attempt in range(max_attempts):
        qty = await get_position_qty_strict(symbol)
        if qty is None:
            print(f"  [HL] Chase {attempt+1}/{max_attempts}: API fail — keep chasing")
            await asyncio.sleep(interval_sec)
            continue
        if qty <= 1e-9:
            print(f"  [HL] Chase close: pozitie inchisa ({attempt} incercari)")
            return True

        # Market reduce-only (aggressive limit IOC)
        result = await _place_market_internal(symbol, close_side, qty, reduce_only=True)
        print(f"  [HL] Chase {attempt+1}/{max_attempts}: {close_side} qty={qty} "
              f"→ {result.get('result')} filled={result.get('filled_qty', 0)}")
        await asyncio.sleep(interval_sec)

    # Toate incercarile epuizate — verifica o ultima data
    qty_final = await get_position_qty_strict(symbol)
    return qty_final is not None and qty_final <= 1e-9


async def _position_open_ts_from_fills(coin: str, addr: str) -> Optional[int]:
    """Ora (ms UTC) la care s-a DESCHIS pozitia curenta, derivata din userFills.

    HL nu expune createdMs in clearinghouseState. Fill-urile HL au insa
    `startPosition` (marimea pozitiei INAINTE de fill) → fill-ul de deschidere
    al pozitiei curente = cel mai RECENT fill (pt coin) cu startPosition ~0
    (adica pozitia era flat inainte de el). Tot ce e mai nou = adds/reduces la
    pozitia curenta. Robust la ordinea listei (luam max time dintre candidati).

    CRITIC pt strategiile cu time-exit (BB MR) portate pe HL: fara asta, la adopt
    opened_ts_ms cade pe fallback (now) → varsta pozitiei gresita → time-exit
    resetat la restart. Cu asta, opened_ts_ms = ora reala fara persistenta."""
    try:
        fills = await _info({"type": "userFills", "user": addr})
    except Exception:
        return None
    if not isinstance(fills, list):
        return None
    cand: list[int] = []
    for f in fills:
        try:
            if (f.get("coin") or "").upper() != coin.upper():
                continue
            if abs(float(f.get("startPosition", 0) or 0)) < 1e-9 and f.get("time"):
                cand.append(int(f["time"]))
        except Exception:
            continue
    return max(cand) if cand else None


async def fetch_open_position(symbol: str,
                              user: Optional[str] = None) -> Optional[dict]:
    """
    Returneaza pozitia deschisa pe `symbol` sau None daca nu exista.

    Format compat Bybit fetch_open_position din BP:
      {
        "direction":    "LONG" / "SHORT",
        "entry_price":  float,
        "qty":          float,                 # absolute, >0
        "sl_price":     float or None,         # din trigger orders (TODO)
        "tp_price":     float or None,         # idem
        "created_ms":   int,                   # ms UTC
      }

    NOTA Phase A: sl_price / tp_price = None (read din openOrders inca
    nu e implementat). Phase B adauga fetch open trigger orders.
    """
    addr = (user or HL_MAIN_ADDRESS or "").lower()
    coin = _coin(symbol)
    state = await _info({"type": "clearinghouseState", "user": addr})

    for ap in state.get("assetPositions", []):
        pos = ap.get("position", {})
        if pos.get("coin") != coin:
            continue
        szi = float(pos.get("szi", 0))      # signed size
        if abs(szi) < 1e-12:
            return None
        entry = float(pos.get("entryPx", 0) or 0)
        # created_ms: derivat din userFills (HL n-are createdMs in state).
        # None daca fills indisponibil → caller face fallback la now (adopt),
        # NU la 0 (entry_ts=0 ar da varsta = de la epoca 0 → time-exit instant).
        created = await _position_open_ts_from_fills(coin, addr)
        return {
            "direction":   "LONG" if szi > 0 else "SHORT",
            "entry_price": entry,
            "qty":         abs(szi),
            "sl_price":    None,
            "tp_price":    None,
            "created_ms":  created,
        }
    return None


# ---------------------------------------------------------------------------
# Agent expiration (180-day TTL pe API agents)
# ---------------------------------------------------------------------------

async def fetch_agent_expiration_ms(
    main_address: Optional[str] = None,
    agent_address: Optional[str] = None,
) -> Optional[int]:
    """
    Returneaza validUntil (ms UTC) al agent-ului aprobat, sau None daca
    nu se gaseste / agent revoked.

    HL API agents au TTL 180 zile de la aprobare. Dupa expirare, semnaturile
    de la agent sunt respinse — bot-ul nu mai poate trada (chiar nu mai
    poate muta SL pe pozitie existenta). Critic sa avertizam din timp.

    Endpoint: POST /info {"type":"extraAgents","user":<main>}
    Response: list[{"address": "0x...", "name": "...", "validUntil": <ms>}]
    """
    main = (main_address or HL_MAIN_ADDRESS or "").lower()
    agent = (agent_address or HL_AGENT_ADDRESS or "").lower()
    if not main or not agent:
        return None
    try:
        data = await _info({"type": "extraAgents", "user": main})
    except Exception as e:
        print(f"[HL] fetch_agent_expiration_ms failed: {e}")
        return None
    if not isinstance(data, list):
        return None
    for entry in data:
        try:
            if entry.get("address", "").lower() == agent:
                return int(entry.get("validUntil", 0))
        except Exception:
            continue
    return None


# ===========================================================================
# Phase B — instruments, sizing, trading (signed via EIP-712 agent)
# ===========================================================================

# Cache pt universe metadata. Indexat pe coin name normalized ("ETH").
# Populat de preload_instruments() la pornire si refolosit toata sesiunea.
#
# Per HL meta endpoint, fiecare element din universe contine:
#   - name (str)
#   - szDecimals (int) — qty precision (e.g., ETH=4 -> 0.0001 min step)
#   - maxLeverage (int) — cap leverage
# Index-ul (pozitia in lista) e ASSET ID-ul folosit la order — il salvam si pe el.
_INSTRUMENTS: dict[str, dict] = {}


async def preload_instruments(symbols: Optional[list[str]] = None) -> None:
    """
    Fetch universe metadata si populeaza `_INSTRUMENTS`.

    Daca `symbols` e dat, ridica eroare daca vreunul lipseste din universe
    (sanity check la pornire). Altfel cache-uieste TOT universe-ul.
    """
    # Retry pe fetch-ul meta: e primul apel de retea la boot, iar _info n-are
    # retry intern. Un timeout TRANZITORIU aici ar propaga necontrolat prin
    # bootstrap → crash inainte de orice candle/indicator. Retry; ridicam DOAR
    # la esec persistent (outage real). Meta HL e obligatorie (mapping
    # coin→asset-id, fara fallback env) — de aceea retry, nu fallback.
    meta: dict | list | None = None
    last_exc: Exception | None = None
    for i in range(4):
        try:
            meta = await _info({"type": "meta"})
            break
        except Exception as e:
            last_exc = e
            print(f"[HL] preload_instruments meta attempt {i+1}/4 "
                  f"failed: {e!r}")
            if i < 3:
                await asyncio.sleep(2.0)
    if meta is None:
        raise RuntimeError(
            f"HL meta fetch failed after 4 retries: {last_exc!r}")
    universe = meta.get("universe", [])
    if not universe:
        raise RuntimeError("HL meta endpoint returned empty universe")

    _INSTRUMENTS.clear()
    for idx, item in enumerate(universe):
        coin = item.get("name", "").upper()
        if not coin:
            continue
        _INSTRUMENTS[coin] = {
            "asset_id":      idx,
            "name":          coin,
            "sz_decimals":   int(item.get("szDecimals", 0)),
            "max_leverage":  int(item.get("maxLeverage", 1)),
        }

    if symbols:
        missing = [s for s in (_coin(x) for x in symbols)
                   if s not in _INSTRUMENTS]
        if missing:
            raise RuntimeError(f"Symbols missing from HL universe: {missing}")

    print(f"[HL] preload_instruments: cached {len(_INSTRUMENTS)} coins "
          f"(requested: {symbols or 'all'})")


def _meta(symbol: str) -> dict:
    """Returneaza meta-ul cached pt symbol; raise daca lipseste."""
    coin = _coin(symbol)
    m = _INSTRUMENTS.get(coin)
    if m is None:
        raise RuntimeError(
            f"Instrument '{coin}' not in cache. Did you call preload_instruments()?"
        )
    return m


# ---------------------------------------------------------------------------
# Precision helpers — HL formatting rules
# ---------------------------------------------------------------------------

# HL price rule pt perp: max 5 cifre semnificative AND max (6 - szDecimals)
# zecimale. Pt ETH (szDecimals=4) -> max 2 zecimale -> ex. 1985.34, NU 1985.345.

# BP API: smart_price(p) si smart_qty(q) NU primesc symbol — auto-precision
# pe baza magnitudinii. Folosite pt AFISARE (Telegram, log). Pt payload HL
# folosim _fmt_price/_fmt_qty (cu symbol — respecta sz_decimals per coin).

def smart_price(p: float) -> str:
    """
    Format pret pt AFISARE (Telegram, log). Auto-precision pe magnitudine
    (~5 cifre semnificative). NU folosi pt API payload HL — foloseste
    _fmt_price(p, symbol) care respecta sz_decimals per coin.
    """
    import math
    if not p or not math.isfinite(p) or p <= 0:
        return f"{p}"
    prec = max(2, min(8, 4 - math.floor(math.log10(abs(p)))))
    return f"{p:.{prec}f}"


def smart_qty(q: float) -> str:
    """
    Format quantity pt AFISARE — elimina trailing zeros, pastreaza non-zero.
    NU folosi pt API payload HL.
    """
    if q is None:
        return "0"
    s = f"{q:.8f}".rstrip("0").rstrip(".")
    return s if s else "0"


def _fmt_price(p: float, symbol: Optional[str] = None) -> str:
    """
    Format pret pt API HL — respecta regula HL: max 5 sig figs AND max
    (6 - szDecimals) zecimale. ETH (sz=4) -> max 2 zec.

    Returneaza string (HL respinge floats cu precizie excesiva).
    """
    if p <= 0:
        return "0"
    coin = _coin(symbol or "ETH")
    sz_dec = _INSTRUMENTS.get(coin, {}).get("sz_decimals", 4)
    max_dec = max(0, 6 - sz_dec)

    import math
    if p >= 100000:
        rounded = round(p)
    else:
        digits_before = max(1, int(math.floor(math.log10(abs(p)))) + 1)
        sig_decimals = max(0, 5 - digits_before)
        decimals = min(sig_decimals, max_dec)
        rounded = round(p, decimals)
    s = f"{rounded:.{max_dec}f}".rstrip("0").rstrip(".")
    return s if s else "0"


def _fmt_qty(q: float, symbol: Optional[str] = None) -> str:
    """Format qty pt API HL — rotunjit la sz_decimals al coin-ului."""
    coin = _coin(symbol or "ETH")
    sz_dec = _INSTRUMENTS.get(coin, {}).get("sz_decimals", 4)
    rounded = round(q, sz_dec)
    s = f"{rounded:.{sz_dec}f}".rstrip("0").rstrip(".")
    return s if s else "0"


def _qty_step(symbol: Optional[str] = None) -> float:
    sz_dec = _INSTRUMENTS.get(_coin(symbol or "ETH"), {}).get("sz_decimals", 4)
    return 10 ** (-sz_dec)


def _qty_prec(symbol: Optional[str] = None) -> int:
    return _INSTRUMENTS.get(_coin(symbol or "ETH"), {}).get("sz_decimals", 4)


# get_balance — BP signature (no args), returneaza acelasi total ca get_balance_usdc
async def get_balance() -> Optional[float]:
    """
    Alias BP-compat — returneaza total USDC tradabil (perp + spot pe Unified).
    Folosit DOAR pt cap-ul de siguranta din position_sizing SI pt sync_equity
    (shared_equity — sursa pt sizing viitoarelor trade-uri + mesajul BOT PORNIT).

    Retry 4x/1s: un singur fail tranzitoriu (fara retry inainte) lasa
    sync_equity() cu shared_equity NEACTUALIZAT (stale) → sizing viitoarelor
    trade-uri + "Account init" calculate pe o valoare veche, potential foarte
    diferita de soldul real (incident 2026-07-08: NEAR supradimensionat).
    """
    last_exc: Optional[Exception] = None
    for i in range(4):
        try:
            return await get_balance_usdc()
        except Exception as e:
            last_exc = e
            print(f"[HL] get_balance attempt {i+1}/4 failed: {e!r}")
            if i < 3:
                await asyncio.sleep(1.0)
    print(f"[HL] get_balance FAILED after 4 retries: {last_exc!r}")
    return None


# ---------------------------------------------------------------------------
# Sizing snapshot — mirror din BP Bybit exchange_api.sizing_snapshot
# ---------------------------------------------------------------------------

def sizing_snapshot(balance: float,
                    risk_frac: float,
                    entry_price: float,
                    sl_price: float,
                    bybit_balance: Optional[float] = None,
                    symbol: Optional[str] = None) -> dict:
    """
    Sizing snapshot BP-compat signature.

    balance       = state.account (LOCAL equity, compound)
    risk_frac     = fractie (NU %), 0.10 = 10%
    bybit_balance = balanta exchange REALA (pt cap leverage_max ×bybit_balance)
    symbol        = pt qty_step / qty_precision per coin din _INSTRUMENTS

    Returneaza dict cu shape compat BP (vezi `position_sizing.sizing_snapshot`).
    """
    import os
    leverage_max = float(os.getenv("LEVERAGE_MAX", "10"))

    if entry_price <= 0 or sl_price <= 0 or balance <= 0:
        return {"valid": False, "reason": "invalid input (entry/sl/balance)",
                "qty": 0.0, "capped": False, "notional": 0.0,
                "actual_notional": 0.0, "actual_risk": 0.0,
                "max_notional": 0.0, "leverage_max": leverage_max,
                "sl_pct": 0.0, "risk_amount": 0.0}

    sl_pct = abs(entry_price - sl_price) / entry_price * 100
    risk_amount = balance * risk_frac
    qty_raw = risk_amount / abs(entry_price - sl_price)
    notional = qty_raw * entry_price

    # Cap pe bybit_balance × leverage_max × 0.95 (margin safety)
    capped = False
    max_notional = 0.0
    if bybit_balance and bybit_balance > 0:
        max_notional = 0.95 * bybit_balance * leverage_max
        if notional > max_notional:
            capped = True
            notional = max_notional
            qty_raw = notional / entry_price

    step = _qty_step(symbol)
    qty = (int(qty_raw / step)) * step    # floor pt safety
    actual_notional = qty * entry_price
    actual_risk = qty * abs(entry_price - sl_price)

    valid = qty > 0 and actual_notional >= 10.0   # HL min $10 notional
    reason = ""
    if qty <= 0:
        reason = "qty=0 after rounding (too small)"
    elif actual_notional < 10.0:
        reason = f"notional ${actual_notional:.2f} < $10 (HL min)"

    return {
        "valid":           valid,
        "reason":          reason,
        "sl_pct":          sl_pct,
        "risk_amount":     risk_amount,
        "notional":        risk_amount / abs(entry_price - sl_price) * entry_price,
        "qty":             qty,
        "actual_notional": actual_notional,
        "actual_risk":     actual_risk,
        "capped":          capped,
        "max_notional":    max_notional,
        "leverage_max":    leverage_max,
    }


# ---------------------------------------------------------------------------
# Signing + POST /exchange (EIP-712)
# ---------------------------------------------------------------------------

# Nonce monoton crescator — HL respinge nonces ne-crescatoare. Folosim
# ms timestamp + small counter pt cazuri rare cand 2 actions sunt trimise
# in acelasi ms (atomic increment).
_last_nonce: int = 0

def _next_nonce() -> int:
    global _last_nonce
    now_ms = int(time.time() * 1000)
    if now_ms <= _last_nonce:
        now_ms = _last_nonce + 1
    _last_nonce = now_ms
    return now_ms


async def _sign_and_post(action: dict, vault_address: Optional[str] = None) -> dict:
    """
    Semneaza `action` cu agent PK (EIP-712 L1) si POST la /exchange.

    Daca vault_address=None -> trade in numele main wallet-ului (HL_MAIN_ADDRESS).
    Returneaza response JSON parsat. Raise pe HTTP error.
    """
    if _agent_acct is None:
        raise RuntimeError("HL_AGENT_PRIVATE_KEY not set — cannot sign")

    # Import lazy ca sa nu impunem SDK la import time pentru Phase A read-only
    from hyperliquid.utils.signing import sign_l1_action

    nonce = _next_nonce()

    # SDK semneaza action; vault_address None pentru trade ca main wallet
    signature = sign_l1_action(
        wallet=_agent_acct,
        action=action,
        active_pool=vault_address,
        nonce=nonce,
        expires_after=None,
        is_mainnet=HL_IS_MAINNET,
    )

    payload = {
        "action":    action,
        "nonce":     nonce,
        "signature": signature,
    }
    if vault_address:
        payload["vaultAddress"] = vault_address

    import core.rate_limiter as rl
    await rl.wait_token()
    cli = _client()
    r = await cli.post("/exchange", json=payload)
    r.raise_for_status()
    resp = r.json()
    # HL returneaza {"status": "ok"|"err", "response": ...}
    if resp.get("status") != "ok":
        raise RuntimeError(f"HL exchange action failed: {resp}")
    return resp


# ---------------------------------------------------------------------------
# Trading actions
# ---------------------------------------------------------------------------

# HL nu are order type "Market". Folosim limit IOC cu pret aggressively
# above (LONG) / below (SHORT) market — buffer 0.5% similar cu UI HL.
_SLIPPAGE_BUFFER = 0.005


# ----------------------------------------------------------------------------
# Trading pause flag — set via /api/pause si /api/stop endpoints din main.py.
# Cand True, functiile de order respinge SILENT ENTRY-urile (reduce_only=False).
# EXIT-urile (reduce_only=True) trec mereu — necesare pt SL/TP, chase_close si
# /api/stop care market-close-uieste pozitiile existente. Guard pus la
# choke-point-urile low-level (_place_market_internal + _place_alo) prin care
# trec TOATE entry-urile (maker_entry_or_market routes through them).
# ----------------------------------------------------------------------------
_TRADING_PAUSED: bool = False


def set_trading_paused(paused: bool) -> None:
    """Setter pt flag global _TRADING_PAUSED. Apelat din /api/pause, /api/resume,
    /api/stop endpoint-uri din main.py."""
    global _TRADING_PAUSED
    _TRADING_PAUSED = bool(paused)


def is_trading_paused() -> bool:
    """Returneaza starea curenta a flag-ului. Folosit de /api/status + heartbeat."""
    return _TRADING_PAUSED


async def _place_market_internal(symbol: str, side: str, qty: float,
                                  reduce_only: bool = False) -> dict:
    # Pause gate: doar ENTRY-urile (reduce_only=False) sunt respinse. EXIT-urile
    # trec mereu — pause-ul NU blocheaza inchiderea pozitiilor existente.
    if _TRADING_PAUSED and not reduce_only:
        print(f"  [PAUSE] _place_market_internal rejected — trading paused "
              f"(symbol={symbol} side={side} qty={qty})")
        return {"result": "rejected", "filled_qty": 0.0, "avg_price": 0.0,
                "order_id": 0, "raw": None}
    """
    Helper intern: plaseaza limit IOC cu pret agresiv (~"market" pe HL).
    Return shape compat BP maker_entry_or_market.
    """
    coin = _coin(symbol)
    meta = _meta(coin)

    mids = await _info({"type": "allMids"})
    mid_str = mids.get(coin)
    if not mid_str:
        return {"result": "rejected", "filled_qty": 0.0, "avg_price": 0.0,
                "order_id": 0, "raw": None}
    mid = float(mid_str)

    is_buy = side.upper() in ("BUY", "B", "LONG")
    limit_px = mid * (1 + _SLIPPAGE_BUFFER) if is_buy else mid * (1 - _SLIPPAGE_BUFFER)

    order_wire = {
        "a": meta["asset_id"],
        "b": is_buy,
        "p": _fmt_price(limit_px, coin),
        "s": _fmt_qty(qty, coin),
        "r": bool(reduce_only),
        "t": {"limit": {"tif": "Ioc"}},
    }
    action = {"type": "order", "orders": [order_wire], "grouping": "na"}

    try:
        resp = await _sign_and_post(action)
    except Exception as e:
        print(f"[HL] place market failed: {e}")
        return {"result": "rejected", "filled_qty": 0.0, "avg_price": 0.0,
                "order_id": 0, "raw": None}

    statuses = resp.get("response", {}).get("data", {}).get("statuses", [])
    if not statuses:
        return {"result": "rejected", "filled_qty": 0.0, "avg_price": 0.0,
                "order_id": 0, "raw": resp}
    st = statuses[0]
    if "filled" in st:
        f = st["filled"]
        return {
            "result":     "market",
            "filled_qty": float(f.get("totalSz", 0)),
            "avg_price":  float(f.get("avgPx", 0)),
            "order_id":   int(f.get("oid", 0)),
            "raw":        resp,
        }
    if "error" in st:
        print(f"[HL] order rejected: {st['error']}")
    return {"result": "rejected", "filled_qty": 0.0, "avg_price": 0.0,
            "order_id": 0, "raw": resp}


async def _place_alo(symbol: str, side: str, px: float, qty: float,
                      reduce_only: bool = False) -> Optional[int]:
    """
    Place limit order cu tif=Alo (post-only). Returneaza oid daca placed,
    None pe rejected (would-cross or other error).
    """
    # Pause gate: Alo e folosit DOAR ca maker entry → respinge cand paused.
    # Exit-urile nu trec prin _place_alo (folosesc _place_market_internal).
    if _TRADING_PAUSED and not reduce_only:
        print(f"  [PAUSE] _place_alo rejected — trading paused "
              f"(symbol={symbol} side={side} qty={qty})")
        return None
    coin = _coin(symbol)
    meta = _meta(coin)
    is_buy = side.upper() in ("BUY", "B", "LONG")
    # cloid unic per ordin — echivalentul HL al orderLinkId din BP (commit
    # 6ed3bb2). Pe timeout AMBIGUU (_sign_and_post arunca DUPA ce actiunea a
    # ajuns la HL — ordinul POATE fi resting), fara id client nu-l putem regasi
    # → ramane ORFAN, iar maker_entry_or_market trimite fallback Ioc pe qty
    # intreg → DUBLA pozitie cand orfanul umple. Cu cloid: lookup + recover sau
    # cancel defensiv. cloid HL = 16 bytes hex; uuid4().hex are exact 32 hex.
    cloid = f"0x{uuid.uuid4().hex}"
    order_wire = {
        "a": meta["asset_id"],
        "b": is_buy,
        "p": _fmt_price(px, coin),
        "s": _fmt_qty(qty, coin),
        "r": bool(reduce_only),
        "t": {"limit": {"tif": "Alo"}},   # Add Liquidity Only = post-only
        "c": cloid,
    }
    action = {"type": "order", "orders": [order_wire], "grouping": "na"}
    try:
        resp = await _sign_and_post(action)
    except Exception as e:
        # AMBIGUU: exceptia poate veni DUPA ce HL a primit actiunea (timeout
        # retea / raspuns pierdut) → ordinul POATE fi resting. Spre deosebire
        # de Bybit (unde _post conflateaza rejection si timeout), pe HL DOAR
        # aceasta cale e ambigua — calea "error in st" de mai jos e rejection
        # CERT. Lookup pe cloid ca sa distingem orfan de esec (mirror 6ed3bb2).
        print(f"[HL] _place_alo failed: {e}")
        return await _recover_orphan_alo(symbol, coin, meta, cloid)
    statuses = resp.get("response", {}).get("data", {}).get("statuses", [])
    if not statuses:
        return None
    st = statuses[0]
    if "resting" in st:
        return int(st["resting"].get("oid", 0))
    if "filled" in st:
        # Filled immediately (race conditions, normally Alo doesn't fill but possible)
        return int(st["filled"].get("oid", 0))
    if "error" in st:
        # Alo rejected (would cross, etc.) — HL a raspuns EXPLICIT, ordinul cert
        # nu exista → caller decide fallback la Ioc (safe, nu e ambiguu)
        return None
    return None


async def _recover_orphan_alo(symbol: str, coin: str, meta: dict,
                              cloid: str) -> Optional[int]:
    """Pe timeout ambiguu in _place_alo: regaseste ordinul dupa cloid via
    `orderStatus`. Mirror BP 6ed3bb2 (lookup orderLinkId pe /v5/order/realtime):

      - resting/filled → ORPHAN RECOVERY: returneaza oid-ul real, flow-ul maker
        continua normal (poll → cancel la timeout → Ioc doar pe remainder).
      - unknownOid / canceled / rejected → cert inactiv → None (fallback safe).
      - lookup-ul insusi esuat (info down) → cancel defensiv pe cloid
        (best-effort, omoara eventualul orfan) apoi None.
    """
    try:
        res = await _info({"type": "orderStatus",
                           "user": HL_MAIN_ADDRESS, "oid": cloid})
    except Exception as e:
        print(f"[HL] _place_alo {symbol}: stare AMBIGUA (place si lookup esuate)"
              f" — cancel defensiv cloid={cloid} ({e})")
        await _cancel_by_cloid(coin, meta, cloid)
        return None
    order = res.get("order") if isinstance(res, dict) else None
    if order:
        inner = order.get("order", {})
        status = order.get("status", "")
        if status in ("open", "filled"):
            oid = int(inner.get("oid", 0))
            print(f"[HL] _place_alo {symbol}: raspuns pierdut dar ordinul EXISTA"
                  f" — orphan recovery (oid={oid}, status={status})")
            return oid
    # unknownOid sau status canceled/rejected → cert nu e activ
    return None


async def _cancel_by_cloid(coin: str, meta: dict, cloid: str) -> bool:
    """Cancel order dupa cloid (best-effort, defensiv pe orfan ambiguu). HL
    action `cancelByCloid` (asset index + cloid hex)."""
    action = {"type": "cancelByCloid",
              "cancels": [{"asset": meta["asset_id"], "cloid": cloid}]}
    try:
        await _sign_and_post(action)
        return True
    except Exception as e:
        print(f"[HL] _cancel_by_cloid {cloid} failed: {e}")
        return False


async def _get_order_status(symbol: str, oid: int) -> Optional[dict]:
    """
    Returneaza {"sz", "totalSz", "leavesSz", "status"} pt un order, sau None
    daca nu se gaseste in openOrders. Folosit pt polling fill status.
    """
    addr = HL_MAIN_ADDRESS
    try:
        oo = await _info({"type": "openOrders", "user": addr})
    except Exception:
        return None
    if not isinstance(oo, list):
        return None
    for o in oo:
        if int(o.get("oid", 0)) == oid:
            return {
                "sz":       float(o.get("sz", 0) or 0),
                "origSz":   float(o.get("origSz", 0) or 0),
                "leavesSz": float(o.get("sz", 0) or 0),   # HL n-are leavesSz dedicat
                "status":   "open",
            }
    return None    # not in openOrders = fully filled OR cancelled


async def _cancel_order(symbol: str, oid: int) -> bool:
    """Cancel order by oid. Returneaza True pe succes."""
    coin = _coin(symbol)
    meta = _meta(coin)
    action = {"type": "cancel", "cancels": [{"a": meta["asset_id"], "o": int(oid)}]}
    try:
        await _sign_and_post(action)
        return True
    except Exception as e:
        print(f"[HL] _cancel_order {oid} failed: {e}")
        return False


async def maker_entry_or_market(symbol:      str,
                                side:        str,
                                qty:         float,
                                top:         Optional[dict] = None,
                                timeout_sec: int   = 5,
                                fallback:    str   = "market",
                                min_qty:     float = 0.0,
                                reduce_only: bool = False) -> dict:
    """
    BP-compat signature. Maker-first entry pattern pe HL:

    1. ENTRY: Place limit Alo (post-only) la best bid (Buy) / best ask (Sell).
       Daca Alo rejected instant (would cross) → fallback imediat.
    2. Poll `timeout_sec × 1s` openOrders pana cand qty=0 (filled) sau timeout.
    3. Timeout:
       - fallback="market": cancel Alo + Ioc market pe REMAINDER (anti-double-fill).
       - fallback="skip":   doar cancel Alo, returneaza partial maker fill.

    Pt EXIT-uri (reduce_only=True) — sare peste maker direct la market
    pt siguranta executiei (BP convention).

    Returneaza shape BP-compat:
      {"result": "maker"|"market"|"mixed"|"rejected"|"skip",
       "filled_qty": float, "avg_price": float, "order_id": int, "raw": dict}
    """
    # Exit-uri (reduce_only): direct market — siguranta > economie
    if reduce_only or timeout_sec <= 0:
        return await _place_market_internal(symbol, side, qty, reduce_only=reduce_only)

    coin = _coin(symbol)
    is_buy = side.upper() in ("BUY", "B", "LONG")

    # Best bid/ask via l2Book (HL); fallback la allMids daca esueaza
    maker_px = None
    try:
        l2 = await _info({"type": "l2Book", "coin": coin})
        levels = l2.get("levels", [])
        if len(levels) >= 2:
            bids = levels[0]  # buy side, descending
            asks = levels[1]  # sell side, ascending
            if is_buy and bids:
                maker_px = float(bids[0].get("px", 0))  # best bid
            elif not is_buy and asks:
                maker_px = float(asks[0].get("px", 0))  # best ask
    except Exception as e:
        print(f"[HL] l2Book failed for maker entry: {e}")

    if maker_px is None or maker_px <= 0:
        # Couldn't get bid/ask → fallback la market direct
        print(f"[HL] maker_entry: no bid/ask → fallback Ioc")
        return await _place_market_internal(symbol, side, qty, reduce_only=False)

    # Pozitia INAINTE de Alo — baseline pt masurarea fill-ului real din delta
    # de pozitie la timeout (ground truth, NU openOrders care e gol post-fill).
    pos_before = await get_position_qty_strict(coin)
    if pos_before is None:
        pos_before = 0.0   # entry-from-flat (cazul comun); pyramiding+API-fail = edge acceptat

    # Place Alo
    oid = await _place_alo(symbol, side, maker_px, qty, reduce_only=False)
    if oid is None:
        # Alo rejected → fallback Ioc
        print(f"[HL] maker_entry: Alo @ {maker_px} rejected → fallback Ioc")
        return await _place_market_internal(symbol, side, qty, reduce_only=False)

    print(f"[HL] maker_entry: Alo {side} @ {maker_px} qty={qty} oid={oid}")

    # Poll for fill
    initial_qty = qty
    for _ in range(timeout_sec):
        await asyncio.sleep(1.0)
        status = await _get_order_status(symbol, oid)
        if status is None:
            # Not in openOrders = filled OR cancelled by exchange
            print(f"[HL] maker_entry: Alo oid={oid} no longer open → assumed filled")
            return {
                "result":     "maker",
                "filled_qty": initial_qty,
                "avg_price":  maker_px,
                "order_id":   oid,
                "raw":        None,
            }
        if status["leavesSz"] <= 1e-9:
            return {
                "result":     "maker",
                "filled_qty": initial_qty,
                "avg_price":  maker_px,
                "order_id":   oid,
                "raw":        None,
            }

    # Timeout: CANCEL INTAI (opreste orice fill — fara race intre citire si
    # cancel), apoi masoara fill-ul REAL din DELTA DE POZITIE (ground truth).
    # NU folosim openOrders/_get_order_status: e GOL daca Alo s-a umplut deja
    # → ar da filled=0 → market pe qty intreg → DUBLURA mare.
    await _cancel_order(symbol, oid)
    await asyncio.sleep(0.5)            # lasa cancel + fill-uri sa se settleze
    pos_after = None
    for _ in range(3):                  # retry pe blip API tranzitoriu
        pos_after = await get_position_qty_strict(coin)
        if pos_after is not None:
            break
        await asyncio.sleep(0.5)
    if pos_after is None:
        # Persistent API fail → NU putem sti cat a umplut. NU dam market
        # (mai bine under-fill decat dublura). Reconcilierea defense-in-depth
        # prinde o eventuala pozitie orfana.
        print("[HL] maker_entry: pozitie indisponibila post-cancel — SKIP market "
              "(anti-dublura; verifica reconcilierea)")
        return {"result": "skip", "filled_qty": 0.0, "avg_price": maker_px,
                "order_id": oid, "raw": None}
    filled_so_far = max(0.0, pos_after - pos_before)
    remainder = max(0.0, initial_qty - filled_so_far)

    if fallback == "skip" or remainder <= min_qty:
        # Skip market fallback — return whatever maker filled (poate 0)
        return {
            "result":     "maker" if filled_so_far > 0 else "skip",
            "filled_qty": filled_so_far,
            "avg_price":  maker_px,
            "order_id":   oid,
            "raw":        None,
        }

    # Market on remainder
    print(f"[HL] maker_entry: timeout, maker filled {filled_so_far}/{initial_qty}, "
          f"market fallback {remainder}")
    market_res = await _place_market_internal(symbol, side, remainder, reduce_only=False)
    if market_res["filled_qty"] > 0 and filled_so_far > 0:
        # Mixed: weighted avg
        total = filled_so_far + market_res["filled_qty"]
        avg = (filled_so_far * maker_px + market_res["filled_qty"] * market_res["avg_price"]) / total
        return {
            "result":     "mixed",
            "filled_qty": total,
            "avg_price":  avg,
            "order_id":   market_res["order_id"],
            "raw":        market_res.get("raw"),
        }
    elif filled_so_far > 0:
        # Only maker filled
        return {
            "result":     "maker",
            "filled_qty": filled_so_far,
            "avg_price":  maker_px,
            "order_id":   oid,
            "raw":        None,
        }
    return market_res  # only market filled (or both 0)


# Alias public pt place_market (legacy compat)
place_market_order = _place_market_internal


# ----------------------------------------------------------------------------
# Wrappere publice BP-compat — contractul BP core/exchange_api expune aceste
# functii cu semnaturile de mai jos; main.py / strategiile le pot apela
# identic pe orice fork. NU folosi _place_* / _cancel_* direct din strategie.
# ----------------------------------------------------------------------------

async def place_market(symbol: str, side: str, qty: float,
                       reduce_only: bool = False) -> Optional[str]:
    """BP-compat: plaseaza market (limit IOC agresiv pe HL) si returneaza
    order_id ca string sau None pe rejected — EXACT contractul BP, spre
    deosebire de _place_market_internal care returneaza dict-ul complet."""
    r = await _place_market_internal(symbol, side, qty, reduce_only=reduce_only)
    oid = r.get("order_id") if r else 0
    return str(oid) if oid else None


async def place_limit_postonly(symbol: str, side: str, price: float,
                               qty: float, reduce_only: bool = False
                               ) -> Optional[str]:
    """BP-compat: limit post-only (Alo pe HL). Returneaza order_id string
    sau None pe rejected (would-cross). Pause gate mostenit din _place_alo."""
    oid = await _place_alo(symbol, side, price, qty, reduce_only=reduce_only)
    return str(oid) if oid is not None else None


async def place_stop_limit(symbol: str, side: str, price: float,
                           qty: float, trigger: float,
                           direction: int) -> Optional[str]:
    """BP-compat STUB: Bybit suporta pending stop-limit ENTRY orders; HL
    nativ are doar trigger orders reduce-only (exit). Strategiile HL folosesc
    maker_entry_or_market pe semnal in loc de pending stop-entry.

    Returneaza None (= ordin neacceptat, contractul BP pe rejection) cu
    warning EXPLICIT — NU silent, ca sa fie vizibil in log daca o strategie
    portata incearca pattern-ul Bybit."""
    print(f"  [HL] place_stop_limit NEIMPLEMENTAT pe HL (pending stop-entry "
          f"nu exista nativ) — folositi maker_entry_or_market. "
          f"(symbol={symbol} side={side} trigger={trigger} qty={qty})")
    return None


async def cancel_order(symbol: str, order_id) -> None:
    """BP-compat: cancel order dupa id. Accepta str sau int, None = no-op."""
    if not order_id:
        return
    await _cancel_order(symbol, int(order_id))


async def amend_order(symbol: str, order_id: str,
                      price: Optional[float] = None,
                      qty:   Optional[float] = None) -> bool:
    """BP-compat STUB: Bybit are /v5/order/amend (modify in-place); HL nu
    expune amend echivalent in adaptorul curent. Returneaza False (= amend
    nereusit, contractul BP) cu warning explicit — caller-ul BP face fallback
    pe cancel+create cand primeste False."""
    print(f"  [HL] amend_order NEIMPLEMENTAT pe HL adapter — returnez False; "
          f"caller-ul trebuie sa faca cancel+replace. "
          f"(symbol={symbol} oid={order_id} price={price} qty={qty})")
    return False


async def get_order_status(symbol: str, order_id) -> Optional[dict]:
    """BP-compat: status ordin sau None daca nu mai e in openOrders.
    Field-uri HL: {"sz", "origSz", "leavesSz", "status"} (vs Bybit
    orderStatus/cumExecQty/leavesQty — consumatorii verifica doar None/leaves)."""
    if not order_id:
        return None
    return await _get_order_status(symbol, int(order_id))


# HL error strings that should be treated as NO-OP success (idempotent).
# Equivalent to BP's _NOOP_RETCODES={34040} ("trading-stop not modified").
# Cazuri: SL identic la acelasi price + retry → HL respinge "duplicate" / "open
# orders already at this price". Treat ca succes ca sa nu trimitem fals Telegram
# critical din `armed_set_sl` Layer 2 retry.
_SL_NOOP_ERRORS = (
    "already exists",
    "duplicate",
    "no change",
    "would not result in",
    "already an open trigger",
)


def _is_sl_noop_error(err_str: str) -> bool:
    """Returneaza True daca eroarea HL e idempotent (SL deja set corect)."""
    s = (err_str or "").lower()
    return any(needle in s for needle in _SL_NOOP_ERRORS)


async def set_position_sl(symbol:     str,
                          sl_price:   float,
                          tp_price:   Optional[float] = None,
                          is_initial: bool = True,
                          max_retries: int = 4,
                          send_tg_on_fail: bool = True) -> bool:
    """
    BP-compat signature (incl. max_retries + send_tg_on_fail). Plaseaza
    trigger order reduce-only pt SL pe pozitia activa.

    Cand tp_price > 0, plaseaza ATOMIC si trigger TP (tpsl="tp") in acelasi
    action ca SL → AMBELE trigger orders sub o singura semnatura (model
    BP-HL nativ, nu extensie V4). SL fail blocheaza retry, TP fail = warning
    + continue (TP nu e critic safety).

    `max_retries`: 4 default (Layer 1 retry). 1 = fail-fast (Layer 0/Layer 2 calls).
    `send_tg_on_fail`: True trimite Telegram critical pe esec final (SL).
    Backoff intre retries: [0, 1, 2, 4][:max_retries].

    Returneaza True pe succes (SL OK; TP best-effort), False pe esec SL final.
    Inferre direction din pos curenta (LONG -> sell SL/TP, SHORT -> buy SL/TP).
    """
    coin = _coin(symbol)
    backoff = [0, 1, 2, 4][:max(1, max_retries)]
    last_err = None

    for attempt, wait in enumerate(backoff, start=1):
        if wait > 0:
            await asyncio.sleep(wait)
        try:
            meta = _meta(coin)
            pos = await fetch_open_position(coin)
            if not pos:
                last_err = "no open position"
                print(f"[HL] set_position_sl FAIL #{attempt}/{len(backoff)}: {last_err}")
                continue

            is_buy_sl = pos["direction"] == "SHORT"
            sl_str = _fmt_price(sl_price, coin)
            qty_str = _fmt_qty(pos["qty"], coin)

            # "SET" semantics (nu "add"): sterge trigger-ele existente INAINTE de
            # a plasa noile → set_position_sl INLOCUIESTE, nu stacuieste (altfel
            # fiecare trailing update adauga inca un SL/TP reduce-only). Bybit
            # e atomic (setTradingStop); pe HL SL/TP = order-e separate.
            # cancel_all_stops = cancel_all_orders pe coin. Fereastra scurta fara
            # SL intre cancel→place; backstop = SL software in strategie + panic.
            try:
                await cancel_all_stops(coin)
            except Exception as e:
                print(f"[HL] set_position_sl cancel_all_stops: {e!r}")

            sl_order_wire = {
                "a": meta["asset_id"],
                "b": is_buy_sl,
                "p": sl_str,
                "s": qty_str,
                "r": True,
                "t": {"trigger": {"isMarket": True, "triggerPx": sl_str, "tpsl": "sl"}},
            }

            # V4_HL: atomic TP trigger order (mirror V4 Bybit).
            # HL accepta acelasi API ca SL, doar tpsl="tp" + price diferit. TP
            # foloseste ACELASI side ca SL (reduce-only, inchide pozitia) → is_buy_sl.
            # Plasam ambele ordere intr-o singura tranzactie ca sa fie atomic in
            # acelasi sign+post call.
            orders_list = [sl_order_wire]
            if tp_price is not None and tp_price > 0:
                tp_str = _fmt_price(tp_price, coin)
                tp_order_wire = {
                    "a": meta["asset_id"],
                    "b": is_buy_sl,           # acelasi side (reduce-only inchide pozitia)
                    "p": tp_str,
                    "s": qty_str,
                    "r": True,
                    "t": {"trigger": {"isMarket": True, "triggerPx": tp_str, "tpsl": "tp"}},
                }
                orders_list.append(tp_order_wire)

            action = {"type": "order", "orders": orders_list, "grouping": "na"}

            resp = await _sign_and_post(action)
            statuses = resp.get("response", {}).get("data", {}).get("statuses", [])
            # SL status = statuses[0]. TP status (daca tp_price dat) = statuses[1].
            sl_status = statuses[0] if statuses else {}
            if isinstance(sl_status, dict) and "error" in sl_status:
                last_err = sl_status["error"]
                # Idempotent NO-OP: SL already at this price (retry pe acelasi
                # value). HL respinge dar functional e succes.
                if _is_sl_noop_error(last_err):
                    print(f"[HL] set_position_sl NO-OP (already set): {last_err}")
                    # Check TP status too daca avem
                    if len(statuses) > 1 and isinstance(statuses[1], dict):
                        tp_st = statuses[1]
                        if "error" in tp_st and not _is_sl_noop_error(tp_st["error"]):
                            print(f"[HL] TP not set (SL ok): {tp_st['error']}")
                    return True
                print(f"[HL] set_position_sl FAIL #{attempt}/{len(backoff)}: {last_err}")
                continue
            # SL OK. Verifica si TP daca am incercat (statuses[1]).
            # TP failure NU blocheaza succesul — SL e critica safety, TP e profit.
            if len(statuses) > 1 and isinstance(statuses[1], dict) and "error" in statuses[1]:
                tp_err = statuses[1]["error"]
                if not _is_sl_noop_error(tp_err):
                    print(f"[HL] TP not set (SL ok pe {symbol}): {tp_err}")
            return True
        except Exception as e:
            last_err = repr(e)
            print(f"[HL] set_position_sl FAIL #{attempt}/{len(backoff)}: {last_err}")

    # All retries exhausted
    if send_tg_on_fail:
        try:
            import core.telegram_bot as _tg
            # WARNING, nu HALT: botul continua (pozitia ruleaza, fallback software
            # _check_sl_tp / reconcile la close). HALT doar cand se opreste.
            await _tg.send_warning(
                f"SL nu a putut fi setat pe {symbol}",
                f"<b>Eroare ultima:</b> <code>{last_err}</code>\n"
                f"<b>SL țintit:</b> <code>{sl_price}</code>\n"
                f"<b>Retries:</b> <code>{len(backoff)}</code>\n\n"
                f"Pozitia poate ramane neprotejata — verifica manual pe HL UI."
            )
        except Exception as tg_e:
            print(f"[HL] set_position_sl Telegram critical send failed: {tg_e}")
    return False


async def cancel_all_orders(symbol: str) -> int:
    """
    Cancel TOATE ordinile open pe `symbol` (trigger sau resting). Util la
    cleanup dupa close manual sau la halt. Returneaza nr cancelled.
    """
    coin = _coin(symbol)
    addr = HL_MAIN_ADDRESS
    open_orders = await _info({"type": "openOrders", "user": addr})
    if not isinstance(open_orders, list):
        return 0

    targets = [o for o in open_orders if o.get("coin") == coin]
    if not targets:
        return 0

    meta = _meta(coin)
    cancels = [{"a": meta["asset_id"], "o": int(o["oid"])} for o in targets]
    action = {"type": "cancel", "cancels": cancels}
    try:
        await _sign_and_post(action)
        return len(cancels)
    except Exception as e:
        print(f"[HL] cancel_all_orders failed: {e}")
        return 0


# ---------------------------------------------------------------------------
# Enhance fetch_open_position cu SL/TP din openOrders
# ---------------------------------------------------------------------------
# (Override-uieste varianta Phase A cu un wrapper care merge la openOrders
# si pune SL/TP daca exista trigger orders reduce-only.)

_fetch_open_position_phase_a = fetch_open_position   # type: ignore


async def fetch_open_position(symbol: str,                 # noqa: F811
                              user: Optional[str] = None) -> Optional[dict]:
    """
    Phase B: la pozitia returnata de Phase A, adauga sl_price/tp_price din
    trigger orders open.

    IMPORTANT: folosim `frontendOpenOrders` (NU `openOrders`) — endpoint-ul
    de baza `openOrders` NU returneaza `triggerPx`/`isTrigger` pe trigger
    orders. Doar `frontendOpenOrders` are toate field-urile: `isTrigger`,
    `triggerPx`, `isPositionTpsl`, `orderType` ("Stop Market" / "Stop Limit").

    Surse SL detectate:
      - Position-level TP/SL plantat din UI HL ("+ Add Stop Loss" pe panel):
        `isPositionTpsl=True`, `sz="0.0"` (close position dinamic), `isTrigger=True`
      - Standalone trigger plantat de bot via place trigger order:
        `isPositionTpsl=False` (sau lipseste), `sz=qty fix`, `isTrigger=True`

    Distinctie SL vs TP: pe direction + trigger price vs entry.
      LONG:  px < entry -> SL,  px > entry -> TP
      SHORT: px > entry -> SL,  px < entry -> TP
    """
    pos = await _fetch_open_position_phase_a(symbol, user)
    if not pos:
        return None

    coin = _coin(symbol)
    addr = (user or HL_MAIN_ADDRESS or "").lower()
    try:
        oo = await _info({"type": "frontendOpenOrders", "user": addr})
    except Exception as e:
        print(f"[HL] fetch_open_position: frontendOpenOrders fetch failed: {e}")
        return pos

    if not isinstance(oo, list):
        return pos

    for o in oo:
        if o.get("coin") != coin:
            continue
        if not o.get("reduceOnly"):
            continue
        if not o.get("isTrigger"):
            continue
        trig = o.get("triggerPx")
        if not trig:
            continue
        try:
            px = float(trig)
        except Exception:
            continue
        if pos["direction"] == "LONG":
            if px < pos["entry_price"]:
                pos["sl_price"] = px
            else:
                pos["tp_price"] = px
        else:   # SHORT
            if px > pos["entry_price"]:
                pos["sl_price"] = px
            else:
                pos["tp_price"] = px
    return pos


# ---------------------------------------------------------------------------
# PnL fetch from userFills (windowed)
# ---------------------------------------------------------------------------

# HL retine fills cel putin ~7-30 zile in info endpoint. Limitam window-ul
# la 7 zile pt safety (similar cu Bybit BP _PNL_LOOKBACK_MAX_MS).
_PNL_LOOKBACK_MAX_MS = 7 * 24 * 3600 * 1000


async def fetch_pnl_for_trade(symbol: str,
                              entry_ts_ms: int,
                              exit_ts_ms:  int,
                              settle_delay_sec: float = 2.0) -> dict:
    """
    Trage PnL-ul total pentru UN trade logical din userFills (HL).
    Contract IDENTIC cu BP core/exchange_api.fetch_pnl_for_trade (Bybit):
    semnatura, settle delay, retry backoff, chei return.

    Algoritm (mirror BP):
      1. Asteapta `settle_delay_sec` — indexer-ul HL userFillsByTime are lag
         de secunde pana inregistreaza fill-urile (incident 10.06.2026:
         chase_close → fetch imediat → 0 closing fills → trade raportat
         $0.00 in loc de -$9.33).
      2. Fetch userFillsByTime cu window [entry-60s, max(exit+5min, now+60s)],
         start clamped la _PNL_LOOKBACK_MAX_MS.
      3. `relevant` = fill-urile care INCHID pozitia ("Close"/flip in dir) —
         echivalentul inregistrarilor closed-pnl Bybit. Daca relevant=[] →
         retry cu backoff 2s, 5s, 10s (mirror BP, total ~17s extra).

    SEMANTICA PnL — net dupa fees, ca Bybit: closedPnl pe HL e DOAR price-PnL
    (fee raportat separat per fill — verificat empiric 10.06.2026:
    sum(closedPnl)=-9.0153 = exact (entry-exit)×qty; fees=0.3165 separat).
    Bybit closedPnl INCLUDE fees. Pt paritate: pnl = Σ closedPnl − Σ fees
    (fees pe TOATE fill-urile coin-ului din window: entry + exit).

    Returneaza (chei identice BP):
      {
        "pnl":       float,   # net dupa fees (principal + piramide)
        "fees":      float,   # fees totale din fills
        "n_fills":   int,     # cate closing fills s-au agregat
        "avg_entry": float,   # VWAP fill-uri "Open ..." din window
        "avg_exit":  float,   # VWAP closing fills
        "raw":       list,    # closing fills raw (pt debug)
      }
    """
    if settle_delay_sec > 0:
        await asyncio.sleep(settle_delay_sec)

    addr = (HL_MAIN_ADDRESS or "").lower()
    coin = _coin(symbol)

    # Marja: 60s inainte de entry. DEFENSIVE CLAMP la _PNL_LOOKBACK_MAX_MS
    # (mirror BP — protejeaza contra entry_ts_ms vechi pe pozitii adopted;
    # fara clamp, toate close-urile pe symbol din fereastra ar fi sumate).
    raw_start_ms = entry_ts_ms - 60_000
    clamped_start_ms = exit_ts_ms - _PNL_LOOKBACK_MAX_MS
    start_ms = max(raw_start_ms, clamped_start_ms)
    if start_ms > raw_start_ms:
        print(f"  [HL] fetch_pnl_for_trade {symbol}: entry_ts_ms "
              f"vechi de >{_PNL_LOOKBACK_MAX_MS // (3600*1000)}h — window "
              f"clamped la [{start_ms}, ...] (raw start={raw_start_ms}). "
              f"Fill-uri inchise inainte de clamp NU intra in PnL.")
    # exit_ts_ms vine de la strategie ca bar ts; fill-ul real (mai ales pe
    # chase_close fortat) poate fi mai tarziu pe wall-clock → limita
    # superioara = max(exit+5min, now+60s), mirror BP.
    end_limit_ms = max(exit_ts_ms + 300_000, int(time.time() * 1000) + 60_000)

    body = {
        "type":      "userFillsByTime",
        "user":      addr,
        "startTime": start_ms,
        "endTime":   end_limit_ms,
    }

    coin_fills: list = []
    relevant: list = []
    for attempt, retry_delay in enumerate([0, 2.0, 5.0, 10.0]):
        if retry_delay > 0:
            await asyncio.sleep(retry_delay)
        try:
            fills = await _info(body)
        except Exception as e:
            print(f"  [HL] fetch_pnl_for_trade userFillsByTime failed: {e}")
            fills = None
        coin_fills = ([f for f in fills if f.get("coin") == coin]
                      if isinstance(fills, list) else [])
        # Closing fills = echivalentul records closed-pnl Bybit. Entry fills
        # au closedPnl=0; doar cele care INCHID pozitia ("Close ..." sau flip
        # "Short > Long") conteaza pt PnL si pt conditia de retry.
        relevant = [f for f in coin_fills
                    if "Close" in str(f.get("dir", ""))
                    or ">" in str(f.get("dir", ""))]
        if relevant:
            if attempt > 0:
                print(f"  [HL] closing fills gasite dupa retry #{attempt} "
                      f"({len(relevant)} fills)")
            break
        if attempt < 3:
            print(f"  [HL] closing fills gol (retry {attempt + 1}/3 in "
                  f"{[2.0, 5.0, 10.0][attempt]:g}s)  "
                  f"fills_total={len(coin_fills)}")

    if not relevant:
        print(f"  [HL] WARNING: niciun closing fill pentru trade "
              f"{entry_ts_ms}-{exit_ts_ms} dupa 4 incercari (~17s)  "
              f"fills_in_response={len(coin_fills)}  "
              f"window=[{start_ms},{end_limit_ms}]")
        return {"pnl": 0.0, "fees": 0.0, "n_fills": 0,
                "avg_entry": 0.0, "avg_exit": 0.0, "raw": []}

    price_pnl = sum(float(f.get("closedPnl", 0) or 0) for f in relevant)
    fees = sum(float(f.get("fee", 0) or 0) for f in coin_fills)

    open_fills = [f for f in coin_fills if "Open" in str(f.get("dir", ""))]
    open_sz  = sum(float(f.get("sz", 0) or 0) for f in open_fills)
    close_sz = sum(float(f.get("sz", 0) or 0) for f in relevant)
    avg_entry = (sum(float(f.get("px", 0) or 0) * float(f.get("sz", 0) or 0)
                     for f in open_fills) / open_sz) if open_sz else 0.0
    avg_exit  = (sum(float(f.get("px", 0) or 0) * float(f.get("sz", 0) or 0)
                     for f in relevant) / close_sz) if close_sz else 0.0

    return {
        "pnl":       round(price_pnl - fees, 4),
        "fees":      round(fees, 4),
        "n_fills":   len(relevant),
        "avg_entry": round(avg_entry, 4),
        "avg_exit":  round(avg_exit, 4),
        "raw":       relevant,
    }


# ============================================================================
# V4 compat shims — semnaturi identice cu V4 Bybit pt zero-edit main.py
# ============================================================================

async def get_market_info(symbol: str) -> dict:
    """V4 Bybit-style market info. Pe HL: derivam din _INSTRUMENTS (preload).
    Returneaza dict identic cu V4: qty_step, qty_prec, price_prec, min_qty, tick_size.

    BUGFIX: _meta() expune chei snake_case (sz_decimals), NU camelCase HL
    (szDecimals/pxDecimals). Vechiul cod citea cheile gresite → toate cadeau pe
    default → qty_step=0.01, min_qty=1 pt ORICE coin → orice entry skip-uit
    (BTC la ~$100k cu min_qty=1 = $100k notional). Folosim helperii corecti
    _qty_step/_qty_prec + regula HL de pret (max 6-sz_dec zecimale, vezi _fmt_price).
    """
    if _coin(symbol) not in _INSTRUMENTS:
        await preload_instruments([_coin(symbol)])
    sz_dec = _qty_prec(symbol)               # = sz_decimals al coin-ului
    qty_step = _qty_step(symbol)             # = 10**(-sz_dec)
    price_prec = max(0, 6 - sz_dec)          # regula HL perp (idem _fmt_price)
    tick_size = 10 ** (-price_prec) if price_prec > 0 else 1.0
    return {
        "qty_step": qty_step,
        "qty_prec": sz_dec,
        "price_prec": price_prec,
        "min_qty": qty_step,                 # min HL = 1 step (notional $10 e in sizing)
        "tick_size": tick_size,
    }


async def set_leverage(symbol: str, leverage: int) -> bool:
    """V4 Bybit-style. Pe HL: updateLeverage action (cross/isolated).
    Returneaza True pe success. Pe failure: log + False (V4 trateaza idempotent).
    """
    try:
        # Bugfix: foloseste asset_id (key real din _meta), nu "idx" (key inexistenta
        # care intoarcea 0 → set_leverage pe asset 0 = BTC pt orice simbol).
        action = {
            "type": "updateLeverage",
            "asset": _meta(symbol)["asset_id"],
            "isCross": False,  # isolated default (consistent cu V4 Bybit isolated)
            "leverage": int(leverage),
        }
        r = await _sign_and_post(action)
        ok = r.get("status") == "ok"
        if not ok:
            print(f"  [HL] set_leverage {symbol} → {r}")
        return ok
    except Exception as e:
        print(f"  [HL] set_leverage {symbol} error: {e}")
        return False


def round_qty_down(qty: float, step: float) -> float:
    """V4 compat helper. Pe HL: step e calculat din szDecimals."""
    if step <= 0:
        return qty
    import math as _m
    return _m.floor(qty / step) * step
