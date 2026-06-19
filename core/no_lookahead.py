"""
no_lookahead.py — Utilitati anti-lookahead
============================================
Lookahead = folosirea de date care nu ar fi fost disponibile la momentul
deciziei. Reguli stricte pt strategia ichimoku live:

1. BARA CURENTA IN CURS e INTERZISA pentru decizii — folosim doar bare
   inchise pentru indicatori si signals.

2. In on_kline cu confirmed=False, bara inca se formeaza — NU e bara
   inchisa. Asteapta confirmed=True (sau ts urmator) inainte sa generezi
   semnal.

3. Warmup-ul (400 bare istorice de la API) trebuie filtrat: bara curenta
   poate fi inca in formare la momentul fetch-ului.
"""
from __future__ import annotations

import time


_INTERVAL_MS = {
    "1": 60_000,
    "3": 180_000,
    "5": 300_000,
    "15": 900_000,
    "30": 1_800_000,
    "60": 3_600_000,
    "120": 7_200_000,
    "240": 14_400_000,     # 4H
    "360": 21_600_000,
    "720": 43_200_000,
    "D": 86_400_000,
    "W": 604_800_000,
}


# Map config-style timeframes ("4h", "1h") la Bybit interval strings ("240", "60")
_TF_MAP = {
    "1m": "1", "3m": "3", "5m": "5", "15m": "15", "30m": "30",
    "1h": "60", "2h": "120", "4h": "240", "6h": "360", "12h": "720",
    "1d": "D", "1w": "W",
}


def tf_to_interval(tf: str) -> str:
    """Convert config tf string ('4h') to Bybit interval ('240')."""
    return _TF_MAP.get(tf.lower(), tf)


def interval_ms(interval: str) -> int:
    ms = _INTERVAL_MS.get(interval)
    if ms is None:
        raise ValueError(f"Unknown interval: {interval}")
    return ms


def current_bar_open_ms(now_ms: int, interval: str) -> int:
    """Ora deschiderii barei curente (cea IN CURS)."""
    imms = interval_ms(interval)
    return (now_ms // imms) * imms


def last_closed_bar_open_ms(now_ms: int, interval: str) -> int:
    """Ora deschiderii ULTIMEI BARE INCHISE."""
    return current_bar_open_ms(now_ms, interval) - interval_ms(interval)


def filter_closed_bars(bars: list, interval: str,
                       now_ms: int | None = None) -> list:
    """
    Elimina barele neclose din warmup. Accepta:
      - list[list]: [[ts_ms, o, h, l, c, v, ...], ...]  (Bybit kline format)
      - list[dict]: [{'ts': ts_ms, ...}, ...]

    Ramane doar barele cu open_ts < cutoff (cea curenta).
    """
    if now_ms is None:
        now_ms = int(time.time() * 1000)
    cutoff = current_bar_open_ms(now_ms, interval)
    if not bars:
        return bars
    sample = bars[0]
    if isinstance(sample, dict):
        return [b for b in bars if int(b["ts"]) < cutoff]
    # list/tuple — primul element = ts_ms
    return [b for b in bars if int(b[0]) < cutoff]
