"""
telegram_bot.py — Notificari Telegram pentru Ichimoku2

Env vars:
    TELEGRAM_TOKEN   = "123:ABC..."
    TELEGRAM_CHAT_ID = "12345"
    BOT_NAME         = "ichi1" / "ichi2"        — apare in header
    STRATEGY_NAME    = "Hull+Ichimoku"

Functii:
    send(title, body)           — mesaj normal
    send_critical(title, body)  — prefix HALT (anomalii care cer interventie)
    send_raw(text)              — bypass header

Spre deosebire de boilerplate (1 simbol per bot), Ichimoku2 are 3 perechi
per bot — symbol-ul e parametru per-mesaj, nu env global.
"""
from __future__ import annotations

import html
import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional

import httpx

TELEGRAM_API = "https://api.telegram.org"

_DAYS_RO = ["Luni", "Marti", "Miercuri", "Joi", "Vineri", "Sambata", "Duminica"]


def fmt_time(ts) -> str:
    """ts: seconds, milliseconds sau datetime → 'Luni, 07.05.2026  19:42' (Bucharest)."""
    tz_name = os.getenv("CHART_TZ", "Europe/Bucharest")
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("Europe/Bucharest")

    if isinstance(ts, datetime):
        dt = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
    else:
        v = float(ts)
        if v > 1e12:
            v /= 1000.0
        dt = datetime.fromtimestamp(v, tz=timezone.utc)

    local = dt.astimezone(tz)
    return f"{_DAYS_RO[local.weekday()]}, {local.strftime('%d.%m.%Y  %H:%M')}"


def dir_emoji(direction: str) -> str:
    """
    Emoji colorat pt direction trade (vizibilitate vizuala in Telegram).
    Telegram NU suporta culori arbitrare in text — emoji prefixate sunt
    workaround-ul standard ca user-ul sa identifice rapid LONG vs SHORT
    in stream-ul de notificari.

    LONG  -> 🟢 (cerc verde)
    SHORT -> 🔴 (cerc rosu)
    alta  -> "" (gol, no emoji)
    """
    d = (direction or "").upper()
    if d == "LONG":
        return "🟢"
    if d == "SHORT":
        return "🔴"
    return ""


def pnl_emoji(pnl: float) -> str:
    """
    Emoji pt PnL realizat: scan rapid pe Telegram intre win-uri (bani)
    si loss-uri (sange) fara sa citesti suma. Bonus: vizual diferentiat
    de `dir_emoji` (LONG/SHORT) — nu se confunda 🟢 (LONG) cu un PnL
    pozitiv care s-ar putea sa fie pe un trade SHORT castigator.

    pnl >= 0  -> 💰 (money bag — castig)
    pnl <  0  -> 🩸 (blood drop — pierdere)
    """
    return "💰" if (pnl or 0) >= 0 else "🩸"


def _header(symbol: Optional[str] = None) -> str:
    name = html.escape(os.getenv("BOT_NAME", "ichimoku2"))
    strategy = html.escape(os.getenv("STRATEGY_NAME", "Hull+Ichimoku").strip())
    label = f"{name} · {strategy}" if strategy else name
    if symbol:
        sym_safe = html.escape(symbol)
        return f"🤖 <b>[{label}]</b> <code>{sym_safe}</code>"
    return f"🤖 <b>[{label}]</b>"


async def send(title: str, body: str = "", symbol: Optional[str] = None) -> None:
    safe_title = html.escape(title)
    text = f"{_header(symbol)}\n<b>{safe_title}</b>"
    if body:
        text += f"\n{body}"
    await send_raw(text)


async def send_critical(title: str, body: str = "",
                        symbol: Optional[str] = None) -> None:
    safe_title = html.escape(title)
    text = f"{_header(symbol)}\n🚨 <b>HALT — {safe_title}</b>"
    if body:
        text += f"\n{body}"
    await send_raw(text)


async def send_warning(title: str, body: str = "",
                       symbol: Optional[str] = None) -> None:
    """Mesaj WARNING — prefix '⚠️ WARN'. Folosit pt pauze, alerte non-critice."""
    safe_title = html.escape(title)
    text = f"{_header(symbol)}\n⚠️ <b>WARN — {safe_title}</b>"
    if body:
        text += f"\n{body}"
    await send_raw(text)


async def send_info(title: str, body: str = "",
                    symbol: Optional[str] = None) -> None:
    """Mesaj INFO — prefix 'ℹ️'. Folosit pt notificari neutre (resume etc.)."""
    safe_title = html.escape(title)
    text = f"{_header(symbol)}\nℹ️ <b>{safe_title}</b>"
    if body:
        text += f"\n{body}"
    await send_raw(text)


async def send_raw(text: str) -> None:
    token = os.getenv("TELEGRAM_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print(f"  [TG] not configured — {text[:100]}")
        return
    try:
        async with httpx.AsyncClient(timeout=10) as c:
            r = await c.post(f"{TELEGRAM_API}/bot{token}/sendMessage", json={
                "chat_id": chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            })
            if r.status_code != 200:
                print(f"  [TG] HTTP {r.status_code}: {r.text[:200]}")
    except Exception as e:
        print(f"  [TG] error: {e}")
