"""
bot_control.py — Pause/Resume/Stop kit pt integrare cu dashboard agregat.

Adaptat din BP bot_control_kit.py pentru V4 (multi-pair, FastAPI).

API:
  set_paused(True/False)              — toggle flag global
  is_paused() -> bool                  — check pt logica de entry
  check_token(query_token) -> bool, msg — auth pe RESET_TOKEN env

Dashboard apeleaza (cu token in query):
  POST {BOT_CONTROL_URL}/api/pause?token=...
  POST {BOT_CONTROL_URL}/api/resume?token=...
  POST {BOT_CONTROL_URL}/api/stop?token=...   (stop = pauza + market-close
                                                 toate pozitiile)

ENV vars necesare:
  BOT_CONTROL_URL  — http://<container>:8104 — dashboard scrie in state.db
                      ca sa stie unde sa POST-eze. Format: schema://host:port,
                      FARA /api/.
  RESET_TOKEN      — token partajat. Gol → toate POST-urile primesc 403.

Stack YAML (compose) necesita:
  networks:
    - bots
  + top-level:
  networks:
    bots:
      external: true
Plus volume /srv/bots/dashboard:/dashboard (read-only, doar daca botul
publica/citeste din state.db).
"""
from __future__ import annotations

import asyncio
import hmac
import os
import threading

_TRADING_PAUSED = False
_PAUSE_LOCK = threading.Lock()


def set_paused(value: bool) -> None:
    global _TRADING_PAUSED
    with _PAUSE_LOCK:
        _TRADING_PAUSED = bool(value)


def is_paused() -> bool:
    return _TRADING_PAUSED


def check_token(token: str) -> tuple[bool, str]:
    """Verifica token din query string vs RESET_TOKEN env. Gol → refuza tot."""
    expected = (os.getenv("RESET_TOKEN", "") or "").strip()
    if not expected:
        return False, "RESET_TOKEN not configured on bot"
    # Constant-time compare — evita timing side-channel (chiar daca threat model
    # e reteaua interna `bots`, compare_digest e gratis). .encode() → bytes ca
    # sa nu arunce pe eventuale non-ASCII. (port din BP 38528a6/ca8c948)
    if not hmac.compare_digest(token.encode(), expected.encode()):
        return False, "Invalid token"
    return True, ""
