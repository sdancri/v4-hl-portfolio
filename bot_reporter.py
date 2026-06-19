"""
bot_reporter.py — helper minimal pe care îl importă fiecare bot.

SELF-CONTAINED: nu depinde de alte fișiere. Copiezi DOAR acest fișier în
containerul botului. (Schema e inline mai jos, identică cu db.py al
dashboard-ului — dacă schimbi una, schimb-o și pe cealaltă.)

Se integrează în logica existentă fără să schimbe arhitectura:
  - heartbeat()      -> apelat des (la fiecare candle / la 30-60s) cu starea curentă
  - record_trade()   -> apelat la închiderea fiecărui trade

Streak-ul curent (win/loss) e gestionat aici și scris odată cu heartbeat-ul,
ca să nu trebuiască recalculat de fiecare dată în dashboard.

Pe VPS pune db_path = "/dashboard/state.db" (bind mount partajat
între containere). Scrierile sunt mici și atomice.
"""

import sqlite3
import time
from pathlib import Path

# Schema — sursa de adevăr e db.py al dashboard-ului; ținută identică aici
# ca bot_reporter.py să fie copiabil singur, fără db.py.
_SCHEMA = """
CREATE TABLE IF NOT EXISTS bot_state (
    bot_id          TEXT PRIMARY KEY,
    bot_name        TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    timeframe       TEXT,
    status          TEXT NOT NULL,
    equity          REAL,
    open_side       TEXT,
    open_entry      REAL,
    open_pnl        REAL,
    cur_win_streak  INTEGER DEFAULT 0,
    cur_loss_streak INTEGER DEFAULT 0,
    last_heartbeat  REAL NOT NULL,
    control_url     TEXT
);
CREATE TABLE IF NOT EXISTS trades (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    bot_id      TEXT NOT NULL,
    bot_name    TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    closed_ts   REAL NOT NULL,
    pnl         REAL NOT NULL,
    pnl_pct     REAL,
    exit_reason TEXT,
    side        TEXT
);
CREATE TABLE IF NOT EXISTS bot_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    bot_name    TEXT NOT NULL,
    ts          REAL NOT NULL,
    type        TEXT NOT NULL,
    label       TEXT
);
CREATE INDEX IF NOT EXISTS idx_trades_bot  ON trades(bot_id);
CREATE INDEX IF NOT EXISTS idx_trades_time ON trades(closed_ts);
CREATE INDEX IF NOT EXISTS idx_events_name ON bot_events(bot_name);
"""


def _get_conn(db_path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


class BotReporter:
    def __init__(
        self,
        bot_id: str,
        bot_name: str,
        symbol: str,
        timeframe: str = "",
        db_path: str | Path = "state.db",
        control_url: str | None = None,
    ):
        self.bot_id = bot_id
        self.bot_name = bot_name
        self.symbol = symbol
        self.timeframe = timeframe
        self.db_path = db_path
        # URL la care dashboard-ul ajunge la botul ăsta pentru control
        # (pause/resume/stop). Setează-l per bot din env, ex.
        # control_url=os.getenv("BOT_CONTROL_URL"). NULL = necontrolabil.
        self.control_url = control_url
        self._cur_win = 0
        self._cur_loss = 0
        # creează tabelele dacă nu există (idempotent)
        conn = _get_conn(db_path)
        try:
            conn.executescript(_SCHEMA)
            # Migrare idempotentă: adaugă control_url pe DB-uri vechi
            # (CREATE TABLE IF NOT EXISTS nu adaugă coloane noi).
            cols = {r[1] for r in conn.execute("PRAGMA table_info(bot_state)")}
            if cols and "control_url" not in cols:
                conn.execute("ALTER TABLE bot_state ADD COLUMN control_url TEXT")
            tcols = {r[1] for r in conn.execute("PRAGMA table_info(trades)")}
            if tcols and "side" not in tcols:
                conn.execute("ALTER TABLE trades ADD COLUMN side TEXT")
            conn.commit()
        finally:
            conn.close()

    def heartbeat(
        self,
        status: str,                 # "running" | "paused" | "error"
        equity: float,
        open_side: str | None = None,   # "long" | "short" | None
        open_entry: float | None = None,
        open_pnl: float | None = None,
    ) -> None:
        """Scrie/actualizează starea curentă a botului (UPSERT)."""
        conn = _get_conn(self.db_path)
        try:
            conn.execute(
                """
                INSERT INTO bot_state
                  (bot_id, bot_name, symbol, timeframe, status, equity,
                   open_side, open_entry, open_pnl,
                   cur_win_streak, cur_loss_streak, last_heartbeat, control_url)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(bot_id) DO UPDATE SET
                  bot_name=excluded.bot_name,
                  symbol=excluded.symbol,
                  timeframe=excluded.timeframe,
                  status=excluded.status,
                  equity=excluded.equity,
                  open_side=excluded.open_side,
                  open_entry=excluded.open_entry,
                  open_pnl=excluded.open_pnl,
                  cur_win_streak=excluded.cur_win_streak,
                  cur_loss_streak=excluded.cur_loss_streak,
                  last_heartbeat=excluded.last_heartbeat,
                  control_url=excluded.control_url
                """,
                (
                    self.bot_id, self.bot_name, self.symbol, self.timeframe,
                    status, equity, open_side, open_entry, open_pnl,
                    self._cur_win, self._cur_loss, time.time(), self.control_url,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def record_trade(
        self,
        pnl: float,
        pnl_pct: float | None = None,
        exit_reason: str = "",
        closed_ts: float | None = None,
        side: str | None = None,
    ) -> None:
        """
        Înregistrează un trade închis. Win = pnl > 0 (NU pe exit_reason!).
        Actualizează și streak-ul curent.
        """
        is_win = pnl > 0
        if is_win:
            self._cur_win += 1
            self._cur_loss = 0
        else:
            self._cur_loss += 1
            self._cur_win = 0

        conn = _get_conn(self.db_path)
        try:
            conn.execute(
                """
                INSERT INTO trades
                  (bot_id, bot_name, symbol, closed_ts, pnl, pnl_pct, exit_reason, side)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    self.bot_id, self.bot_name, self.symbol,
                    closed_ts or time.time(), pnl, pnl_pct, exit_reason,
                    (side or "").lower() or None,
                ),
            )
            conn.commit()
        finally:
            conn.close()

    def record_event(
        self,
        type: str,                       # "launch" | "version" | "restart" | "shutdown" | "bug" | "note"
        label: str = "",
        ts: float | None = None,
    ) -> None:
        """Înregistrează un eveniment pe care dashboard-ul îl suprapune ca linie
        verticală pe graficul de equity. Apelează-l ex. la pornire
        (record_event("launch", "v1.0")) sau după un deploy
        (record_event("version", "v1.3 — fix SL")).

        NOTĂ: bot_events e cheiat pe bot_name (NU bot_id/symbol). Pe boti
        multi-pair (V4), emite UN SINGUR eveniment per bot, nu per simbol —
        altfel N linii duplicate pe equity chart.
        """
        conn = _get_conn(self.db_path)
        try:
            conn.execute(
                "INSERT INTO bot_events (bot_name, ts, type, label) VALUES (?,?,?,?)",
                (self.bot_name, ts or time.time(), type, label),
            )
            conn.commit()
        finally:
            conn.close()


# Exemplu de integrare într-un bot:
#
#   import os
#   from bot_reporter import BotReporter
#   reporter = BotReporter("VSE_1_KAIA", "VSE_1", "KAIAUSDT", "1H",
#                          db_path="/srv/bots/dashboard/state.db",
#                          # URL la care dashboard-ul (pe aceeași rețea Docker)
#                          # ajunge la botul ăsta. Setează per bot în stack.
#                          control_url=os.getenv("BOT_CONTROL_URL"))
#
#   # în bucla principală, la fiecare candle confirmat:
#   reporter.heartbeat("running", equity=equity,
#                      open_side=pos_side, open_entry=entry, open_pnl=upnl)
#
#   # la închiderea unui trade:
#   reporter.record_trade(pnl=realized_pnl, pnl_pct=realized_pct,
#                         exit_reason=reason)
