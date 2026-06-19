"""
monitoring.py — Operational instrumentation pentru bot-uri BP
=============================================================

Trei capabilitati, partajate intre `main.py` (single-pair) si `main_multi.py`
(multi-pair):

  1. memory_monitor()              — task async, log RSS/VMS/threads/fds/gc_obj
                                     la fiecare 5min. La spike >RSS_ALERT_MB:
                                     one-shot diagnostic dump (top 20 types via
                                     gc.get_objects + /proc/<pid>/status) +
                                     pre-OOM Telegram alert.

  2. install_signal_handlers()     — intercepteaza SIGTERM/SIGINT/SIGHUP.
                                     Loggeaza numele semnalului in
                                     SHUTDOWN_SIGNAL["name"] (citit ulterior in
                                     lifespan finally pentru Telegram shutdown
                                     notification).

  3. install_asyncio_exception_handler() — task-uri care arunca exception fara
                                     sa fie await-ed sunt loggate cu traceback.
                                     Altfel apar doar la GC ca
                                     "Task exception was never retrieved".

ENV vars relevante:
  MEM_MON_INTERVAL_SEC   — interval log RSS (default 300 = 5min).
  MEM_MON_RSS_ALERT_MB   — threshold pt diagnostic dump + pre-OOM TG alert
                           (default 320). Tunat ca sa fire INAINTE de
                           docker-compose `mem_limit` (default 384m) →
                           ~83% headroom. Daca cresti `mem_limit`, mareste
                           proportional.

DE CE pre-OOM alert e necesar in BP:
  Telegram CRASH alert din `sys.excepthook` (in main.py / main_multi.py) NU se
  invoca la SIGKILL. OOM killer trimite SIGKILL — proces moare instant, fara
  chance sa ruleze cod Python. Singura fereastra de alerta e INAINTE de kill,
  detectand RSS care creste catre limita. _memory_monitor()'s threshold trigger
  acopera exact acest gap.

INTERPRETARE EXIT CODES post-mortem (verifica cu
`docker inspect <container> --format '{{.State.ExitCode}} OOMKilled={{.State.OOMKilled}}'`):

  ExitCode  OOMKilled  Signal log               Cauza
  ────────  ─────────  ───────────────────────  ──────────────────────────────
  137       true       (none — SIGKILL)         OOM killer (sau `docker kill -9`)
  143       false      SIGTERM logged           `docker stop`, restart policy
  130       false      SIGINT logged            Ctrl-C manual
  129       false      SIGHUP logged            Parent shell hangup
  0         false      varies                   Lifespan returned graceful
  != 0      false      varies                   Uncaught exception (vezi log)

  IMPORTANT: ExitCode=0 + OOMKilled=false NU e OOM. E shutdown gratios
  (eventual cu silent exit din lifespan / try-except mut). Investigatie
  diferita.
"""
from __future__ import annotations

import asyncio
import os
import signal
import traceback
from typing import Any, Awaitable, Callable, Optional


# State partajat — citit din lifespan finally pt Telegram shutdown notification.
# Updated de signal handler.
SHUTDOWN_SIGNAL: dict[str, Optional[str]] = {"name": None}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def install_signal_handlers() -> None:
    """
    Intercepteaza SIGTERM/SIGINT/SIGHUP. Loggeaza numele si seteaza
    SHUTDOWN_SIGNAL["name"] pt referinta ulterioara in shutdown notification.

    Limitare cunoscuta: SIGKILL (OOM, `docker kill -9`) NU poate fi
    interceptat. Detectia OOM se face via memory_monitor pre-OOM alert si
    via post-mortem `docker inspect` (OOMKilled flag).
    """
    def _on_signal(signum: int, _frame: Any) -> None:
        try:
            name = signal.Signals(signum).name
        except ValueError:
            name = f"signum={signum}"
        SHUTDOWN_SIGNAL["name"] = name
        print(f"  [SIGNAL] received {name} ({signum}) — initiating shutdown",
              flush=True)

    for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        try:
            signal.signal(sig, _on_signal)
        except (OSError, ValueError) as e:
            # Windows nu are SIGHUP; threads non-main nu pot inregistra handlers.
            print(f"  [SIGNAL] register {sig.name} failed: {e}")


def install_asyncio_exception_handler() -> None:
    """
    Global asyncio task exception handler. Task-uri spawned via
    `asyncio.create_task(...)` care arunca exception FARA caller care le
    await-uieste sunt logged silent (doar la GC, dupa N secunde).
    Cu handler-ul ăsta apar imediat in log cu traceback complet.
    """
    def _handler(loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exc = context.get("exception")
        task = context.get("task") or context.get("future")
        msg = context.get("message", "")
        print(f"  [ASYNCIO_EXC] task={task} msg={msg!r}", flush=True)
        if isinstance(exc, BaseException):
            tb = "".join(traceback.format_exception(type(exc), exc,
                                                    exc.__traceback__))
            print(f"  [ASYNCIO_EXC] traceback:\n{tb}", flush=True)

    try:
        asyncio.get_event_loop().set_exception_handler(_handler)
    except RuntimeError:
        # No running loop yet — caller should invoke after loop creation.
        # (FastAPI lifespan rulează după ce loop-ul există, deci ok.)
        pass


async def memory_monitor(
    bot_name:           str,
    tg_alert:           Optional[Callable[[str, str], Awaitable[None]]] = None,
    interval_sec:       Optional[int] = None,
    rss_alert_mb:       Optional[float] = None,
) -> None:
    """
    Background task — log periodic memory + diagnostic la spike + pre-OOM alert.

    Args:
      bot_name:     pt prefix in Telegram alert (identificare bot).
      tg_alert:     `tg.send_critical` sau None (skip Telegram alerts).
      interval_sec: override MEM_MON_INTERVAL_SEC env (default 300).
      rss_alert_mb: override MEM_MON_RSS_ALERT_MB env (default 320).

    Comportament:
      - La fiecare interval: log RSS/VMS/threads/fds/gc_obj.
      - La PRIMUL sample cu RSS > rss_alert_mb:
        - Dump top 20 obiecte Python (gc.get_objects() + Counter).
        - Dump /proc/<pid>/status (Linux only).
        - Trimite Telegram CRITICAL (pre-OOM warning).
        - Set flag local "snapshot_taken" — nu mai trigger din nou
          (one-shot per boot, fara spam).

    Distinctie diagnostic (`gc_obj` count):
      - gc_obj creste monoton + RSS creste = leak Python-level
        (referinte care nu se elibereaza — circular refs, caches uitate).
      - RSS creste dar gc_obj stabil = leak C-extension (numpy/pandas
        buffers, httpx connection pool, websockets sockets).

    Safe fail: psutil import fail / sample fail / /proc absent (Mac/Windows)
    nu opresc task-ul — logam si continuam.
    """
    interval = interval_sec if interval_sec is not None else _env_int(
        "MEM_MON_INTERVAL_SEC", 300)
    alert_mb = (rss_alert_mb if rss_alert_mb is not None
                else float(os.getenv("MEM_MON_RSS_ALERT_MB", "320")))

    try:
        import psutil
        proc = psutil.Process()
    except Exception as e:
        print(f"  [MEM_MON] psutil unavailable ({e}) — task disabled",
              flush=True)
        return

    print(f"  [MEM_MON] started: interval={interval}s  "
          f"alert_threshold={alert_mb:.0f}MB", flush=True)

    snapshot_taken = False

    while True:
        try:
            import gc
            mi = proc.memory_info()
            rss_mb = mi.rss / 1024 / 1024
            vms_mb = mi.vms / 1024 / 1024
            n_threads = proc.num_threads()
            n_fds = proc.num_fds() if hasattr(proc, "num_fds") else -1
            n_gc_obj = len(gc.get_objects())
            print(
                f"  [MEM_MON] RSS={rss_mb:.1f}MB  VMS={vms_mb:.1f}MB  "
                f"threads={n_threads}  fds={n_fds}  gc_obj={n_gc_obj}",
                flush=True,
            )

            if rss_mb > alert_mb and not snapshot_taken:
                snapshot_taken = True
                print(
                    f"  [MEM_MON] ⚠️  RSS SPIKE {rss_mb:.1f}MB > {alert_mb:.0f}MB"
                    f" — one-shot diagnostic dump",
                    flush=True,
                )
                # Top 20 types — vedem dintr-o privire daca 10000× Trade
                # sau 50000× dict ccxt.
                top20_text = "(unavailable)"
                try:
                    from collections import Counter
                    type_counts = Counter(type(o).__name__
                                          for o in gc.get_objects())
                    top20 = type_counts.most_common(20)
                    print(f"  [MEM_MON] Top 20 types by count:", flush=True)
                    for tname, cnt in top20:
                        print(f"  [MEM_MON]   {cnt:>8d}  {tname}", flush=True)
                    top20_text = "\n".join(f"{cnt:>8d}  {t}"
                                           for t, cnt in top20[:10])
                except Exception as e:
                    print(f"  [MEM_MON] type_counts failed: {e}", flush=True)

                # /proc/<pid>/status (Linux). Tacem pe Mac/Windows.
                proc_status_text = ""
                try:
                    with open(f"/proc/{os.getpid()}/status") as f:
                        proc_status_text = f.read()
                    print(f"  [MEM_MON] /proc/status:\n{proc_status_text}",
                          flush=True)
                except Exception as e:
                    print(f"  [MEM_MON] /proc/status unavailable: {e}",
                          flush=True)

                # Pre-OOM Telegram alert. CRITICAL — user-ul are timp sa
                # investigheze inainte sa fie killed (excepthook NU se
                # invoca la SIGKILL, deci asta e singura fereastra).
                if tg_alert is not None:
                    try:
                        await tg_alert(
                            f"⚠️ {bot_name} — HIGH MEMORY USAGE",
                            f"<b>RSS:</b> {rss_mb:.1f} MB "
                            f"(threshold {alert_mb:.0f} MB)\n"
                            f"<b>VMS:</b> {vms_mb:.1f} MB\n"
                            f"<b>Threads:</b> {n_threads}  "
                            f"<b>FDs:</b> {n_fds}  "
                            f"<b>gc_obj:</b> {n_gc_obj}\n"
                            f"\nApropiere de mem_limit Docker — risc OOM kill.\n"
                            f"Diagnostic top types (in loguri):\n"
                            f"<pre>{top20_text}</pre>"
                        )
                    except Exception as e:
                        print(f"  [MEM_MON] tg_alert failed: {e}",
                              flush=True)
        except Exception as e:
            print(f"  [MEM_MON] sample failed: {e}", flush=True)

        await asyncio.sleep(interval)
