#!/usr/bin/env python3
"""
Connectivity test pt Hyperliquid — Phase A.

Citeste din .env si valideaza:
  1. Agent PK -> adresa derivata match cu ce-ai aprobat pe HL UI
  2. Agent valabil (extraAgents endpoint) + zile pana la expirare
  3. Balanta USDC (read main wallet)
  4. Candles ETH 30m (info `candleSnapshot`)
  5. Pozitia curenta (if any)

NU plaseaza ordere. NU semneaza nimic catre exchange endpoint. 100%
read-only — sigur sa rulezi pe mainnet.

Run:
  cd /home/dan/Python/BOILERPLATE/BP_HyperLiquid
  pip install --break-system-packages -r requirements.txt
  python3 scripts/test_connectivity.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import time
from pathlib import Path


def _load_dotenv(path: Path) -> None:
    """Lightweight .env loader (no extra dep). Strips inline `# comment`."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        # Strip inline comment (anything after unquoted #)
        if "#" in v:
            v = v.split("#", 1)[0]
        v = v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)


# Load .env BEFORE importing exchange module (it reads env at import time)
_HERE = Path(__file__).resolve().parent.parent
_load_dotenv(_HERE / ".env")
sys.path.insert(0, str(_HERE))

from core import exchange_hyperliquid as ex   # noqa: E402


def _hr(s: str = "") -> None:
    print(f"  {s}")
    print("  " + "─" * 68)


async def main() -> int:
    print()
    print("=" * 70)
    print("  HYPERLIQUID CONNECTIVITY TEST")
    print("=" * 70)

    # ----- 1) Agent PK -> address sanity --------------------------------
    _hr("1) AGENT KEY")
    if not ex.HL_AGENT_ADDRESS:
        print("  ✗ HL_AGENT_PRIVATE_KEY missing or invalid in .env")
        return 1
    print(f"  Agent PK -> address: {ex.HL_AGENT_ADDRESS}")
    if ex.HL_AGENT_ADDRESS_EXPECTED:
        if ex.HL_AGENT_ADDRESS_EXPECTED == ex.HL_AGENT_ADDRESS:
            print(f"  ✓ matches HL_AGENT_ADDRESS in .env")
        else:
            print(f"  ✗ MISMATCH vs .env HL_AGENT_ADDRESS={ex.HL_AGENT_ADDRESS_EXPECTED}")
            return 1
    else:
        print("  (HL_AGENT_ADDRESS not set in .env — skip cross-check)")

    if not ex.HL_MAIN_ADDRESS:
        print("  ✗ HL_MAIN_ADDRESS missing in .env")
        return 1
    print(f"  Main wallet:         {ex.HL_MAIN_ADDRESS}")

    # ----- 2) Agent valid + expiration ---------------------------------
    _hr("2) AGENT APPROVAL ON HL")
    valid_until_ms = await ex.fetch_agent_expiration_ms()
    if valid_until_ms is None:
        print("  ✗ Agent NOT found in main wallet's extraAgents.")
        print("    Posible: (a) nu ai aprobat-o pe HL UI, (b) ai aprobat alta")
        print("    cheie, (c) endpoint extraAgents schimbat. Verifica pe")
        print("    https://app.hyperliquid.xyz/API")
        return 1
    now_ms = int(time.time() * 1000)
    days_left = (valid_until_ms - now_ms) / 86_400_000.0
    print(f"  validUntil:          {valid_until_ms}  "
          f"({_fmt_ms(valid_until_ms)})")
    if days_left < 0:
        print(f"  ✗ EXPIRED {abs(days_left):.1f} days ago — re-genereaza si re-aproba.")
        return 1
    elif days_left < 14:
        print(f"  ⚠  EXPIRES in {days_left:.1f} days (re-genereaza curand)")
    else:
        print(f"  ✓ EXPIRES in {days_left:.1f} days")

    # ----- 3) Balance USDC ---------------------------------------------
    _hr("3) USDC BALANCE")
    try:
        bal_total = await ex.get_balance_usdc()         # perp + spot
        bal_spot = await ex.get_balance_spot_usdc()
        bal_perp_only = bal_total - bal_spot
        print(f"  Perp account:    ${bal_perp_only:,.2f} USDC  "
              f"(collateral lockuit + uPnL)")
        print(f"  Spot account:    ${bal_spot:,.2f} USDC  "
              f"(free cash; auto-colateral pe Unified)")
        print(f"  Total tradabil:  ${bal_total:,.2f} USDC  ← folosit de bot")
        if bal_total < 11:
            print(f"  ⚠  Sub $11 — HL minim notional pe ordine e ~$10.")
        else:
            print(f"  ✓ Suficient pt trading (min $10 notional pe ordine).")
    except Exception as e:
        print(f"  ✗ FAILED: {e!r}")
        return 1

    # ----- 4) Candles ETH 30m ------------------------------------------
    _hr("4) CANDLES ETH 30m")
    try:
        kl = await ex.get_kline("ETH", "30m", limit=5)
        print(f"  ✓ fetched {len(kl)} candles (newest first)")
        for i, row in enumerate(kl[:3]):
            ts_ms = int(row[0])
            print(f"     [{i}] ts={_fmt_ms(ts_ms)}  "
                  f"o={row[1]} h={row[2]} l={row[3]} c={row[4]} v={row[5]}")
    except Exception as e:
        print(f"  ✗ FAILED: {e!r}")
        return 1

    # ----- 5) Current position -----------------------------------------
    _hr("5) CURRENT ETH POSITION")
    try:
        pos = await ex.fetch_open_position("ETH")
        if pos is None:
            print("  ✓ no open position (flat)")
        else:
            print(f"  ✓ {pos['direction']}  qty={pos['qty']}  "
                  f"entry={pos['entry_price']}")
            print(f"     sl_price={pos['sl_price']}  tp_price={pos['tp_price']}  "
                  f"(None pana la Phase B)")
    except Exception as e:
        print(f"  ✗ FAILED: {e!r}")
        return 1

    _hr()
    print("  ALL PHASE A CHECKS PASSED ✓")
    print()
    await ex.close_client()
    return 0


def _fmt_ms(ms: int) -> str:
    import datetime as dt
    return dt.datetime.fromtimestamp(ms / 1000, tz=dt.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
