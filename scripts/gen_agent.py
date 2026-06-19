#!/usr/bin/env python3
"""
Genereaza un API agent wallet pentru Hyperliquid.

RULEAZA OFFLINE / IN TERMINAL LOCAL (NU pe VPS, NU in chat).
Output-ul contine CHEIA PRIVATA — TREBUIE sa ramana doar pe masina ta + .env.

Cum se foloseste:
  cd /home/dan/Python/BOILERPLATE/BP_HyperLiquid
  pip install eth-account
  python3 scripts/gen_agent.py

Apoi:
  1. Salveaza output-ul in .env (HL_AGENT_PRIVATE_KEY=0x...)
  2. Du-te pe https://app.hyperliquid.xyz/API
  3. Click "Authorize API Wallet" (NU "Generate" — vrem sa folosim adresa
     pe care AM generat-o noi local, nu o cheie creata pe site-ul HL).
  4. Lipeste "Agent Address" tiparit mai jos in field-ul "API Wallet Address".
  5. Pune un nume (ex: "my-hl-bot") in "Name".
  6. Semneaza cu Rabby (costa 1 USDC din contul HL).
  7. Sterge fereastra terminalului (sau cel putin scroll-back) ca sa nu
     ramana cheia privata in history.

Securitate:
  - Cheia agent semneaza DOAR ordere si gestioneaza pozitii in numele
    wallet-ului tau principal Rabby.
  - NU poate retrage USDC, NU poate transfera fonduri off-platform.
  - Daca compromiti cheia, pierderea maxima = bot-ul deschide trade-uri
    proaste, dar fondurile raman in contul HL.
"""
import secrets
from eth_account import Account


def main() -> None:
    # 32 random bytes -> secp256k1 private key
    pk_bytes = secrets.token_bytes(32)
    acct = Account.from_key(pk_bytes)

    print()
    print("=" * 70)
    print("  HYPERLIQUID API AGENT — GENERATED")
    print("=" * 70)
    print()
    print(f"  Agent Address   : {acct.address}")
    print(f"  Private Key     : 0x{pk_bytes.hex()}")
    print()
    print("=" * 70)
    print("  NEXT STEPS")
    print("=" * 70)
    print()
    print("  1. Copy Agent Address ^^ (the 0x... public address).")
    print("  2. Go to https://app.hyperliquid.xyz/API")
    print("  3. Click 'Authorize API Wallet' → paste address + name it.")
    print("  4. Sign with Rabby (costs 1 USDC from your HL account).")
    print()
    print("  5. Copy Private Key ^^ into your .env (DO NOT commit it):")
    print(f"       HL_AGENT_PRIVATE_KEY=0x{pk_bytes.hex()}")
    print(f"       HL_AGENT_ADDRESS={acct.address}")
    print()
    print("  6. Also add your MAIN wallet (Rabby) public address:")
    print("       HL_MAIN_ADDRESS=0xYOUR_RABBY_ADDRESS_HERE")
    print()
    print("  7. Clear terminal scrollback to avoid leaving the key on disk:")
    print("       printf '\\033c'   # clears scrollback in most terminals")
    print()


if __name__ == "__main__":
    main()
