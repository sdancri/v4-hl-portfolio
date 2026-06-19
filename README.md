# V4_HL Portfolio Bot — Hyperliquid

Variant Hyperliquid al [V4 Portfolio Bot](https://github.com/sdancri/v4-portfolio) (Bybit). Acelasi framework multi-strategie (Bollinger Bands Mean Reversion + Hull MA + Ichimoku Cloud), portat la HL via adaptorul `core/exchange_api.py` (model: [BP_HyperLiquid](https://github.com/sdancri/bp_hyperliquid)).

## Strategie

| Pereche | Strategie | Logica |
|---|---|---|
| **BTC** | `bb_mr` | Bollinger Bands Mean Reversion (cross-back + RSI extrem) |
| **TIA** | `hi` | Hull MA + Ichimoku Cloud (trend follow) |
| **NEAR** | `hi` | Hull MA + Ichimoku Cloud (trend follow) |

## Diferente fata de V4 Bybit

| Aspect | V4 Bybit | V4_HL |
|---|---|---|
| **Auth** | API key + secret HMAC | Wallet EIP-712 (`HL_MAIN_ADDRESS` + `HL_AGENT_PRIVATE_KEY`) |
| **Simbol** | `BTCUSDT` | `BTC` |
| **TP** | atomic Bybit Market | atomic HL trigger order (`tpsl="tp"`, reduce-only) |
| **SL** | atomic Bybit Market | atomic HL trigger order (`tpsl="sl"`, reduce-only) |
| **Klines** | REST + WS dual stream | WS multiplexat (candle + orderUpdates + userEvents) |
| **Auth expira** | nu | **agent key EXPIRA la 180 zile** → monitor |
| **Taker fee** | 0.055% | 0.045% |

## Setup HL (one-time)

### 1. Genereaza agent key

```bash
python3 scripts/gen_agent.py
```

Pas-cu-pas: HL UI → Settings → API → **Generate** agent wallet → aprobi cu Rabby/MetaMask → copiezi private key-ul agent (apare o data!).

### 2. Setup .env

```bash
cp .env.example .env
```

Editeaza:
```env
HL_MAIN_ADDRESS=0xRabby_address_cu_USDC_pe_HL
HL_AGENT_PRIVATE_KEY=cheia_agent_din_pasul_1
```

### 3. Test connectivity (opt)

```bash
python3 scripts/test_connectivity.py
```

Verifica: balance USDC, kline fetch BTC, signing test, WS connection.

## Deploy pe VPS via Portainer

Identic cu V4 Bybit, doar imagine si compose differ:

```bash
# One-time setup pe VPS:
mkdir -p /srv/bots/V4_HL/{logs,data} /srv/bots/dashboard
docker network create bots  # daca nu exista deja
```

Apoi in Portainer Stack:
1. Add stack → Web editor → paste [compose.V4_HL.yml](compose.V4_HL.yml)
2. Env vars: `HL_MAIN_ADDRESS`, `HL_AGENT_PRIVATE_KEY`, `TELEGRAM_*`, `RESET_TOKEN`, `BOT_VERSION` (opt)
3. Deploy → Portainer pulleaza `sdancri/v4_hl_portfolio:latest`
4. Chart: `http://<vps>:8204/`

## Setari config

`config/config_v4_hl.yaml` — identic cu V4 Bybit dar:
- `taker_fee: 0.00045` (HL vs Bybit 0.00055)
- `leverage: 5` / `leverage_max: 5` (mai conservativ vs HL native max 50x)
- Symboluri fara `USDT` suffix

## Caveats si TODO

### Limitari curente (vs V4 Bybit)

- ✅ **TP atomic** (V4_HL extinde BP-HL): `set_position_sl(sl_price, tp_price)` plaseaza AMBELE trigger orders intr-o singura tranzactie semnata (SL `tpsl="sl"` + TP `tpsl="tp"`, ambele reduce-only). Paritate completa cu V4 Bybit `setTradingStop`. Cost: +1 ordin in payload, zero call API extra.
- ⚠️ **Agent expiration**: cheia agent HL expira la **180 zile**. Bot-ul **nu monitorizeaza** asta automat momentan. Foloseste `ex.fetch_agent_expiration_ms()` periodic — recomandare TODO: alerta Telegram cu 14 zile inainte.
- ⚠️ **HL leverage UI**: la primul boot, V4_HL apeleaza `set_leverage(symbol, 5)` per pereche. Verifica in HL UI ca s-a aplicat. Daca nu, seteaza manual in HL Settings.

### Test status

✓ **Structura cod compileaza** + smoke test (36/43 pass — restul sunt V4-Bybit specific test pe care le voi adapta separat).

⚠️ **NU TESTAT LIVE** pe HL inca. Recomandare puternica:
1. Start pe HL **testnet** (`HL_TESTNET=1` in env)
2. Verifica entry/SL/TP/close pe testnet
3. Cand totul e ok, switch la mainnet cu **$100 maxim** prima saptamana

### TODO de viitor

- [ ] Adapt `scripts/smoke_test.py` la V4_HL (in loc de V4 Bybit)
- [ ] Backtest harness (`scripts/backtest_v4_hl.py`) cu OHLCV HL
- [ ] Alerta Telegram pre-expirare agent key
- [ ] HL-specific reconcile pe userEvents (V4 reconcile e Bybit pattern)
- [ ] Live test si tuning fee math (TAKER_FEE_RATE)

## Imagine + repo

- **DockerHub:** `sdancri/v4_hl_portfolio:latest`
- **GitHub:** https://github.com/sdancri/v4-hl-portfolio
