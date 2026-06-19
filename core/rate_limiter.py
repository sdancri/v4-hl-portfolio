"""
rate_limiter.py — Token bucket pentru REST requests Bybit V5
=============================================================
Bybit V5 limits:
  - Market data (public):     120 req/5s per IP  = 24 req/s
  - Order management:         10 req/s per UID
  - Position endpoints:       10 req/s per UID

Defaults conservative: 5 req/s, burst 10. Override via env:
    RATE_LIMIT_PER_SEC=10
    RATE_LIMIT_BURST=20
"""
from __future__ import annotations

import asyncio
import os
import time


class TokenBucket:
    def __init__(self, rate_per_sec: float, burst: int) -> None:
        self.rate = float(rate_per_sec)
        self.burst = int(burst)
        self.tokens: float = float(burst)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> float:
        async with self._lock:
            waited = 0.0
            while True:
                now = time.monotonic()
                elapsed = now - self._last
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self._last = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return waited

                need = 1.0 - self.tokens
                wait = need / self.rate
                waited += wait
                await asyncio.sleep(wait)


_bucket = TokenBucket(
    rate_per_sec=float(os.getenv("RATE_LIMIT_PER_SEC", "5")),
    burst=int(os.getenv("RATE_LIMIT_BURST", "10")),
)


async def wait_token() -> None:
    waited = await _bucket.acquire()
    if waited > 0.1:
        print(f"  [RATE] Throttled {waited*1000:.0f}ms")
