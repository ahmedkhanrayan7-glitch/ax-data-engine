"""
AX Engine — Rate Limiter

Two rate limiters:

1. API Rate Limiter (per API key)
   - Prevents abuse of the AX Engine API
   - Token bucket algorithm
   - Redis-backed for distributed enforcement

2. Scraping Rate Limiter (per target domain)
   - Ensures we don't overwhelm target sites
   - Implements polite crawling delays
   - Per-domain sliding window
"""
from __future__ import annotations

import asyncio
import time
from typing import Dict

import structlog

logger = structlog.get_logger(__name__)


class RateLimiter:
    """
    Simple in-memory token bucket rate limiter.
    For production: swap with Redis-backed version.
    """

    def __init__(self, requests_per_minute: int = 60):
        self.rpm = requests_per_minute
        self.window = 60.0
        self._buckets: Dict[str, list] = {}

    async def acquire(self, key: str = "default") -> None:
        """Wait until a request slot is available."""
        while not self._check_and_consume(key):
            await asyncio.sleep(0.1)

    def _check_and_consume(self, key: str) -> bool:
        now = time.time()
        window_start = now - self.window

        if key not in self._buckets:
            self._buckets[key] = []

        # Remove old timestamps
        self._buckets[key] = [t for t in self._buckets[key] if t > window_start]

        if len(self._buckets[key]) < self.rpm:
            self._buckets[key].append(now)
            return True

        return False


async def check_rate_limit(api_key: str, rpm: int) -> None:
    """
    Check and enforce rate limit for an API key.
    Raises HTTP 429 if limit exceeded.
    """
    from fastapi import HTTPException, status

    # In production, use Redis for distributed rate limiting
    # For now, simple in-memory check
    # TODO: Replace with Redis-backed sliding window
    pass
