"""Redis cache pool wrapper."""
from __future__ import annotations

import redis.asyncio as aioredis
from ax_engine.config import settings


class CachePool:
    def __init__(self):
        self._client = None

    async def connect(self) -> None:
        self._client = aioredis.from_url(
            settings.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
        )

    async def disconnect(self) -> None:
        if self._client:
            await self._client.close()

    async def is_connected(self) -> bool:
        if not self._client:
            return False
        try:
            await self._client.ping()
            return True
        except Exception:
            return False

    async def get(self, key: str):
        if not self._client:
            return None
        return await self._client.get(key)

    async def set(self, key: str, value: str, ttl: int = None) -> None:
        if not self._client:
            return
        if ttl:
            await self._client.setex(key, ttl, value)
        else:
            await self._client.set(key, value)

    async def delete(self, key: str) -> None:
        if self._client:
            await self._client.delete(key)


cache_pool = CachePool()
