"""Health check endpoints."""
from __future__ import annotations

import time
from fastapi import APIRouter
from ax_engine.api.models.responses import HealthResponse
from ax_engine.config import settings

router = APIRouter()
_start_time = time.time()


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    from ax_engine.database.connection import db_pool
    from ax_engine.infrastructure.cache import cache_pool

    db_status = "ok" if await db_pool.is_connected() else "degraded"
    cache_status = "ok" if await cache_pool.is_connected() else "degraded"

    return HealthResponse(
        status="ok" if db_status == "ok" and cache_status == "ok" else "degraded",
        version=settings.APP_VERSION,
        uptime_seconds=time.time() - _start_time,
        database=db_status,
        cache=cache_status,
        workers={},
    )
