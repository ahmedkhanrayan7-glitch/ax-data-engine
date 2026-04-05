"""
AX Decision Intelligence Engine — FastAPI Application Entry Point

Boot sequence:
  1. Initialize structured logging
  2. Connect to PostgreSQL pool
  3. Connect to Redis
  4. Load spaCy NLP models into memory
  5. Start Prometheus metrics
  6. Mount API routes
"""
from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import structlog
import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import ORJSONResponse
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

from ax_engine.api.routes import search, jobs, health
from ax_engine.config import settings
from ax_engine.database.connection import db_pool
from ax_engine.infrastructure.cache import cache_pool
from ax_engine.engines.decision_maker.nlp_engine import NLPEngine
from ax_engine.utils.logging import configure_logging

logger = structlog.get_logger(__name__)

# ── Prometheus metrics ────────────────────────────────────────────
REQUEST_COUNT = Counter(
    "ax_http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"],
)
REQUEST_LATENCY = Histogram(
    "ax_http_request_duration_seconds",
    "HTTP request latency",
    ["method", "endpoint"],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
)
SEARCH_JOBS_TOTAL = Counter(
    "ax_search_jobs_total",
    "Total search jobs submitted",
    ["niche", "status"],
)


# ── Application lifecycle ─────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator:
    configure_logging(settings.LOG_LEVEL)
    logger.info("ax_engine.starting", version=settings.APP_VERSION, env=settings.ENVIRONMENT)

    # Connect database pool
    await db_pool.connect()
    logger.info("database.connected")

    # Connect Redis
    await cache_pool.connect()
    logger.info("cache.connected")

    # Pre-load NLP models (expensive — do once at boot)
    nlp = NLPEngine()
    await nlp.initialize()
    app.state.nlp = nlp
    logger.info("nlp.models_loaded")

    logger.info("ax_engine.ready")
    yield

    # Graceful shutdown
    await db_pool.disconnect()
    await cache_pool.disconnect()
    logger.info("ax_engine.shutdown_complete")


# ── App factory ───────────────────────────────────────────────────
def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        description="Production-grade decision-maker intelligence extraction platform.",
        docs_url="/docs" if settings.DEBUG else None,
        redoc_url="/redoc" if settings.DEBUG else None,
        default_response_class=ORJSONResponse,
        lifespan=lifespan,
    )

    # ── Middleware ────────────────────────────────────────────────
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(GZipMiddleware, minimum_size=1000)

    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next) -> Response:
        start = time.perf_counter()
        response = await call_next(request)
        duration = time.perf_counter() - start

        endpoint = request.url.path
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=endpoint,
            status=response.status_code,
        ).inc()
        REQUEST_LATENCY.labels(
            method=request.method,
            endpoint=endpoint,
        ).observe(duration)

        response.headers["X-Response-Time"] = f"{duration:.4f}s"
        response.headers["X-AX-Version"] = settings.APP_VERSION
        return response

    # ── Routes ────────────────────────────────────────────────────
    app.include_router(health.router, tags=["Health"])
    app.include_router(search.router, prefix="/api/v1", tags=["Search"])
    app.include_router(jobs.router, prefix="/api/v1", tags=["Jobs"])

    # ── Prometheus metrics endpoint ───────────────────────────────
    if settings.PROMETHEUS_ENABLED:
        @app.get(settings.METRICS_PATH, include_in_schema=False)
        async def metrics():
            return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


app = create_app()


if __name__ == "__main__":
    uvicorn.run(
        "ax_engine.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        workers=settings.API_WORKERS,
        loop="uvloop",
        log_level=settings.LOG_LEVEL.lower(),
        reload=settings.DEBUG,
    )
