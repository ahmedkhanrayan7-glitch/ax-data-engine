"""
AX Engine — Celery Task Definitions

Tasks are thin wrappers around the orchestrator.
They handle:
  - Async → sync bridging (Celery is sync, orchestrator is async)
  - Progress reporting to Celery state
  - Error handling and retries
  - Webhook delivery on completion
"""
from __future__ import annotations

import asyncio
import time
from typing import Any, Dict

import httpx
import structlog
from celery import Task
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded

from ax_engine.workers.celery_app import celery_app

logger = structlog.get_logger(__name__)


class BaseTask(Task):
    """Base task with common retry logic."""

    abstract = True
    max_retries = 3
    default_retry_delay = 30

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        logger.error(
            "task.failed",
            task_id=task_id,
            task=self.name,
            error=str(exc),
        )

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        logger.warning(
            "task.retrying",
            task_id=task_id,
            task=self.name,
            error=str(exc),
        )


@celery_app.task(
    bind=True,
    base=BaseTask,
    name="ax_engine.workers.tasks.run_search_pipeline",
    queue="discovery",
)
def run_search_pipeline(
    self,
    job_id: str,
    request_data: Dict[str, Any],
    api_key: str,
) -> Dict[str, Any]:
    """
    Main pipeline task. Runs the full search orchestrator.
    Reports progress via Celery state updates.
    """
    start_time = time.time()
    log = logger.bind(job_id=job_id)
    log.info("task.pipeline_start")

    try:
        # Update state to STARTED
        self.update_state(
            state="STARTED",
            meta={"progress": 0, "total_found": 0, "processed": 0},
        )

        # Parse request
        from ax_engine.api.models.requests import SearchRequest
        request = SearchRequest(**request_data)

        # Progress callback to update Celery state
        def progress_callback(processed: int, total: int):
            pct = int((processed / total) * 100) if total else 0
            self.update_state(
                state="STARTED",
                meta={
                    "progress": pct,
                    "total_found": total,
                    "processed": processed,
                },
            )

        # Run async orchestrator in sync context
        results = _run_async(
            _execute_pipeline(request, progress_callback)
        )

        # Serialize results
        result_dicts = [r.model_dump() for r in results]

        elapsed = time.time() - start_time
        log.info(
            "task.pipeline_complete",
            results=len(results),
            elapsed=round(elapsed, 2),
        )

        output = {
            "results": result_dicts,
            "total_found": len(results),
            "processed": len(results),
            "processing_time_seconds": round(elapsed, 2),
            "metadata": {
                "niche": request.niche,
                "location": request.location,
                "depth": request.depth,
            },
        }

        # Deliver webhook if configured
        if request.webhook_url:
            _run_async(_deliver_webhook(request.webhook_url, job_id, output))

        return output

    except SoftTimeLimitExceeded:
        log.warning("task.soft_time_limit_exceeded")
        raise

    except Exception as exc:
        log.error("task.pipeline_error", error=str(exc), exc_info=True)
        try:
            raise self.retry(exc=exc, countdown=30)
        except MaxRetriesExceededError:
            raise


@celery_app.task(name="ax_engine.workers.tasks.cleanup_expired_jobs")
def cleanup_expired_jobs() -> None:
    """Periodic cleanup of expired job results from Redis."""
    logger.info("task.cleanup_start")
    # Celery's result backend handles TTL automatically via result_expires
    # This task is a placeholder for additional cleanup logic
    logger.info("task.cleanup_complete")


@celery_app.task(name="ax_engine.workers.tasks.health_ping")
def health_ping() -> dict:
    """Heartbeat task for monitoring."""
    return {"status": "ok", "timestamp": time.time()}


# ── Helpers ────────────────────────────────────────────────────────

def _run_async(coro) -> Any:
    """Run an async coroutine from a sync Celery task."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            raise RuntimeError
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(coro)


async def _execute_pipeline(request, progress_callback):
    """Initialize components and run the orchestrator."""
    from ax_engine.engines.decision_maker.nlp_engine import NLPEngine
    from ax_engine.core.orchestrator import SearchOrchestrator

    nlp = NLPEngine()
    await nlp.initialize()

    orchestrator = SearchOrchestrator(nlp=nlp, progress_callback=progress_callback)
    return await orchestrator.run(request)


async def _deliver_webhook(url: str, job_id: str, payload: dict) -> None:
    """POST results to customer webhook URL."""
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            await client.post(
                url,
                json={"job_id": job_id, "status": "completed", "data": payload},
                headers={"X-AX-Job-Id": job_id, "X-AX-Version": "1.0"},
            )
        logger.info("webhook.delivered", url=url, job_id=job_id)
    except Exception as e:
        logger.warning("webhook.failed", url=url, job_id=job_id, error=str(e))
