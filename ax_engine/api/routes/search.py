"""
AX Engine — Search API Routes

POST /api/v1/search      — Submit a new intelligence search job
GET  /api/v1/search/sync — Synchronous search (shallow depth only, <30s)
"""
from __future__ import annotations

import uuid
from typing import Annotated

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Request, status
from fastapi.responses import ORJSONResponse

from ax_engine.api.models.requests import SearchDepth, SearchRequest
from ax_engine.api.models.responses import JobStatus, SearchJobResponse
from ax_engine.config import settings
from ax_engine.infrastructure.rate_limiter import check_rate_limit
from ax_engine.workers.tasks import run_search_pipeline

logger = structlog.get_logger(__name__)
router = APIRouter()


async def get_api_key(
    x_ax_api_key: Annotated[str | None, Header(alias="X-AX-API-Key")] = None,
) -> str:
    """
    API key authentication dependency.
    In production, validate against database of active subscriptions.
    """
    if not x_ax_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key. Pass X-AX-API-Key header.",
        )
    # TODO: Replace with actual DB lookup + subscription tier check
    if x_ax_api_key == "dev_key_bypass" and settings.DEBUG:
        return x_ax_api_key
    if len(x_ax_api_key) < 20:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key.",
        )
    return x_ax_api_key


@router.post(
    "/search",
    response_model=SearchJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Submit a decision-maker intelligence search",
    description="""
    Submits an async search job. Returns a job_id to poll.

    **Depth modes:**
    - `shallow`: Business names + basic info only (~5s/lead)
    - `standard`: + website crawl + contact extraction (~15s/lead)
    - `deep`: + enrichment + opportunity signals + scoring (~45s/lead)

    **Rate limits:** 60 requests/minute per API key.
    """,
)
async def submit_search(
    request: Request,
    payload: SearchRequest,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(get_api_key),
) -> SearchJobResponse:
    # Rate limit check
    await check_rate_limit(api_key, settings.RATE_LIMIT_RPM)

    job_id = str(uuid.uuid4())

    log = logger.bind(
        job_id=job_id,
        niche=payload.niche,
        location=payload.location,
        depth=payload.depth,
        max_results=payload.max_results,
    )
    log.info("search_job.submitted")

    # Estimate completion time based on depth × results
    time_per_lead = {"shallow": 5, "standard": 15, "deep": 45}
    estimated_seconds = time_per_lead[payload.depth] * payload.max_results

    # Enqueue Celery task
    task = run_search_pipeline.apply_async(
        args=[job_id, payload.model_dump(), api_key],
        task_id=job_id,
        queue="discovery",
        countdown=0,
    )

    log.info("search_job.enqueued", task_id=task.id)

    return SearchJobResponse(
        job_id=job_id,
        status=JobStatus.PENDING,
        message=f"Job queued. Searching for '{payload.niche}' in '{payload.location}'.",
        estimated_completion_seconds=estimated_seconds,
        poll_url=f"/api/v1/jobs/{job_id}",
        webhook_url=payload.webhook_url,
    )


@router.post(
    "/search/sync",
    status_code=status.HTTP_200_OK,
    summary="Synchronous shallow search (max 10 results, 30s timeout)",
    description="Blocks until results are ready. Limited to shallow depth + 10 results for SLA guarantees.",
)
async def sync_search(
    request: Request,
    payload: SearchRequest,
    api_key: str = Depends(get_api_key),
) -> ORJSONResponse:
    await check_rate_limit(api_key, settings.RATE_LIMIT_RPM // 4)  # Stricter limit for sync

    if payload.depth not in (SearchDepth.SHALLOW, SearchDepth.STANDARD):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Sync endpoint only supports shallow/standard depth. Use async /search for deep.",
        )
    if payload.max_results > 10:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Sync endpoint limited to 10 results. Use async /search for more.",
        )

    from ax_engine.core.orchestrator import SearchOrchestrator

    orchestrator = SearchOrchestrator(nlp=request.app.state.nlp)
    results = await orchestrator.run(payload)

    return ORJSONResponse(content={"results": [r.model_dump() for r in results]})
