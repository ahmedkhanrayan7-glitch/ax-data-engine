"""
AX Engine — Job Status & Results Routes

GET  /api/v1/jobs/{job_id}          — Poll job status + results
GET  /api/v1/jobs/{job_id}/export   — Export results as CSV/XLSX
DELETE /api/v1/jobs/{job_id}        — Cancel a pending job
"""
from __future__ import annotations

import structlog
from fastapi import APIRouter, Depends, HTTPException, Path, Query, status
from fastapi.responses import StreamingResponse

from ax_engine.api.models.responses import JobResultResponse, JobStatus
from ax_engine.api.routes.search import get_api_key
from ax_engine.workers.celery_app import celery_app

logger = structlog.get_logger(__name__)
router = APIRouter()


@router.get(
    "/jobs/{job_id}",
    response_model=JobResultResponse,
    summary="Poll search job status and results",
)
async def get_job_status(
    job_id: str = Path(..., description="Job ID returned from POST /search"),
    include_results: bool = Query(default=True, description="Include lead results in response"),
    api_key: str = Depends(get_api_key),
) -> JobResultResponse:
    task = celery_app.AsyncResult(job_id)

    if task.state == "PENDING":
        return JobResultResponse(
            job_id=job_id,
            status=JobStatus.PENDING,
            progress=0,
        )

    if task.state == "STARTED":
        meta = task.info or {}
        return JobResultResponse(
            job_id=job_id,
            status=JobStatus.RUNNING,
            progress=meta.get("progress", 0),
            total_found=meta.get("total_found", 0),
            processed=meta.get("processed", 0),
        )

    if task.state == "SUCCESS":
        result = task.result or {}
        return JobResultResponse(
            job_id=job_id,
            status=JobStatus.COMPLETED,
            progress=100,
            total_found=result.get("total_found", 0),
            processed=result.get("processed", 0),
            results=result.get("results", []) if include_results else [],
            processing_time_seconds=result.get("processing_time_seconds"),
            metadata=result.get("metadata", {}),
        )

    if task.state == "FAILURE":
        return JobResultResponse(
            job_id=job_id,
            status=JobStatus.FAILED,
            errors=[str(task.info)],
        )

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"Job {job_id} not found or expired.",
    )


@router.get(
    "/jobs/{job_id}/export",
    summary="Export job results as CSV or XLSX",
)
async def export_job_results(
    job_id: str = Path(...),
    format: str = Query(default="csv", pattern="^(csv|xlsx)$"),
    api_key: str = Depends(get_api_key),
) -> StreamingResponse:
    task = celery_app.AsyncResult(job_id)

    if task.state != "SUCCESS":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Job is not completed yet. Current state: {task.state}",
        )

    results = task.result.get("results", [])

    if format == "csv":
        from ax_engine.utils.export import results_to_csv
        content = results_to_csv(results)
        media_type = "text/csv"
        filename = f"ax_leads_{job_id[:8]}.csv"
    else:
        from ax_engine.utils.export import results_to_xlsx
        content = results_to_xlsx(results)
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        filename = f"ax_leads_{job_id[:8]}.xlsx"

    return StreamingResponse(
        iter([content]),
        media_type=media_type,
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@router.delete(
    "/jobs/{job_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Cancel a pending or running job",
)
async def cancel_job(
    job_id: str = Path(...),
    api_key: str = Depends(get_api_key),
) -> None:
    celery_app.control.revoke(job_id, terminate=True)
    logger.info("job.cancelled", job_id=job_id, api_key=api_key[:8])
