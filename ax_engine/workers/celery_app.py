"""
AX Engine — Celery Application

Queue architecture:
  - discovery  : Business discovery tasks (maps scraping, heavy I/O)
  - crawl      : Website crawling tasks (Playwright-heavy)
  - enrichment : Data enrichment tasks (API calls, NLP)
  - default    : Everything else

Routing is configured to separate high-resource tasks from fast tasks,
allowing independent scaling of worker pools.
"""
from __future__ import annotations

from celery import Celery
from celery.schedules import crontab
from kombu import Exchange, Queue

from ax_engine.config import settings

# Create Celery app
celery_app = Celery(
    "ax_engine",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=["ax_engine.workers.tasks"],
)

# ── Configuration ─────────────────────────────────────────────────
celery_app.conf.update(
    # Serialization
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # Timeouts
    task_soft_time_limit=settings.CELERY_TASK_SOFT_TIME_LIMIT,
    task_time_limit=settings.CELERY_TASK_TIME_LIMIT,

    # Retry
    task_max_retries=settings.CELERY_MAX_RETRIES,
    task_acks_late=True,
    task_reject_on_worker_lost=True,

    # Result backend
    result_expires=settings.JOB_RESULT_TTL,
    result_backend_transport_options={
        "retry_policy": {"timeout": 5.0},
    },

    # Worker
    worker_prefetch_multiplier=1,  # Fair distribution, no task hoarding
    worker_max_tasks_per_child=100,  # Restart workers after 100 tasks (memory leak prevention)

    # Monitoring
    task_send_sent_event=True,
    worker_send_task_events=True,
)

# ── Queue definitions ─────────────────────────────────────────────
default_exchange = Exchange("ax_default", type="direct")
discovery_exchange = Exchange("ax_discovery", type="direct")
crawl_exchange = Exchange("ax_crawl", type="direct")
enrichment_exchange = Exchange("ax_enrichment", type="direct")

celery_app.conf.task_queues = (
    Queue("default", default_exchange, routing_key="default"),
    Queue("discovery", discovery_exchange, routing_key="discovery"),
    Queue("crawl", crawl_exchange, routing_key="crawl"),
    Queue("enrichment", enrichment_exchange, routing_key="enrichment"),
)

celery_app.conf.task_default_queue = "default"
celery_app.conf.task_default_exchange = "ax_default"
celery_app.conf.task_default_routing_key = "default"

# ── Task routing ──────────────────────────────────────────────────
celery_app.conf.task_routes = {
    "ax_engine.workers.tasks.run_search_pipeline": {"queue": "discovery"},
    "ax_engine.workers.tasks.crawl_website": {"queue": "crawl"},
    "ax_engine.workers.tasks.enrich_company": {"queue": "enrichment"},
    "ax_engine.workers.tasks.cleanup_expired_jobs": {"queue": "default"},
}

# ── Scheduled tasks (Celery Beat) ─────────────────────────────────
celery_app.conf.beat_schedule = {
    # Clean up expired job results every hour
    "cleanup-expired-jobs": {
        "task": "ax_engine.workers.tasks.cleanup_expired_jobs",
        "schedule": crontab(minute=0),  # Every hour
    },
    # Health check ping every 5 minutes
    "health-ping": {
        "task": "ax_engine.workers.tasks.health_ping",
        "schedule": 300,  # Every 5 minutes
    },
}
