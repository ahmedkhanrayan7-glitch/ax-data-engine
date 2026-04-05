"""
AX Engine — Central Configuration
All settings sourced from environment variables with sane defaults.
"""
from __future__ import annotations

from functools import lru_cache
from typing import List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # ── App ──────────────────────────────────────────────────────
    APP_NAME: str = "AX Decision Intelligence Engine"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    LOG_LEVEL: str = "INFO"
    ENVIRONMENT: str = "production"

    # ── API ──────────────────────────────────────────────────────
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_WORKERS: int = 4
    API_KEY_HEADER: str = "X-AX-API-Key"

    # Rate limiting: requests per minute per API key
    RATE_LIMIT_RPM: int = 60
    RATE_LIMIT_BURST: int = 10

    # ── Database ─────────────────────────────────────────────────
    DATABASE_URL: str = "postgresql+asyncpg://ax_user:ax_pass@localhost:5432/ax_db"
    DATABASE_POOL_SIZE: int = 20
    DATABASE_MAX_OVERFLOW: int = 40
    DATABASE_POOL_TIMEOUT: int = 30

    # ── Redis / Cache ────────────────────────────────────────────
    REDIS_URL: str = "redis://localhost:6379/0"
    CACHE_TTL_SECONDS: int = 3600         # 1 hour for search results
    JOB_RESULT_TTL: int = 86400           # 24 hours for job results

    # ── Celery ───────────────────────────────────────────────────
    CELERY_BROKER_URL: str = "redis://localhost:6379/1"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/2"
    CELERY_TASK_SOFT_TIME_LIMIT: int = 300    # 5 min soft limit
    CELERY_TASK_TIME_LIMIT: int = 600         # 10 min hard limit
    CELERY_MAX_RETRIES: int = 3

    # ── Scraping ─────────────────────────────────────────────────
    # Playwright concurrency limits
    MAX_BROWSER_CONTEXTS: int = 10
    MAX_PAGES_PER_CONTEXT: int = 5
    PAGE_TIMEOUT_MS: int = 30_000
    NAVIGATION_TIMEOUT_MS: int = 60_000

    # Respectful scraping — delays in seconds
    MIN_REQUEST_DELAY: float = 1.0
    MAX_REQUEST_DELAY: float = 4.0
    CRAWL_POLITENESS_DELAY: float = 2.0

    # ── Proxy Pool ───────────────────────────────────────────────
    PROXY_POOL_URL: Optional[str] = None          # External proxy pool API
    PROXY_ROTATION_STRATEGY: str = "round_robin"   # round_robin | random | sticky
    PROXY_MAX_FAILURES: int = 3                   # Ban proxy after N consecutive fails
    PROXY_BAN_DURATION: int = 600                 # Seconds before unbanning

    # Static proxy list fallback (comma-separated)
    PROXY_LIST: str = ""

    @property
    def proxy_list_parsed(self) -> List[str]:
        return [p.strip() for p in self.PROXY_LIST.split(",") if p.strip()]

    # ── CAPTCHA ──────────────────────────────────────────────────
    CAPTCHA_SERVICE: str = "2captcha"             # 2captcha | anticaptcha | capsolver
    CAPTCHA_API_KEY: Optional[str] = None
    CAPTCHA_TIMEOUT: int = 120

    # ── NLP ──────────────────────────────────────────────────────
    SPACY_MODEL_EN: str = "en_core_web_lg"
    SPACY_MODEL_MULTILINGUAL: str = "xx_ent_wiki_sm"
    NLP_CONFIDENCE_THRESHOLD: float = 0.6

    # ── Email Validation ─────────────────────────────────────────
    SMTP_VERIFY_TIMEOUT: int = 10
    EMAIL_VERIFY_CATCH_ALL_SCORE: int = 50       # Score for catch-all domains
    MX_CACHE_TTL: int = 3600

    # ── Enrichment ───────────────────────────────────────────────
    BUILTWITH_API_KEY: Optional[str] = None       # Tech stack detection
    CLEARBIT_API_KEY: Optional[str] = None        # Company enrichment
    HUNTER_API_KEY: Optional[str] = None          # Email hunter

    # ── Scoring Weights ──────────────────────────────────────────
    SCORE_WEIGHT_DATA_COMPLETENESS: float = 0.25
    SCORE_WEIGHT_CONTACT_AVAILABILITY: float = 0.30
    SCORE_WEIGHT_OPPORTUNITY_SIGNALS: float = 0.30
    SCORE_WEIGHT_DECISION_MAKER_CONFIDENCE: float = 0.15

    # ── Monitoring ───────────────────────────────────────────────
    SENTRY_DSN: Optional[str] = None
    PROMETHEUS_ENABLED: bool = True
    METRICS_PATH: str = "/metrics"

    # ── Job Limits ───────────────────────────────────────────────
    MAX_BUSINESSES_PER_QUERY: int = 100
    MAX_CONCURRENT_JOBS: int = 50

    @field_validator("LOG_LEVEL")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid:
            raise ValueError(f"LOG_LEVEL must be one of {valid}")
        return v.upper()


@lru_cache
def get_settings() -> Settings:
    """Cached settings singleton — avoids re-reading env on every call."""
    return Settings()


settings = get_settings()
