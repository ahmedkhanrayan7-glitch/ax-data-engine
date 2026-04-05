# ─────────────────────────────────────────────────────────────────
# AX Decision Intelligence Engine — Multi-stage Dockerfile
# ─────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libpq-dev curl libxml2-dev libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# Download spaCy models
RUN python -m spacy download en_core_web_lg && \
    python -m spacy download xx_ent_wiki_sm

# Install Playwright browsers
RUN playwright install chromium --with-deps

COPY . .

# ── API target ─────────────────────────────────────────────────────
FROM base AS api
EXPOSE 8000
CMD ["uvicorn", "ax_engine.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4", "--loop", "uvloop"]

# ── Worker target ──────────────────────────────────────────────────
FROM base AS worker
CMD ["celery", "-A", "ax_engine.workers.celery_app", "worker", "--loglevel=info"]
