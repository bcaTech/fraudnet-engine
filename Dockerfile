FROM python:3.11-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        libpq-dev \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first for better layer caching.
COPY pyproject.toml /app/
RUN pip install --upgrade pip setuptools wheel \
    && pip install -e .[dev]

COPY . /app

EXPOSE 8000

# No image-level HEALTHCHECK. The image is reused by the API,
# Celery worker/beat, and Kafka consumer services — they don't all
# listen on :8000. Healthchecks are defined per-service in
# docker-compose.yml so each gets the right probe (HTTP for the API,
# disabled for the workers).

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
