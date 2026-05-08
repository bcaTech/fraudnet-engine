"""Structured logging configuration.

Uses :mod:`structlog` to emit JSON-encoded events in production and
human-readable output in development. Standard-library loggers are routed
through structlog so third-party libraries (FastAPI, SQLAlchemy, neo4j)
participate in the same pipeline.
"""

from __future__ import annotations

import logging
import sys

import structlog

from .settings import get_settings


def configure_logging() -> None:
    """Configure root logger and structlog. Call once at process start."""

    settings = get_settings()
    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    timestamper = structlog.processors.TimeStamper(fmt="iso", utc=True)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        timestamper,
    ]

    if settings.environment == "development":
        renderer: structlog.types.Processor = structlog.dev.ConsoleRenderer(colors=True)
    else:
        renderer = structlog.processors.JSONRenderer()

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(message)s")  # structlog produces the full record
    )

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Quiet down chatty libraries by default.
    for name in ("uvicorn.access", "neo4j.notifications"):
        logging.getLogger(name).setLevel(max(level, logging.WARNING))


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a structlog logger bound to ``name``."""

    return structlog.get_logger(name)
