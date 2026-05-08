"""Database session factories for relational state.

Two factories are exposed:

- :func:`get_async_engine` / :func:`get_async_session` — used by the FastAPI
  request lifecycle and the Kafka consumers (asyncpg under the hood).
- :func:`get_sync_engine` / :func:`get_sync_session` — used by the demo seeder,
  Alembic migrations, and the Celery workers (psycopg2 under the hood).

Both share the same Postgres database; the URLs are sourced from settings
(``database_url`` for async, ``database_url_sync`` for sync).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from functools import lru_cache

from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker

from config.settings import get_settings


@lru_cache(maxsize=1)
def get_async_engine() -> AsyncEngine:
    settings = get_settings()
    return create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
        future=True,
    )


@lru_cache(maxsize=1)
def get_sync_engine() -> Engine:
    settings = get_settings()
    return create_engine(
        settings.database_url_sync,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
        future=True,
    )


@lru_cache(maxsize=1)
def _async_session_factory() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(get_async_engine(), expire_on_commit=False)


@lru_cache(maxsize=1)
def _sync_session_factory() -> sessionmaker[Session]:
    return sessionmaker(get_sync_engine(), expire_on_commit=False, future=True)


@asynccontextmanager
async def get_async_session() -> AsyncIterator[AsyncSession]:
    async with _async_session_factory()() as session:
        yield session


@contextmanager
def get_sync_session() -> Iterator[Session]:
    with _sync_session_factory()() as session:
        yield session


__all__ = [
    "get_async_engine",
    "get_sync_engine",
    "get_async_session",
    "get_sync_session",
]
