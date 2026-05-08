"""FastAPI dependency providers.

Centralised so routes can ``Depends(get_neo4j)`` etc. without importing the
implementation. Tests can override these via ``app.dependency_overrides``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from core.graph.client import Neo4jClient, get_neo4j_client
from db.session import get_async_session


async def neo4j_dep() -> AsyncIterator[Neo4jClient]:
    yield get_neo4j_client()


async def db_session_dep() -> AsyncIterator[AsyncSession]:
    async with get_async_session() as session:
        yield session


Neo4jDep = Annotated[Neo4jClient, Depends(neo4j_dep)]
DBSessionDep = Annotated[AsyncSession, Depends(db_session_dep)]
