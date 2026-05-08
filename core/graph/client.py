"""Neo4j async driver wrapper.

Provides a single :class:`Neo4jClient` exposing helpers for parameterised
queries. The client owns one driver and one connection pool — it is created
during FastAPI startup and closed on shutdown.

All callers must pass parameters via the ``params`` dict; never interpolate
user-supplied values into Cypher strings (rule 1 in CLAUDE.md).
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from datetime import date, datetime, time
from typing import Any

from neo4j import AsyncDriver, AsyncGraphDatabase, AsyncSession
from neo4j.time import Date as Neo4jDate
from neo4j.time import DateTime as Neo4jDateTime
from neo4j.time import Duration as Neo4jDuration
from neo4j.time import Time as Neo4jTime

from config.logging import get_logger
from config.settings import Settings, get_settings

logger = get_logger(__name__)


class Neo4jClient:
    """Async wrapper around the Neo4j driver.

    Use as a singleton (see :func:`get_neo4j_client`). The driver internally
    pools connections, so concurrent calls are safe.
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or get_settings()
        self._driver: AsyncDriver | None = None

    # -- lifecycle --------------------------------------------------------

    async def connect(self) -> None:
        if self._driver is not None:
            return
        s = self._settings
        self._driver = AsyncGraphDatabase.driver(
            s.neo4j_uri,
            auth=(s.neo4j_user, s.neo4j_password.get_secret_value()),
            max_connection_pool_size=s.neo4j_max_pool_size,
            connection_timeout=s.neo4j_connection_timeout_s,
        )
        await self._driver.verify_connectivity()
        logger.info("neo4j.connected", uri=s.neo4j_uri)

    async def close(self) -> None:
        if self._driver is not None:
            await self._driver.close()
            self._driver = None
            logger.info("neo4j.closed")

    @property
    def driver(self) -> AsyncDriver:
        if self._driver is None:
            raise RuntimeError("Neo4jClient.connect() must be called before use")
        return self._driver

    # -- session helpers --------------------------------------------------

    @asynccontextmanager
    async def session(self, database: str | None = None) -> AsyncIterator[AsyncSession]:
        async with self.driver.session(database=database or self._settings.neo4j_database) as s:
            yield s

    # -- query helpers ----------------------------------------------------

    async def execute_read(
        self,
        cypher: str,
        params: Mapping[str, Any] | None = None,
        *,
        database: str | None = None,
    ) -> list[dict[str, Any]]:
        """Run a read-only query in a managed transaction. Returns list of records."""

        async with self.session(database=database) as session:
            result = await session.execute_read(_run_query, cypher, dict(params or {}))
        return result

    async def execute_write(
        self,
        cypher: str,
        params: Mapping[str, Any] | None = None,
        *,
        database: str | None = None,
    ) -> list[dict[str, Any]]:
        """Run a write query in a managed transaction. Returns list of records."""

        async with self.session(database=database) as session:
            result = await session.execute_write(_run_query, cypher, dict(params or {}))
        return result

    async def execute_many_write(
        self,
        statements: list[tuple[str, Mapping[str, Any]]],
        *,
        database: str | None = None,
    ) -> None:
        """Run a sequence of write statements inside a single transaction."""

        async with self.session(database=database) as session:

            async def _run(tx):  # type: ignore[no-untyped-def]
                for cypher, params in statements:
                    await tx.run(cypher, dict(params))

            await session.execute_write(_run)

    async def health(self) -> bool:
        """Return True if Neo4j is reachable."""

        try:
            await self.driver.verify_connectivity()
            return True
        except Exception:  # noqa: BLE001 — health check should never raise
            return False


def _coerce(value: Any) -> Any:
    """Convert neo4j temporal types to plain Python equivalents.

    The neo4j driver returns its own ``DateTime``/``Date``/``Time``/``Duration``
    types, which Pydantic (and orjson) cannot serialise out of the box when
    they appear inside untyped ``properties`` dicts on the graph payload.
    Recursively walking the result here keeps every route working without
    needing per-endpoint conversion.
    """

    if isinstance(value, Neo4jDateTime):
        return value.to_native()  # → datetime.datetime
    if isinstance(value, Neo4jDate):
        return value.to_native()  # → datetime.date
    if isinstance(value, Neo4jTime):
        return value.to_native()  # → datetime.time
    if isinstance(value, Neo4jDuration):
        return str(value)  # ISO-8601 duration string
    if isinstance(value, dict):
        return {k: _coerce(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_coerce(v) for v in value]
    if isinstance(value, (datetime, date, time, str, int, float, bool)) or value is None:
        return value
    return value


async def _run_query(tx, cypher: str, params: dict[str, Any]) -> list[dict[str, Any]]:  # type: ignore[no-untyped-def]
    cursor = await tx.run(cypher, params)
    return [_coerce(record.data()) async for record in cursor]


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_client: Neo4jClient | None = None


def get_neo4j_client() -> Neo4jClient:
    """Return the process-wide :class:`Neo4jClient`.

    Caller is responsible for invoking ``await client.connect()`` once during
    application startup. The FastAPI lifespan handler does this.
    """

    global _client
    if _client is None:
        _client = Neo4jClient()
    return _client
