"""Periodic dashboard-metrics publisher.

Runs as a single background task inside the API process. Every
``METRICS_INTERVAL_S`` seconds it queries the same Cypher used by
``/api/dashboard/metrics`` (plus a couple of Postgres rollups) and
publishes the snapshot to the WS metrics channel. The bridge fans it out
to every connected ``/ws/metrics`` client.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import func, select

from config.logging import get_logger
from core.graph.client import get_neo4j_client
from core.graph.queries import DASHBOARD_METRICS
from db.models import Alert, Takedown
from db.session import get_async_session

from .publisher import CH_METRICS, publish

logger = get_logger(__name__)


METRICS_INTERVAL_S: int = 5


async def _snapshot() -> dict[str, Any]:
    client = get_neo4j_client()
    rows = await client.execute_read(DASHBOARD_METRICS)
    if rows:
        r = rows[0]
        graph = {
            "active_clusters": int(r.get("active_clusters") or 0),
            "wallets_under_review": int(r.get("wallets_under_review") or 0),
            "high_risk_agents": int(r.get("high_risk_agents") or 0),
            "takedowns_completed": int(r.get("takedowns_completed") or 0),
            "estimated_fraud_value": float(r.get("estimated_fraud_value") or 0.0),
        }
    else:
        graph = {
            "active_clusters": 0,
            "wallets_under_review": 0,
            "high_risk_agents": 0,
            "takedowns_completed": 0,
            "estimated_fraud_value": 0.0,
        }

    async with get_async_session() as db:
        unack = (
            await db.execute(select(func.count(Alert.id)).where(Alert.acknowledged.is_(False)))
        ).scalar_one()
        critical_unack = (
            await db.execute(
                select(func.count(Alert.id)).where(
                    Alert.acknowledged.is_(False), Alert.severity == "critical"
                )
            )
        ).scalar_one()
        active_takedowns = (
            await db.execute(
                select(func.count(Takedown.id)).where(
                    Takedown.status.in_(("pending", "approved", "in_progress"))
                )
            )
        ).scalar_one()

    return {
        **graph,
        "unacknowledged_alerts": int(unack),
        "critical_unacknowledged_alerts": int(critical_unack),
        "active_takedowns": int(active_takedowns),
        "snapshot_at": datetime.now(UTC).isoformat(),
    }


class MetricsPublisher:
    def __init__(self, interval_s: int = METRICS_INTERVAL_S) -> None:
        self._interval_s = interval_s
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="ws.metrics_publisher")
        logger.info("ws.metrics.started", interval_s=self._interval_s)

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        logger.info("ws.metrics.stopped")

    async def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                payload = await _snapshot()
                await publish(CH_METRICS, "metrics.snapshot", payload)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — survive a transient DB hiccup
                logger.warning("ws.metrics.snapshot_error", error=str(exc))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self._interval_s)
            except TimeoutError:
                continue
            else:
                return  # stop requested
