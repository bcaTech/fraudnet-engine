"""Reporting tasks (analytics snapshots, evidence package generation).

The evidence-package task delegates to :mod:`core.evidence.builder`.
The analytics-snapshot task currently logs the headline KPIs; once the
lakehouse is live it will write a daily Iceberg snapshot.
"""

from __future__ import annotations

from collections.abc import Coroutine
from typing import Any, TypeVar

from config.logging import configure_logging, get_logger

from .celery_app import app

configure_logging()
logger = get_logger(__name__)

T = TypeVar("T")


def _run_async(coro: Coroutine[Any, Any, T]) -> T:
    """Shared sync→async bridge. Delegates to the canonical helper in
    :mod:`tasks.periodic`."""

    from .periodic import _run_async as _shared_run_async

    return _shared_run_async(coro)


@app.task(name="tasks.report_tasks.generate_analytics_snapshot")
def generate_analytics_snapshot() -> dict[str, Any]:
    """Compute and log a daily KPI snapshot. Production version writes
    to ``features_snapshots`` Iceberg; for now we just record the
    numbers so they land in the worker log + Celery result backend."""

    async def _go() -> dict[str, Any]:
        from core.graph.client import get_neo4j_client
        from core.graph.queries import DASHBOARD_METRICS

        client = get_neo4j_client()
        try:
            if client._driver is None:
                await client.connect()
        except AttributeError:
            await client.connect()
        rows = await client.execute_read(DASHBOARD_METRICS)
        return dict(rows[0]) if rows else {}

    try:
        result = _run_async(_go())
        snapshot = {
            "active_clusters": int(result.get("active_clusters") or 0),
            "wallets_under_review": int(result.get("wallets_under_review") or 0),
            "high_risk_agents": int(result.get("high_risk_agents") or 0),
            "takedowns_completed": int(result.get("takedowns_completed") or 0),
            "estimated_fraud_value": float(result.get("estimated_fraud_value") or 0.0),
        }
        logger.info("celery.report.snapshot", **snapshot)
        return snapshot
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.report.snapshot.error", error=str(exc))
        raise


@app.task(name="tasks.report_tasks.build_evidence_package")
def build_evidence_package(cluster_id: str, case_id: str | None = None) -> dict[str, Any]:
    """Build an evidence package via :mod:`core.evidence.builder`.

    Returns the persisted package summary (id, version, page_count,
    size, file_path). Use this to enqueue evidence builds from outside
    the takedown workflow — analyst pre-print, scheduled refresh, etc.
    """

    async def _go() -> dict[str, Any]:
        from core.evidence.builder import build_for_cluster
        from core.graph.client import get_neo4j_client

        client = get_neo4j_client()
        try:
            if client._driver is None:
                await client.connect()
        except AttributeError:
            await client.connect()
        pkg = await build_for_cluster(cluster_id, case_id=case_id, generated_by="celery")
        return {
            "id": pkg.id,
            "cluster_id": pkg.cluster_id,
            "case_id": pkg.case_id,
            "version": pkg.version,
            "page_count": pkg.page_count,
            "file_size": pkg.file_size,
            "file_path": pkg.file_path,
        }

    try:
        result = _run_async(_go())
        logger.info("celery.report.evidence.complete", **result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "celery.report.evidence.error",
            cluster_id=cluster_id,
            error=str(exc),
        )
        raise
