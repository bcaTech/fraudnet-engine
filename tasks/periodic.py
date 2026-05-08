"""Scheduled (Celery beat) tasks.

Most are still heartbeat stubs while the real implementations land in
``core/mesh``, ``integration``, and ``law_enforcement``. The
:func:`evaluate_scheduled_rules` task is wired to the real rules engine.
"""

from __future__ import annotations

import asyncio

from config.logging import configure_logging, get_logger
from rules.engine import evaluate_scheduled as run_scheduled_rules

from .celery_app import app

configure_logging()
logger = get_logger(__name__)


def _heartbeat(name: str) -> dict[str, str]:
    logger.info("celery.beat.heartbeat", task=name, status="stub")
    return {"task": name, "status": "stub"}


def _run_async(coro):
    """Drive an async coroutine from a sync Celery task. We always create a
    fresh loop because Celery worker pools recycle threads/processes and
    we don't want to inherit a stale event loop."""

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:  # noqa: BLE001
            pass
        loop.close()
        asyncio.set_event_loop(None)


@app.task(name="tasks.periodic.apply_temporal_decay")
def apply_temporal_decay() -> dict[str, str]:
    return _heartbeat("apply_temporal_decay")


@app.task(name="tasks.periodic.rescore_active_clusters")
def rescore_active_clusters() -> dict:
    """Run community detection + centrality across every active cluster.

    Delegates to :func:`tasks.mesh_tasks.rescore_active_clusters_task` so
    the heavy lifting (NetworkX import + analytics) lives in one place.
    """

    from .mesh_tasks import rescore_active_clusters_task

    return rescore_active_clusters_task(limit=30)


@app.task(name="tasks.periodic.scancom_batch_import")
def scancom_batch_import() -> dict[str, str]:
    return _heartbeat("scancom_batch_import")


@app.task(name="tasks.periodic.sleeper_wallet_scan")
def sleeper_wallet_scan() -> dict[str, str]:
    return _heartbeat("sleeper_wallet_scan")


@app.task(name="tasks.periodic.campaign_detection")
def campaign_detection() -> dict[str, str]:
    return _heartbeat("campaign_detection")


@app.task(name="tasks.periodic.evaluate_scheduled_rules")
def evaluate_scheduled_rules() -> dict:
    """Run every live, scheduled-mode rule against current graph state.

    Wired to :func:`rules.engine.evaluate_scheduled` — no longer a stub.
    Returns a stats dict (rules evaluated, matches, actions executed,
    triggers written) so the result is surfaced in the Celery result
    backend and operator logs.
    """

    try:
        result = _run_async(run_scheduled_rules())
        logger.info("rules.scheduled.task_complete", **result)
        return result
    except Exception as exc:  # noqa: BLE001 — Celery should record the failure
        logger.error("rules.scheduled.task_error", error=str(exc))
        raise


@app.task(name="tasks.periodic.process_inbound_integration")
def process_inbound_integration() -> dict[str, str]:
    return _heartbeat("process_inbound_integration")


@app.task(name="tasks.periodic.process_outbound_integration")
def process_outbound_integration() -> dict[str, str]:
    return _heartbeat("process_outbound_integration")


@app.task(name="tasks.periodic.rules_performance_aggregation")
def rules_performance_aggregation() -> dict[str, str]:
    return _heartbeat("rules_performance_aggregation")


@app.task(name="tasks.periodic.external_operator_health_check")
def external_operator_health_check() -> dict[str, str]:
    return _heartbeat("external_operator_health_check")


@app.task(name="tasks.periodic.law_enforcement_case_reminders")
def law_enforcement_case_reminders() -> dict[str, str]:
    return _heartbeat("law_enforcement_case_reminders")
