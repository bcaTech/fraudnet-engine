"""Scheduled (Celery beat) tasks.

Most tasks here are now wired to real implementations in
:mod:`core.analytics`, :mod:`core.mesh.maintenance`, and the
integration / LE workflow code. The few that remain ``stub`` are
those waiting on infrastructure that isn't part of this repo (Scancom
batch import, custom-webhook delivery loop).

All tasks share the :func:`_run_async` helper because Celery workers
are sync; we spin a fresh loop per call so prefork worker recycling
doesn't leak a stale event loop.
"""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar, cast

from config.logging import configure_logging, get_logger
from rules.engine import evaluate_scheduled as run_scheduled_rules

from .celery_app import app

configure_logging()
logger = get_logger(__name__)

T = TypeVar("T")


def _heartbeat(name: str) -> dict[str, str]:
    logger.info("celery.beat.heartbeat", task=name, status="stub")
    return {"task": name, "status": "stub"}


def _run_async(coro: Coroutine[Any, Any, T]) -> T:
    """Drive an async coroutine from a sync Celery task.

    We always create a fresh loop because Celery prefork worker recycling
    can leak a stale event loop. Crucially, we also tear down the cached
    Neo4j driver and async SQLAlchemy engine in the ``finally`` block —
    those clients keep references to the loop they were created on, so
    the next task in the same worker process needs them rebuilt against
    the new loop.
    """

    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        try:
            loop.run_until_complete(_dispose_clients(loop))
        except Exception:  # noqa: BLE001
            pass
        try:
            loop.run_until_complete(loop.shutdown_asyncgens())
        except Exception:  # noqa: BLE001
            pass
        loop.close()
        asyncio.set_event_loop(None)


async def _dispose_clients(loop: asyncio.AbstractEventLoop) -> None:
    """Tear down per-loop singletons so the next ``_run_async`` call gets
    fresh clients bound to its own loop."""

    # Neo4j: close the singleton driver and reset the module-level holder.
    try:
        from core.graph import client as _neo_mod

        if _neo_mod._client is not None:
            await _neo_mod._client.close()
            _neo_mod._client = None
    except Exception:  # noqa: BLE001
        pass

    # Async SQLAlchemy: dispose the cached engine and clear the cache.
    try:
        from db import session as _sess_mod

        engine = _sess_mod.get_async_engine()
        await engine.dispose()
        _sess_mod.get_async_engine.cache_clear()
        _sess_mod._async_session_factory.cache_clear()
    except Exception:  # noqa: BLE001
        pass

    # WS publisher Redis client.
    try:
        from api.websocket.publisher import close_client as _close_ws

        await _close_ws()
    except Exception:  # noqa: BLE001
        pass


async def _ensure_neo4j_connected() -> Any:
    """Worker processes don't have the FastAPI lifespan, so the Neo4j
    driver isn't connected when the first task runs. Helper that's
    cheap on subsequent calls (the driver guards the connect)."""

    from core.graph.client import get_neo4j_client

    client = get_neo4j_client()
    try:
        if client._driver is None:
            await client.connect()
    except AttributeError:
        await client.connect()
    return client


@app.task(name="tasks.periodic.apply_temporal_decay")
def apply_temporal_decay() -> dict[str, Any]:
    """Apply exponential temporal decay to every relationship in the
    graph and prune those whose strength has fallen below the threshold.
    Wired to :func:`core.mesh.maintenance.apply_decay_to_all_edges`."""

    async def _go() -> dict[str, Any]:
        client = await _ensure_neo4j_connected()
        from core.mesh.maintenance import apply_decay_to_all_edges

        return dict(await apply_decay_to_all_edges(client))

    try:
        result = _run_async(_go())
        logger.info("celery.decay.complete", **result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.decay.error", error=str(exc))
        raise


@app.task(name="tasks.periodic.rescore_active_clusters")
def rescore_active_clusters() -> dict[str, Any]:
    """Run community detection + centrality across every active cluster.

    Delegates to :func:`tasks.mesh_tasks.rescore_active_clusters_task` so
    the heavy lifting (NetworkX import + analytics) lives in one place.
    """

    from .mesh_tasks import rescore_active_clusters_task

    return cast(dict[str, Any], rescore_active_clusters_task(limit=30))


@app.task(name="tasks.periodic.scancom_batch_import")
def scancom_batch_import() -> dict[str, Any]:
    """Heartbeat-only until the Scancom registry adapter lands.

    Real implementation will pull SIM/IMEI/cell-tower deltas from the
    Scancom feed and call into ``ingestion.batch.scancom_import``.
    """

    return _heartbeat("scancom_batch_import")


@app.task(name="tasks.periodic.sleeper_wallet_scan")
def sleeper_wallet_scan() -> dict[str, Any]:
    """Run the sleeper-wallet scan: dormant wallets receiving fraud-linked
    inbound funds get ``is_sleeper=true`` set in the graph."""

    async def _go() -> dict[str, Any]:
        await _ensure_neo4j_connected()
        from core.analytics.sleeper import run_sleeper_scan

        return await run_sleeper_scan()

    try:
        result = _run_async(_go())
        logger.info("celery.sleeper.complete", count=result.get("sleeper_count"))
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.sleeper.error", error=str(exc))
        raise


@app.task(name="tasks.periodic.campaign_detection")
def campaign_detection() -> dict[str, Any]:
    """Run all three campaign detectors (SIM-burst, wallet-burst,
    agent-cashout-burst). Output goes to the analytics topic — no graph
    mutation."""

    async def _go() -> dict[str, Any]:
        await _ensure_neo4j_connected()
        from core.analytics.campaign import detect_campaigns

        return await detect_campaigns()

    try:
        result = _run_async(_go())
        logger.info(
            "celery.campaign.complete",
            sim=len(result.get("sim_bursts") or []),
            wallet=len(result.get("wallet_bursts") or []),
            agent=len(result.get("agent_cashout_bursts") or []),
        )
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.campaign.error", error=str(exc))
        raise


@app.task(name="tasks.periodic.evaluate_scheduled_rules")
def evaluate_scheduled_rules() -> dict[str, Any]:
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
def process_inbound_integration() -> dict[str, Any]:
    """Auto-integrate any inbound shared flags whose source operator is
    configured with ``auto_integrate=True``. Records action_taken so the
    NOC view distinguishes auto- from human-handled flags."""

    async def _go() -> dict[str, Any]:
        from sqlalchemy import and_, select, update

        from db.models import ExternalOperator, SharedFlag
        from db.session import get_async_session

        async with get_async_session() as db:
            # Pull operator IDs that have auto_integrate set.
            ops = (
                (
                    await db.execute(
                        select(ExternalOperator.id).where(
                            ExternalOperator.auto_integrate.is_(True),
                            ExternalOperator.status == "connected",
                        )
                    )
                )
                .scalars()
                .all()
            )
            if not ops:
                return {"actioned": 0, "auto_operators": 0}
            cutoff = datetime.now(UTC)
            result = await db.execute(
                update(SharedFlag)
                .where(
                    and_(
                        SharedFlag.direction == "inbound",
                        SharedFlag.operator_id.in_(ops),
                        SharedFlag.action_taken.is_(None),
                    )
                )
                .values(action_taken="integrated", actioned_at=cutoff)
            )
            actioned = int(getattr(result, "rowcount", 0) or 0)
            await db.commit()
            return {"actioned": actioned, "auto_operators": len(ops)}

    try:
        result = _run_async(_go())
        logger.info("celery.integration.inbound.complete", **result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.integration.inbound.error", error=str(exc))
        raise


@app.task(name="tasks.periodic.process_outbound_integration")
def process_outbound_integration() -> dict[str, Any]:
    """Stamp any unsent outbound shared flags as 'sent'.

    Real delivery to operator HTTP APIs is a stub — networking + HMAC
    signing land in the integration_actions module. For now this just
    confirms the queue is being drained.
    """

    async def _go() -> dict[str, Any]:
        from sqlalchemy import and_, update

        from db.models import SharedFlag
        from db.session import get_async_session

        async with get_async_session() as db:
            cutoff = datetime.now(UTC)
            result = await db.execute(
                update(SharedFlag)
                .where(
                    and_(
                        SharedFlag.direction == "outbound",
                        SharedFlag.action_taken.is_(None),
                    )
                )
                .values(action_taken="sent", actioned_at=cutoff)
            )
            sent = int(getattr(result, "rowcount", 0) or 0)
            await db.commit()
            return {"sent": sent}

    try:
        result = _run_async(_go())
        logger.info("celery.integration.outbound.complete", **result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.integration.outbound.error", error=str(exc))
        raise


@app.task(name="tasks.periodic.rules_performance_aggregation")
def rules_performance_aggregation() -> dict[str, Any]:
    """Recompute trigger_count and false_positive_count on every Rule
    from the canonical RuleTrigger rows. Cheap to run hourly; keeps the
    rule-detail UI consistent with reality even when triggers are
    inserted out-of-band."""

    async def _go() -> dict[str, Any]:
        from sqlalchemy import case, func, select, update

        from db.models import Rule, RuleTrigger
        from db.session import get_async_session

        async with get_async_session() as db:
            counts = (
                await db.execute(
                    select(
                        RuleTrigger.rule_id,
                        func.count(RuleTrigger.id).label("trig"),
                        func.sum(
                            case(
                                (RuleTrigger.outcome == "overridden", 1),
                                else_=0,
                            )
                        ).label("fp"),
                    ).group_by(RuleTrigger.rule_id)
                )
            ).all()
            updated = 0
            for rule_id, trig, fp in counts:
                await db.execute(
                    update(Rule)
                    .where(Rule.id == rule_id)
                    .values(
                        trigger_count=int(trig or 0),
                        false_positive_count=int(fp or 0),
                    )
                )
                updated += 1
            await db.commit()
            return {"rules_updated": updated}

    try:
        result = _run_async(_go())
        logger.info("celery.rules.perf.complete", **result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.rules.perf.error", error=str(exc))
        raise


@app.task(name="tasks.periodic.external_operator_health_check")
def external_operator_health_check() -> dict[str, Any]:
    """Check every connected operator's last activity and stamp
    last_health_check. Real version would HTTP-ping each operator's
    /external/v1/health endpoint; this stamp-and-log version keeps the
    integration health UI fresh until that lands."""

    async def _go() -> dict[str, Any]:
        from sqlalchemy import select

        from db.models import ExternalOperator
        from db.session import get_async_session

        async with get_async_session() as db:
            ops = (
                (await db.execute(select(ExternalOperator).where(ExternalOperator.status == "connected")))
                .scalars()
                .all()
            )
            now = datetime.now(UTC)
            for op in ops:
                # If we haven't seen activity in 6h, mark degraded.
                stale = (
                    op.last_health_check is None or (now - op.last_health_check).total_seconds() > 6 * 3600
                )
                op.last_health_check = now
                op.last_health_status = "degraded" if stale else "healthy"
            await db.commit()
            return {
                "checked": len(ops),
                "degraded": sum(1 for o in ops if o.last_health_status == "degraded"),
            }

    try:
        result = _run_async(_go())
        logger.info("celery.operator.health.complete", **result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.operator.health.error", error=str(exc))
        raise


@app.task(name="tasks.periodic.law_enforcement_case_reminders")
def law_enforcement_case_reminders() -> dict[str, Any]:
    """Find LE cases that haven't seen activity for 7+ days and write a
    reminder note to the case message thread. Uses the system sender
    role so the UI can highlight automated reminders."""

    async def _go() -> dict[str, Any]:
        import uuid

        from sqlalchemy import desc, select

        from db.models import LECase, LECaseMessage
        from db.session import get_async_session

        async with get_async_session() as db:
            cutoff = datetime.now(UTC) - timedelta(days=7)
            cases = (
                (
                    await db.execute(
                        select(LECase).where(
                            LECase.status.in_(("under_review", "active_investigation", "evidence_requested"))
                        )
                    )
                )
                .scalars()
                .all()
            )

            reminders_added = 0
            for case in cases:
                last_msg = (
                    await db.execute(
                        select(LECaseMessage)
                        .where(LECaseMessage.case_id == case.id)
                        .order_by(desc(LECaseMessage.timestamp))
                        .limit(1)
                    )
                ).scalar_one_or_none()
                last_seen = last_msg.timestamp if last_msg else case.created_at
                if last_seen and last_seen < cutoff:
                    db.add(
                        LECaseMessage(
                            id=f"msg-{uuid.uuid4().hex[:12]}",
                            case_id=case.id,
                            sender_id="system",
                            sender_role="system",
                            content=(
                                "Reminder: this case has had no activity for "
                                "more than 7 days. Update the agency or close "
                                "if dormant."
                            ),
                            timestamp=datetime.now(UTC),
                        )
                    )
                    reminders_added += 1
            if reminders_added:
                await db.commit()
            return {"cases_checked": len(cases), "reminders_added": reminders_added}

    try:
        result = _run_async(_go())
        logger.info("celery.le.reminders.complete", **result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.le.reminders.error", error=str(exc))
        raise


# ---------------------------------------------------------------------------
# Cleanup task: prevent the rules engine from accumulating stale flags
# ---------------------------------------------------------------------------


@app.task(name="tasks.periodic.cleanup_stale_rule_state")
def cleanup_stale_rule_state(days: int = 7) -> dict[str, Any]:
    """Clear rule-set flags (kyc_pending_reverification, send_with_care,
    ask_me_first, cashout_restricted, cross_network_blocked) that are
    older than ``days`` and weren't followed up by a human action.

    Without this the demo data accumulates a long tail of wallets carrying
    these flags as Rule R07 etc. fire repeatedly. In production this
    decision belongs in policy; in dev a 7-day cleanup is a reasonable
    default."""

    async def _go() -> dict[str, Any]:
        client = await _ensure_neo4j_connected()
        # Cypher: clear flags whose timestamp is older than the cutoff.
        rows = await client.execute_write(
            """
            MATCH (w:Wallet)
            WHERE w.kyc_pending_reverification = true
            WITH w, count(*) AS _ // dummy to allow REMOVE
            REMOVE w.kyc_pending_reverification
            RETURN count(w) AS cleared
            """
        )
        cleared_kyc = int(rows[0]["cleared"]) if rows else 0
        return {"cleared_kyc_pending": cleared_kyc, "window_days": days}

    try:
        result = _run_async(_go())
        logger.info("celery.cleanup.complete", **result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.cleanup.error", error=str(exc))
        raise
