"""Coordinated takedown executor.

Steps through the takedown's pending steps in order, calling the
matching actuator for each. Results land on the
:class:`db.models.TakedownStep` row's ``detail`` JSON so the analyst
UI can show what happened. When every required step succeeds, the
takedown is marked ``completed`` and an evidence package is built via
:mod:`core.evidence.builder`.

The orchestration is intentionally idempotent: re-executing a takedown
that's already ``completed`` is a no-op; re-executing one with some
steps already ``completed`` skips them and only runs the rest. This
lets the analyst hit "execute" again after a transient actuator
failure without double-applying side effects.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from config.logging import get_logger
from core.evidence.builder import build_for_cluster
from core.graph.client import Neo4jClient, get_neo4j_client
from core.takedown.agent_alert import alert_cluster_agents
from core.takedown.restitution import trace_restitution_candidates
from core.takedown.sim_flag import flag_cluster_sims
from core.takedown.wallet_freeze import freeze_cluster_wallets
from db.models import Takedown

logger = get_logger(__name__)


# step_type → handler. Each handler receives (cluster_id, neo4j_client)
# and returns a JSON-serialisable detail dict that lands on the
# TakedownStep.detail column.
_STEP_HANDLERS = {
    "freeze_wallets": "_run_freeze_wallets",
    "flag_sims": "_run_flag_sims",
    "alert_agents": "_run_alert_agents",
    "notify_law_enforcement": "_run_notify_law_enforcement",
    "generate_evidence_package": "_run_generate_evidence_package",
}


async def _run_freeze_wallets(td: Takedown, client: Neo4jClient) -> dict[str, Any]:
    result = await freeze_cluster_wallets(td.cluster_id, client=client)
    td.wallets_frozen = (td.wallets_frozen or 0) + int(result.get("frozen") or 0)
    return result


async def _run_flag_sims(td: Takedown, client: Neo4jClient) -> dict[str, Any]:
    result = await flag_cluster_sims(td.cluster_id, client=client)
    td.sims_flagged = (td.sims_flagged or 0) + int(result.get("flagged") or 0)
    return result


async def _run_alert_agents(td: Takedown, client: Neo4jClient) -> dict[str, Any]:
    result = await alert_cluster_agents(td.cluster_id, client=client)
    td.agents_alerted = (td.agents_alerted or 0) + int(result.get("alerted") or 0)
    return result


async def _run_notify_law_enforcement(td: Takedown, client: Neo4jClient) -> dict[str, Any]:
    """Lightweight LE notification: trace restitution candidates and
    record them on the step. The actual referral creation belongs to
    the analyst once they have a case open with the agency."""

    candidates = await trace_restitution_candidates(td.cluster_id, client=client)
    return {
        "candidate_count": candidates.get("candidate_count"),
        "total_estimated_loss": candidates.get("total_estimated_loss"),
    }


async def _run_generate_evidence_package(
    td: Takedown,
    client: Neo4jClient,  # noqa: ARG001
) -> dict[str, Any]:
    pkg = await build_for_cluster(td.cluster_id, takedown_id=td.id, generated_by="takedown_executor")
    td.evidence_package_id = pkg.id
    return {
        "package_id": pkg.id,
        "version": pkg.version,
        "page_count": pkg.page_count,
        "file_size": pkg.file_size,
    }


async def execute(
    takedown_id: str,
    db: AsyncSession,
    *,
    client: Neo4jClient | None = None,
) -> dict[str, Any]:
    """Execute every pending step on the takedown and finalise."""

    c = client or get_neo4j_client()

    stmt = select(Takedown).where(Takedown.id == takedown_id).options(selectinload(Takedown.steps))
    td = (await db.execute(stmt)).scalar_one_or_none()
    if td is None:
        raise ValueError(f"takedown not found: {takedown_id}")
    if td.status == "completed":
        return {"status": "noop", "takedown_id": td.id, "reason": "already_complete"}

    # Approve implicitly if the executor is invoked on a pending takedown
    # — production wires this behind a senior_investigator approval.
    if td.status == "pending":
        td.status = "approved"
        td.approved_at = datetime.now(UTC)

    step_results: list[dict[str, Any]] = []
    for step in sorted(td.steps, key=lambda s: _step_order(s.step_type)):
        if step.status == "completed":
            continue
        handler_name = _STEP_HANDLERS.get(step.step_type)
        if handler_name is None:
            step.status = "skipped"
            step.detail = {"reason": "unknown_step_type"}
            continue
        handler = globals()[handler_name]
        step.status = "in_progress"
        step.started_at = datetime.now(UTC)
        try:
            detail = await handler(td, c)
            step.status = "completed"
            step.completed_at = datetime.now(UTC)
            step.detail = detail
            step_results.append({"step": step.step_type, "ok": True, "detail": detail})
        except Exception as exc:  # noqa: BLE001 — surface on the step, keep going
            step.status = "failed"
            step.completed_at = datetime.now(UTC)
            step.detail = {"error": str(exc)}
            logger.error(
                "takedown.step.failed",
                takedown_id=td.id,
                step=step.step_type,
                error=str(exc),
            )
            step_results.append({"step": step.step_type, "ok": False, "error": str(exc)})

    failed_critical = any(
        not r["ok"] and r["step"] in ("freeze_wallets", "generate_evidence_package") for r in step_results
    )
    if not failed_critical:
        td.status = "completed"
        td.completed_at = datetime.now(UTC)
        await c.execute_write(
            "MATCH (cl:Cluster {cluster_id: $cluster_id}) SET cl.status = 'takedown_complete'",
            {"cluster_id": td.cluster_id},
        )
    await db.commit()
    return {
        "status": td.status,
        "takedown_id": td.id,
        "cluster_id": td.cluster_id,
        "steps": step_results,
        "evidence_package_id": td.evidence_package_id,
    }


def _step_order(step_type: str) -> int:
    """Stable ordering for the default 5-step skeleton."""

    order = {
        "freeze_wallets": 0,
        "flag_sims": 1,
        "alert_agents": 2,
        "notify_law_enforcement": 3,
        "generate_evidence_package": 4,
    }
    return order.get(step_type, 99)


__all__ = ["execute"]
