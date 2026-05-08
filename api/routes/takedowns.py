"""Takedown coordination endpoints.

Takedowns aggregate per-step state (freeze wallets → flag SIMs → alert
agents → notify LE → generate evidence package) so the NOC can see how a
coordinated takedown is progressing across the operator + Scancom + LE
surfaces in real time.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from api.auth.rbac import ROLE_INVESTIGATOR, ROLE_SENIOR_INVESTIGATOR, require_role
from api.dependencies import DBSessionDep, Neo4jDep
from api.schemas import APIResponse, Meta, ok
from api.websocket.publisher import CH_CLUSTER_UPDATES, publish
from db.models import Takedown, TakedownStep

router = APIRouter(prefix="/api/takedowns", tags=["takedowns"])


_DEFAULT_STEPS = [
    "freeze_wallets",
    "flag_sims",
    "alert_agents",
    "notify_law_enforcement",
    "generate_evidence_package",
]


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _step_to_dict(s: TakedownStep) -> dict[str, Any]:
    return {
        "id": s.id,
        "step_type": s.step_type,
        "status": s.status,
        "started_at": s.started_at.isoformat() if s.started_at else None,
        "completed_at": s.completed_at.isoformat() if s.completed_at else None,
        "detail": s.detail,
    }


def _td_to_dict(t: Takedown, *, include_steps: bool = False) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": t.id,
        "cluster_id": t.cluster_id,
        "status": t.status,
        "initiated_by": t.initiated_by,
        "initiated_at": t.initiated_at.isoformat() if t.initiated_at else None,
        "approved_by": t.approved_by,
        "approved_at": t.approved_at.isoformat() if t.approved_at else None,
        "completed_at": t.completed_at.isoformat() if t.completed_at else None,
        "wallets_frozen": t.wallets_frozen,
        "sims_flagged": t.sims_flagged,
        "agents_alerted": t.agents_alerted,
        "evidence_package_id": t.evidence_package_id,
        "summary": t.summary,
    }
    if include_steps:
        payload["steps"] = [
            _step_to_dict(s)
            for s in sorted(t.steps, key=lambda s: s.started_at or datetime.min.replace(tzinfo=timezone.utc))
        ]
    return payload


# ---------------------------------------------------------------------------
# List + detail
# ---------------------------------------------------------------------------


@router.get("")
async def list_takedowns(
    db: DBSessionDep,
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
    status_filter: str | None = Query(
        None,
        alias="status",
        pattern="^(pending|approved|in_progress|completed|aborted)$",
    ),
    cluster_id: str | None = None,
) -> APIResponse[list[dict[str, Any]]]:
    base = select(Takedown)
    if status_filter:
        base = base.where(Takedown.status == status_filter)
    if cluster_id:
        base = base.where(Takedown.cluster_id == cluster_id)

    total = (await db.execute(select(func.count()).select_from(base.subquery()))).scalar_one()
    page_q = (
        base.order_by(Takedown.initiated_at.desc())
        .offset((page - 1) * per_page)
        .limit(per_page)
    )
    rows = (await db.execute(page_q)).scalars().all()

    return APIResponse(
        data=[_td_to_dict(t) for t in rows],
        meta=Meta(total=int(total), page=page, per_page=per_page),
        errors=[],
    )


@router.get("/{takedown_id}")
async def get_takedown(takedown_id: str, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    stmt = (
        select(Takedown)
        .where(Takedown.id == takedown_id)
        .options(selectinload(Takedown.steps))
    )
    td = (await db.execute(stmt)).scalar_one_or_none()
    if td is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "takedown not found")
    return ok(_td_to_dict(td, include_steps=True))


# ---------------------------------------------------------------------------
# Initiate
# ---------------------------------------------------------------------------


class TakedownInitiate(BaseModel):
    cluster_id: str = Field(..., min_length=1, max_length=64)
    summary: str | None = Field(None, max_length=500)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR))],
)
async def initiate_takedown(
    payload: TakedownInitiate,
    db: DBSessionDep,
    neo4j: Neo4jDep,
) -> APIResponse[dict[str, Any]]:
    """Create a new takedown for ``cluster_id``. Validates the cluster
    exists in Neo4j, then writes the Takedown row + the default 5-step
    skeleton (all in ``pending`` state) so the workflow UI has something to
    render immediately."""

    cluster_check = await neo4j.execute_read(
        """
        MATCH (c:Cluster {cluster_id: $cluster_id})
        RETURN c.cluster_id AS cluster_id, c.name AS name,
               c.confidence_score AS confidence,
               c.estimated_fraud_value AS estimated_fraud_value
        """,
        {"cluster_id": payload.cluster_id},
    )
    if not cluster_check:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "cluster not found")
    c = cluster_check[0]

    summary = payload.summary or (
        f"Coordinated takedown for {c.get('name') or c['cluster_id']} "
        f"(confidence {float(c.get('confidence') or 0):.2f}, "
        f"est. value GHS {float(c.get('estimated_fraud_value') or 0):.0f})."
    )

    td = Takedown(
        id=_new_id("td"),
        cluster_id=c["cluster_id"],
        initiated_by="system",  # TODO once auth lands
        initiated_at=datetime.now(timezone.utc),
        status="pending",
        wallets_frozen=0,
        sims_flagged=0,
        agents_alerted=0,
        summary=summary,
    )
    db.add(td)
    for step_type in _DEFAULT_STEPS:
        db.add(
            TakedownStep(
                id=_new_id("tdstep"),
                takedown_id=td.id,
                step_type=step_type,
                status="pending",
            )
        )
    # Tag the cluster as takedown_pending so dashboards reflect it immediately.
    await neo4j.execute_write(
        """
        MATCH (c:Cluster {cluster_id: $cluster_id})
        SET c.status = 'takedown_pending'
        """,
        {"cluster_id": c["cluster_id"]},
    )
    await db.commit()

    detail = (
        await db.execute(
            select(Takedown)
            .where(Takedown.id == td.id)
            .options(selectinload(Takedown.steps))
        )
    ).scalar_one()
    payload = _td_to_dict(detail, include_steps=True)
    await publish(
        CH_CLUSTER_UPDATES,
        "cluster.takedown_initiated",
        {
            "cluster_id": detail.cluster_id,
            "takedown_id": detail.id,
            "status": detail.status,
            "summary": detail.summary,
        },
    )
    return ok(payload)


# ---------------------------------------------------------------------------
# Approval / readiness (lightweight implementations — full executor lives in
# core/takedown/executor.py)
# ---------------------------------------------------------------------------


@router.post(
    "/{takedown_id}/approve",
    dependencies=[Depends(require_role(ROLE_SENIOR_INVESTIGATOR))],
)
async def approve_takedown(takedown_id: str, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    td = (
        await db.execute(select(Takedown).where(Takedown.id == takedown_id))
    ).scalar_one_or_none()
    if td is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "takedown not found")
    if td.status not in ("pending", "approved"):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"takedown is in status '{td.status}' and cannot be approved",
        )
    if td.status == "pending":
        td.status = "approved"
        td.approved_at = datetime.now(timezone.utc)
        td.approved_by = "system"
        await db.commit()
        await db.refresh(td)
    return ok(_td_to_dict(td))


@router.get("/{takedown_id}/readiness")
async def takedown_readiness(
    takedown_id: str, db: DBSessionDep, neo4j: Neo4jDep
) -> APIResponse[dict[str, Any]]:
    """Stub readiness check that surfaces the cluster's confidence + size
    plus simple heuristics. The real assessment lives in
    ``core.takedown.readiness``."""

    td = (
        await db.execute(select(Takedown).where(Takedown.id == takedown_id))
    ).scalar_one_or_none()
    if td is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "takedown not found")

    rows = await neo4j.execute_read(
        """
        MATCH (c:Cluster {cluster_id: $cluster_id})
        OPTIONAL MATCH (n)-[:BELONGS_TO]->(c)
        WITH c, count(DISTINCT n) AS members
        OPTIONAL MATCH (a:Agent)-[:LINKED_TO]->(c)
        WITH c, members, count(DISTINCT a) AS linked_agents
        RETURN c.confidence_score AS confidence,
               c.estimated_fraud_value AS estimated_fraud_value,
               members,
               linked_agents
        """,
        {"cluster_id": td.cluster_id},
    )
    if not rows:
        return ok(
            {
                "ready": False,
                "score": 0.0,
                "checks": [{"name": "cluster_exists", "ok": False}],
            }
        )
    r = rows[0]
    confidence = float(r.get("confidence") or 0.0)
    members = int(r.get("members") or 0)
    linked_agents = int(r.get("linked_agents") or 0)

    checks = [
        {"name": "confidence_above_0_70", "ok": confidence >= 0.70, "value": confidence},
        {"name": "members_above_4", "ok": members >= 4, "value": members},
        {"name": "linked_agents_present", "ok": linked_agents >= 1, "value": linked_agents},
    ]
    score = sum(1 for c in checks if c["ok"]) / len(checks)
    return ok(
        {
            "takedown_id": takedown_id,
            "cluster_id": td.cluster_id,
            "ready": score >= 0.66,
            "score": round(score, 2),
            "checks": checks,
            "estimated_fraud_value": float(r.get("estimated_fraud_value") or 0.0),
        }
    )
