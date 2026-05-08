"""Takedown coordination endpoints.

Takedowns aggregate per-step state (freeze wallets → flag SIMs → alert
agents → notify LE → generate evidence package) so the NOC can see how a
coordinated takedown is progressing across the operator + Scancom + LE
surfaces in real time.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import desc, func, select
from sqlalchemy.orm import selectinload

from api.auth.jwt import TokenClaims
from api.auth.rbac import (
    ROLE_INVESTIGATOR,
    ROLE_SENIOR_INVESTIGATOR,
    require_role,
)
from api.dependencies import DBSessionDep, Neo4jDep
from api.schemas import APIResponse, Meta, ok
from api.websocket.publisher import CH_CLUSTER_UPDATES, publish, takedown_channel
from core.takedown.executor import execute as execute_takedown
from core.takedown.readiness import assess as assess_readiness
from db.models import EvidencePackage, Takedown, TakedownStep

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
            for s in sorted(t.steps, key=lambda s: s.started_at or datetime.min.replace(tzinfo=UTC))
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
    page_q = base.order_by(Takedown.initiated_at.desc()).offset((page - 1) * per_page).limit(per_page)
    rows = (await db.execute(page_q)).scalars().all()

    return APIResponse(
        data=[_td_to_dict(t) for t in rows],
        meta=Meta(total=int(total), page=page, per_page=per_page),
        errors=[],
    )


@router.get("/{takedown_id}")
async def get_takedown(takedown_id: str, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    stmt = select(Takedown).where(Takedown.id == takedown_id).options(selectinload(Takedown.steps))
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


@router.post("", status_code=status.HTTP_201_CREATED)
async def initiate_takedown(
    payload: TakedownInitiate,
    db: DBSessionDep,
    neo4j: Neo4jDep,
    user: Annotated[TokenClaims, Depends(require_role(ROLE_INVESTIGATOR))],
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
        initiated_by=user.username or user.sub,
        initiated_at=datetime.now(UTC),
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
        await db.execute(select(Takedown).where(Takedown.id == td.id).options(selectinload(Takedown.steps)))
    ).scalar_one()
    response_body = _td_to_dict(detail, include_steps=True)
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
    return ok(response_body)


# ---------------------------------------------------------------------------
# Approval / readiness (lightweight implementations — full executor lives in
# core/takedown/executor.py)
# ---------------------------------------------------------------------------


@router.post("/{takedown_id}/approve")
async def approve_takedown(
    takedown_id: str,
    db: DBSessionDep,
    user: Annotated[TokenClaims, Depends(require_role(ROLE_SENIOR_INVESTIGATOR))],
) -> APIResponse[dict[str, Any]]:
    td = (await db.execute(select(Takedown).where(Takedown.id == takedown_id))).scalar_one_or_none()
    if td is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "takedown not found")
    if td.status not in ("pending", "approved"):
        raise HTTPException(
            status.HTTP_409_CONFLICT,
            f"takedown is in status '{td.status}' and cannot be approved",
        )
    if td.status == "pending":
        td.status = "approved"
        td.approved_at = datetime.now(UTC)
        td.approved_by = user.username or user.sub
        await db.commit()
        await db.refresh(td)
        await publish(
            takedown_channel(td.id),
            "takedown.approved",
            {"takedown_id": td.id, "approved_at": td.approved_at.isoformat()},
        )
    return ok(_td_to_dict(td))


@router.post(
    "/{takedown_id}/complete",
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR))],
)
async def complete_takedown(
    takedown_id: str, db: DBSessionDep, neo4j: Neo4jDep
) -> APIResponse[dict[str, Any]]:
    """Run every pending step on the takedown.

    Delegates to :func:`core.takedown.executor.execute`, which calls
    the matching actuator for each step (freeze wallets, flag SIMs,
    alert agents, trace restitution candidates, build evidence
    package), records the per-step result on
    ``TakedownStep.detail``, and finalises the takedown when no
    critical step has failed. Idempotent: re-calling on a completed
    takedown is a no-op.
    """

    pre = (await db.execute(select(Takedown).where(Takedown.id == takedown_id))).scalar_one_or_none()
    if pre is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "takedown not found")

    try:
        outcome = await execute_takedown(takedown_id, db, client=neo4j)
    except Exception as exc:  # noqa: BLE001 — surface to caller
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"takedown execution failed: {exc}",
        ) from exc

    # Re-fetch the takedown with steps for the response payload.
    stmt = select(Takedown).where(Takedown.id == takedown_id).options(selectinload(Takedown.steps))
    td = (await db.execute(stmt)).scalar_one()

    # Broadcast per-step + final result to the WS feeds.
    for step in outcome.get("steps") or []:
        await publish(
            takedown_channel(td.id),
            "takedown.step_completed" if step.get("ok") else "takedown.step_failed",
            {
                "takedown_id": td.id,
                "step": step.get("step"),
                "ok": step.get("ok"),
                "detail": step.get("detail") if step.get("ok") else None,
                "error": step.get("error") if not step.get("ok") else None,
            },
        )
    if td.status == "completed":
        await publish(
            CH_CLUSTER_UPDATES,
            "cluster.takedown_complete",
            {
                "cluster_id": td.cluster_id,
                "takedown_id": td.id,
                "evidence_package_id": td.evidence_package_id,
            },
        )
        await publish(
            takedown_channel(td.id),
            "takedown.completed",
            {
                "takedown_id": td.id,
                "completed_at": td.completed_at.isoformat() if td.completed_at else None,
                "evidence_package_id": td.evidence_package_id,
                "wallets_frozen": td.wallets_frozen,
                "sims_flagged": td.sims_flagged,
                "agents_alerted": td.agents_alerted,
            },
        )
    return ok(_td_to_dict(td, include_steps=True))


@router.get("/{takedown_id}/evidence-package")
async def download_evidence_package(takedown_id: str, db: DBSessionDep) -> StreamingResponse:
    """Stream the latest evidence-package PDF for a takedown."""

    td = (await db.execute(select(Takedown).where(Takedown.id == takedown_id))).scalar_one_or_none()
    if td is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "takedown not found")

    pkg = (
        await db.execute(
            select(EvidencePackage)
            .where(EvidencePackage.takedown_id == takedown_id)
            .order_by(desc(EvidencePackage.version))
            .limit(1)
        )
    ).scalar_one_or_none()
    if pkg is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no evidence package for this takedown")

    # Pull bytes out of MinIO. file_path is ``s3://bucket/key`` or
    # ``local://key`` when MinIO was unavailable at build time.
    pdf_bytes = await _fetch_evidence_bytes(pkg)
    headers = {
        "Content-Disposition": f'attachment; filename="evidence-{pkg.id}.pdf"',
        "X-Evidence-Hash": pkg.file_hash,
        "X-Evidence-Version": str(pkg.version),
    }
    return StreamingResponse(iter([pdf_bytes]), media_type="application/pdf", headers=headers)


async def _fetch_evidence_bytes(pkg: EvidencePackage) -> bytes:
    """Resolve a stored evidence package back into raw bytes."""

    if pkg.file_path.startswith("s3://"):
        from minio import Minio

        from config.settings import get_settings

        s = get_settings()
        client = Minio(
            s.minio_endpoint,
            access_key=s.minio_access_key,
            secret_key=s.minio_secret_key.get_secret_value(),
            secure=s.minio_secure,
        )
        # ``s3://bucket/key/with/slashes``
        without_scheme = pkg.file_path[len("s3://") :]
        bucket, _, key = without_scheme.partition("/")
        try:
            response = client.get_object(bucket, key)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status.HTTP_502_BAD_GATEWAY,
                f"failed to fetch evidence package from MinIO: {exc}",
            ) from exc

    # Local fallback: rebuild on demand. The hash stored at build time
    # matches the original payload, so a rebuild may produce a fresh
    # hash if state has shifted — that's an acceptable trade-off in dev.
    from core.evidence.builder import _gather  # noqa: PLC0415
    from core.evidence.export import render_pdf  # noqa: PLC0415

    payload = await _gather(pkg.cluster_id)
    pdf_bytes, _ = render_pdf(payload)
    return pdf_bytes


@router.get("/{takedown_id}/readiness")
async def takedown_readiness(
    takedown_id: str, db: DBSessionDep, neo4j: Neo4jDep
) -> APIResponse[dict[str, Any]]:
    """Pre-takedown readiness assessment.

    Delegates to :func:`core.takedown.readiness.assess`, which runs
    six checks against the cluster's graph state (confidence, member
    count, linked agents, unfrozen wallets, fund-flow evidence) and
    returns a normalised score plus per-check detail.
    """

    td = (await db.execute(select(Takedown).where(Takedown.id == takedown_id))).scalar_one_or_none()
    if td is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "takedown not found")

    report = await assess_readiness(td.cluster_id, client=neo4j)
    return ok(
        {
            "takedown_id": takedown_id,
            "cluster_id": report.cluster_id,
            "ready": report.ready,
            "score": report.score,
            "checks": [{"name": ch.name, "ok": ch.ok, **ch.detail} for ch in report.checks],
            "estimated_fraud_value": report.estimated_fraud_value,
        }
    )
