"""Alert feed: list, acknowledge, and aggregate stats.

Backed by the Postgres ``alerts`` table seeded by ``scripts.seed_demo_data``
and (eventually) populated in real-time by the rules engine + Kafka
consumers.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import func, select

from api.dependencies import DBSessionDep
from api.schemas import APIResponse, Meta, ok
from db.models import Alert

router = APIRouter(prefix="/api/alerts", tags=["alerts"])


def _alert_to_dict(a: Alert) -> dict[str, Any]:
    return {
        "id": a.id,
        "created_at": a.created_at.isoformat() if a.created_at else None,
        "type": a.type,
        "severity": a.severity,
        "title": a.title,
        "description": a.description,
        "target_type": a.target_type,
        "target_id": a.target_id,
        "cluster_id": a.cluster_id,
        "acknowledged": a.acknowledged,
        "acknowledged_at": a.acknowledged_at.isoformat() if a.acknowledged_at else None,
        "acknowledged_by": a.acknowledged_by,
        "rule_id": a.rule_id,
        "extra": a.extra,
    }


# --- /stats must be declared before /{alert_id}/... so it isn't shadowed ----


@router.get("/stats")
async def alert_stats(db: DBSessionDep) -> APIResponse[dict]:
    """Aggregate counts by severity, type, and acknowledged state.

    Powers the alert-volume charts in the analytics view.
    """

    by_severity = (
        await db.execute(select(Alert.severity, func.count()).group_by(Alert.severity))
    ).all()
    by_type = (
        await db.execute(select(Alert.type, func.count()).group_by(Alert.type))
    ).all()
    ack_counts = (
        await db.execute(select(Alert.acknowledged, func.count()).group_by(Alert.acknowledged))
    ).all()
    total = (await db.execute(select(func.count()).select_from(Alert))).scalar_one()

    return ok(
        {
            "total": int(total),
            "by_severity": {sev or "unknown": int(c) for sev, c in by_severity},
            "by_type": {t or "unknown": int(c) for t, c in by_type},
            "acknowledged": {str(bool(k)): int(c) for k, c in ack_counts},
        }
    )


@router.get("")
async def list_alerts(
    db: DBSessionDep,
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
    severity: str | None = Query(None, pattern="^(low|medium|high|critical)$"),
    type: str | None = Query(None, alias="type"),
    acknowledged: bool | None = None,
    cluster_id: str | None = None,
    target_id: str | None = None,
) -> APIResponse[list[dict]]:
    """List alerts most-recent-first with the usual filter knobs."""

    conditions = []
    if severity is not None:
        conditions.append(Alert.severity == severity)
    if type is not None:
        conditions.append(Alert.type == type)
    if acknowledged is not None:
        conditions.append(Alert.acknowledged == acknowledged)
    if cluster_id is not None:
        conditions.append(Alert.cluster_id == cluster_id)
    if target_id is not None:
        conditions.append(Alert.target_id == target_id)

    base = select(Alert)
    if conditions:
        base = base.where(*conditions)

    total = (
        await db.execute(select(func.count()).select_from(base.subquery()))
    ).scalar_one()

    page_q = (
        base.order_by(Alert.created_at.desc())
        .offset((page - 1) * per_page)
        .limit(per_page)
    )
    rows = (await db.execute(page_q)).scalars().all()

    return APIResponse(
        data=[_alert_to_dict(a) for a in rows],
        meta=Meta(total=int(total), page=page, per_page=per_page),
        errors=[],
    )


@router.post("/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: str,
    db: DBSessionDep,
) -> APIResponse[dict]:
    """Mark an alert as acknowledged. Idempotent — re-acking is a no-op
    that returns the current state."""

    alert = (
        await db.execute(select(Alert).where(Alert.id == alert_id))
    ).scalar_one_or_none()
    if alert is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "alert not found")

    if not alert.acknowledged:
        alert.acknowledged = True
        alert.acknowledged_at = datetime.now(timezone.utc)
        alert.acknowledged_by = "system"  # TODO: replace once auth lands.
        await db.commit()
        await db.refresh(alert)

    return ok(_alert_to_dict(alert))


@router.post("/{alert_id}/dismiss")
async def dismiss_alert(
    alert_id: str,
    db: DBSessionDep,
    reason: str | None = Query(None, max_length=255),
) -> APIResponse[dict]:
    """Dismiss an alert. Persisted as ``acknowledged=true`` with the
    dismissal reason captured in ``extra.dismissal_reason``."""

    alert = (
        await db.execute(select(Alert).where(Alert.id == alert_id))
    ).scalar_one_or_none()
    if alert is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "alert not found")

    alert.acknowledged = True
    alert.acknowledged_at = datetime.now(timezone.utc)
    alert.acknowledged_by = "system"
    extra = dict(alert.extra or {})
    extra["dismissed"] = True
    if reason:
        extra["dismissal_reason"] = reason
    alert.extra = extra
    await db.commit()
    await db.refresh(alert)

    return ok(_alert_to_dict(alert))
