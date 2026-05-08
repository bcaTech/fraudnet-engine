"""NOC dashboard endpoints.

These power the FraudNet NOC Dashboard's home view. All responses are wrapped
in :class:`APIResponse` and cached aggressively (5–30s TTL via Redis at the
service layer — TODO when the cache module lands).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Query
from sqlalchemy import func, select

from api.dependencies import DBSessionDep, Neo4jDep
from api.schemas import APIResponse, Meta, ok
from core.graph.queries import (
    CLUSTER_OVERVIEW,
    DASHBOARD_METRICS,
)
from db.models import Alert, Takedown

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/metrics")
async def get_metrics(neo4j: Neo4jDep) -> APIResponse[dict]:
    """KPI summary for the NOC dashboard top-of-page tiles."""

    rows = await neo4j.execute_read(DASHBOARD_METRICS)
    if not rows:
        payload = {
            "active_clusters": 0,
            "wallets_under_review": 0,
            "high_risk_agents": 0,
            "takedowns_completed": 0,
            "estimated_fraud_value": 0.0,
        }
    else:
        r = rows[0]
        payload = {
            "active_clusters": int(r.get("active_clusters") or 0),
            "wallets_under_review": int(r.get("wallets_under_review") or 0),
            "high_risk_agents": int(r.get("high_risk_agents") or 0),
            "takedowns_completed": int(r.get("takedowns_completed") or 0),
            "estimated_fraud_value": float(r.get("estimated_fraud_value") or 0.0),
        }
    payload["generated_at"] = datetime.now(timezone.utc).isoformat()
    return ok(payload)


@router.get("/cluster-overview")
async def get_cluster_overview(
    neo4j: Neo4jDep,
    limit: int = Query(20, ge=1, le=100),
) -> APIResponse[list[dict]]:
    """Mini-graph metadata for the dashboard's cluster strip."""

    rows = await neo4j.execute_read(CLUSTER_OVERVIEW, {"limit": limit})
    payload = [
        {
            **(row.get("cluster") or {}),
            "member_count": int(row.get("member_count") or 0),
        }
        for row in rows
    ]
    return ok(payload)


@router.get("/alert-feed")
async def get_alert_feed(
    db: DBSessionDep,
    page: int = Query(1, ge=1),
    per_page: int = Query(20, ge=1, le=100),
    severity: str | None = Query(None, pattern="^(low|medium|high|critical)$"),
    acknowledged: bool | None = None,
) -> APIResponse[list[dict]]:
    """Recent alert feed, most-recent-first.

    Mirrors a slice of ``/api/alerts`` shaped for the NOC home tile. Filter by
    ``severity`` and/or ``acknowledged`` state.
    """

    base = select(Alert)
    if severity:
        base = base.where(Alert.severity == severity)
    if acknowledged is not None:
        base = base.where(Alert.acknowledged == acknowledged)

    total = (
        await db.execute(select(func.count()).select_from(base.subquery()))
    ).scalar_one()
    rows = (
        (
            await db.execute(
                base.order_by(Alert.created_at.desc())
                .offset((page - 1) * per_page)
                .limit(per_page)
            )
        )
        .scalars()
        .all()
    )

    payload = [
        {
            "id": a.id,
            "created_at": a.created_at.isoformat() if a.created_at else None,
            "type": a.type,
            "severity": a.severity,
            "title": a.title,
            "target_type": a.target_type,
            "target_id": a.target_id,
            "cluster_id": a.cluster_id,
            "acknowledged": a.acknowledged,
        }
        for a in rows
    ]
    return APIResponse(
        data=payload,
        meta=Meta(total=int(total), page=page, per_page=per_page),
        errors=[],
    )


@router.get("/activity-timeline")
async def get_activity_timeline(
    neo4j: Neo4jDep,
    hours: int = Query(24, ge=1, le=168),
) -> APIResponse[dict]:
    """Hourly transaction volume + fraud overlay for the last ``hours``.

    Returns two parallel series suitable for direct charting.
    """

    since = datetime.now(timezone.utc) - timedelta(hours=hours)
    rows = await neo4j.execute_read(
        """
        MATCH (t:Transaction)
        WHERE t.timestamp >= datetime($since)
        WITH datetime({
            year: t.timestamp.year,
            month: t.timestamp.month,
            day: t.timestamp.day,
            hour: t.timestamp.hour
        }) AS bucket,
        coalesce(t.flagged, false) AS flagged
        RETURN bucket,
               count(*)                        AS total,
               sum(CASE WHEN flagged THEN 1 ELSE 0 END) AS flagged_count
        ORDER BY bucket ASC
        """,
        {"since": since.isoformat()},
    )
    series = {
        "buckets": [str(r["bucket"]) for r in rows],
        "total": [int(r["total"]) for r in rows],
        "flagged": [int(r["flagged_count"]) for r in rows],
    }
    return ok({"window_hours": hours, **series})


@router.get("/recent-takedowns")
async def get_recent_takedowns(
    db: DBSessionDep,
    limit: int = Query(10, ge=1, le=50),
) -> APIResponse[list[dict]]:
    """Latest takedowns, most-recently-initiated first."""

    rows = (
        (
            await db.execute(
                select(Takedown)
                .order_by(Takedown.initiated_at.desc())
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )
    payload = [
        {
            "id": t.id,
            "cluster_id": t.cluster_id,
            "status": t.status,
            "initiated_at": t.initiated_at.isoformat() if t.initiated_at else None,
            "completed_at": t.completed_at.isoformat() if t.completed_at else None,
            "wallets_frozen": t.wallets_frozen,
            "sims_flagged": t.sims_flagged,
            "agents_alerted": t.agents_alerted,
            "summary": t.summary,
        }
        for t in rows
    ]
    return APIResponse(
        data=payload,
        meta=Meta(total=len(payload), extra={"limit": limit}),
        errors=[],
    )
