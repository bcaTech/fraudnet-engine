"""Agent endpoints: list, GeoJSON map, and detail.

Agents are the cashout layer of the fraud network — the ``classification``
field labels them on the ``clean → incidental → exploited → complicit``
spectrum. The map endpoint returns a GeoJSON ``FeatureCollection`` so the
frontend can drop the data straight into Mapbox / MapLibre layers.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, status

from api.auth.rbac import ROLE_INVESTIGATOR, require_role
from api.dependencies import Neo4jDep
from api.schemas import APIResponse, Meta, ok
from core.graph.queries import GET_AGENT

router = APIRouter(prefix="/api/agents", tags=["agents"])


_RISK_BUCKETS = [
    (0.80, "complicit"),
    (0.55, "exploited"),
    (0.30, "incidental"),
    (0.0, "clean"),
]


def _risk_band(score: float) -> str:
    for threshold, label in _RISK_BUCKETS:
        if score >= threshold:
            return label
    return "clean"


# ---------------------------------------------------------------------------
# Map (GeoJSON) — declared before /{agent_id} so it isn't shadowed
# ---------------------------------------------------------------------------


@router.get("/map")
async def agents_map(
    neo4j: Neo4jDep,
    classification: str | None = Query(None, pattern="^(clean|incidental|exploited|complicit)$"),
    min_risk: float | None = Query(None, ge=0.0, le=1.0),
) -> APIResponse[dict[str, Any]]:
    """GeoJSON ``FeatureCollection`` for the agent risk map."""

    rows = await neo4j.execute_read(
        """
        MATCH (a:Agent)
        WHERE ($classification IS NULL OR a.classification = $classification)
          AND ($min_risk IS NULL OR coalesce(a.risk_score, 0.0) >= $min_risk)
          AND a.lat IS NOT NULL AND a.lng IS NOT NULL
        OPTIONAL MATCH (a)-[r:LINKED_TO]->(c:Cluster)
        WITH a, collect(DISTINCT c.cluster_id) AS clusters,
             coalesce(sum(r.fraud_cashout_count), 0) AS fraud_cashouts
        RETURN
            a.agent_id AS agent_id,
            a.name AS name,
            a.lat AS lat,
            a.lng AS lng,
            a.area_name AS area_name,
            coalesce(a.risk_score, 0.0) AS risk_score,
            a.classification AS classification,
            coalesce(a.suspended, false) AS suspended,
            clusters,
            fraud_cashouts
        """,
        {"classification": classification, "min_risk": min_risk},
    )

    features = [
        {
            "type": "Feature",
            "id": r["agent_id"],
            "geometry": {
                "type": "Point",
                "coordinates": [float(r["lng"]), float(r["lat"])],
            },
            "properties": {
                "agent_id": r["agent_id"],
                "name": r["name"],
                "area": r.get("area_name"),
                "risk_score": float(r.get("risk_score") or 0.0),
                "risk_band": _risk_band(float(r.get("risk_score") or 0.0)),
                "classification": r.get("classification"),
                "suspended": bool(r.get("suspended")),
                "clusters": r.get("clusters") or [],
                "fraud_cashout_count": int(r.get("fraud_cashouts") or 0),
            },
        }
        for r in rows
    ]
    return ok({"type": "FeatureCollection", "features": features})


@router.get("")
async def list_agents(
    neo4j: Neo4jDep,
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
    classification: str | None = Query(None, pattern="^(clean|incidental|exploited|complicit)$"),
    area: str | None = None,
    min_risk: float | None = Query(None, ge=0.0, le=1.0),
    suspended: bool | None = None,
    sort_by: str = Query("risk_score", pattern="^(risk_score|monthly_volume|fraud_cashout_rate)$"),
) -> APIResponse[list[dict[str, Any]]]:
    """List agents with the usual filter knobs. Sorted high-risk-first."""

    skip = (page - 1) * per_page
    params = {
        "classification": classification,
        "area": area,
        "min_risk": min_risk,
        "suspended": suspended,
        "skip": skip,
        "limit": per_page,
    }
    sort_clause = {
        "risk_score": "coalesce(a.risk_score, 0.0)",
        "monthly_volume": "coalesce(a.monthly_volume, 0.0)",
        "fraud_cashout_rate": "coalesce(a.fraud_cashout_rate, 0.0)",
    }[sort_by]

    rows = await neo4j.execute_read(
        f"""
        MATCH (a:Agent)
        WHERE ($classification IS NULL OR a.classification = $classification)
          AND ($area IS NULL OR a.area_name = $area)
          AND ($min_risk IS NULL OR coalesce(a.risk_score, 0.0) >= $min_risk)
          AND ($suspended IS NULL OR coalesce(a.suspended, false) = $suspended)
        RETURN a {{
            .agent_id, .name, .lat, .lng, .area_name, .registration_date,
            .risk_score, .classification, .monthly_volume, .fraud_cashout_rate,
            .float_avg, .suspended, .suspension_date
        }} AS agent
        ORDER BY {sort_clause} DESC
        SKIP $skip
        LIMIT $limit
        """,  # noqa: S608 — sort_clause is from a hard-coded allow-list above.
        params,
    )

    total = await neo4j.execute_read(
        """
            MATCH (a:Agent)
            WHERE ($classification IS NULL OR a.classification = $classification)
              AND ($area IS NULL OR a.area_name = $area)
              AND ($min_risk IS NULL OR coalesce(a.risk_score, 0.0) >= $min_risk)
              AND ($suspended IS NULL OR coalesce(a.suspended, false) = $suspended)
            RETURN count(a) AS n
            """,
        {k: v for k, v in params.items() if k not in ("skip", "limit")},
    )
    total_n = int(total[0]["n"]) if total else 0

    return APIResponse(
        data=[r["agent"] for r in rows],
        meta=Meta(total=total_n, page=page, per_page=per_page),
        errors=[],
    )


# ---------------------------------------------------------------------------
# Detail / cashout patterns
# ---------------------------------------------------------------------------


@router.get("/{agent_id}")
async def get_agent(agent_id: str, neo4j: Neo4jDep) -> APIResponse[dict[str, Any]]:
    rows = await neo4j.execute_read(GET_AGENT, {"agent_id": agent_id})
    if not rows:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return ok(
        {
            **(rows[0]["agent"] or {}),
            "linked_clusters": rows[0].get("linked_clusters") or [],
        }
    )


@router.get("/{agent_id}/cashout-patterns")
async def cashout_patterns(
    agent_id: str,
    neo4j: Neo4jDep,
    days: int = Query(30, ge=1, le=180),
) -> APIResponse[dict[str, Any]]:
    """Hour-of-day × day-of-week heat-map for the agent's cashout activity."""

    rows = await neo4j.execute_read(
        """
        MATCH (a:Agent {agent_id: $agent_id})
        OPTIONAL MATCH (w:Wallet)-[r:CASHED_OUT_AT]->(a)
        WHERE r.timestamp >= datetime() - duration({days: $days})
        WITH r.timestamp.dayOfWeek AS dow, r.timestamp.hour AS hour, r.amount AS amount
        WHERE dow IS NOT NULL
        RETURN dow, hour, count(*) AS tx_count, sum(coalesce(amount, 0.0)) AS volume
        ORDER BY dow, hour
        """,
        {"agent_id": agent_id, "days": days},
    )
    if not rows:
        # Confirm the agent at least exists, so we can return a friendly empty grid.
        check = await neo4j.execute_read(GET_AGENT, {"agent_id": agent_id})
        if not check:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")

    grid = [
        {
            "dow": int(r["dow"]),
            "hour": int(r["hour"]),
            "tx_count": int(r["tx_count"] or 0),
            "volume": float(r["volume"] or 0.0),
        }
        for r in rows
    ]
    return ok({"agent_id": agent_id, "window_days": days, "buckets": grid})


# ---------------------------------------------------------------------------
# Mutations: suspend / warn
# ---------------------------------------------------------------------------


@router.post(
    "/{agent_id}/suspend",
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR))],
)
async def suspend_agent(agent_id: str, neo4j: Neo4jDep) -> APIResponse[dict[str, Any]]:
    rows = await neo4j.execute_write(
        """
        MATCH (a:Agent {agent_id: $agent_id})
        SET a.suspended = true,
            a.suspension_date = datetime()
        RETURN a {.agent_id, .name, .suspended, .suspension_date, .classification, .risk_score} AS agent
        """,
        {"agent_id": agent_id},
    )
    if not rows:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return ok(rows[0]["agent"] or {})


@router.post(
    "/{agent_id}/warn",
    dependencies=[Depends(require_role(ROLE_INVESTIGATOR))],
)
async def warn_agent(agent_id: str, neo4j: Neo4jDep) -> APIResponse[dict[str, Any]]:
    rows = await neo4j.execute_write(
        """
        MATCH (a:Agent {agent_id: $agent_id})
        SET a.warnings_count = coalesce(a.warnings_count, 0) + 1,
            a.last_warning_at = datetime()
        RETURN a {.agent_id, .name, .warnings_count, .last_warning_at} AS agent
        """,
        {"agent_id": agent_id},
    )
    if not rows:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "agent not found")
    return ok(rows[0]["agent"] or {})
