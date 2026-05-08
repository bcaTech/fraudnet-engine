"""Analytics endpoints — KPI rollups, time series, and distributions.

These power the analytics dashboard's headline numbers and charts. All
queries go through the same Neo4j read path as the live dashboard, so
the shapes are interchangeable on the frontend (just longer time
windows).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Query
from sqlalchemy import func, select

from api.dependencies import DBSessionDep, Neo4jDep
from api.schemas import APIResponse, ok
from db.models import Alert, Rule, RuleTrigger, Takedown

router = APIRouter(prefix="/api/analytics", tags=["analytics"])


# ---------------------------------------------------------------------------
# KPIs
# ---------------------------------------------------------------------------


@router.get("/kpis")
async def get_kpis(
    neo4j: Neo4jDep,
    db: DBSessionDep,
    days: int = Query(30, ge=1, le=365),
) -> APIResponse[dict[str, Any]]:
    """Headline KPIs for the analytics dashboard."""

    since = datetime.now(timezone.utc) - timedelta(days=days)

    graph_rows = await neo4j.execute_read(
        """
        CALL {
            MATCH (c:Cluster)
            WHERE datetime(c.seed_date) >= datetime($since)
            RETURN count(c) AS new_clusters,
                   sum(coalesce(c.estimated_fraud_value, 0.0)) AS new_fraud_value
        }
        CALL {
            MATCH (c:Cluster)
            WHERE c.status IN ['active', 'investigating', 'takedown_pending']
            RETURN count(c) AS active_clusters,
                   sum(coalesce(c.estimated_fraud_value, 0.0)) AS active_fraud_value
        }
        CALL {
            MATCH (w:Wallet) WHERE w.status = 'frozen'
            RETURN count(w) AS frozen_wallets
        }
        CALL {
            MATCH (w:Wallet) WHERE w.status = 'flagged'
            RETURN count(w) AS flagged_wallets
        }
        CALL {
            MATCH (a:Agent) WHERE a.classification IN ['exploited', 'complicit']
            RETURN count(a) AS high_risk_agents
        }
        CALL {
            MATCH (t:Transaction)
            WHERE t.timestamp >= datetime($since)
            RETURN count(t) AS tx_total,
                   sum(CASE WHEN t.flagged THEN 1 ELSE 0 END) AS tx_flagged,
                   sum(coalesce(t.amount, 0.0)) AS tx_volume
        }
        RETURN new_clusters, new_fraud_value,
               active_clusters, active_fraud_value,
               frozen_wallets, flagged_wallets, high_risk_agents,
               tx_total, tx_flagged, tx_volume
        """,
        {"since": since.isoformat()},
    )
    g = graph_rows[0] if graph_rows else {}

    # Postgres counts: alerts in window, rule triggers in window, completed takedowns.
    alerts_total = (
        await db.execute(
            select(func.count(Alert.id)).where(Alert.created_at >= since)
        )
    ).scalar_one()
    alerts_critical = (
        await db.execute(
            select(func.count(Alert.id)).where(
                Alert.created_at >= since, Alert.severity == "critical"
            )
        )
    ).scalar_one()
    triggers_total = (
        await db.execute(
            select(func.count(RuleTrigger.id)).where(
                RuleTrigger.triggered_at >= since
            )
        )
    ).scalar_one()
    takedowns_completed = (
        await db.execute(
            select(func.count(Takedown.id)).where(
                Takedown.status == "completed", Takedown.completed_at >= since
            )
        )
    ).scalar_one()
    rules_live = (
        await db.execute(select(func.count(Rule.id)).where(Rule.status == "live"))
    ).scalar_one()

    return ok(
        {
            "window_days": days,
            "since": since.isoformat(),
            "graph": {
                "new_clusters": int(g.get("new_clusters") or 0),
                "new_fraud_value": float(g.get("new_fraud_value") or 0.0),
                "active_clusters": int(g.get("active_clusters") or 0),
                "active_fraud_value": float(g.get("active_fraud_value") or 0.0),
                "frozen_wallets": int(g.get("frozen_wallets") or 0),
                "flagged_wallets": int(g.get("flagged_wallets") or 0),
                "high_risk_agents": int(g.get("high_risk_agents") or 0),
                "tx_total": int(g.get("tx_total") or 0),
                "tx_flagged": int(g.get("tx_flagged") or 0),
                "tx_volume": float(g.get("tx_volume") or 0.0),
            },
            "workflow": {
                "alerts_total": int(alerts_total),
                "alerts_critical": int(alerts_critical),
                "rule_triggers": int(triggers_total),
                "takedowns_completed": int(takedowns_completed),
                "rules_live": int(rules_live),
            },
        }
    )


# ---------------------------------------------------------------------------
# Time series
# ---------------------------------------------------------------------------


@router.get("/clusters-over-time")
async def clusters_over_time(
    neo4j: Neo4jDep,
    days: int = Query(30, ge=1, le=365),
) -> APIResponse[dict[str, Any]]:
    """Daily count of clusters seeded over the window."""

    since = datetime.now(timezone.utc) - timedelta(days=days)
    rows = await neo4j.execute_read(
        """
        MATCH (c:Cluster)
        WITH c, datetime(c.seed_date) AS seeded
        WHERE seeded >= datetime($since)
        WITH date(seeded) AS day, c
        RETURN day,
               count(c) AS new_clusters,
               sum(coalesce(c.estimated_fraud_value, 0.0)) AS fraud_value
        ORDER BY day ASC
        """,
        {"since": since.isoformat()},
    )
    return ok(
        {
            "window_days": days,
            "buckets": [str(r["day"]) for r in rows],
            "new_clusters": [int(r["new_clusters"] or 0) for r in rows],
            "fraud_value": [float(r["fraud_value"] or 0.0) for r in rows],
        }
    )


@router.get("/fraud-value")
async def fraud_value_over_time(
    neo4j: Neo4jDep,
    days: int = Query(30, ge=1, le=365),
) -> APIResponse[dict[str, Any]]:
    """Daily fraud-value prevented = sum of flagged transaction amounts.

    Proxy until evidence packages can attribute exact recoveries.
    """

    since = datetime.now(timezone.utc) - timedelta(days=days)
    rows = await neo4j.execute_read(
        """
        MATCH (t:Transaction)
        WHERE t.timestamp >= datetime($since) AND t.flagged = true
        WITH date(t.timestamp) AS day, t
        RETURN day,
               count(t) AS flagged_count,
               sum(coalesce(t.amount, 0.0)) AS flagged_value
        ORDER BY day ASC
        """,
        {"since": since.isoformat()},
    )
    return ok(
        {
            "window_days": days,
            "buckets": [str(r["day"]) for r in rows],
            "flagged_count": [int(r["flagged_count"] or 0) for r in rows],
            "flagged_value": [float(r["flagged_value"] or 0.0) for r in rows],
        }
    )


# ---------------------------------------------------------------------------
# Distributions
# ---------------------------------------------------------------------------


@router.get("/seed-sources")
async def seed_sources(neo4j: Neo4jDep) -> APIResponse[dict[str, int]]:
    """Distribution of cluster seeds by ``seed_type``."""

    rows = await neo4j.execute_read(
        """
        MATCH (c:Cluster)
        RETURN c.seed_type AS seed_type, count(c) AS n
        ORDER BY n DESC
        """
    )
    return ok({(r["seed_type"] or "unknown"): int(r["n"]) for r in rows})


@router.get("/agent-classification")
async def agent_classification(neo4j: Neo4jDep) -> APIResponse[dict[str, int]]:
    rows = await neo4j.execute_read(
        """
        MATCH (a:Agent)
        RETURN coalesce(a.classification, 'unknown') AS classification,
               count(a) AS n
        ORDER BY n DESC
        """
    )
    return ok({r["classification"]: int(r["n"]) for r in rows})


@router.get("/top-nodes")
async def top_nodes(
    neo4j: Neo4jDep,
    metric: str = Query("risk_score", pattern="^(risk_score|degree|fraud_volume)$"),
    limit: int = Query(20, ge=1, le=100),
) -> APIResponse[list[dict[str, Any]]]:
    """Top-N highest-risk wallets / most-connected wallets / biggest cashout
    agents, depending on ``metric``."""

    if metric == "risk_score":
        rows = await neo4j.execute_read(
            """
            MATCH (w:Wallet)
            RETURN w.wallet_id AS id, 'wallet' AS type,
                   coalesce(w.risk_score, 0.0) AS score,
                   w.cluster_id AS cluster_id, w.status AS status, w.msisdn AS msisdn
            ORDER BY score DESC
            LIMIT $limit
            """,
            {"limit": limit},
        )
        return ok(
            [
                {
                    "id": r["id"],
                    "type": r["type"],
                    "score": float(r["score"] or 0.0),
                    "cluster_id": r.get("cluster_id"),
                    "status": r.get("status"),
                    "msisdn": r.get("msisdn"),
                }
                for r in rows
            ]
        )

    if metric == "degree":
        rows = await neo4j.execute_read(
            """
            MATCH (w:Wallet)
            WITH w, COUNT { (w)-[]-() } AS degree
            RETURN w.wallet_id AS id, 'wallet' AS type,
                   degree AS score,
                   w.cluster_id AS cluster_id, w.status AS status
            ORDER BY score DESC
            LIMIT $limit
            """,
            {"limit": limit},
        )
        return ok(
            [
                {
                    "id": r["id"],
                    "type": r["type"],
                    "score": int(r["score"] or 0),
                    "cluster_id": r.get("cluster_id"),
                    "status": r.get("status"),
                }
                for r in rows
            ]
        )

    # fraud_volume — agents ranked by inbound flagged-cashout total.
    rows = await neo4j.execute_read(
        """
        MATCH (w:Wallet)-[r:CASHED_OUT_AT]->(a:Agent)
        WHERE w.cluster_id IS NOT NULL
        WITH a, sum(coalesce(r.amount, 0.0)) AS volume, count(r) AS tx_count
        RETURN a.agent_id AS id, 'agent' AS type, volume AS score,
               tx_count, a.classification AS status, a.area_name AS area
        ORDER BY score DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )
    return ok(
        [
            {
                "id": r["id"],
                "type": r["type"],
                "score": float(r["score"] or 0.0),
                "tx_count": int(r["tx_count"] or 0),
                "status": r.get("status"),
                "area": r.get("area"),
            }
            for r in rows
        ]
    )
