"""Chronological event timeline for a cluster.

Walks a cluster's nodes and surfaces every dated touch-point — the seed
event, member-join dates, transactions involving members, agent
cash-outs, alert creations — in a single time-ordered list. Used by the
evidence builder and the investigator UI.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select

from core.graph.client import Neo4jClient, get_neo4j_client
from db.models import Alert, Takedown
from db.session import get_async_session


async def build_timeline(
    cluster_id: str,
    *,
    max_events: int = 500,
    client: Neo4jClient | None = None,
) -> list[dict[str, Any]]:
    """Return a chronologically-ordered list of events for ``cluster_id``."""

    c = client or get_neo4j_client()
    events: list[dict[str, Any]] = []

    # Seed event from the cluster node itself.
    rows = await c.execute_read(
        """
        MATCH (cl:Cluster {cluster_id: $cluster_id})
        RETURN cl.seed_date AS ts, cl.seed_node_id AS node_id,
               cl.seed_type AS node_type, cl.confidence_score AS confidence,
               cl.name AS cluster_name
        """,
        {"cluster_id": cluster_id},
    )
    if rows:
        r = rows[0]
        events.append(
            {
                "kind": "cluster_seed",
                "timestamp": str(r.get("ts")) if r.get("ts") else None,
                "description": (
                    f"Cluster {r.get('cluster_name') or cluster_id} seeded from "
                    f"{r.get('node_type')} {r.get('node_id')} (confidence "
                    f"{float(r.get('confidence') or 0):.2f})."
                ),
                "metadata": {
                    "node_id": r.get("node_id"),
                    "node_type": r.get("node_type"),
                    "confidence": float(r.get("confidence") or 0.0),
                },
            }
        )

    # Internal SENT_TO transactions between cluster members.
    rows = await c.execute_read(
        """
        MATCH (cl:Cluster {cluster_id: $cluster_id})
        MATCH (a:Wallet)-[:BELONGS_TO]->(cl)
        MATCH (b:Wallet)-[:BELONGS_TO]->(cl)
        MATCH (a)-[r:SENT_TO]->(b)
        RETURN
            a.wallet_id AS src,
            b.wallet_id AS dst,
            r.tx_id AS tx_id,
            r.amount AS amount,
            r.timestamp AS ts
        ORDER BY r.timestamp ASC
        LIMIT $limit
        """,
        {"cluster_id": cluster_id, "limit": max_events // 2},
    )
    for r in rows:
        events.append(
            {
                "kind": "transaction",
                "timestamp": str(r.get("ts")) if r.get("ts") else None,
                "description": (f"{r.get('src')} → {r.get('dst')} GHS {float(r.get('amount') or 0):.2f}"),
                "metadata": {
                    "tx_id": r.get("tx_id"),
                    "src_wallet_id": r.get("src"),
                    "dst_wallet_id": r.get("dst"),
                    "amount": float(r.get("amount") or 0.0),
                },
            }
        )

    # Cashouts at agents linked to this cluster.
    rows = await c.execute_read(
        """
        MATCH (cl:Cluster {cluster_id: $cluster_id})
        MATCH (w:Wallet)-[:BELONGS_TO]->(cl)
        MATCH (w)-[r:CASHED_OUT_AT]->(a:Agent)
        RETURN w.wallet_id AS wallet_id,
               a.agent_id AS agent_id,
               a.area_name AS area,
               r.amount AS amount,
               r.timestamp AS ts
        ORDER BY r.timestamp ASC
        LIMIT $limit
        """,
        {"cluster_id": cluster_id, "limit": max_events // 4},
    )
    for r in rows:
        events.append(
            {
                "kind": "cashout",
                "timestamp": str(r.get("ts")) if r.get("ts") else None,
                "description": (
                    f"{r.get('wallet_id')} cashed out GHS "
                    f"{float(r.get('amount') or 0):.2f} at agent "
                    f"{r.get('agent_id')} ({r.get('area') or '?'})"
                ),
                "metadata": {
                    "wallet_id": r.get("wallet_id"),
                    "agent_id": r.get("agent_id"),
                    "amount": float(r.get("amount") or 0.0),
                    "area": r.get("area"),
                },
            }
        )

    # Workflow events from Postgres: alerts touching this cluster, takedowns.
    async with get_async_session() as db:
        alerts = (
            (
                await db.execute(
                    select(Alert)
                    .where(Alert.cluster_id == cluster_id)
                    .order_by(Alert.created_at.asc())
                    .limit(max_events // 4)
                )
            )
            .scalars()
            .all()
        )
        for a in alerts:
            events.append(
                {
                    "kind": "alert",
                    "timestamp": a.created_at.isoformat() if a.created_at else None,
                    "description": f"[{a.severity}] {a.title}",
                    "metadata": {
                        "alert_id": a.id,
                        "type": a.type,
                        "severity": a.severity,
                        "target_type": a.target_type,
                        "target_id": a.target_id,
                        "acknowledged": a.acknowledged,
                    },
                }
            )

        takedowns = (
            (
                await db.execute(
                    select(Takedown)
                    .where(Takedown.cluster_id == cluster_id)
                    .order_by(Takedown.initiated_at.asc())
                )
            )
            .scalars()
            .all()
        )
        for t in takedowns:
            events.append(
                {
                    "kind": "takedown_initiated",
                    "timestamp": t.initiated_at.isoformat() if t.initiated_at else None,
                    "description": f"Takedown {t.id} initiated (status: {t.status}).",
                    "metadata": {"takedown_id": t.id, "status": t.status},
                }
            )
            if t.completed_at is not None:
                events.append(
                    {
                        "kind": "takedown_completed",
                        "timestamp": t.completed_at.isoformat(),
                        "description": (
                            f"Takedown {t.id} completed. "
                            f"Wallets frozen: {t.wallets_frozen}, "
                            f"SIMs flagged: {t.sims_flagged}, "
                            f"agents alerted: {t.agents_alerted}."
                        ),
                        "metadata": {
                            "takedown_id": t.id,
                            "wallets_frozen": t.wallets_frozen,
                            "sims_flagged": t.sims_flagged,
                            "agents_alerted": t.agents_alerted,
                        },
                    }
                )

    # Sort by ISO timestamp string (works for ISO-8601), undated last.
    events.sort(key=lambda e: e.get("timestamp") or "9999")
    return events[:max_events]
