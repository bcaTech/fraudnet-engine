"""Victim restitution tracking.

Records that funds may need to be returned to victims after a
takedown. The function looks up wallets that received fund flows
*from* the cluster's wallets — those are the candidate victim sources
— and records them on the takedown's metadata for the analyst review.

Real implementation pulls the structured fund-trace from
:mod:`core.evidence.fund_trace` and posts a structured notice to the
MoMo BSS for each candidate. Here we just compute the candidates and
return them; the notification step is a stub.
"""

from __future__ import annotations

from typing import Any

from config.logging import get_logger
from core.graph.client import Neo4jClient, get_neo4j_client

logger = get_logger(__name__)


async def trace_restitution_candidates(
    cluster_id: str, *, since_days: int = 90,
    client: Neo4jClient | None = None,
) -> dict[str, Any]:
    """Find wallets that originated fund flows into the cluster — those
    are the most likely victim sources entitled to restitution."""

    c = client or get_neo4j_client()
    rows = await c.execute_read(
        """
        MATCH (cl:Cluster {cluster_id: $cluster_id})
        MATCH (member:Wallet)-[:BELONGS_TO]->(cl)
        MATCH (origin:Wallet)-[r:SENT_TO]->(member)
        WHERE r.timestamp >= datetime() - duration({days: $days})
          AND origin.cluster_id IS NULL
        WITH origin, sum(coalesce(r.amount, 0.0)) AS lost_amount,
             count(r) AS tx_count, max(r.timestamp) AS last_tx
        WHERE lost_amount > 0
        RETURN
            origin.wallet_id AS wallet_id,
            origin.msisdn    AS msisdn,
            lost_amount,
            tx_count,
            toString(last_tx) AS last_tx
        ORDER BY lost_amount DESC
        LIMIT 200
        """,
        {"cluster_id": cluster_id, "days": since_days},
    )
    candidates = [
        {
            "wallet_id": r.get("wallet_id"),
            "msisdn": r.get("msisdn"),
            "lost_amount": float(r.get("lost_amount") or 0.0),
            "tx_count": int(r.get("tx_count") or 0),
            "last_tx": r.get("last_tx"),
        }
        for r in rows
    ]
    total = sum(x["lost_amount"] for x in candidates)
    logger.info(
        "takedown.restitution.candidates",
        cluster_id=cluster_id,
        candidate_count=len(candidates),
        total_estimated=round(total, 2),
    )
    return {
        "cluster_id": cluster_id,
        "candidate_count": len(candidates),
        "total_estimated_loss": round(total, 2),
        "candidates": candidates,
    }
