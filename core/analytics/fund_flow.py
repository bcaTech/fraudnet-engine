"""Fund-flow tracing.

For an investigator: given a starting wallet (the "victim" or seed), trace
every fund path through intermediaries to a cash-out point. Output is
shaped two ways:

- :func:`trace` — a Sankey-friendly ``{nodes, links}`` payload, ready for
  the frontend's fund-flow visualisation.
- :func:`paths` — a list of explicit, ranked fund paths
  (victim → mules → cashout agent), useful for evidence packs and the
  law-enforcement export.
"""

from __future__ import annotations

from typing import Any

from config.logging import get_logger
from core.graph.client import Neo4jClient, get_neo4j_client

logger = get_logger(__name__)


async def trace(
    seed_wallet_id: str,
    *,
    max_depth: int = 5,
    since_days: int = 30,
    client: Neo4jClient | None = None,
) -> dict[str, Any]:
    """Sankey ``{nodes, links}`` rooted at ``seed_wallet_id``.

    Edges are aggregated across multiple transactions between the same
    pair so the diagram stays readable.
    """

    c = client or get_neo4j_client()
    rows = await c.execute_read(
        """
        MATCH path = (seed:Wallet {wallet_id: $seed})-[:SENT_TO*1..$max_depth]->(end)
        WHERE all(rel IN relationships(path)
                  WHERE rel.timestamp >= datetime() - duration({days: $since_days}))
        UNWIND relationships(path) AS r
        WITH startNode(r) AS src, endNode(r) AS dst, r
        OPTIONAL MATCH (dst)-[c:CASHED_OUT_AT]->(a:Agent)
        WITH src, dst, r, collect(DISTINCT a.agent_id) AS cashout_agents
        RETURN
            coalesce(src.wallet_id, src.agent_id) AS source_id,
            labels(src)[0] AS source_kind,
            coalesce(dst.wallet_id, dst.agent_id) AS target_id,
            labels(dst)[0] AS target_kind,
            sum(coalesce(r.amount, 0.0)) AS total_amount,
            count(r) AS tx_count,
            cashout_agents
        """.replace("$max_depth", str(max(1, min(int(max_depth), 8)))),
        {"seed": seed_wallet_id, "since_days": since_days},
    )

    nodes: dict[str, dict[str, Any]] = {}
    links: list[dict[str, Any]] = []
    for r in rows:
        sid, tid = r.get("source_id"), r.get("target_id")
        if not sid or not tid:
            continue
        nodes.setdefault(sid, {"id": sid, "kind": r.get("source_kind")})
        nodes.setdefault(tid, {"id": tid, "kind": r.get("target_kind")})
        links.append(
            {
                "source": sid,
                "target": tid,
                "value": float(r.get("total_amount") or 0.0),
                "tx_count": int(r.get("tx_count") or 0),
                "downstream_cashout_agents": r.get("cashout_agents") or [],
            }
        )
    return {"seed": seed_wallet_id, "nodes": list(nodes.values()), "links": links}


async def paths(
    seed_wallet_id: str,
    *,
    max_depth: int = 5,
    min_amount: float = 0.0,
    limit: int = 20,
    since_days: int = 30,
    client: Neo4jClient | None = None,
) -> list[dict[str, Any]]:
    """Explicit ranked fund paths from ``seed_wallet_id`` to cash-out
    agents. Each path returns its sequence of wallets, the cash-out
    agent, and the bottleneck amount along the path."""

    c = client or get_neo4j_client()
    rows = await c.execute_read(
        f"""
        MATCH p = (seed:Wallet {{wallet_id: $seed}})
                  -[:SENT_TO*1..{max(1, min(int(max_depth), 8))}]->
                  (last:Wallet)-[c:CASHED_OUT_AT]->(a:Agent)
        WHERE all(rel IN relationships(p)
                  WHERE type(rel) <> 'CASHED_OUT_AT'
                    AND coalesce(rel.amount, 0.0) >= $min_amount
                    AND rel.timestamp >= datetime() - duration({{days: $since_days}}))
        WITH p, c, a,
             [n IN nodes(p) | coalesce(n.wallet_id, n.agent_id)] AS path_nodes,
             reduce(
                 m = 1.0e18,
                 r IN [r IN relationships(p) WHERE type(r) = 'SENT_TO']
                 | CASE WHEN coalesce(r.amount, 0.0) < m THEN coalesce(r.amount, 0.0) ELSE m END
             ) AS bottleneck
        RETURN
            path_nodes,
            a.agent_id AS cashout_agent_id,
            a.area_name AS cashout_area,
            coalesce(c.amount, 0.0) AS cashout_amount,
            bottleneck,
            length(p) - 1 AS hops
        ORDER BY cashout_amount DESC
        LIMIT $limit
        """,
        {
            "seed": seed_wallet_id,
            "min_amount": min_amount,
            "limit": limit,
            "since_days": since_days,
        },
    )
    return [
        {
            "path": r.get("path_nodes") or [],
            "hops": int(r.get("hops") or 0),
            "cashout_agent_id": r.get("cashout_agent_id"),
            "cashout_area": r.get("cashout_area"),
            "cashout_amount": float(r.get("cashout_amount") or 0.0),
            "bottleneck_amount": float(r.get("bottleneck") or 0.0),
        }
        for r in rows
    ]
