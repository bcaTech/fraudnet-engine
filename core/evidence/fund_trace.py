"""Evidence-grade fund tracing.

Builds on :mod:`core.analytics.fund_flow` but enriches the result with
the full identity snapshot of every node along the path, the agent's
geographic context, and a per-path summary suitable for inclusion in a
law-enforcement evidence pack.
"""

from __future__ import annotations

from typing import Any

from core.analytics.fund_flow import paths as analytics_paths
from core.analytics.fund_flow import trace as analytics_trace
from core.graph.client import Neo4jClient, get_neo4j_client


async def trace_for_evidence(
    cluster_id: str,
    *,
    seed_wallet_ids: list[str] | None = None,
    max_paths: int = 10,
    max_depth: int = 5,
    since_days: int = 90,
    client: Neo4jClient | None = None,
) -> dict[str, Any]:
    """Compute fund-flow evidence for ``cluster_id``.

    If ``seed_wallet_ids`` is omitted we use the cluster's central wallets
    (highest-confidence members) as starting points.
    """

    c = client or get_neo4j_client()

    if not seed_wallet_ids:
        rows = await c.execute_read(
            """
            MATCH (cl:Cluster {cluster_id: $cluster_id})
            MATCH (w:Wallet)-[r:BELONGS_TO]->(cl)
            RETURN w.wallet_id AS wallet_id, r.role AS role,
                   coalesce(r.confidence, 0.0) AS confidence
            ORDER BY confidence DESC
            LIMIT 5
            """,
            {"cluster_id": cluster_id},
        )
        seed_wallet_ids = [r["wallet_id"] for r in rows if r.get("wallet_id")]

    sankey_per_seed: dict[str, dict[str, Any]] = {}
    paths_by_seed: dict[str, list[dict[str, Any]]] = {}
    total_traced_value = 0.0
    cashout_agents: dict[str, dict[str, Any]] = {}

    for seed in seed_wallet_ids:
        sankey = await analytics_trace(
            seed, max_depth=max_depth, since_days=since_days, client=c
        )
        sankey_per_seed[seed] = sankey
        seed_paths = await analytics_paths(
            seed,
            max_depth=max_depth,
            limit=max_paths,
            since_days=since_days,
            client=c,
        )
        paths_by_seed[seed] = seed_paths
        for p in seed_paths:
            total_traced_value += float(p.get("cashout_amount") or 0.0)
            agent_id = p.get("cashout_agent_id")
            if agent_id:
                bucket = cashout_agents.setdefault(
                    agent_id,
                    {
                        "agent_id": agent_id,
                        "area": p.get("cashout_area"),
                        "total_amount": 0.0,
                        "path_count": 0,
                    },
                )
                bucket["total_amount"] += float(p.get("cashout_amount") or 0.0)
                bucket["path_count"] += 1

    return {
        "cluster_id": cluster_id,
        "seed_wallets": seed_wallet_ids,
        "sankey_per_seed": sankey_per_seed,
        "paths_per_seed": paths_by_seed,
        "total_traced_value": round(total_traced_value, 2),
        "cashout_agents_summary": sorted(
            cashout_agents.values(), key=lambda a: a["total_amount"], reverse=True
        ),
    }
