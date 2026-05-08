"""Centrality metrics for cluster subgraphs.

Identifies the structurally important nodes inside a cluster — the
likely coordinators, mules, and hubs that should be the focus of an
investigator's attention.

Computed metrics: degree, betweenness, eigenvector, PageRank. The
expensive ones (betweenness, eigenvector) are skipped on large graphs
to keep the scheduled batch under budget.
"""

from __future__ import annotations

import asyncio
from typing import Any

import networkx as nx

from config.logging import get_logger
from core.graph.client import Neo4jClient, get_neo4j_client

from ._graph_loader import load_cluster_subgraph

logger = get_logger(__name__)


_BETWEENNESS_NODE_CAP = 500
_EIGENVECTOR_NODE_CAP = 500


def _compute_metrics(g: nx.MultiDiGraph) -> dict[str, dict[str, float]]:
    if g.number_of_nodes() == 0:
        return {}
    h = nx.DiGraph()
    h.add_nodes_from(g.nodes())
    for u, v, data in g.edges(data=True):
        w = float(data.get("strength") or 0.0) + 1.0
        if h.has_edge(u, v):
            h[u][v]["weight"] += w
        else:
            h.add_edge(u, v, weight=w)

    metrics: dict[str, dict[str, float]] = {
        "degree": {n: float(h.degree(n)) for n in h.nodes()},
    }

    # PageRank is cheap and well-defined on directed graphs.
    try:
        metrics["pagerank"] = nx.pagerank(h, alpha=0.85, weight="weight")
    except Exception as exc:  # noqa: BLE001
        logger.warning("analytics.centrality.pagerank_failed", error=str(exc))

    # Betweenness on the undirected projection (cheaper, more analyst-meaningful
    # for "who's the bottleneck"). Cap node count to avoid runaway runs.
    if h.number_of_nodes() <= _BETWEENNESS_NODE_CAP:
        try:
            u_g = h.to_undirected()
            metrics["betweenness"] = nx.betweenness_centrality(u_g, weight="weight")
        except Exception as exc:  # noqa: BLE001
            logger.warning("analytics.centrality.betweenness_failed", error=str(exc))

    # Eigenvector — only on the largest weakly-connected component to stay
    # numerically stable, and only on smaller graphs.
    if h.number_of_nodes() <= _EIGENVECTOR_NODE_CAP:
        try:
            largest = max(nx.weakly_connected_components(h), key=len)
            sub = h.subgraph(largest).copy()
            # Power-iteration eigenvector centrality on the directed graph.
            ec = nx.eigenvector_centrality_numpy(sub, weight="weight")
            # Pad missing nodes with 0.
            metrics["eigenvector"] = {n: float(ec.get(n, 0.0)) for n in h.nodes()}
        except Exception as exc:  # noqa: BLE001
            logger.warning("analytics.centrality.eigenvector_failed", error=str(exc))

    return metrics


async def _persist(
    cluster_id: str,
    metrics: dict[str, dict[str, float]],
    *,
    client: Neo4jClient | None = None,
) -> None:
    if not metrics:
        return
    c = client or get_neo4j_client()
    # Build per-node update rows.
    node_ids: set[str] = set()
    for m in metrics.values():
        node_ids.update(m.keys())
    rows = []
    for node_id in node_ids:
        rows.append(
            {
                "id": node_id,
                "degree": float(metrics.get("degree", {}).get(node_id, 0.0)),
                "pagerank": float(metrics.get("pagerank", {}).get(node_id, 0.0)),
                "betweenness": float(metrics.get("betweenness", {}).get(node_id, 0.0)),
                "eigenvector": float(metrics.get("eigenvector", {}).get(node_id, 0.0)),
            }
        )
    if not rows:
        return
    await c.execute_write(
        """
        UNWIND $rows AS row
        MATCH (n)
        WHERE n.wallet_id = row.id OR n.imei = row.id OR n.imsi = row.id
           OR n.msisdn  = row.id OR n.agent_id = row.id
        SET n.centrality_degree = row.degree,
            n.centrality_pagerank = row.pagerank,
            n.centrality_betweenness = row.betweenness,
            n.centrality_eigenvector = row.eigenvector
        """,
        {"rows": rows},
    )


def _summarise(metrics: dict[str, dict[str, float]], top_n: int = 5) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for name, scores in metrics.items():
        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
        summary[f"top_{name}"] = [{"id": k, "score": round(v, 5)} for k, v in ranked]
    return summary


async def compute_for_cluster(cluster_id: str, *, persist: bool = True) -> dict[str, Any]:
    g = await load_cluster_subgraph(cluster_id)
    if g.number_of_nodes() == 0:
        return {"cluster_id": cluster_id, "skipped": True}
    metrics = await asyncio.to_thread(_compute_metrics, g)
    if persist:
        await _persist(cluster_id, metrics)
    return {
        "cluster_id": cluster_id,
        "node_count": g.number_of_nodes(),
        "edge_count": g.number_of_edges(),
        **_summarise(metrics),
    }


async def compute_for_active_clusters(*, limit: int = 30) -> dict[str, Any]:
    client = get_neo4j_client()
    rows = await client.execute_read(
        """
        MATCH (c:Cluster)
        WHERE c.status IN ['active', 'investigating', 'takedown_pending']
        RETURN c.cluster_id AS cluster_id
        ORDER BY coalesce(c.confidence_score, 0.0) DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )
    results = []
    for row in rows:
        try:
            results.append(await compute_for_cluster(row["cluster_id"]))
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "analytics.centrality.cluster_failed",
                cluster_id=row["cluster_id"],
                error=str(exc),
            )
            results.append({"cluster_id": row["cluster_id"], "error": str(exc)})
    return {"clusters_processed": len(results), "results": results}
