"""Community detection over cluster subgraphs.

Two algorithms are exposed:

- :func:`detect_louvain` — modularity-maximising Louvain. Stronger
  separation when the underlying graph has clear community structure;
  preferred for the scheduled batch run.
- :func:`detect_label_propagation` — fast streaming-friendly fallback.
  Used when Louvain is too expensive or when an analyst wants a
  fresh-but-cheap re-grouping.

Results are written to the graph as ``community_id`` properties on each
node so downstream queries (cluster detail, centrality) can filter by
sub-community.
"""

from __future__ import annotations

import asyncio
from typing import Any

import networkx as nx

from config.logging import get_logger
from core.graph.client import Neo4jClient, get_neo4j_client

from ._graph_loader import load_cluster_subgraph

logger = get_logger(__name__)


def _to_undirected(g: nx.MultiDiGraph) -> nx.Graph:
    """Collapse the multi-digraph to a simple undirected graph weighted by
    edge strength sums — what every NX community algorithm expects."""

    h: nx.Graph = nx.Graph()
    h.add_nodes_from(g.nodes(data=True))
    for u, v, data in g.edges(data=True):
        if h.has_edge(u, v):
            h[u][v]["weight"] += float(data.get("strength") or 0.0) + 1.0
        else:
            h.add_edge(u, v, weight=float(data.get("strength") or 0.0) + 1.0)
    return h


def _louvain_communities(g: nx.Graph) -> list[set[str]]:
    """Run NetworkX 3.x ``louvain_communities``. Falls back to Label
    Propagation if Louvain is unavailable in the installed version."""

    try:
        from networkx.algorithms.community import louvain_communities

        return [set(c) for c in louvain_communities(g, weight="weight", seed=42)]
    except Exception as exc:  # noqa: BLE001
        logger.warning("analytics.community.louvain_unavailable", error=str(exc))
        from networkx.algorithms.community import label_propagation_communities

        return [set(c) for c in label_propagation_communities(g)]


def _label_prop_communities(g: nx.Graph) -> list[set[str]]:
    from networkx.algorithms.community import asyn_lpa_communities

    return [set(c) for c in asyn_lpa_communities(g, weight="weight", seed=42)]


async def _persist(
    cluster_id: str,
    communities: list[set[str]],
    *,
    client: Neo4jClient | None = None,
) -> None:
    if not communities:
        return
    c = client or get_neo4j_client()
    rows = []
    for idx, members in enumerate(communities):
        for member_id in members:
            rows.append({"id": member_id, "community_id": f"{cluster_id}::C{idx:02d}"})
    if not rows:
        return
    # Match by any of the natural keys.
    await c.execute_write(
        """
        UNWIND $rows AS row
        MATCH (n)
        WHERE n.wallet_id = row.id OR n.imei = row.id OR n.imsi = row.id
           OR n.msisdn  = row.id OR n.agent_id = row.id
        SET n.community_id = row.community_id
        """,
        {"rows": rows},
    )


def _summarise(communities: list[set[str]]) -> dict[str, Any]:
    sizes = sorted((len(c) for c in communities), reverse=True)
    return {
        "community_count": len(communities),
        "largest_size": sizes[0] if sizes else 0,
        "smallest_size": sizes[-1] if sizes else 0,
        "size_distribution": sizes[:10],
    }


async def detect_louvain(cluster_id: str, *, persist: bool = True) -> dict[str, Any]:
    g = await load_cluster_subgraph(cluster_id)
    if g.number_of_nodes() == 0:
        return {"cluster_id": cluster_id, "community_count": 0, "skipped": True}
    h = _to_undirected(g)
    communities = await asyncio.to_thread(_louvain_communities, h)
    if persist:
        await _persist(cluster_id, communities)
    return {"cluster_id": cluster_id, "algorithm": "louvain", **_summarise(communities)}


async def detect_label_propagation(cluster_id: str, *, persist: bool = True) -> dict[str, Any]:
    g = await load_cluster_subgraph(cluster_id)
    if g.number_of_nodes() == 0:
        return {"cluster_id": cluster_id, "community_count": 0, "skipped": True}
    h = _to_undirected(g)
    communities = await asyncio.to_thread(_label_prop_communities, h)
    if persist:
        await _persist(cluster_id, communities)
    return {"cluster_id": cluster_id, "algorithm": "label_prop", **_summarise(communities)}


async def detect_for_active_clusters(*, algorithm: str = "louvain", limit: int = 50) -> dict[str, Any]:
    """Run community detection across every active cluster. Returns a
    summary dict suitable for logging / Celery result inspection."""

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
    runner = detect_louvain if algorithm == "louvain" else detect_label_propagation
    results = []
    for row in rows:
        try:
            results.append(await runner(row["cluster_id"]))
        except Exception as exc:  # noqa: BLE001 — keep the batch alive
            logger.warning(
                "analytics.community.cluster_failed",
                cluster_id=row["cluster_id"],
                error=str(exc),
            )
            results.append({"cluster_id": row["cluster_id"], "error": str(exc)})
    return {
        "algorithm": algorithm,
        "clusters_processed": len(results),
        "results": results,
    }
