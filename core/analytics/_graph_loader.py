"""Helper: load a cluster's subgraph from Neo4j into NetworkX.

The analytics modules all need a NetworkX representation of a cluster's
graph. Centralising the loader here keeps the Cypher in one place and
makes it easy to swap node-set definitions later (e.g. include 1-hop
neighbours of cluster members).
"""

from __future__ import annotations

import networkx as nx

from core.graph.client import Neo4jClient, get_neo4j_client


_CLUSTER_GRAPH_QUERY = """
MATCH (c:Cluster {cluster_id: $cluster_id})
OPTIONAL MATCH (n)-[:BELONGS_TO]->(c)
WITH collect(DISTINCT n) AS members
UNWIND members AS m
OPTIONAL MATCH (m)-[r]-(o)
WHERE o IN members AND type(r) <> 'BELONGS_TO'
RETURN
    [x IN members | {
        id: coalesce(x.wallet_id, x.imei, x.imsi, x.msisdn, x.agent_id),
        labels: labels(x),
        risk_score: coalesce(x.risk_score, 0.0)
    }] AS nodes,
    collect(DISTINCT {
        src: coalesce(startNode(r).wallet_id, startNode(r).imei,
                      startNode(r).imsi, startNode(r).msisdn,
                      startNode(r).agent_id),
        dst: coalesce(endNode(r).wallet_id, endNode(r).imei,
                      endNode(r).imsi, endNode(r).msisdn,
                      endNode(r).agent_id),
        type: type(r),
        strength: coalesce(r.strength, 0.0),
        amount: coalesce(r.amount, 0.0)
    }) AS edges
"""


async def load_cluster_subgraph(
    cluster_id: str, *, client: Neo4jClient | None = None
) -> nx.MultiDiGraph:
    """Return a directed multi-graph for the cluster, with node attrs
    ``labels`` and ``risk_score`` and edge attrs ``type``, ``strength``,
    ``amount``."""

    c = client or get_neo4j_client()
    rows = await c.execute_read(_CLUSTER_GRAPH_QUERY, {"cluster_id": cluster_id})
    g: nx.MultiDiGraph = nx.MultiDiGraph()
    if not rows:
        return g
    for n in rows[0].get("nodes") or []:
        if not n.get("id"):
            continue
        g.add_node(
            str(n["id"]),
            labels=n.get("labels") or [],
            risk_score=float(n.get("risk_score") or 0.0),
        )
    for e in rows[0].get("edges") or []:
        if not e or not e.get("src") or not e.get("dst"):
            continue
        g.add_edge(
            str(e["src"]),
            str(e["dst"]),
            type=e.get("type"),
            strength=float(e.get("strength") or 0.0),
            amount=float(e.get("amount") or 0.0),
        )
    return g
