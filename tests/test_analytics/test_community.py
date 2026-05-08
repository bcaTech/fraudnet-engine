"""Pure-unit tests for community detection over a synthetic graph.

These tests don't touch Neo4j — they construct an in-memory NetworkX
graph that mimics the shape produced by :func:`load_cluster_subgraph`
and verify the algorithms separate two well-defined communities.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import networkx as nx
import pytest

from core.analytics.community import (
    _label_prop_communities,
    _louvain_communities,
    _to_undirected,
    detect_louvain,
)


def _two_clique_graph() -> nx.MultiDiGraph:
    """Build a deliberately easy-to-cluster graph: two cliques of 5 nodes
    each, joined by a single bridge edge."""

    g: nx.MultiDiGraph = nx.MultiDiGraph()
    a_nodes = [f"A{i}" for i in range(5)]
    b_nodes = [f"B{i}" for i in range(5)]
    for n in a_nodes + b_nodes:
        g.add_node(n, labels=["Wallet"], risk_score=0.5)
    # Dense intra-cluster edges
    for i, u in enumerate(a_nodes):
        for v in a_nodes[i + 1 :]:
            g.add_edge(u, v, type="SENT_TO", strength=0.9)
            g.add_edge(v, u, type="SENT_TO", strength=0.9)
    for i, u in enumerate(b_nodes):
        for v in b_nodes[i + 1 :]:
            g.add_edge(u, v, type="SENT_TO", strength=0.9)
            g.add_edge(v, u, type="SENT_TO", strength=0.9)
    # One thin bridge
    g.add_edge("A0", "B0", type="SENT_TO", strength=0.05)
    return g


def test_to_undirected_collapses_multi_edges() -> None:
    g = nx.MultiDiGraph()
    g.add_node("a")
    g.add_node("b")
    g.add_edge("a", "b", strength=0.4)
    g.add_edge("a", "b", strength=0.3)
    g.add_edge("b", "a", strength=0.2)
    h = _to_undirected(g)
    assert h.number_of_edges() == 1  # Three multi-edges collapse to one
    # Weight is the sum of strengths plus +1 per edge constant.
    assert h["a"]["b"]["weight"] > 0.5


def test_louvain_separates_two_cliques() -> None:
    h = _to_undirected(_two_clique_graph())
    communities = _louvain_communities(h)
    # We may get exactly 2 communities, possibly more if Louvain prunes
    # singletons; what we require is that the A and B nodes never end up
    # in the same community.
    a_names = {f"A{i}" for i in range(5)}
    b_names = {f"B{i}" for i in range(5)}
    a_community = next(c for c in communities if c & a_names)
    b_community = next(c for c in communities if c & b_names)
    assert a_community != b_community
    assert not (a_community & b_names)
    assert not (b_community & a_names)


def test_label_propagation_runs_without_error() -> None:
    h = _to_undirected(_two_clique_graph())
    communities = _label_prop_communities(h)
    assert all(isinstance(c, set) for c in communities)
    assert sum(len(c) for c in communities) == h.number_of_nodes()


def test_detect_louvain_skips_empty_graph() -> None:
    """When the cluster has no nodes the loader returns an empty graph;
    detect_louvain should return a `skipped` summary rather than crash."""

    async def run():
        with patch(
            "core.analytics.community.load_cluster_subgraph",
            return_value=nx.MultiDiGraph(),
        ):
            return await detect_louvain("CLUSTER-EMPTY", persist=False)

    out = asyncio.run(run())
    assert out["skipped"] is True
    assert out["community_count"] == 0


def test_detect_louvain_returns_summary_for_real_graph() -> None:
    g = _two_clique_graph()

    async def run():
        with patch(
            "core.analytics.community.load_cluster_subgraph", return_value=g
        ):
            return await detect_louvain("CLUSTER-T1", persist=False)

    out = asyncio.run(run())
    assert out["algorithm"] == "louvain"
    assert out["community_count"] >= 2
    assert out["largest_size"] >= 4
