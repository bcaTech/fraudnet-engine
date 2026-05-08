"""Tests for mesh BFS expansion.

The expansion algorithm is tightly coupled to its Cypher contract. A
true unit test would have to fake every Cypher call. Here we use a
small in-memory mock that returns canned Cypher responses for two
scenarios: seed-not-found, and a tiny three-node cluster.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import pytest

from config.constants import ExpansionConfig
from core.mesh.expansion import expand_from_seed
from core.mesh.seed import Seed


class _FakeNeo4j:
    """Just enough of the Neo4jClient surface for expansion to run."""

    def __init__(self, responses: dict[str, list[dict[str, Any]]]) -> None:
        # responses keyed by a substring matched against the cypher text.
        self._responses = responses
        self._calls: list[tuple[str, Mapping[str, Any]]] = []

    async def execute_read(self, cypher: str, params: Mapping[str, Any] | None = None):  # noqa: D401
        self._calls.append((cypher, params or {}))
        for needle, payload in self._responses.items():
            if needle in cypher:
                # Allow callable payload for parameter-aware responses.
                if callable(payload):
                    return payload(params or {})
                return payload
        return []

    async def execute_write(self, *args, **kwargs):  # noqa: D401, ANN001
        # Persist isn't exercised in these tests.
        return []


def test_expansion_raises_when_seed_not_found() -> None:
    fake = _FakeNeo4j({"MATCH (n:Wallet": []})
    seed = Seed(node_id="MOMO-MISSING", node_type="wallet", confidence=0.8, source="analyst")

    async def run():
        await expand_from_seed(seed, client=fake, persist=False)

    with pytest.raises(ValueError) as exc_info:
        asyncio.run(run())
    assert "Seed not found" in str(exc_info.value)


def test_expansion_returns_seed_only_when_no_neighbours() -> None:
    """Seed lookup succeeds, but the seed has no neighbours — the cluster
    should contain just the seed itself."""

    seed_eid = "n:0"
    fake = _FakeNeo4j(
        {
            "MATCH (n:Wallet": [
                {
                    "eid": seed_eid,
                    "labels": ["Wallet"],
                    "props": {"wallet_id": "MOMO-001", "risk_score": 0.5},
                }
            ],
            # Neighbour fetch returns nothing — terminates the BFS.
            "WHERE elementId(n)": [],
        }
    )
    seed = Seed(node_id="MOMO-001", node_type="wallet", confidence=0.8, source="analyst")

    result = asyncio.run(expand_from_seed(seed, client=fake, persist=False))
    assert result.node_count == 1
    assert result.nodes[0].natural_id == "MOMO-001"
    assert result.confidence_score == 0.8
    assert result.edges == []


def test_expansion_three_node_cluster_confidence() -> None:
    """Seed → A → B at depths 0/1/2.

    Confirms the BFS expands across two hops, applies the distance
    discount per hop (so deeper nodes carry less confidence), and
    dedups when a reverse edge re-points at an already-discovered node.
    """

    seed_eid = "n:seed"
    a_eid = "n:a"
    b_eid = "n:b"

    def _neighbours_for(params: Mapping[str, Any]) -> list[dict[str, Any]]:
        if params.get("id") == "MOMO-SEED":
            return [
                {
                    "source_eid": seed_eid,
                    "target_eid": a_eid,
                    "target_labels": ["Wallet"],
                    "target_props": {"wallet_id": "MOMO-A", "risk_score": 0.6},
                    "rel_type": "SENT_TO",
                    "rel_props": {"strength": 0.9, "amount": 1000.0},
                    "strength": 0.9,
                }
            ]
        if params.get("id") == "MOMO-A":
            return [
                # Reverse edge back to seed — should not produce a new node.
                {
                    "source_eid": a_eid,
                    "target_eid": seed_eid,
                    "target_labels": ["Wallet"],
                    "target_props": {"wallet_id": "MOMO-SEED"},
                    "rel_type": "SENT_TO",
                    "rel_props": {"strength": 0.85},
                    "strength": 0.85,
                },
                {
                    "source_eid": a_eid,
                    "target_eid": b_eid,
                    "target_labels": ["Wallet"],
                    "target_props": {"wallet_id": "MOMO-B", "risk_score": 0.55},
                    "rel_type": "SENT_TO",
                    "rel_props": {"strength": 0.85},
                    "strength": 0.85,
                },
            ]
        return []

    fake = _FakeNeo4j(
        {
            "MATCH (n:Wallet": [
                {
                    "eid": seed_eid,
                    "labels": ["Wallet"],
                    "props": {"wallet_id": "MOMO-SEED", "risk_score": 0.7},
                }
            ],
            # The expansion's neighbour-fetch query references n[$key];
            # match on that stable substring.
            "WHERE n[$key]": _neighbours_for,
        }
    )
    seed = Seed(
        node_id="MOMO-SEED", node_type="wallet", confidence=0.9, source="analyst"
    )

    # Lower the threshold so the test focuses on BFS shape rather than the
    # confidence-formula tuning. The default 0.25 includes weights that
    # require behavioural/predictive signal we don't supply in this fake.
    config = ExpansionConfig(expansion_threshold=0.05, max_depth=3)
    result = asyncio.run(
        expand_from_seed(seed, client=fake, persist=False, config=config)
    )
    natural_ids = {n.natural_id for n in result.nodes}
    assert natural_ids == {"MOMO-SEED", "MOMO-A", "MOMO-B"}

    by_id = {n.natural_id: n for n in result.nodes}
    assert by_id["MOMO-SEED"].confidence == pytest.approx(0.9)
    # Distance discount: deeper nodes carry less confidence.
    assert by_id["MOMO-SEED"].confidence > by_id["MOMO-A"].confidence
    assert by_id["MOMO-A"].confidence > by_id["MOMO-B"].confidence
    # Cluster confidence is in [0, 1] and reflects real signal.
    assert 0.0 < result.confidence_score <= 1.0

    edge_pairs = {(e["source_eid"], e["target_eid"], e["type"]) for e in result.edges}
    assert (seed_eid, a_eid, "SENT_TO") in edge_pairs
    assert (a_eid, b_eid, "SENT_TO") in edge_pairs
    assert result.node_count == 3
