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
from unittest.mock import AsyncMock

import pytest

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
    seed = Seed(
        node_id="MOMO-MISSING", node_type="wallet", confidence=0.8, source="analyst"
    )

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
    seed = Seed(
        node_id="MOMO-001", node_type="wallet", confidence=0.8, source="analyst"
    )

    result = asyncio.run(expand_from_seed(seed, client=fake, persist=False))
    assert result.node_count == 1
    assert result.nodes[0].natural_id == "MOMO-001"
    assert result.confidence_score == 0.8
    assert result.edges == []
