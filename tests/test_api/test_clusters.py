"""Cluster-route integration tests.

Marked ``integration`` because they exercise the live Neo4j-backed API.
Run with::

    pytest -m integration tests/test_api/test_clusters.py

The fixtures assume the demo seed has been loaded (15 clusters present).
"""

from __future__ import annotations

import os

import httpx
import pytest

API_BASE = os.environ.get("FRAUDNET_API_BASE", "http://localhost:8000")


pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def client() -> httpx.Client:
    with httpx.Client(base_url=API_BASE, timeout=10.0) as c:
        yield c


def test_list_clusters_returns_paginated_envelope(client: httpx.Client) -> None:
    r = client.get("/api/clusters", params={"per_page": 5})
    assert r.status_code == 200
    body = r.json()
    assert "data" in body and isinstance(body["data"], list)
    assert "meta" in body and body["meta"].get("per_page") == 5
    if body["data"]:
        # Cluster shape sanity
        c0 = body["data"][0]
        assert "cluster_id" in c0
        assert "confidence_score" in c0


def test_get_cluster_detail(client: httpx.Client) -> None:
    listing = client.get("/api/clusters", params={"per_page": 1}).json()
    if not listing["data"]:
        pytest.skip("no clusters seeded — skipping detail test")
    cid = listing["data"][0]["cluster_id"]
    r = client.get(f"/api/clusters/{cid}")
    assert r.status_code == 200
    body = r.json()["data"]
    assert body["cluster_id"] == cid
    assert "member_count" in body


def test_get_cluster_graph_returns_typed_payload(client: httpx.Client) -> None:
    listing = client.get(
        "/api/clusters", params={"per_page": 5, "min_confidence": 0.6}
    ).json()
    if not listing["data"]:
        pytest.skip("no qualifying clusters seeded")
    # Pick the cluster with the largest member count to avoid empty graphs.
    target = max(listing["data"], key=lambda c: c.get("node_count") or 0)
    cid = target["cluster_id"]
    r = client.get(f"/api/clusters/{cid}/graph")
    assert r.status_code == 200
    body = r.json()["data"]
    assert "nodes" in body and "edges" in body
    if body["nodes"]:
        n0 = body["nodes"][0]
        assert "id" in n0 and "type" in n0 and "properties" in n0


def test_get_unknown_cluster_returns_404(client: httpx.Client) -> None:
    r = client.get("/api/clusters/CLUSTER-DOES-NOT-EXIST")
    assert r.status_code == 404
