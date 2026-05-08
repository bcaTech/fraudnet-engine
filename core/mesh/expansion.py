"""Breadth-first mesh expansion from a fraud seed.

The algorithm walks outward from the seed, scoring each discovered node
against the seed and pruning anything whose preliminary confidence falls
below ``expansion_threshold``. Nodes whose multiple paths converge (i.e. you
can reach them through several independent intermediaries) get a small
convergence bonus, capped to prevent runaway clusters.

High-level flow (per CLAUDE.md):

    1. Start with seed at given confidence.
    2. For each node, retrieve all edges sorted by strength (Cypher
       ``EXPAND_NEIGHBOURS``), already filtered to a per-node neighbour cap.
    3. For each connected node:
         a. preliminary = parent_conf * edge_strength * (distance_discount ^ depth)
         b. count independent paths to other discovered nodes; add bonus.
         c. if preliminary >= threshold, add to cluster + queue for expansion.
    4. Stop at depth limit, node-count cap, or no new qualifying nodes.
    5. Compute density / isolation metrics and persist the cluster.

The expansion is purely read-only against Neo4j until step 5; all writes
happen via :mod:`core.mesh.clustering`.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from config.constants import ExpansionConfig, ScoringWeights
from config.logging import get_logger
from core.graph.client import Neo4jClient
from core.graph.queries import EXPAND_NEIGHBOURS

from .clustering import ClusterPersistence
from .decay import decayed_strength
from .scoring import calculate_node_confidence
from .seed import Seed, resolve_lookup

logger = get_logger(__name__)


# Map graph labels back to API-facing node types.
_LABEL_TO_TYPE = {
    "Wallet": ("wallet", "wallet_id"),
    "Handset": ("handset", "imei"),
    "SIM": ("sim", "imsi"),
    "PhoneNumber": ("phone", "msisdn"),
    "Agent": ("agent", "agent_id"),
    "Transaction": ("transaction", "tx_id"),
    "CellTower": ("cell_tower", "cell_id"),
}


@dataclass
class _DiscoveredNode:
    eid: str
    node_type: str
    natural_id: str
    label: str
    depth: int
    confidence: float
    edge_strength_sum: float
    convergence_factor: int
    parent_eids: set[str] = field(default_factory=set)
    properties: dict[str, Any] = field(default_factory=dict)


@dataclass
class ExpansionResult:
    cluster_id: str
    seed: Seed
    nodes: list[_DiscoveredNode]
    edges: list[dict[str, Any]]  # raw rel records (source_eid, target_eid, type, props)
    confidence_score: float
    density: float
    isolation_score: float
    duration_ms: float

    @property
    def node_count(self) -> int:
        return len(self.nodes)

    def to_summary(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "seed_node_id": self.seed.node_id,
            "seed_type": self.seed.node_type,
            "seed_confidence": self.seed.confidence,
            "seed_source": self.seed.source,
            "node_count": self.node_count,
            "edge_count": len(self.edges),
            "confidence_score": self.confidence_score,
            "density": self.density,
            "isolation_score": self.isolation_score,
            "duration_ms": self.duration_ms,
        }


# Per-node neighbour cap to keep individual hops bounded. Production
# tuning: pull this from settings if it ever needs adjustment.
_NEIGHBOUR_LIMIT = 200


async def expand_from_seed(
    seed: Seed,
    *,
    client: Neo4jClient,
    config: ExpansionConfig | None = None,
    weights: ScoringWeights | None = None,
    persist: bool = True,
    persistence: ClusterPersistence | None = None,
) -> ExpansionResult:
    """Run BFS expansion from ``seed`` and (optionally) persist the cluster.

    Parameters
    ----------
    seed
        The starting node. Use ``Seed(node_id=..., node_type=..., confidence=...)``.
    client
        A connected Neo4j client.
    config
        Tuning knobs (depth, threshold, discount, convergence bonus). Defaults
        to ``ExpansionConfig()``.
    weights
        Confidence-scoring weights. Defaults to ``ScoringWeights()``.
    persist
        When True (default), the resulting cluster is written to Neo4j via
        :class:`ClusterPersistence`.
    persistence
        Optional override for the persistence helper (used in tests).
    """

    cfg = config or ExpansionConfig()
    sw = weights or ScoringWeights()
    started = datetime.now(UTC)

    label, key = resolve_lookup(seed.node_type)

    # ---- find the seed in Neo4j -----------------------------------------
    seed_records = await client.execute_read(
        f"MATCH (n:{label} {{ {key}: $id }}) "
        "RETURN elementId(n) AS eid, labels(n) AS labels, properties(n) AS props",
        {"id": seed.node_id},
    )
    if not seed_records:
        raise ValueError(f"Seed not found in graph: {seed.node_type}={seed.node_id!r}")
    seed_record = seed_records[0]
    seed_eid: str = seed_record["eid"]

    discovered: dict[str, _DiscoveredNode] = {}
    queue: deque[str] = deque()

    seed_node = _DiscoveredNode(
        eid=seed_eid,
        node_type=seed.node_type,
        natural_id=seed.node_id,
        label=label,
        depth=0,
        confidence=max(0.0, min(1.0, seed.confidence)),
        edge_strength_sum=0.0,
        convergence_factor=0,
        properties=seed_record["props"] or {},
    )
    discovered[seed_eid] = seed_node
    queue.append(seed_eid)

    visited_edges: set[tuple[str, str, str]] = set()
    edge_records: list[dict[str, Any]] = []

    while queue and len(discovered) < cfg.max_nodes:
        parent_eid = queue.popleft()
        parent = discovered[parent_eid]
        if parent.depth >= cfg.max_depth:
            continue

        rows = await _fetch_neighbours(client, parent)

        for row in rows:
            target_eid: str = row["target_eid"]
            target_labels: list[str] = row["target_labels"]
            target_props: dict[str, Any] = row["target_props"] or {}
            rel_type: str = row["rel_type"]
            rel_props: dict[str, Any] = row["rel_props"] or {}

            if rel_type == "BELONGS_TO":
                # Cluster membership edges aren't part of the fraud topology.
                continue

            edge_key = (parent_eid, target_eid, rel_type)
            if edge_key in visited_edges:
                continue
            visited_edges.add(edge_key)
            edge_records.append(
                {
                    "source_eid": parent_eid,
                    "target_eid": target_eid,
                    "type": rel_type,
                    "props": rel_props,
                }
            )

            edge_strength = _live_strength(rel_type, rel_props)

            # ---- preliminary confidence ---------------------------------
            distance_discount_term = cfg.distance_discount ** (parent.depth + 1)
            seed_proximity = parent.confidence * edge_strength * distance_discount_term

            existing = discovered.get(target_eid)
            convergence = (existing.convergence_factor if existing else 0) + (
                1 if existing and parent_eid not in existing.parent_eids else 0
            )
            convergence_bonus = min(cfg.convergence_bonus * convergence, cfg.convergence_cap)

            preliminary = calculate_node_confidence(
                seed_proximity=seed_proximity,
                edge_strength_sum=((existing.edge_strength_sum if existing else 0.0) + edge_strength),
                convergence_factor=convergence,
                behavioral_score=float(target_props.get("behavioral_score", 0.0)),
                predictive_score=float(target_props.get("predictive_score", 0.0)),
                negative_evidence=_negative_evidence(target_props),
                weights=sw,
            )
            preliminary = min(1.0, preliminary + convergence_bonus)

            if preliminary < cfg.expansion_threshold:
                continue

            target_type, target_key = _resolve_label(target_labels)
            target_natural_id = (
                str(target_props.get(target_key))
                if target_key and target_props.get(target_key) is not None
                else target_eid
            )

            if existing is None:
                node = _DiscoveredNode(
                    eid=target_eid,
                    node_type=target_type,
                    natural_id=target_natural_id,
                    label=target_labels[0] if target_labels else "Unknown",
                    depth=parent.depth + 1,
                    confidence=preliminary,
                    edge_strength_sum=edge_strength,
                    convergence_factor=convergence,
                    parent_eids={parent_eid},
                    properties=target_props,
                )
                discovered[target_eid] = node
                queue.append(target_eid)
            else:
                existing.confidence = max(existing.confidence, preliminary)
                existing.edge_strength_sum += edge_strength
                existing.convergence_factor = convergence
                existing.parent_eids.add(parent_eid)

    # ---- cluster-level metrics -----------------------------------------
    cluster_confidence = _aggregate_confidence(discovered.values())
    density = _density(len(discovered), len(edge_records))
    isolation = _isolation_score(discovered.values(), edge_records)

    cluster_id = f"cluster-{uuid.uuid4().hex[:12]}"
    duration_ms = (datetime.now(UTC) - started).total_seconds() * 1000.0

    result = ExpansionResult(
        cluster_id=cluster_id,
        seed=seed,
        nodes=list(discovered.values()),
        edges=edge_records,
        confidence_score=cluster_confidence,
        density=density,
        isolation_score=isolation,
        duration_ms=duration_ms,
    )

    logger.info(
        "mesh.expansion.complete",
        cluster_id=cluster_id,
        seed=seed.node_id,
        seed_type=seed.node_type,
        nodes=result.node_count,
        edges=len(result.edges),
        confidence=cluster_confidence,
        duration_ms=duration_ms,
    )

    if persist:
        helper = persistence or ClusterPersistence(client)
        await helper.persist_expansion(result)

    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _fetch_neighbours(client: Neo4jClient, node: _DiscoveredNode) -> list[dict[str, Any]]:
    """Pull a single node's neighbours, ordered by strength."""

    if node.label not in _LABEL_TO_TYPE:
        return []
    _, key = _LABEL_TO_TYPE[node.label]
    return await client.execute_read(
        EXPAND_NEIGHBOURS,
        {
            "id": node.properties.get(key, node.natural_id),
            "key": key,
            "limit": _NEIGHBOUR_LIMIT,
        },
    )


def _resolve_label(labels: list[str]) -> tuple[str, str | None]:
    for lab in labels:
        if lab in _LABEL_TO_TYPE:
            return _LABEL_TO_TYPE[lab]
    return ("unknown", None)


def _live_strength(rel_type: str, rel_props: dict[str, Any]) -> float:
    """Return the current strength of an edge, applying decay if a timestamp exists."""

    base = float(rel_props.get("strength", 0.4))
    last_seen = rel_props.get("last_seen") or rel_props.get("timestamp")
    if isinstance(last_seen, str):
        try:
            last_seen = datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
        except ValueError:
            last_seen = None
    if isinstance(last_seen, datetime):
        return decayed_strength(rel_type, base, last_seen)
    return base


def _negative_evidence(props: dict[str, Any]) -> float:
    """Heuristic legitimate-history discount in [0, 1] based on a node's properties.

    For wallets: long account age and verified KYC reduce confidence in fraud.
    """

    score = 0.0
    age_days = props.get("account_age")
    if isinstance(age_days, (int, float)) and age_days > 365:
        score += 0.4
    if props.get("kyc_tier") in (2, 3):
        score += 0.3
    if props.get("status") == "active" and props.get("flagged") is False:
        score += 0.1
    return min(score, 1.0)


def _aggregate_confidence(nodes: Iterable[_DiscoveredNode]) -> float:
    """Cluster-level confidence: weighted average of member confidences,
    biased toward the seed's neighbourhood (smaller depth → higher weight).
    """

    total = 0.0
    weight = 0.0
    for n in nodes:
        w = 2.0 if n.depth == 0 else 1.0 / (1.0 + n.depth)
        total += n.confidence * w
        weight += w
    return total / weight if weight else 0.0


def _density(node_count: int, edge_count: int) -> float:
    """Edge density relative to a complete graph on ``node_count`` nodes."""

    if node_count < 2:
        return 0.0
    max_edges = node_count * (node_count - 1) / 2
    if max_edges == 0:
        return 0.0
    return min(edge_count / max_edges, 1.0)


def _isolation_score(
    nodes: Iterable[_DiscoveredNode],
    edges: list[dict[str, Any]],
) -> float:
    """Approximate isolation: high when most edges remain inside the cluster.

    For this online estimate we treat the in-cluster edge count over the
    expected baseline (``node_count``) as a proxy. Maintenance jobs refine
    this with full neighbour counts in the background.
    """

    nodes_list = list(nodes)
    if not nodes_list:
        return 0.0
    in_cluster = len(edges)
    expected = max(len(nodes_list), 1)
    return min(in_cluster / (expected * 1.5), 1.0)


# Re-exported so callers can ``from core.mesh.expansion import expand_from_seed``.
__all__ = ["expand_from_seed", "ExpansionResult", "Seed"]


# Convenience for ad-hoc sync usage (e.g. the seed CLI script).
def expand_from_seed_sync(*args: Any, **kwargs: Any) -> ExpansionResult:
    """Blocking wrapper around :func:`expand_from_seed`."""

    return asyncio.run(expand_from_seed(*args, **kwargs))
