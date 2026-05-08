"""Continuous mesh maintenance jobs.

Run from Celery (see ``tasks/periodic.py``):

- :func:`apply_decay_to_all_edges` — exponential decay + prune below threshold.
- :func:`rescore_active_clusters`  — recompute cluster-level confidence and
  estimated fraud value from current member confidences.
"""

from __future__ import annotations

from datetime import UTC, datetime

from config.constants import (
    CLUSTER_STATUS_ACTIVE,
    CLUSTER_STATUS_DISSOLVED,
    CLUSTER_STATUS_INVESTIGATING,
    DECAY_PRUNE_THRESHOLD,
    EDGE_HALF_LIFE_DAYS,
)
from config.logging import get_logger
from core.graph.client import Neo4jClient

from .decay import apply_decay

logger = get_logger(__name__)


# Single Cypher query that does the heavy lifting in-database. The batch is
# scoped per relationship type so we can use the type-specific half-life.
_DECAY_RECOMPUTE = """
MATCH ()-[r]->()
WHERE type(r) = $rel_type AND r.last_seen IS NOT NULL
WITH r,
     duration.inDays(r.last_seen, datetime()).days AS days_elapsed,
     coalesce(r.base_strength, r.strength) AS base
WITH r, base,
     CASE WHEN days_elapsed < 0 THEN 0 ELSE days_elapsed END AS days_elapsed
WITH r, base * exp(-($ln2 / $half_life) * days_elapsed) AS new_strength
SET r.strength = new_strength
WITH r, new_strength
WHERE new_strength < $prune_threshold
DELETE r
RETURN count(*) AS pruned
"""


_LN2 = 0.6931471805599453  # ln(2)


async def apply_decay_to_all_edges(client: Neo4jClient) -> dict[str, int]:
    """Recompute live edge strengths and prune stale edges.

    Returns a mapping of ``rel_type → pruned_count``.
    """

    pruned: dict[str, int] = {}
    for rel_type, half_life in EDGE_HALF_LIFE_DAYS.items():
        rows = await client.execute_write(
            _DECAY_RECOMPUTE,
            {
                "rel_type": rel_type,
                "half_life": half_life,
                "prune_threshold": DECAY_PRUNE_THRESHOLD,
                "ln2": _LN2,
            },
        )
        pruned[rel_type] = int(rows[0]["pruned"]) if rows else 0
    logger.info("mesh.decay.complete", pruned=pruned)
    return pruned


_RESCORE = """
MATCH (c:Cluster)
WHERE c.status IN [$active, $investigating]
OPTIONAL MATCH (n)-[r:BELONGS_TO]->(c)
WITH c, avg(coalesce(r.confidence, 0.0)) AS avg_conf, count(n) AS member_count
SET c.confidence_score = coalesce(avg_conf, 0.0),
    c.node_count = member_count
RETURN c.cluster_id AS cluster_id, member_count, avg_conf
"""


async def rescore_active_clusters(client: Neo4jClient) -> int:
    rows = await client.execute_write(
        _RESCORE,
        {
            "active": CLUSTER_STATUS_ACTIVE,
            "investigating": CLUSTER_STATUS_INVESTIGATING,
        },
    )
    logger.info("mesh.rescore.complete", clusters=len(rows))
    return len(rows)


_DISSOLVE_LOW_CONFIDENCE = """
MATCH (c:Cluster)
WHERE c.status = $active AND c.confidence_score < $threshold
SET c.status = $dissolved,
    c.dissolved_at = datetime()
RETURN count(c) AS dissolved
"""


async def dissolve_low_confidence_clusters(client: Neo4jClient, *, threshold: float = 0.20) -> int:
    rows = await client.execute_write(
        _DISSOLVE_LOW_CONFIDENCE,
        {
            "active": CLUSTER_STATUS_ACTIVE,
            "dissolved": CLUSTER_STATUS_DISSOLVED,
            "threshold": threshold,
        },
    )
    n = int(rows[0]["dissolved"]) if rows else 0
    logger.info(
        "mesh.dissolve.complete",
        dissolved=n,
        threshold=threshold,
        at=datetime.now(UTC).isoformat(),
    )
    return n


# Re-exported so the Celery task module can import a single symbol.
__all__ = [
    "apply_decay_to_all_edges",
    "rescore_active_clusters",
    "dissolve_low_confidence_clusters",
    "apply_decay",
]
