"""Temporal decay engine for edge strengths.

Edges grow stale: a SIM/handset insertion from two years ago is much weaker
evidence of co-membership than one from last week. We model this with simple
exponential decay using a per-edge-type half-life (see
:data:`config.constants.EDGE_HALF_LIFE_DAYS`).

The Celery periodic task ``apply_decay_to_all_edges`` rewrites edge ``strength``
properties in Neo4j and prunes any edge that has decayed below
:data:`config.constants.DECAY_PRUNE_THRESHOLD`.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime

from config.constants import (
    DECAY_PRUNE_THRESHOLD,
    EDGE_HALF_LIFE_DAYS,
)


def apply_decay(strength: float, half_life_days: float, days_elapsed: float) -> float:
    """Return the decayed strength.

    Implements ``strength * exp(-λt)`` where ``λ = ln(2) / half_life``.
    """

    if half_life_days <= 0 or days_elapsed <= 0:
        return strength
    lambda_val = math.log(2.0) / half_life_days
    return strength * math.exp(-lambda_val * days_elapsed)


def days_between(a: datetime, b: datetime) -> float:
    """Return the absolute number of days between two datetimes."""

    if a.tzinfo is None:
        a = a.replace(tzinfo=UTC)
    if b.tzinfo is None:
        b = b.replace(tzinfo=UTC)
    return abs((b - a).total_seconds()) / 86400.0


def half_life_for(edge_type: str) -> float:
    """Look up the half-life for an edge type, falling back to a 30-day default."""

    return EDGE_HALF_LIFE_DAYS.get(edge_type, 30.0)


def decayed_strength(
    edge_type: str,
    base_strength: float,
    last_seen: datetime,
    *,
    now: datetime | None = None,
) -> float:
    """Compute the live strength of an edge as of ``now``."""

    now = now or datetime.now(UTC)
    return apply_decay(base_strength, half_life_for(edge_type), days_between(last_seen, now))


def should_prune(strength: float) -> bool:
    """Return True if the edge has decayed below the pruning threshold."""

    return strength < DECAY_PRUNE_THRESHOLD
