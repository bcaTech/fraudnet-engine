"""Seed identification for mesh expansion.

A *seed* is a node we already believe is fraudulent — it acts as the entry
point for breadth-first expansion. Sources of seeds:

- SafeGuard ``return_to_sender`` and ``didnt_know_you`` events from the
  ``fraudnet.safeguard.events`` Kafka topic.
- Operator alerts and customer complaints surfaced by analysts.
- Inbound intelligence from law enforcement or other operators.

This module exposes the dataclass used to feed the expansion algorithm.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(frozen=True)
class Seed:
    """A starting point for mesh expansion."""

    node_id: str
    """Stable identifier (wallet_id, imei, agent_id, ...)."""

    node_type: str
    """One of: wallet, handset, sim, agent, phone."""

    confidence: float
    """Initial confidence in [0, 1]. Higher = more trusted seed."""

    source: str
    """Origin tag: 'safeguard', 'analyst', 'inbound_intel', 'law_enforcement', ..."""

    seeded_at: datetime = None  # type: ignore[assignment]
    """When this seed was generated. Defaults to now()."""

    def __post_init__(self) -> None:
        if self.seeded_at is None:
            object.__setattr__(self, "seeded_at", datetime.now(timezone.utc))


# Map of node_type → (graph label, property key) pairs used to look the seed
# up in Neo4j. Centralised so callers don't repeat the mapping.
NODE_TYPE_LOOKUP: dict[str, tuple[str, str]] = {
    "wallet": ("Wallet", "wallet_id"),
    "handset": ("Handset", "imei"),
    "sim": ("SIM", "imsi"),
    "phone": ("PhoneNumber", "msisdn"),
    "agent": ("Agent", "agent_id"),
}


def resolve_lookup(node_type: str) -> tuple[str, str]:
    """Return the ``(label, key)`` pair to use when querying Neo4j for ``node_type``.

    Raises ``ValueError`` if the type is unknown.
    """

    try:
        return NODE_TYPE_LOOKUP[node_type.lower()]
    except KeyError as exc:
        raise ValueError(f"Unsupported seed node_type: {node_type!r}") from exc
