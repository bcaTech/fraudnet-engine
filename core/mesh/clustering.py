"""Cluster lifecycle: persistence, merging, and dissolution.

After :func:`core.mesh.expansion.expand_from_seed` produces an
:class:`ExpansionResult`, this module is responsible for writing the cluster
back to Neo4j and for managing its lifecycle:

- **Persistence** — create the ``Cluster`` node and link members via
  ``BELONGS_TO`` edges, also writing ``cluster_id`` / ``confidence_score``
  onto each member node so downstream queries don't require traversal.
- **Merging** — when expansion produces a cluster that overlaps an existing
  one (cross-membership > 30%), merge into the larger cluster.
- **Dissolution** — when re-scoring drops confidence below threshold for an
  extended period, mark a cluster ``dissolved`` and detach members.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from config.constants import CLUSTER_STATUS_ACTIVE
from config.logging import get_logger
from core.graph.client import Neo4jClient
from core.graph.queries import ATTACH_MEMBER, UPSERT_CLUSTER

if TYPE_CHECKING:  # avoid a circular import — expansion imports clustering.
    from .expansion import ExpansionResult

logger = get_logger(__name__)


_LABEL_KEY: dict[str, str] = {
    "Wallet": "wallet_id",
    "Handset": "imei",
    "SIM": "imsi",
    "PhoneNumber": "msisdn",
    "Agent": "agent_id",
}


class ClusterPersistence:
    """Writes :class:`ExpansionResult` instances back to Neo4j."""

    def __init__(self, client: Neo4jClient) -> None:
        self.client = client

    async def persist_expansion(self, result: ExpansionResult) -> None:
        """Upsert the cluster node and attach members.

        Side effects on each member node:
        - sets ``cluster_id`` to ``result.cluster_id``
        - sets ``confidence_score`` to the per-node confidence
        """

        seed_label = _LABEL_KEY.get(_label_for_seed(result.seed.node_type))
        await self.client.execute_write(
            UPSERT_CLUSTER,
            {
                "cluster_id": result.cluster_id,
                "name": _default_name(result),
                "seed_type": result.seed.node_type,
                "seed_date": result.seed.seeded_at.isoformat(),
                "seed_node_id": result.seed.node_id,
                "node_count": result.node_count,
                "confidence_score": result.confidence_score,
                "status": CLUSTER_STATUS_ACTIVE,
                "estimated_fraud_value": _estimate_fraud_value(result),
                "density": result.density,
                "isolation_score": result.isolation_score,
            },
        )
        if seed_label is None:
            logger.warning("mesh.persist.unknown_seed_label", seed_type=result.seed.node_type)

        joined_at = datetime.now(UTC).isoformat()
        statements: list[tuple[str, Mapping[str, Any]]] = []
        for node in result.nodes:
            label = node.label
            if label not in _LABEL_KEY:
                continue
            statements.append(
                (
                    ATTACH_MEMBER,
                    {
                        "cluster_id": result.cluster_id,
                        "node_label": label,
                        "node_key": _LABEL_KEY[label],
                        "node_id": node.natural_id,
                        "confidence": node.confidence,
                        "joined_date": joined_at,
                        "role": "seed" if node.depth == 0 else None,
                    },
                )
            )
        if statements:
            await self.client.execute_many_write(statements)
        logger.info(
            "mesh.persist.complete",
            cluster_id=result.cluster_id,
            members_attached=len(statements),
        )


def _label_for_seed(node_type: str) -> str:
    return {
        "wallet": "Wallet",
        "handset": "Handset",
        "sim": "SIM",
        "phone": "PhoneNumber",
        "agent": "Agent",
    }.get(node_type.lower(), "Wallet")


def _default_name(result: ExpansionResult) -> str:
    short = result.cluster_id.split("-")[-1]
    return f"Cluster {short.upper()} ({result.seed.source})"


def _estimate_fraud_value(result: ExpansionResult) -> float:
    """Best-effort fraud-value estimate from member balances and edge volumes.

    This is a rough lower bound — the analytics layer recomputes it from
    transactional history once the cluster is steady.
    """

    total = 0.0
    for n in result.nodes:
        if n.label == "Wallet":
            balance = n.properties.get("balance")
            if isinstance(balance, (int, float)):
                total += float(balance)
    for e in result.edges:
        amount = e.get("props", {}).get("amount")
        if isinstance(amount, (int, float)):
            total += float(amount) * 0.1  # conservative discount
    return round(total, 2)
