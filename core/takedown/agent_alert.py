"""Alert agents linked to a takedown cluster.

For each agent with a ``LINKED_TO`` edge to the cluster:

- Increment ``warnings_count``
- Stamp ``last_warning_at``
- (stub) Send the agent an SMS via the operator notification system

The graph mutation is real; the SMS dispatch is a documented stub for
the production notification integration.
"""

from __future__ import annotations

from typing import Any

from config.logging import get_logger
from core.graph.client import Neo4jClient, get_neo4j_client

logger = get_logger(__name__)


async def alert_cluster_agents(
    cluster_id: str, *, client: Neo4jClient | None = None
) -> dict[str, Any]:
    c = client or get_neo4j_client()
    rows = await c.execute_write(
        """
        MATCH (a:Agent)-[:LINKED_TO]->(cl:Cluster {cluster_id: $cluster_id})
        SET a.warnings_count = coalesce(a.warnings_count, 0) + 1,
            a.last_warning_at = datetime(),
            a.last_warning_source = 'takedown'
        RETURN a.agent_id AS agent_id, a.area_name AS area
        """,
        {"cluster_id": cluster_id},
    )
    alerted = [
        {"agent_id": r["agent_id"], "area": r.get("area")} for r in rows if r.get("agent_id")
    ]
    for entry in alerted:
        await send_agent_warning(entry["agent_id"])
    logger.info(
        "takedown.agent_alert.complete",
        cluster_id=cluster_id,
        count=len(alerted),
    )
    return {"alerted": len(alerted), "agents": alerted}


async def send_agent_warning(agent_id: str) -> bool:  # noqa: ARG001
    """Stub for the agent SMS notification path.

    Real implementation: enqueue a templated SMS through the operator
    notification service ("Your float has been flagged for review,
    please contact the FraudNet desk."). Idempotent on
    ``(agent_id, takedown_id)`` to avoid double-sends if the executor
    retries.
    """

    logger.debug("takedown.agent_alert.external_stub", agent_id=agent_id)
    return True
