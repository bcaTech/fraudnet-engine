"""Agent classification: clean / incidental / exploited / complicit.

Sits on top of :mod:`scoring` and provides batch helpers for the
periodic re-classification job. Persists ``risk_score`` and
``classification`` back to the graph so downstream queries (the agent
list, the agent map, rule conditions like ``agent.fraud_cashout_rate``)
read consistent values.
"""

from __future__ import annotations

from typing import Any

from config.logging import get_logger
from core.graph.client import Neo4jClient, get_neo4j_client

from .geographic import baselines_by_area
from .scoring import AgentScore, calculate_agent_risk, fetch_agent_inputs

logger = get_logger(__name__)


async def classify_one(
    agent_id: str, *, persist: bool = True, client: Neo4jClient | None = None
) -> AgentScore:
    c = client or get_neo4j_client()
    inputs = await fetch_agent_inputs(agent_id, client=c)
    baselines = await baselines_by_area(client=c)
    baseline = baselines.get(inputs.area_name) if inputs.area_name else None
    score = calculate_agent_risk(inputs, area_baseline=baseline)
    if persist:
        await c.execute_write(
            """
            MATCH (a:Agent {agent_id: $agent_id})
            SET a.risk_score = $risk_score,
                a.classification = $classification,
                a.fraud_cashout_rate = $fraud_rate,
                a.classified_at = datetime()
            """,
            {
                "agent_id": score.agent_id,
                "risk_score": score.risk_score,
                "classification": score.classification,
                "fraud_rate": inputs.fraud_cashout_concentration,
            },
        )
    return score


async def classify_all(
    *, persist: bool = True, limit: int = 200
) -> dict[str, Any]:
    """Re-classify every agent. Used by the daily classification batch."""

    c = get_neo4j_client()
    rows = await c.execute_read(
        "MATCH (a:Agent) RETURN a.agent_id AS agent_id LIMIT $limit",
        {"limit": limit},
    )
    by_class: dict[str, int] = {}
    scored = 0
    errors = 0
    for r in rows:
        try:
            score = await classify_one(r["agent_id"], persist=persist, client=c)
            by_class[score.classification] = by_class.get(score.classification, 0) + 1
            scored += 1
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "agents.classify.error", agent_id=r.get("agent_id"), error=str(exc)
            )
            errors += 1
    logger.info(
        "agents.classify.complete", scored=scored, errors=errors, by_class=by_class
    )
    return {
        "scored": scored,
        "errors": errors,
        "by_class": by_class,
    }
