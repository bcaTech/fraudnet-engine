"""Area-adjusted baselines for agent risk scoring.

Computes per-area average fraud-cashout rates and monthly volumes so
the agent risk scorer can normalise an individual agent's stats against
the local norm. An agent in a high-fraud area shouldn't be penalised
for matching the local baseline; an agent in a low-fraud area whose
stats look ordinary in absolute terms might still be an outlier
locally.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.graph.client import Neo4jClient, get_neo4j_client


@dataclass
class AreaBaseline:
    area_name: str
    agent_count: int
    mean_fraud_rate: float
    mean_monthly_volume: float


_BASELINE_QUERY = """
MATCH (a:Agent)
WHERE a.area_name IS NOT NULL
WITH a.area_name AS area, collect(a) AS agents
RETURN
    area AS area_name,
    size(agents) AS agent_count,
    avg(coalesce([x IN agents | x.fraud_cashout_rate][0], 0.0)) AS mean_fraud_rate,
    avg(coalesce([x IN agents | x.monthly_volume][0], 0.0)) AS mean_monthly_volume
"""


_PER_AREA_QUERY = """
MATCH (a:Agent)
WHERE a.area_name IS NOT NULL
RETURN
    a.area_name AS area_name,
    count(a) AS agent_count,
    avg(coalesce(a.fraud_cashout_rate, 0.0)) AS mean_fraud_rate,
    avg(coalesce(a.monthly_volume, 0.0)) AS mean_monthly_volume
ORDER BY area_name
"""


async def baselines_by_area(
    *, client: Neo4jClient | None = None
) -> dict[str, AreaBaseline]:
    c = client or get_neo4j_client()
    rows = await c.execute_read(_PER_AREA_QUERY)
    return {
        r["area_name"]: AreaBaseline(
            area_name=r["area_name"],
            agent_count=int(r.get("agent_count") or 0),
            mean_fraud_rate=float(r.get("mean_fraud_rate") or 0.0),
            mean_monthly_volume=float(r.get("mean_monthly_volume") or 0.0),
        )
        for r in rows
    }


def baseline_deviation(
    *, agent_fraud_rate: float, baseline: AreaBaseline | None
) -> float:
    """Return a [0, 1] score capturing how much the agent's fraud rate
    exceeds its local baseline. ``0`` means the agent matches or beats
    the baseline; ``1`` means it's at least 5× the baseline."""

    if baseline is None or baseline.mean_fraud_rate <= 0.0:
        return min(1.0, agent_fraud_rate)
    ratio = agent_fraud_rate / baseline.mean_fraud_rate
    return float(max(0.0, min(1.0, (ratio - 1.0) / 4.0)))
