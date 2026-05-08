"""Pre-takedown readiness assessment.

Returns a structured score so the analyst UI can show "this cluster is
N/3 ready" before the takedown is approved. Cheaper-than-execution
checks: cluster confidence, member count, presence of cash-out agents,
existence of an unfrozen wallet (otherwise the takedown adds no
state), and the existence of a fund-flow path (otherwise there's no
evidence story).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from core.graph.client import Neo4jClient, get_neo4j_client


@dataclass
class ReadinessCheck:
    name: str
    ok: bool
    detail: dict[str, Any]


@dataclass
class ReadinessReport:
    cluster_id: str
    ready: bool
    score: float  # [0, 1] = pass_count / total_checks
    checks: list[ReadinessCheck]
    estimated_fraud_value: float


_CYPHER = """
MATCH (cl:Cluster {cluster_id: $cluster_id})
OPTIONAL MATCH (n)-[:BELONGS_TO]->(cl)
WITH cl,
     count(DISTINCT n) AS member_count,
     count(DISTINCT CASE WHEN n:Wallet AND coalesce(n.status, 'active') <> 'frozen'
                         THEN n END) AS unfrozen_wallets
OPTIONAL MATCH (a:Agent)-[:LINKED_TO]->(cl)
WITH cl, member_count, unfrozen_wallets, count(DISTINCT a) AS linked_agents
OPTIONAL MATCH (member:Wallet)-[:BELONGS_TO]->(cl)
OPTIONAL MATCH (member)-[r:CASHED_OUT_AT]->()
WITH cl, member_count, unfrozen_wallets, linked_agents, count(r) AS cashout_edges
RETURN
    cl.cluster_id AS cluster_id,
    coalesce(cl.confidence_score, 0.0)        AS confidence,
    coalesce(cl.estimated_fraud_value, 0.0)   AS estimated_fraud_value,
    member_count,
    unfrozen_wallets,
    linked_agents,
    cashout_edges
"""


_MIN_CONFIDENCE = 0.70
_MIN_MEMBERS = 4


async def assess(cluster_id: str, *, client: Neo4jClient | None = None) -> ReadinessReport:
    c = client or get_neo4j_client()
    rows = await c.execute_read(_CYPHER, {"cluster_id": cluster_id})
    if not rows:
        return ReadinessReport(
            cluster_id=cluster_id,
            ready=False,
            score=0.0,
            checks=[ReadinessCheck("cluster_exists", False, {})],
            estimated_fraud_value=0.0,
        )
    r = rows[0]
    confidence = float(r.get("confidence") or 0.0)
    member_count = int(r.get("member_count") or 0)
    unfrozen_wallets = int(r.get("unfrozen_wallets") or 0)
    linked_agents = int(r.get("linked_agents") or 0)
    cashout_edges = int(r.get("cashout_edges") or 0)

    checks = [
        ReadinessCheck("cluster_exists", True, {"cluster_id": cluster_id}),
        ReadinessCheck(
            "confidence_above_threshold",
            confidence >= _MIN_CONFIDENCE,
            {"value": confidence, "threshold": _MIN_CONFIDENCE},
        ),
        ReadinessCheck(
            "members_above_threshold",
            member_count >= _MIN_MEMBERS,
            {"value": member_count, "threshold": _MIN_MEMBERS},
        ),
        ReadinessCheck(
            "linked_agents_present",
            linked_agents >= 1,
            {"value": linked_agents},
        ),
        ReadinessCheck(
            "unfrozen_wallets_present",
            unfrozen_wallets >= 1,
            {"value": unfrozen_wallets},
        ),
        ReadinessCheck(
            "fund_flow_evidence",
            cashout_edges >= 1,
            {"value": cashout_edges},
        ),
    ]
    pass_count = sum(1 for ch in checks if ch.ok)
    score = pass_count / len(checks)
    return ReadinessReport(
        cluster_id=cluster_id,
        ready=score >= 0.66,
        score=round(score, 2),
        checks=checks,
        estimated_fraud_value=float(r.get("estimated_fraud_value") or 0.0),
    )
