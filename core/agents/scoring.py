"""Agent risk scoring.

Implements the seven-component risk model documented in CLAUDE.md
§ "Agent Risk Scoring":

- fraud_wallet_concentration — share of cashouts coming from wallets
  flagged as fraud-linked
- velocity_clustering — multiple cashouts from different wallets in
  short windows
- amount_pattern_score — round-amount / structuring patterns
- geographic_anomaly — wallets cashing out far from their home cell
  tower
- float_anomaly — unusual float prep before / after fraud bursts
- historical_deviation — deviation from this agent's own 90-day baseline
- area_baseline — area-adjusted comparison via :mod:`geographic`

The function is pure — fetch the inputs once via
:func:`fetch_agent_inputs`, then call :func:`calculate_agent_risk`. This
makes the math easy to unit test and lets the same scorer drive both
the periodic batch and the live `/api/agents/:id` recompute.
"""

from __future__ import annotations

from dataclasses import dataclass

from core.graph.client import Neo4jClient, get_neo4j_client

from .geographic import AreaBaseline, baseline_deviation


@dataclass
class AgentInputs:
    agent_id: str
    area_name: str | None
    fraud_cashout_concentration: float
    velocity_clustering: float
    amount_pattern_score: float
    geographic_anomaly: float
    float_anomaly: float
    historical_deviation: float


@dataclass
class AgentScore:
    agent_id: str
    risk_score: float  # [0, 1]
    classification: str  # clean | incidental | exploited | complicit
    components: dict[str, float]


# Component weights — sum to 1.0 (the area-baseline term acts as a
# multiplier on the geographic_anomaly component, not a separate
# additive term).
WEIGHTS: dict[str, float] = {
    "fraud_wallet_concentration": 0.35,
    "velocity_clustering": 0.20,
    "amount_pattern_score": 0.10,
    "geographic_anomaly": 0.10,
    "float_anomaly": 0.05,
    "historical_deviation": 0.20,
}


CLASS_THRESHOLDS = (
    (0.80, "complicit"),
    (0.55, "exploited"),
    (0.30, "incidental"),
    (0.0, "clean"),
)


_AGENT_INPUTS_CYPHER = """
MATCH (a:Agent {agent_id: $agent_id})
OPTIONAL MATCH (w:Wallet)-[r:CASHED_OUT_AT]->(a)
WHERE r.timestamp >= datetime() - duration({days: 30})
WITH a, collect({wallet: w, ts: r.timestamp, amount: r.amount}) AS cashouts
WITH a,
     [c IN cashouts WHERE c.wallet IS NOT NULL] AS valid,
     size([c IN cashouts WHERE c.wallet.cluster_id IS NOT NULL]) AS fraud_cashouts,
     size(cashouts) AS total_cashouts,
     [c IN cashouts WHERE coalesce(c.amount, 0.0) % 100 = 0] AS round_amounts
WITH a, valid, fraud_cashouts, total_cashouts, size(round_amounts) AS round_count
RETURN
    a.agent_id      AS agent_id,
    a.area_name     AS area_name,
    coalesce(a.fraud_cashout_rate, 0.0) AS recorded_fraud_rate,
    coalesce(a.monthly_volume, 0.0)     AS monthly_volume,
    coalesce(a.float_avg, 0.0)           AS float_avg,
    fraud_cashouts,
    total_cashouts,
    round_count
"""


def _safe_div(num: float, den: float) -> float:
    return float(num / den) if den else 0.0


async def fetch_agent_inputs(agent_id: str, *, client: Neo4jClient | None = None) -> AgentInputs:
    c = client or get_neo4j_client()
    rows = await c.execute_read(_AGENT_INPUTS_CYPHER, {"agent_id": agent_id})
    if not rows:
        raise ValueError(f"agent not found: {agent_id}")
    r = rows[0]

    total_cashouts = float(r.get("total_cashouts") or 0)
    fraud_cashouts = float(r.get("fraud_cashouts") or 0)
    round_count = float(r.get("round_count") or 0)
    monthly_volume = float(r.get("monthly_volume") or 0.0)
    float_avg = float(r.get("float_avg") or 0.0)

    # Concentration: fraction of recent cashouts that came from
    # cluster-linked wallets.
    fraud_concentration = _safe_div(fraud_cashouts, total_cashouts)

    # Velocity clustering: 30+ cashouts in 30 days as a saturating
    # signal — anything above 60 saturates to 1.0.
    velocity_clustering = min(1.0, total_cashouts / 60.0)

    # Amount-pattern: share of cashouts that are round amounts.
    amount_pattern_score = _safe_div(round_count, total_cashouts)

    # Geographic anomaly: not derivable from current schema (cell-tower
    # cross-reference is wired only on Handsets); leave at 0 until the
    # link lands.
    geographic_anomaly = 0.0

    # Float anomaly: very high float relative to monthly volume is a
    # weak signal of mule-ready preparation.
    float_anomaly = min(1.0, _safe_div(float_avg, max(monthly_volume, 1.0)) * 50.0)

    # Historical deviation: compare current 30d fraud rate to recorded
    # rolling baseline. Saturating delta.
    recorded = float(r.get("recorded_fraud_rate") or 0.0)
    historical_deviation = min(1.0, max(0.0, fraud_concentration - recorded) * 5.0)

    return AgentInputs(
        agent_id=str(r["agent_id"]),
        area_name=r.get("area_name"),
        fraud_cashout_concentration=fraud_concentration,
        velocity_clustering=velocity_clustering,
        amount_pattern_score=amount_pattern_score,
        geographic_anomaly=geographic_anomaly,
        float_anomaly=float_anomaly,
        historical_deviation=historical_deviation,
    )


def classification_for(score: float) -> str:
    for threshold, label in CLASS_THRESHOLDS:
        if score >= threshold:
            return label
    return "clean"


def calculate_agent_risk(
    inputs: AgentInputs,
    *,
    area_baseline: AreaBaseline | None = None,
    weights: dict[str, float] = WEIGHTS,
) -> AgentScore:
    """Combine the inputs into a single ``[0, 1]`` risk score plus a
    discrete classification."""

    # Apply area-baseline modulation to the geographic component.
    geo_term = inputs.geographic_anomaly
    if area_baseline is not None:
        geo_term = max(
            geo_term,
            baseline_deviation(
                agent_fraud_rate=inputs.fraud_cashout_concentration,
                baseline=area_baseline,
            ),
        )

    components = {
        "fraud_wallet_concentration": inputs.fraud_cashout_concentration,
        "velocity_clustering": inputs.velocity_clustering,
        "amount_pattern_score": inputs.amount_pattern_score,
        "geographic_anomaly": geo_term,
        "float_anomaly": inputs.float_anomaly,
        "historical_deviation": inputs.historical_deviation,
    }
    score = sum(components[k] * weights.get(k, 0.0) for k in components)
    score = max(0.0, min(1.0, score))
    return AgentScore(
        agent_id=inputs.agent_id,
        risk_score=round(score, 4),
        classification=classification_for(score),
        components={k: round(v, 4) for k, v in components.items()},
    )


__all__ = [
    "AgentInputs",
    "AgentScore",
    "WEIGHTS",
    "fetch_agent_inputs",
    "calculate_agent_risk",
    "classification_for",
]
