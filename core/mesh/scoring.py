"""Confidence scoring for nodes and edges in a fraud mesh.

The score combines a small set of signals into a normalised [0, 1] confidence
that a given node belongs to the cluster of its seed:

- ``seed_proximity``      — distance-discounted closeness to the nearest seed
- ``edge_strength_sum``   — sum of strengths of edges pointing into the cluster
- ``convergence_factor``  — number of independent paths into the fraud subgraph
- ``behavioral_score``    — output of transactional pattern analysis ([0,1])
- ``predictive_score``    — output of the GNN ([0,1])
- ``negative_evidence``   — legitimate history discount ([0,1])

The five positive signals are linearly combined with weights from
:class:`ScoringWeights`; the negative signal applies a multiplicative discount.
The result is clamped to [0, 1].
"""

from __future__ import annotations

from dataclasses import dataclass

from config.constants import ScoringWeights


def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value


def calculate_node_confidence(
    *,
    seed_proximity: float,
    edge_strength_sum: float,
    convergence_factor: float,
    behavioral_score: float = 0.0,
    predictive_score: float = 0.0,
    negative_evidence: float = 0.0,
    weights: ScoringWeights | None = None,
) -> float:
    """Return the confidence score in [0, 1].

    ``seed_proximity`` is expected pre-discounted (the caller knows the depth);
    ``edge_strength_sum`` and ``convergence_factor`` are normalised by
    ``weights.edge_norm`` and ``weights.conv_norm`` respectively, saturating
    at 1.0.
    """

    w = weights or ScoringWeights()
    seed_proximity = _clamp01(seed_proximity)
    behavioral = _clamp01(behavioral_score)
    predictive = _clamp01(predictive_score)
    negative = _clamp01(negative_evidence)
    edge_norm = max(w.edge_norm, 1e-9)
    conv_norm = max(w.conv_norm, 1e-9)

    raw = (
        w.seed_proximity * seed_proximity
        + w.edge_strength * min(edge_strength_sum / edge_norm, 1.0)
        + w.convergence * min(convergence_factor / conv_norm, 1.0)
        + w.behavioral * behavioral
        + w.predictive * predictive
    )
    discount = max(0.0, 1.0 - w.negative * negative)
    return _clamp01(raw * discount)


@dataclass(frozen=True)
class EdgeStrengthInputs:
    """Inputs for computing the strength of a single edge.

    Edge strength reflects how strongly the edge implies that its endpoints
    belong to the same fraud cluster. Different edge types weight inputs
    differently — see :func:`calculate_edge_strength`.
    """

    edge_type: str
    co_occurrence_count: int = 0
    duration_days: float = 0.0
    amount_total: float = 0.0
    velocity_anomaly: float = 0.0
    days_since_last: float = 0.0


# Per-edge-type base weights. Higher = stronger implication of cluster
# membership for a fresh occurrence.
_EDGE_BASE: dict[str, float] = {
    "INSERTED_IN": 0.85,
    "HAS_NUMBER": 0.80,
    "OWNS_WALLET": 0.90,
    "SENT_TO": 0.55,
    "CASHED_OUT_AT": 0.50,
    "CASHED_IN_AT": 0.40,
    "CONNECTED_TO": 0.20,
    "CO_LOCATED_WITH": 0.65,
}


def calculate_edge_strength(inputs: EdgeStrengthInputs) -> float:
    """Compute an edge's structural strength in [0, 1] *before* temporal decay.

    Decay is applied separately by :func:`core.mesh.decay.apply_decay` so the
    structural component can be cached cheaply.
    """

    base = _EDGE_BASE.get(inputs.edge_type, 0.4)

    # Co-occurrence saturates quickly — 5 hits is "definitely related".
    cooc_term = min(inputs.co_occurrence_count / 5.0, 1.0) * 0.20

    # Duration bonus tops out at 90 days.
    duration_term = min(max(inputs.duration_days, 0.0) / 90.0, 1.0) * 0.10

    # High-velocity / round-number / structuring patterns add up to +15%.
    velocity_term = _clamp01(inputs.velocity_anomaly) * 0.15

    score = base + cooc_term + duration_term + velocity_term
    return _clamp01(score)
