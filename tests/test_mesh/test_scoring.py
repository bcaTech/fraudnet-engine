"""Unit tests for confidence and edge-strength scoring."""

from __future__ import annotations

from config.constants import ScoringWeights
from core.mesh.scoring import (
    EdgeStrengthInputs,
    calculate_edge_strength,
    calculate_node_confidence,
)


def test_confidence_clamps_to_zero_for_no_signal() -> None:
    score = calculate_node_confidence(
        seed_proximity=0.0,
        edge_strength_sum=0.0,
        convergence_factor=0.0,
    )
    assert score == 0.0


def test_confidence_clamps_to_one_under_max_signal() -> None:
    score = calculate_node_confidence(
        seed_proximity=1.0,
        edge_strength_sum=10.0,  # well above edge_norm
        convergence_factor=10.0,
        behavioral_score=1.0,
        predictive_score=1.0,
        negative_evidence=0.0,
    )
    assert 0.95 <= score <= 1.0


def test_negative_evidence_discounts_score() -> None:
    inputs = dict(
        seed_proximity=0.8,
        edge_strength_sum=2.0,
        convergence_factor=2.0,
        behavioral_score=0.5,
        predictive_score=0.5,
    )
    high = calculate_node_confidence(**inputs, negative_evidence=0.0)
    low = calculate_node_confidence(**inputs, negative_evidence=1.0)
    assert low < high


def test_weights_are_respected() -> None:
    """Doubling seed_proximity weight should pull score toward seed signal."""

    base = ScoringWeights()
    boosted = ScoringWeights(
        seed_proximity=base.seed_proximity * 2,
        edge_strength=base.edge_strength,
        convergence=base.convergence,
        behavioral=base.behavioral,
        predictive=base.predictive,
        negative=base.negative,
    )
    args = dict(
        seed_proximity=1.0,
        edge_strength_sum=0.0,
        convergence_factor=0.0,
    )
    assert calculate_node_confidence(**args, weights=boosted) > calculate_node_confidence(
        **args, weights=base
    )


def test_edge_strength_in_unit_range() -> None:
    s = calculate_edge_strength(
        EdgeStrengthInputs(
            edge_type="SENT_TO",
            co_occurrence_count=10,
            duration_days=120,
            velocity_anomaly=1.0,
        )
    )
    assert 0.0 <= s <= 1.0


def test_edge_strength_unknown_type_falls_back() -> None:
    s = calculate_edge_strength(EdgeStrengthInputs(edge_type="MYSTERY_REL"))
    assert 0.0 <= s <= 1.0
