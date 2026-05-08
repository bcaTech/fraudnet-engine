"""Tunable thresholds, decay rates, and scoring weights for the mesh engine.

These values are surfaced via :class:`ScoringWeights` / :class:`ExpansionConfig`
so individual call-sites can override them (e.g. for backtesting). The defaults
here are also written into the ``ConfigParam`` table on first boot so analysts
can adjust them at runtime via the ``/api/config`` endpoint.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Final

# ---------------------------------------------------------------------------
# Mesh expansion
# ---------------------------------------------------------------------------

DEFAULT_MAX_DEPTH: Final[int] = 4
DEFAULT_EXPANSION_THRESHOLD: Final[float] = 0.25
DEFAULT_DISTANCE_DISCOUNT: Final[float] = 0.7
DEFAULT_CONVERGENCE_BONUS: Final[float] = 0.15
DEFAULT_CONVERGENCE_CAP: Final[float] = 0.30
DEFAULT_MAX_NODES_PER_CLUSTER: Final[int] = 5_000


@dataclass(frozen=True)
class ExpansionConfig:
    """Parameters governing breadth-first mesh expansion.

    See ``core/mesh/expansion.py`` for the algorithm.
    """

    max_depth: int = DEFAULT_MAX_DEPTH
    expansion_threshold: float = DEFAULT_EXPANSION_THRESHOLD
    distance_discount: float = DEFAULT_DISTANCE_DISCOUNT
    convergence_bonus: float = DEFAULT_CONVERGENCE_BONUS
    convergence_cap: float = DEFAULT_CONVERGENCE_CAP
    max_nodes: int = DEFAULT_MAX_NODES_PER_CLUSTER


# ---------------------------------------------------------------------------
# Confidence scoring weights
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScoringWeights:
    """Weights for the linear confidence-score combination.

    The five positive weights should sum to ~1.0 — the negative term is a
    multiplicative discount applied after summation.
    """

    seed_proximity: float = 0.30
    edge_strength: float = 0.20
    convergence: float = 0.15
    behavioral: float = 0.20
    predictive: float = 0.15
    negative: float = 0.40

    # Normalisation constants — values above these saturate to 1.0.
    edge_norm: float = 5.0
    conv_norm: float = 5.0


# ---------------------------------------------------------------------------
# Temporal decay
# ---------------------------------------------------------------------------

# Half-life (in days) for each edge type. Transactional links decay quickly,
# device-identity links persist far longer.
EDGE_HALF_LIFE_DAYS: Final[dict[str, float]] = {
    "SENT_TO": 30.0,
    "CASHED_OUT_AT": 30.0,
    "CASHED_IN_AT": 30.0,
    "INSERTED_IN": 365.0,
    "HAS_NUMBER": 365.0,
    "OWNS_WALLET": 730.0,
    "CONNECTED_TO": 14.0,
    "CO_LOCATED_WITH": 14.0,
}

DECAY_PRUNE_THRESHOLD: Final[float] = 0.05  # below this strength, prune the edge


# ---------------------------------------------------------------------------
# Cluster status / role
# ---------------------------------------------------------------------------

CLUSTER_STATUS_ACTIVE: Final[str] = "active"
CLUSTER_STATUS_INVESTIGATING: Final[str] = "investigating"
CLUSTER_STATUS_DISSOLVED: Final[str] = "dissolved"
CLUSTER_STATUS_TAKEDOWN_PENDING: Final[str] = "takedown_pending"
CLUSTER_STATUS_TAKEDOWN_COMPLETE: Final[str] = "takedown_complete"


# ---------------------------------------------------------------------------
# Wallet status
# ---------------------------------------------------------------------------

WALLET_STATUS_ACTIVE: Final[str] = "active"
WALLET_STATUS_FROZEN: Final[str] = "frozen"
WALLET_STATUS_FLAGGED: Final[str] = "flagged"
WALLET_STATUS_SUSPENDED: Final[str] = "suspended"
WALLET_STATUS_CLOSED: Final[str] = "closed"


# ---------------------------------------------------------------------------
# Agent classification
# ---------------------------------------------------------------------------

AGENT_CLASS_CLEAN: Final[str] = "clean"
AGENT_CLASS_INCIDENTAL: Final[str] = "incidental"
AGENT_CLASS_EXPLOITED: Final[str] = "exploited"
AGENT_CLASS_COMPLICIT: Final[str] = "complicit"

AGENT_RISK_THRESHOLDS: Final[dict[str, float]] = {
    AGENT_CLASS_CLEAN: 0.0,
    AGENT_CLASS_INCIDENTAL: 0.30,
    AGENT_CLASS_EXPLOITED: 0.55,
    AGENT_CLASS_COMPLICIT: 0.80,
}


# ---------------------------------------------------------------------------
# Confidence labels (used in API responses)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ConfidenceBands:
    low: float = 0.30
    medium: float = 0.55
    high: float = 0.75
    critical: float = 0.90


CONFIDENCE_BANDS: Final[ConfidenceBands] = ConfidenceBands()


def confidence_label(score: float) -> str:
    """Map a [0,1] confidence score to a categorical label."""

    if score >= CONFIDENCE_BANDS.critical:
        return "critical"
    if score >= CONFIDENCE_BANDS.high:
        return "high"
    if score >= CONFIDENCE_BANDS.medium:
        return "medium"
    if score >= CONFIDENCE_BANDS.low:
        return "low"
    return "minimal"


# ---------------------------------------------------------------------------
# Kafka topic names — kept here so tests and producers/consumers agree.
# ---------------------------------------------------------------------------

KAFKA_TOPICS: Final[dict[str, str]] = {
    "transactions": "fraudnet.transactions",
    "safeguard": "fraudnet.safeguard.events",
    "auth": "fraudnet.auth.events",
    "sim_swaps": "fraudnet.scancom.sim-swaps",
    "device_events": "fraudnet.scancom.device-events",
    "alerts": "fraudnet.alerts",
    "cluster_updates": "fraudnet.cluster-updates",
    "metric_updates": "fraudnet.metric-updates",
    "rules_triggers": "fraudnet.rules.triggers",
    "rules_actions": "fraudnet.rules.actions",
    "integration_inbound": "fraudnet.integration.inbound",
    "integration_outbound": "fraudnet.integration.outbound",
    "law_enforcement": "fraudnet.law-enforcement",
}


# ---------------------------------------------------------------------------
# Roles for RBAC
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Roles:
    viewer: str = "viewer"
    analyst: str = "analyst"
    investigator: str = "investigator"
    senior_investigator: str = "senior_investigator"
    admin: str = "admin"

    all: tuple[str, ...] = field(
        default=(
            "viewer",
            "analyst",
            "investigator",
            "senior_investigator",
            "admin",
        )
    )


ROLES: Final[Roles] = Roles()

# Role hierarchy: each role implicitly grants all roles ranked at or below it.
ROLE_RANK: Final[dict[str, int]] = {
    ROLES.viewer: 0,
    ROLES.analyst: 1,
    ROLES.investigator: 2,
    ROLES.senior_investigator: 3,
    ROLES.admin: 4,
}
