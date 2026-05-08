"""Pydantic models for graph nodes and edges.

These mirror the Neo4j schema in CLAUDE.md and are used in API responses and
for validating data flowing through ingestion pipelines.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class NodeType(StrEnum):
    WALLET = "wallet"
    HANDSET = "handset"
    SIM = "sim"
    PHONE = "phone"
    AGENT = "agent"
    TRANSACTION = "transaction"
    CELL_TOWER = "cell_tower"
    CLUSTER = "cluster"


class _Base(BaseModel):
    model_config = ConfigDict(extra="ignore", from_attributes=True)


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------


class Wallet(_Base):
    wallet_id: str
    msisdn: str | None = None
    name: str | None = None
    kyc_tier: int | None = None
    creation_date: datetime | None = None
    balance: float | None = None
    status: str = "active"
    risk_score: float = 0.0
    cluster_id: str | None = None
    confidence_score: float = 0.0
    behavioral_score: float = 0.0
    predictive_score: float = 0.0
    is_sleeper: bool = False
    last_activity: datetime | None = None
    freeze_date: datetime | None = None


class Handset(_Base):
    imei: str
    make: str | None = None
    model: str | None = None
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    sim_count: int = 0
    flagged: bool = False
    flag_reason: str | None = None
    flag_date: datetime | None = None


class SIM(_Base):
    imsi: str
    registration_date: datetime | None = None
    msisdn: str | None = None
    status: str = "active"
    swap_count: int = 0
    last_swap_date: datetime | None = None
    flagged: bool = False


class PhoneNumber(_Base):
    msisdn: str
    registration_status: str | None = None
    kyc_tier: int | None = None
    account_age: int | None = None  # days


class Agent(_Base):
    agent_id: str
    name: str | None = None
    lat: float | None = None
    lng: float | None = None
    area_name: str | None = None
    registration_date: datetime | None = None
    risk_score: float = 0.0
    classification: str = "clean"
    monthly_volume: float | None = None
    fraud_cashout_rate: float | None = None
    float_avg: float | None = None
    suspended: bool = False
    suspension_date: datetime | None = None


class Transaction(_Base):
    tx_id: str
    type: str
    amount: float
    timestamp: datetime
    status: str = "completed"
    flagged: bool = False
    flag_reason: str | None = None


class CellTower(_Base):
    cell_id: str
    lac: int | None = None
    lat: float
    lng: float
    coverage_radius_m: float | None = None


class Cluster(_Base):
    cluster_id: str
    name: str | None = None
    seed_type: str
    seed_date: datetime
    seed_node_id: str
    node_count: int = 0
    confidence_score: float = 0.0
    status: str = "active"
    estimated_fraud_value: float = 0.0
    density: float | None = None
    isolation_score: float | None = None


# ---------------------------------------------------------------------------
# Generic graph node / edge for API responses
# ---------------------------------------------------------------------------


class GraphNode(_Base):
    """Generic node payload used in /clusters/:id/graph responses."""

    id: str = Field(..., description="Stable id (wallet_id, imei, imsi, agent_id, ...)")
    type: NodeType
    label: str | None = None
    risk_score: float = 0.0
    confidence_score: float = 0.0
    status: str | None = None
    properties: dict[str, object] = Field(default_factory=dict)


class GraphEdge(_Base):
    """Generic edge payload used in /clusters/:id/graph responses."""

    source: str
    target: str
    type: str
    strength: float = 0.0
    timestamp: datetime | None = None
    properties: dict[str, object] = Field(default_factory=dict)


class GraphPayload(_Base):
    nodes: list[GraphNode]
    edges: list[GraphEdge]


# ---------------------------------------------------------------------------
# Edge types accepted by ingestion / scoring
# ---------------------------------------------------------------------------

EdgeType = Literal[
    "INSERTED_IN",
    "HAS_NUMBER",
    "OWNS_WALLET",
    "SENT_TO",
    "CASHED_OUT_AT",
    "CASHED_IN_AT",
    "CONNECTED_TO",
    "CO_LOCATED_WITH",
    "BELONGS_TO",
    "LINKED_TO",
]
