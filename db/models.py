"""SQLAlchemy ORM models for relational FraudNet state.

The graph (wallets, handsets, SIMs, agents, transactions, clusters) lives in
Neo4j. Postgres holds *workflow* state — alerts, takedowns, rules, operator
integrations, law-enforcement cases, audit logs — anything that benefits
from row-level transactions, indexes, and joins.

Schema is created via Alembic in production. The demo seeder calls
``Base.metadata.create_all`` on a sync engine, which is sufficient for local
dev until the first Alembic migration lands.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    """Declarative base for all relational models."""


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    username: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)
    email: Mapped[str] = mapped_column(String(160), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(256), nullable=False)
    role: Mapped[str] = mapped_column(String(40), nullable=False, default="viewer")
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    last_login: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------


class Alert(Base):
    __tablename__ = "alerts"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    type: Mapped[str] = mapped_column(String(64), index=True)
    severity: Mapped[str] = mapped_column(String(16), index=True)
    title: Mapped[str] = mapped_column(String(255))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    target_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    target_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    cluster_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    acknowledged: Mapped[bool] = mapped_column(Boolean, default=False, index=True)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    acknowledged_by: Mapped[str | None] = mapped_column(String(40), nullable=True)
    rule_id: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    extra: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


# ---------------------------------------------------------------------------
# Rules engine
# ---------------------------------------------------------------------------


class Rule(Base):
    __tablename__ = "rules"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    name: Mapped[str] = mapped_column(String(200))
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str | None] = mapped_column(String(40), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    status: Mapped[str] = mapped_column(String(24), default="draft", index=True)
    conditions: Mapped[dict[str, Any]] = mapped_column(JSON)
    actions: Mapped[list[dict[str, Any]]] = mapped_column(JSON)
    scope: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    evaluation_mode: Mapped[str] = mapped_column(String(24), default="realtime")
    schedule_interval: Mapped[str | None] = mapped_column(String(40), nullable=True)
    expiry_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expiry_triggers: Mapped[int | None] = mapped_column(Integer, nullable=True)
    approved_by: Mapped[str | None] = mapped_column(String(40), nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    trigger_count: Mapped[int] = mapped_column(Integer, default=0)
    false_positive_count: Mapped[int] = mapped_column(Integer, default=0)


class RuleTrigger(Base):
    __tablename__ = "rule_triggers"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    rule_id: Mapped[str] = mapped_column(String(40), ForeignKey("rules.id"), index=True)
    triggered_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow, index=True)
    event_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    node_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    node_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    context: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    actions_executed: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    outcome: Mapped[str] = mapped_column(String(24), default="success")
    overridden_by: Mapped[str | None] = mapped_column(String(40), nullable=True)
    override_reason: Mapped[str | None] = mapped_column(Text, nullable=True)


# ---------------------------------------------------------------------------
# Takedowns
# ---------------------------------------------------------------------------


class Takedown(Base):
    __tablename__ = "takedowns"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    cluster_id: Mapped[str] = mapped_column(String(64), index=True)
    initiated_by: Mapped[str | None] = mapped_column(String(40), nullable=True)
    initiated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    approved_by: Mapped[str | None] = mapped_column(String(40), nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String(24), default="pending", index=True)
    wallets_frozen: Mapped[int] = mapped_column(Integer, default=0)
    sims_flagged: Mapped[int] = mapped_column(Integer, default=0)
    agents_alerted: Mapped[int] = mapped_column(Integer, default=0)
    evidence_package_id: Mapped[str | None] = mapped_column(String(80), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)

    steps: Mapped[list[TakedownStep]] = relationship(back_populates="takedown", cascade="all, delete-orphan")


class TakedownStep(Base):
    __tablename__ = "takedown_steps"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    takedown_id: Mapped[str] = mapped_column(String(40), ForeignKey("takedowns.id"), index=True)
    step_type: Mapped[str] = mapped_column(String(48))
    status: Mapped[str] = mapped_column(String(24), default="pending")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    detail: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    takedown: Mapped[Takedown] = relationship(back_populates="steps")


# ---------------------------------------------------------------------------
# Law enforcement
# ---------------------------------------------------------------------------


class LEAgency(Base):
    __tablename__ = "le_agencies"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    name: Mapped[str] = mapped_column(String(160), unique=True)
    type: Mapped[str] = mapped_column(String(40))
    contact_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    contact_email: Mapped[str | None] = mapped_column(String(160), nullable=True)
    contact_phone: Mapped[str | None] = mapped_column(String(32), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class LECase(Base):
    __tablename__ = "le_cases"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    agency_id: Mapped[str] = mapped_column(String(40), ForeignKey("le_agencies.id"), index=True)
    status: Mapped[str] = mapped_column(String(32), default="open", index=True)
    cluster_ids: Mapped[list[str]] = mapped_column(JSON, default=list)
    created_by: Mapped[str | None] = mapped_column(String(40), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    assigned_officer: Mapped[str | None] = mapped_column(String(120), nullable=True)
    officer_contact: Mapped[str | None] = mapped_column(String(160), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    agency: Mapped[LEAgency] = relationship()
    messages: Mapped[list[LECaseMessage]] = relationship(back_populates="case", cascade="all, delete-orphan")


class LECaseMessage(Base):
    __tablename__ = "le_case_messages"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    case_id: Mapped[str] = mapped_column(String(40), ForeignKey("le_cases.id"), index=True)
    sender_id: Mapped[str] = mapped_column(String(40))
    sender_role: Mapped[str] = mapped_column(String(40))
    content: Mapped[str] = mapped_column(Text)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    attachments: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)

    case: Mapped[LECase] = relationship(back_populates="messages")


class LEOutcome(Base):
    __tablename__ = "le_outcomes"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    case_id: Mapped[str] = mapped_column(String(40), ForeignKey("le_cases.id"), index=True)
    outcome_type: Mapped[str] = mapped_column(String(40))
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    amount_recovered: Mapped[float | None] = mapped_column(Float, nullable=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    reported_by: Mapped[str | None] = mapped_column(String(40), nullable=True)


class APIKey(Base):
    __tablename__ = "api_keys"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    operator_id: Mapped[str | None] = mapped_column(
        String(40), ForeignKey("external_operators.id"), nullable=True, index=True
    )
    key_hash: Mapped[str] = mapped_column(String(80), index=True)
    key_prefix: Mapped[str] = mapped_column(String(16))  # first chars for UI display
    permissions: Mapped[list[str]] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    active: Mapped[bool] = mapped_column(Boolean, default=True)


# ---------------------------------------------------------------------------
# Operator integration
# ---------------------------------------------------------------------------


class ExternalOperator(Base):
    __tablename__ = "external_operators"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    contact_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    contact_email: Mapped[str | None] = mapped_column(String(160), nullable=True)
    technical_contact: Mapped[str | None] = mapped_column(String(160), nullable=True)
    status: Mapped[str] = mapped_column(String(24), default="pending", index=True)
    integration_type: Mapped[str] = mapped_column(String(40), default="bidirectional")
    data_sharing_level: Mapped[str] = mapped_column(String(24), default="hashed")
    masking_rules: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    auto_integrate: Mapped[bool] = mapped_column(Boolean, default=False)
    onboarding_step: Mapped[str] = mapped_column(String(40), default="initial")
    last_health_check: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_health_status: Mapped[str | None] = mapped_column(String(24), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)


class SharedFlag(Base):
    __tablename__ = "shared_flags"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    direction: Mapped[str] = mapped_column(String(12), index=True)  # inbound|outbound
    operator_id: Mapped[str] = mapped_column(String(40), ForeignKey("external_operators.id"), index=True)
    identifier_type: Mapped[str] = mapped_column(String(24))
    identifier_masked: Mapped[str | None] = mapped_column(String(160), nullable=True)
    identifier_hash: Mapped[str] = mapped_column(String(80))
    risk_score: Mapped[float] = mapped_column(Float, default=0.0)
    context: Mapped[str | None] = mapped_column(Text, nullable=True)
    shared_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    action_taken: Mapped[str | None] = mapped_column(String(32), nullable=True)
    actioned_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


# ---------------------------------------------------------------------------
# Evidence
# ---------------------------------------------------------------------------


class EvidencePackage(Base):
    __tablename__ = "evidence_packages"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    cluster_id: Mapped[str] = mapped_column(String(64), index=True)
    case_id: Mapped[str | None] = mapped_column(
        String(40), ForeignKey("le_cases.id"), nullable=True, index=True
    )
    takedown_id: Mapped[str | None] = mapped_column(
        String(40), ForeignKey("takedowns.id"), nullable=True, index=True
    )
    generated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    generated_by: Mapped[str | None] = mapped_column(String(40), nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=1)
    file_hash: Mapped[str] = mapped_column(String(80))
    file_path: Mapped[str] = mapped_column(String(255))
    page_count: Mapped[int] = mapped_column(Integer, default=0)
    file_size: Mapped[int] = mapped_column(Integer, default=0)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)


class EvidenceAccess(Base):
    __tablename__ = "evidence_access"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    package_id: Mapped[str] = mapped_column(String(40), ForeignKey("evidence_packages.id"), index=True)
    accessed_by: Mapped[str] = mapped_column(String(40))
    accessed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_utcnow)
    user_role: Mapped[str | None] = mapped_column(String(40), nullable=True)


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------


class AuditLog(Base):
    """Immutable record of every protected action.

    Written by :mod:`api.middleware.audit` for any HTTP method other
    than GET/HEAD/OPTIONS. PII is redacted at the gateway before
    reaching this row. Production retention policy applies a partition
    + Iceberg archive cycle; for dev we keep everything.
    """

    __tablename__ = "audit_logs"

    id: Mapped[str] = mapped_column(String(40), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=_utcnow, index=True
    )
    actor_id: Mapped[str | None] = mapped_column(String(40), nullable=True, index=True)
    actor_role: Mapped[str | None] = mapped_column(String(40), nullable=True)
    actor_kind: Mapped[str] = mapped_column(String(16), default="user")  # user | service | system
    action: Mapped[str] = mapped_column(String(80), index=True)  # e.g. "alerts.acknowledge"
    method: Mapped[str] = mapped_column(String(8))
    path: Mapped[str] = mapped_column(String(255))
    status_code: Mapped[int] = mapped_column(Integer)
    target_kind: Mapped[str | None] = mapped_column(String(32), nullable=True)
    target_id: Mapped[str | None] = mapped_column(String(80), nullable=True, index=True)
    request_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ip_address: Mapped[str | None] = mapped_column(String(64), nullable=True)
    user_agent: Mapped[str | None] = mapped_column(String(255), nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    extra: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)


__all__ = [
    "Base",
    "User",
    "Alert",
    "Rule",
    "RuleTrigger",
    "Takedown",
    "TakedownStep",
    "LEAgency",
    "LECase",
    "LECaseMessage",
    "LEOutcome",
    "ExternalOperator",
    "SharedFlag",
    "EvidencePackage",
    "EvidenceAccess",
    "APIKey",
    "AuditLog",
]
