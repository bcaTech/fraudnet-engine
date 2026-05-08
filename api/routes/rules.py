"""Rules engine endpoints (read-only listing + templates).

The full rule lifecycle (draft → backtest → shadow → live → paused →
retired), trigger streaming, and "what-if" simulation lives in
``rules/engine.py`` once that module lands. This router exposes only the
read paths the NOC frontend needs today: list rules, fetch rule detail
with trigger summary, and surface the pre-built templates analysts can
fork from.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, status
from sqlalchemy import desc, func, select

from api.dependencies import DBSessionDep
from api.schemas import APIResponse, Meta, ok
from db.models import Rule, RuleTrigger

router = APIRouter(prefix="/api/rules", tags=["rules"])


# ---------------------------------------------------------------------------
# Templates — must be declared before /{rule_id} so it isn't shadowed
# ---------------------------------------------------------------------------


_TEMPLATES: list[dict[str, Any]] = [
    {
        "template_id": "high_velocity_p2p",
        "name": "High-velocity P2P transfers",
        "description": "Catch wallets sending >8 P2P transfers in a 5-minute window.",
        "category": "velocity",
        "default_conditions": {
            "operator": "AND",
            "conditions": [{"field": "node.tx_count_5m", "op": "greater_than", "value": 8}],
        },
        "suggested_actions": [{"type": "freeze_wallet", "params": {}}],
    },
    {
        "template_id": "cross_network_burst",
        "name": "Cross-network bursting",
        "description": "Trigger when >3 cross-network transfers occur in 24h.",
        "category": "cross_network",
        "default_conditions": {
            "operator": "AND",
            "conditions": [{"field": "node.cross_network_24h", "op": "greater_than", "value": 3}],
        },
        "suggested_actions": [{"type": "block_cross_network", "params": {}}],
    },
    {
        "template_id": "sleeper_awakening",
        "name": "Sleeper wallet awakening",
        "description": "Wallet dormant >90 days suddenly transacts.",
        "category": "anomaly",
        "default_conditions": {
            "operator": "AND",
            "conditions": [{"field": "node.dormant_days", "op": "greater_than", "value": 90}],
        },
        "suggested_actions": [{"type": "apply_send_with_care", "params": {}}],
    },
    {
        "template_id": "structured_cashouts",
        "name": "Structured cashouts",
        "description": "Five+ consecutive round-amount cashouts.",
        "category": "structuring",
        "default_conditions": {
            "operator": "AND",
            "conditions": [{"field": "node.round_amount_streak", "op": "greater_than", "value": 5}],
        },
        "suggested_actions": [{"type": "freeze_wallet", "params": {}}],
    },
    {
        "template_id": "agent_fraud_concentration",
        "name": "Agent fraud concentration",
        "description": "Agent with >30% fraud-cashout rate.",
        "category": "agent_risk",
        "default_conditions": {
            "operator": "AND",
            "conditions": [{"field": "agent.fraud_cashout_rate", "op": "greater_than", "value": 0.30}],
        },
        "suggested_actions": [{"type": "downgrade_agent_float", "params": {}}],
    },
    {
        "template_id": "sim_swap_chain",
        "name": "SIM swap chain",
        "description": "Multiple SIM swaps in 30 days.",
        "category": "device",
        "default_conditions": {
            "operator": "AND",
            "conditions": [{"field": "node.sim_swap_count_30d", "op": "greater_than", "value": 2}],
        },
        "suggested_actions": [{"type": "apply_ask_me_first", "params": {}}],
    },
]


@router.get("/templates")
async def list_templates() -> APIResponse[list[dict[str, Any]]]:
    """Pre-built rule templates analysts can fork into draft rules."""

    return ok(_TEMPLATES)


# ---------------------------------------------------------------------------
# List + detail
# ---------------------------------------------------------------------------


def _rule_to_dict(r: Rule) -> dict[str, Any]:
    fp_rate = round(r.false_positive_count / r.trigger_count, 4) if r.trigger_count else 0.0
    return {
        "id": r.id,
        "name": r.name,
        "description": r.description,
        "status": r.status,
        "evaluation_mode": r.evaluation_mode,
        "schedule_interval": r.schedule_interval,
        "scope": r.scope,
        "conditions": r.conditions,
        "actions": r.actions,
        "expiry_date": r.expiry_date.isoformat() if r.expiry_date else None,
        "expiry_triggers": r.expiry_triggers,
        "created_by": r.created_by,
        "created_at": r.created_at.isoformat() if r.created_at else None,
        "updated_at": r.updated_at.isoformat() if r.updated_at else None,
        "approved_by": r.approved_by,
        "approved_at": r.approved_at.isoformat() if r.approved_at else None,
        "trigger_count": r.trigger_count,
        "false_positive_count": r.false_positive_count,
        "false_positive_rate": fp_rate,
    }


@router.get("")
async def list_rules(
    db: DBSessionDep,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    status_filter: str | None = Query(
        None,
        alias="status",
        pattern="^(draft|backtesting|shadow|live|paused|retired)$",
    ),
    created_by: str | None = None,
) -> APIResponse[list[dict[str, Any]]]:
    base = select(Rule)
    if status_filter:
        base = base.where(Rule.status == status_filter)
    if created_by:
        base = base.where(Rule.created_by == created_by)

    total = (await db.execute(select(func.count()).select_from(base.subquery()))).scalar_one()
    page_q = base.order_by(Rule.updated_at.desc()).offset((page - 1) * per_page).limit(per_page)
    rows = (await db.execute(page_q)).scalars().all()
    return APIResponse(
        data=[_rule_to_dict(r) for r in rows],
        meta=Meta(total=int(total), page=page, per_page=per_page),
        errors=[],
    )


@router.get("/{rule_id}")
async def get_rule(rule_id: str, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    rule = (await db.execute(select(Rule).where(Rule.id == rule_id))).scalar_one_or_none()
    if rule is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "rule not found")

    # Trigger summary: last 5 triggers + outcome counts.
    recent = (
        (
            await db.execute(
                select(RuleTrigger)
                .where(RuleTrigger.rule_id == rule_id)
                .order_by(desc(RuleTrigger.triggered_at))
                .limit(5)
            )
        )
        .scalars()
        .all()
    )
    by_outcome = (
        await db.execute(
            select(RuleTrigger.outcome, func.count())
            .where(RuleTrigger.rule_id == rule_id)
            .group_by(RuleTrigger.outcome)
        )
    ).all()

    payload = _rule_to_dict(rule)
    payload["recent_triggers"] = [
        {
            "id": t.id,
            "triggered_at": t.triggered_at.isoformat() if t.triggered_at else None,
            "node_id": t.node_id,
            "node_type": t.node_type,
            "outcome": t.outcome,
            "actions_executed": t.actions_executed,
        }
        for t in recent
    ]
    payload["outcome_counts"] = {o or "unknown": int(c) for o, c in by_outcome}
    return ok(payload)


@router.get("/{rule_id}/triggers")
async def list_rule_triggers(
    rule_id: str,
    db: DBSessionDep,
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    outcome: str | None = Query(None, pattern="^(success|overridden|failed|pending_approval)$"),
) -> APIResponse[list[dict[str, Any]]]:
    base = select(RuleTrigger).where(RuleTrigger.rule_id == rule_id)
    if outcome:
        base = base.where(RuleTrigger.outcome == outcome)
    total = (await db.execute(select(func.count()).select_from(base.subquery()))).scalar_one()
    rows = (
        (
            await db.execute(
                base.order_by(desc(RuleTrigger.triggered_at)).offset((page - 1) * per_page).limit(per_page)
            )
        )
        .scalars()
        .all()
    )

    payload = [
        {
            "id": t.id,
            "rule_id": t.rule_id,
            "triggered_at": t.triggered_at.isoformat() if t.triggered_at else None,
            "node_id": t.node_id,
            "node_type": t.node_type,
            "context": t.context,
            "actions_executed": t.actions_executed,
            "outcome": t.outcome,
            "overridden_by": t.overridden_by,
            "override_reason": t.override_reason,
        }
        for t in rows
    ]
    return APIResponse(
        data=payload,
        meta=Meta(total=int(total), page=page, per_page=per_page),
        errors=[],
    )


@router.get("/{rule_id}/performance")
async def rule_performance(rule_id: str, db: DBSessionDep) -> APIResponse[dict[str, Any]]:
    """Aggregate trigger-rate / fp-rate / override-rate. Lightweight version
    of the analytics surfaced on the rule detail page."""

    rule = (await db.execute(select(Rule).where(Rule.id == rule_id))).scalar_one_or_none()
    if rule is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "rule not found")

    by_outcome = dict(
        (o or "unknown", int(c))
        for o, c in (
            await db.execute(
                select(RuleTrigger.outcome, func.count())
                .where(RuleTrigger.rule_id == rule_id)
                .group_by(RuleTrigger.outcome)
            )
        ).all()
    )
    total = sum(by_outcome.values())
    overrides = by_outcome.get("overridden", 0)

    age_days = max(
        1,
        int((datetime.now(UTC) - (rule.created_at or datetime.now(UTC))).total_seconds() // 86_400),
    )
    return ok(
        {
            "rule_id": rule_id,
            "status": rule.status,
            "trigger_count": rule.trigger_count,
            "false_positive_count": rule.false_positive_count,
            "false_positive_rate": (
                round(rule.false_positive_count / rule.trigger_count, 4) if rule.trigger_count else 0.0
            ),
            "trigger_rate_per_day": round(rule.trigger_count / age_days, 3),
            "override_count": overrides,
            "override_rate": round(overrides / total, 4) if total else 0.0,
            "outcome_counts": by_outcome,
        }
    )
