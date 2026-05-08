"""Escalation actions: open an alert / watchlist / law-enforcement flag.

These actions write to Postgres (Alert table) or set a graph flag, and
publish to the WS alerts feed so the NOC dashboard sees the escalation
in real time.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from core.graph.client import get_neo4j_client
from db.models import Alert
from db.session import get_async_session

from .registry import ActionContext, ActionRegistry, ActionResult


async def _publish_alert_event(event: str, payload: dict) -> None:
    """Lazy-imported WS publisher. Kept inside the function so the rules
    package can be imported in environments (e.g. Celery worker forks)
    that haven't fully resolved the ``api`` package — the publish is
    best-effort and a missing import shouldn't break the rule run."""

    try:
        from api.websocket.publisher import CH_ALERTS, publish

        await publish(CH_ALERTS, event, payload)
    except Exception:  # noqa: BLE001 — never let WS broadcast break a rule
        pass


_TYPE_TO_LABEL = {
    "wallet": ("Wallet", "wallet_id"),
    "agent": ("Agent", "agent_id"),
    "sim": ("SIM", "imsi"),
    "handset": ("Handset", "imei"),
}


async def _add_to_watchlist(ctx: ActionContext) -> ActionResult:
    label_key = _TYPE_TO_LABEL.get(ctx.target_type)
    if label_key is None:
        return ActionResult(
            ok=False, detail={"target_type": ctx.target_type}, error="unsupported target type"
        )
    label, key = label_key
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (n)
        WHERE any(l IN labels(n) WHERE l = $label) AND n[$key] = $id
        SET n.on_watchlist = true,
            n.watchlist_added = datetime(),
            n.watchlist_reason = $reason
        RETURN labels(n) AS labels
        """,
        {
            "label": label,
            "key": key,
            "id": ctx.target,
            "reason": ctx.params.get("reason", "rule_triggered"),
        },
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "on_watchlist": True})


async def _remove_from_watchlist(ctx: ActionContext) -> ActionResult:
    label_key = _TYPE_TO_LABEL.get(ctx.target_type)
    if label_key is None:
        return ActionResult(
            ok=False, detail={"target_type": ctx.target_type}, error="unsupported target type"
        )
    label, key = label_key
    client = get_neo4j_client()
    rows = await client.execute_write(
        """
        MATCH (n)
        WHERE any(l IN labels(n) WHERE l = $label) AND n[$key] = $id
        SET n.on_watchlist = false
        REMOVE n.watchlist_reason, n.watchlist_added
        RETURN n[$key] AS id
        """,
        {"label": label, "key": key, "id": ctx.target},
    )
    if not rows:
        return ActionResult(ok=False, detail={"target": ctx.target}, error="not found")
    return ActionResult(ok=True, detail={"target": ctx.target, "on_watchlist": False})


async def _create_alert(
    db: AsyncSession,
    *,
    rule_id: str,
    target_type: str,
    target_id: str,
    severity: str,
    title: str,
    description: str,
    cluster_id: str | None,
) -> Alert:
    alert = Alert(
        id=f"alert-{uuid.uuid4().hex[:12]}",
        created_at=datetime.now(UTC),
        type="rule_escalation",
        severity=severity,
        title=title,
        description=description,
        target_type=target_type,
        target_id=target_id,
        cluster_id=cluster_id,
        acknowledged=False,
        rule_id=rule_id,
        extra={"escalation_source": "rules_engine"},
    )
    db.add(alert)
    await db.commit()
    await db.refresh(alert)
    return alert


async def _escalate_to_investigator(ctx: ActionContext) -> ActionResult:
    rule_id = str(ctx.trigger.get("rule_id") or "rule:unknown")
    rule_name = str(ctx.trigger.get("rule_name") or "Rule escalation")
    cluster_id = ctx.trigger.get("cluster_id")
    severity = str(ctx.params.get("severity", "high"))

    async with get_async_session() as db:
        alert = await _create_alert(
            db,
            rule_id=rule_id,
            target_type=ctx.target_type,
            target_id=ctx.target,
            severity=severity,
            title=f"Escalation: {rule_name}",
            description=(
                f"{rule_name} triggered against {ctx.target_type} {ctx.target} "
                f"and was escalated to investigator queue."
            ),
            cluster_id=cluster_id if isinstance(cluster_id, str) else None,
        )

    await _publish_alert_event(
        "alert.escalated",
        {
            "id": alert.id,
            "severity": severity,
            "title": alert.title,
            "target_type": ctx.target_type,
            "target_id": ctx.target,
            "rule_id": rule_id,
            "cluster_id": alert.cluster_id,
        },
    )
    return ActionResult(ok=True, detail={"alert_id": alert.id, "severity": severity})


async def _flag_for_law_enforcement(ctx: ActionContext) -> ActionResult:
    label_key = _TYPE_TO_LABEL.get(ctx.target_type)
    if label_key is None:
        return ActionResult(
            ok=False, detail={"target_type": ctx.target_type}, error="unsupported target type"
        )
    label, key = label_key
    client = get_neo4j_client()
    await client.execute_write(
        """
        MATCH (n)
        WHERE any(l IN labels(n) WHERE l = $label) AND n[$key] = $id
        SET n.law_enforcement_flag = true,
            n.law_enforcement_flagged_at = datetime()
        """,
        {"label": label, "key": key, "id": ctx.target},
    )
    # Also create a critical alert so the LE workflow can pick it up.
    rule_id = str(ctx.trigger.get("rule_id") or "rule:unknown")
    rule_name = str(ctx.trigger.get("rule_name") or "Law-enforcement flag")
    cluster_id = ctx.trigger.get("cluster_id")
    async with get_async_session() as db:
        alert = await _create_alert(
            db,
            rule_id=rule_id,
            target_type=ctx.target_type,
            target_id=ctx.target,
            severity="critical",
            title=f"LE referral candidate: {rule_name}",
            description=(f"{ctx.target_type} {ctx.target} flagged for law-enforcement review."),
            cluster_id=cluster_id if isinstance(cluster_id, str) else None,
        )
    return ActionResult(ok=True, detail={"target": ctx.target, "alert_id": alert.id})


def register_all(reg: ActionRegistry) -> None:
    reg.register("add_to_watchlist", _add_to_watchlist, description="Mark node as watch-listed.")
    reg.register("remove_from_watchlist", _remove_from_watchlist, description="Clear watch-list flag.")
    reg.register(
        "escalate_to_investigator",
        _escalate_to_investigator,
        description="Open a high-severity alert in the investigator queue.",
        params_schema={"severity": {"type": "string", "default": "high"}},
    )
    reg.register(
        "flag_for_law_enforcement",
        _flag_for_law_enforcement,
        description="Tag the node for LE review and open a critical alert.",
    )
