"""Rules engine: context building, evaluation, dispatch, persistence.

Two evaluation modes are supported:

- :func:`evaluate_event` — called by the Kafka consumers when an event
  arrives. ``event_context`` is the event + the enriched node snapshot.
- :func:`evaluate_scheduled` — called by Celery beat every 5 minutes.
  Iterates over the relevant graph entities, builds a context per entity,
  and evaluates every active rule against each.

Both paths converge on :func:`_run_rule` which runs the evaluator and,
on a match, dispatches actions through the registry, writes a
:class:`RuleTrigger`, increments the rule's trigger counter, and
deduplicates against a 15-minute Redis key so the same rule cannot
re-fire against the same node.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

import redis.asyncio as redis_async
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from config.logging import get_logger
from config.settings import get_settings
from core.graph.client import Neo4jClient, get_neo4j_client
from db.models import Rule, RuleTrigger
from db.session import get_async_session

from .actions.registry import ActionContext, ActionResult, get_registry
from .evaluator import evaluate, explain

logger = get_logger(__name__)


DEDUP_TTL_SECONDS = 15 * 60


# ---------------------------------------------------------------------------
# Context builders
# ---------------------------------------------------------------------------


@dataclass
class _RunStats:
    rules_evaluated: int = 0
    matches: int = 0
    actions_executed: int = 0
    actions_failed: int = 0
    deduped: int = 0
    errors: int = 0
    triggers_written: int = 0
    by_rule: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "rules_evaluated": self.rules_evaluated,
            "matches": self.matches,
            "actions_executed": self.actions_executed,
            "actions_failed": self.actions_failed,
            "deduped": self.deduped,
            "errors": self.errors,
            "triggers_written": self.triggers_written,
            "by_rule": dict(self.by_rule),
        }


async def _wallet_contexts(
    client: Neo4jClient, *, limit: int
) -> list[dict[str, Any]]:
    """Yield per-wallet contexts. Includes computed fields (account_age_days,
    idle_days, dormant_days) so common rule conditions are satisfiable."""

    rows = await client.execute_read(
        """
        MATCH (w:Wallet)
        WITH w,
             CASE WHEN w.creation_date IS NOT NULL
                  THEN duration.inDays(datetime(w.creation_date), datetime()).days
                  ELSE null END AS account_age_days,
             CASE WHEN w.last_activity IS NOT NULL
                  THEN duration.inDays(datetime(w.last_activity), datetime()).days
                  ELSE null END AS idle_days
        RETURN w.wallet_id AS wallet_id,
               coalesce(w.risk_score, 0.0) AS risk_score,
               coalesce(w.confidence_score, 0.0) AS cluster_confidence,
               coalesce(w.behavioral_score, 0.0) AS behavioral_score,
               coalesce(w.predictive_score, 0.0) AS predictive_score,
               w.status AS status,
               w.kyc_tier AS kyc_tier,
               coalesce(w.is_sleeper, false) AS is_sleeper,
               coalesce(w.on_watchlist, false) AS on_watchlist,
               coalesce(w.send_with_care, false) AS send_with_care,
               coalesce(w.ask_me_first, false) AS ask_me_first,
               w.balance AS balance,
               w.msisdn AS msisdn,
               w.cluster_id AS cluster_id,
               account_age_days,
               idle_days,
               idle_days AS dormant_days
        ORDER BY risk_score DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )
    contexts = []
    for r in rows:
        node = {k: v for k, v in r.items() if k != "wallet_id"}
        node["wallet_id"] = r["wallet_id"]
        contexts.append(
            {
                "_target_type": "wallet",
                "_target_id": r["wallet_id"],
                "_cluster_id": r.get("cluster_id"),
                "node": node,
            }
        )
    return contexts


async def _agent_contexts(
    client: Neo4jClient, *, limit: int
) -> list[dict[str, Any]]:
    rows = await client.execute_read(
        """
        MATCH (a:Agent)
        RETURN a.agent_id AS agent_id,
               coalesce(a.risk_score, 0.0) AS risk_score,
               coalesce(a.fraud_cashout_rate, 0.0) AS fraud_cashout_rate,
               a.classification AS classification,
               coalesce(a.suspended, false) AS suspended,
               coalesce(a.monthly_volume, 0.0) AS monthly_volume,
               a.area_name AS area_name
        ORDER BY fraud_cashout_rate DESC
        LIMIT $limit
        """,
        {"limit": limit},
    )
    return [
        {
            "_target_type": "agent",
            "_target_id": r["agent_id"],
            "_cluster_id": None,
            "agent": {k: v for k, v in r.items()},
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Active-rule fetch
# ---------------------------------------------------------------------------


async def _active_rules(db: AsyncSession, modes: Iterable[str]) -> list[Rule]:
    rows = (
        await db.execute(
            select(Rule).where(
                Rule.status == "live", Rule.evaluation_mode.in_(tuple(modes))
            )
        )
    ).scalars().all()
    return list(rows)


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------


_redis: redis_async.Redis | None = None


async def _dedup_redis() -> redis_async.Redis:
    global _redis
    if _redis is None:
        _redis = redis_async.from_url(get_settings().redis_url, decode_responses=True)
    return _redis


async def _dedup_acquire(rule_id: str, target: str) -> bool:
    """Return True if we should fire (i.e. no recent dup); False to skip."""

    try:
        client = await _dedup_redis()
        key = f"rule:dedup:{rule_id}:{target}"
        return bool(await client.set(key, "1", ex=DEDUP_TTL_SECONDS, nx=True))
    except Exception as exc:  # noqa: BLE001 — Redis outage shouldn't kill the engine
        logger.warning("rules.dedup.redis_error", error=str(exc))
        return True  # fail-open: allow the trigger so we don't lose signal


# ---------------------------------------------------------------------------
# Core: run a single rule against a single context
# ---------------------------------------------------------------------------


async def _run_rule(
    rule: Rule,
    context: dict[str, Any],
    db: AsyncSession,
    stats: _RunStats,
) -> None:
    target_type = context["_target_type"]
    target_id = context["_target_id"]
    try:
        matched = evaluate(rule.conditions or {}, context)
    except Exception as exc:  # noqa: BLE001 — bad rule shouldn't crash the run
        logger.warning(
            "rules.evaluate.error", rule_id=rule.id, error=str(exc)
        )
        stats.errors += 1
        return
    if not matched:
        return

    stats.matches += 1
    if not await _dedup_acquire(rule.id, target_id):
        stats.deduped += 1
        return

    actions: list[dict[str, Any]] = []
    overall_ok = True
    registry = get_registry()
    trigger_meta = {
        "rule_id": rule.id,
        "rule_name": rule.name,
        "cluster_id": context.get("_cluster_id"),
    }
    for action_def in rule.actions or []:
        action_type = str(action_def.get("type", ""))
        params = dict(action_def.get("params") or {})
        ctx = ActionContext(
            target=target_id,
            target_type=target_type,
            params=params,
            trigger=trigger_meta,
        )
        result: ActionResult = await registry.execute(action_type, ctx)
        actions.append(
            {
                "type": action_type,
                "ok": result.ok,
                "detail": result.detail,
                "error": result.error,
            }
        )
        if result.ok:
            stats.actions_executed += 1
        else:
            stats.actions_failed += 1
            overall_ok = False

    # Persist trigger row + bump rule counters.
    db.add(
        RuleTrigger(
            id=f"trig-{uuid.uuid4().hex[:12]}",
            rule_id=rule.id,
            triggered_at=datetime.now(timezone.utc),
            event_id=None,
            node_id=target_id,
            node_type=target_type,
            context={
                "snapshot": {
                    k: v for k, v in context.items() if not k.startswith("_")
                },
                "explanation": explain(rule.conditions or {}, context),
            },
            actions_executed=actions,
            outcome="success" if overall_ok else "failed",
        )
    )
    await db.execute(
        update(Rule)
        .where(Rule.id == rule.id)
        .values(trigger_count=Rule.trigger_count + 1)
    )
    stats.triggers_written += 1
    stats.by_rule[rule.id] = stats.by_rule.get(rule.id, 0) + 1

    # Best-effort WS broadcast — never fails the rule run.
    try:
        from api.websocket.publisher import CH_RULES, publish

        await publish(
            CH_RULES,
            "rule.triggered",
            {
                "rule_id": rule.id,
                "rule_name": rule.name,
                "node_type": target_type,
                "node_id": target_id,
                "outcome": "success" if overall_ok else "failed",
                "actions": [
                    {"type": a["type"], "ok": a["ok"]}
                    for a in actions
                ],
            },
        )
    except Exception:  # noqa: BLE001
        pass


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------


async def evaluate_scheduled(
    *,
    wallet_limit: int = 500,
    agent_limit: int = 200,
) -> dict[str, Any]:
    """Run every live, scheduled-mode rule against the current graph state.

    Returns a stats dict suitable for logging / API exposure.
    """

    client = get_neo4j_client()
    # Ensure driver is connected (Celery worker process won't have lifespan).
    try:
        if client._driver is None:  # type: ignore[attr-defined]
            await client.connect()
    except AttributeError:
        await client.connect()

    stats = _RunStats()
    async with get_async_session() as db:
        rules = await _active_rules(db, modes=("scheduled",))
        if not rules:
            return {"status": "no_rules", **stats.as_dict()}

        wallet_ctxs = await _wallet_contexts(client, limit=wallet_limit)
        agent_ctxs = await _agent_contexts(client, limit=agent_limit)
        all_ctxs = wallet_ctxs + agent_ctxs

        for rule in rules:
            stats.rules_evaluated += 1
            for ctx in all_ctxs:
                await _run_rule(rule, ctx, db, stats)

        await db.commit()

    payload = stats.as_dict()
    payload["status"] = "ok"
    payload["rules_run"] = len(rules)
    payload["contexts_evaluated"] = len(wallet_ctxs) + len(agent_ctxs)
    logger.info("rules.scheduled.complete", **payload)
    return payload


async def evaluate_event(
    event_context: dict[str, Any],
) -> dict[str, Any]:
    """Run live, realtime-mode rules against a single event context.

    The context must include ``_target_type`` and ``_target_id`` plus
    whatever nested keys (``node``, ``agent``, ``alert``, ``auth``) the
    rule conditions reference.
    """

    stats = _RunStats()
    async with get_async_session() as db:
        rules = await _active_rules(db, modes=("realtime",))
        if not rules:
            return {"status": "no_rules", **stats.as_dict()}
        for rule in rules:
            stats.rules_evaluated += 1
            await _run_rule(rule, event_context, db, stats)
        await db.commit()
    payload = stats.as_dict()
    payload["status"] = "ok"
    return payload


__all__ = ["evaluate_scheduled", "evaluate_event"]
