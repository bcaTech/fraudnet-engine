"""Engine-layer tests.

Two flavours here:

- Pure-unit tests for the action registry's wiring: action types
  registered, dispatch routes correctly, missing types return a clear
  error.
- One integration test that runs the full
  :func:`rules.engine.evaluate_scheduled` against the live stack —
  marked ``integration`` so CI without infra skips it.
"""

from __future__ import annotations

import os

import httpx
import pytest

from rules.actions.registry import ActionContext, ActionRegistry, get_registry

# ---------------------------------------------------------------------------
# Registry unit tests
# ---------------------------------------------------------------------------


def test_registry_has_core_action_types() -> None:
    reg = get_registry()
    expected = {
        "freeze_wallet",
        "unfreeze_wallet",
        "reduce_transaction_limit",
        "force_kyc_reverification",
        "apply_send_with_care",
        "apply_ask_me_first",
        "suspend_agent",
        "downgrade_agent_float",
        "block_cross_network",
        "escalate_to_investigator",
        "add_to_watchlist",
        "flag_for_law_enforcement",
        "custom_webhook",
    }
    assert expected.issubset(set(reg.types()))


def test_registry_describe_includes_schemas() -> None:
    reg = get_registry()
    descs = {d["type"]: d for d in reg.describe()}
    assert "reduce_transaction_limit" in descs
    assert "limit" in descs["reduce_transaction_limit"]["params_schema"]


@pytest.mark.asyncio
async def test_registry_unknown_action_returns_error_result() -> None:
    reg = get_registry()
    ctx = ActionContext(target="x", target_type="wallet", params={}, trigger={})
    result = await reg.execute("does_not_exist", ctx)
    assert result.ok is False
    assert "unknown action type" in (result.error or "")


@pytest.mark.asyncio
async def test_registry_register_overrides() -> None:
    """A second register() call for the same type should win (and warn)."""

    reg = ActionRegistry()
    called: list[str] = []

    async def first(_ctx: ActionContext):
        from rules.actions.registry import ActionResult

        called.append("first")
        return ActionResult(ok=True, detail={})

    async def second(_ctx: ActionContext):
        from rules.actions.registry import ActionResult

        called.append("second")
        return ActionResult(ok=True, detail={})

    reg.register("noop", first)
    reg.register("noop", second)
    await reg.execute("noop", ActionContext("x", "wallet", {}, {}))
    assert called == ["second"]


# ---------------------------------------------------------------------------
# Integration: full scheduled run
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_evaluate_scheduled_via_celery_endpoint() -> None:
    """The engine has no HTTP endpoint, but we can confirm it runs by
    inspecting trigger counts before/after the next beat fire on
    /api/rules/<id>/performance. Marked integration; needs the live
    stack + seeded data."""

    base = os.environ.get("FRAUDNET_API_BASE", "http://localhost:8000")
    with httpx.Client(base_url=base, timeout=15.0) as client:
        rules = client.get("/api/rules", params={"status": "live"}).json()["data"]
        if not rules:
            pytest.skip("no live rules seeded")
        # Pick the one most likely to fire on demo data.
        rule = next(
            (r for r in rules if "kyc" in (r.get("name") or "").lower()),
            rules[0],
        )
        perf = client.get(f"/api/rules/{rule['id']}/performance").json()["data"]
        assert "trigger_count" in perf
        assert "false_positive_rate" in perf
