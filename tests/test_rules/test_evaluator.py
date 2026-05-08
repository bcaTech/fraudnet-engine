"""Pure-unit tests for the rules condition-tree evaluator."""

from __future__ import annotations

import pytest

from rules.evaluator import RuleSyntaxError, evaluate, explain


# ---------------------------------------------------------------------------
# Leaf operators
# ---------------------------------------------------------------------------


def test_equals_matches_when_values_equal() -> None:
    ctx = {"node": {"status": "frozen"}}
    tree = {"field": "node.status", "op": "equals", "value": "frozen"}
    assert evaluate(tree, ctx) is True


def test_equals_does_not_match_when_values_differ() -> None:
    ctx = {"node": {"status": "active"}}
    tree = {"field": "node.status", "op": "equals", "value": "frozen"}
    assert evaluate(tree, ctx) is False


def test_greater_than_handles_numeric_strings() -> None:
    ctx = {"node": {"score": "0.92"}}
    tree = {"field": "node.score", "op": "greater_than", "value": 0.7}
    assert evaluate(tree, ctx) is True


def test_in_operator_with_list() -> None:
    ctx = {"node": {"role": "central"}}
    tree = {
        "field": "node.role",
        "op": "in",
        "value": ["central", "accomplice"],
    }
    assert evaluate(tree, ctx) is True


def test_is_null_returns_true_for_missing_field() -> None:
    ctx = {"node": {}}
    tree = {"field": "node.cluster_id", "op": "is_null", "value": None}
    assert evaluate(tree, ctx) is True


def test_between_inclusive_bounds() -> None:
    ctx = {"node": {"risk": 0.5}}
    tree = {"field": "node.risk", "op": "between", "value": [0.3, 0.7]}
    assert evaluate(tree, ctx) is True


# ---------------------------------------------------------------------------
# Missing-field semantics
# ---------------------------------------------------------------------------


def test_missing_field_is_false_for_comparisons() -> None:
    ctx = {"node": {}}
    tree = {"field": "node.score", "op": "greater_than", "value": 0.5}
    assert evaluate(tree, ctx) is False


def test_dotted_path_walks_nested_dicts() -> None:
    ctx = {"a": {"b": {"c": 42}}}
    tree = {"field": "a.b.c", "op": "equals", "value": 42}
    assert evaluate(tree, ctx) is True


def test_dotted_path_short_circuits_on_non_dict() -> None:
    ctx = {"a": "string-not-dict"}
    tree = {"field": "a.b.c", "op": "equals", "value": "anything"}
    assert evaluate(tree, ctx) is False


# ---------------------------------------------------------------------------
# Logical operators
# ---------------------------------------------------------------------------


def test_and_requires_all_children_match() -> None:
    ctx = {"n": {"a": 1, "b": 2}}
    tree = {
        "operator": "AND",
        "conditions": [
            {"field": "n.a", "op": "equals", "value": 1},
            {"field": "n.b", "op": "equals", "value": 2},
        ],
    }
    assert evaluate(tree, ctx) is True


def test_and_is_false_when_any_child_fails() -> None:
    ctx = {"n": {"a": 1, "b": 99}}
    tree = {
        "operator": "AND",
        "conditions": [
            {"field": "n.a", "op": "equals", "value": 1},
            {"field": "n.b", "op": "equals", "value": 2},
        ],
    }
    assert evaluate(tree, ctx) is False


def test_or_short_circuits() -> None:
    ctx = {"n": {"a": 99}}
    tree = {
        "operator": "OR",
        "conditions": [
            {"field": "n.a", "op": "equals", "value": 1},
            {"field": "n.a", "op": "equals", "value": 99},
        ],
    }
    assert evaluate(tree, ctx) is True


def test_not_inverts_single_child() -> None:
    ctx = {"n": {"a": 1}}
    tree = {
        "operator": "NOT",
        "conditions": [{"field": "n.a", "op": "equals", "value": 1}],
    }
    assert evaluate(tree, ctx) is False


def test_nested_and_or_combination() -> None:
    """``risk > 0.7 AND (kyc_tier < 2 OR on_watchlist = true)``."""

    tree = {
        "operator": "AND",
        "conditions": [
            {"field": "node.risk_score", "op": "greater_than", "value": 0.7},
            {
                "operator": "OR",
                "conditions": [
                    {"field": "node.kyc_tier", "op": "less_than", "value": 2},
                    {"field": "node.on_watchlist", "op": "equals", "value": True},
                ],
            },
        ],
    }
    # Matches: high risk + low KYC tier (OR branch satisfied)
    ctx_match = {"node": {"risk_score": 0.85, "kyc_tier": 1, "on_watchlist": False}}
    assert evaluate(tree, ctx_match) is True
    # No match: high risk but KYC tier OK and not watchlisted
    ctx_no = {"node": {"risk_score": 0.85, "kyc_tier": 3, "on_watchlist": False}}
    assert evaluate(tree, ctx_no) is False
    # No match: low risk, even though OR branch satisfied
    ctx_low_risk = {"node": {"risk_score": 0.1, "kyc_tier": 1, "on_watchlist": False}}
    assert evaluate(tree, ctx_low_risk) is False


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


def test_unknown_operator_raises() -> None:
    tree = {"field": "x", "op": "definitely_not_an_op", "value": 1}
    with pytest.raises(RuleSyntaxError):
        evaluate(tree, {})


def test_not_with_multiple_children_raises() -> None:
    tree = {
        "operator": "NOT",
        "conditions": [
            {"field": "a", "op": "equals", "value": 1},
            {"field": "b", "op": "equals", "value": 2},
        ],
    }
    with pytest.raises(RuleSyntaxError):
        evaluate(tree, {})


# ---------------------------------------------------------------------------
# explain()
# ---------------------------------------------------------------------------


def test_explain_returns_per_leaf_result() -> None:
    tree = {
        "operator": "AND",
        "conditions": [
            {"field": "n.a", "op": "equals", "value": 1},
            {"field": "n.b", "op": "equals", "value": 2},
        ],
    }
    out = explain(tree, {"n": {"a": 1, "b": 99}})
    assert out["matched"] is False
    assert out["operator"] == "AND"
    assert len(out["children"]) == 2
    assert out["children"][0]["matched"] is True
    assert out["children"][1]["matched"] is False
    assert out["children"][1]["actual"] == 99
