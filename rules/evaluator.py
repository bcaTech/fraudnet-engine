"""Condition-tree evaluator.

Rules carry a condition tree of the shape::

    {
        "operator": "AND",
        "conditions": [
            {"field": "node.risk_score", "op": "greater_than", "value": 0.7},
            {"field": "agent.fraud_cashout_rate", "op": "greater_than", "value": 0.3},
            {
                "operator": "OR",
                "conditions": [
                    {"field": "node.cluster_confidence", "op": "greater_than", "value": 0.8},
                    {"field": "node.on_watchlist", "op": "equals", "value": true}
                ]
            }
        ]
    }

The evaluator does no I/O — give it a condition tree and a context dict and
it returns a bool. Use the dotted ``field`` paths to drill into nested
context (``node.risk_score`` → ``context["node"]["risk_score"]``).

Missing fields evaluate to ``None`` and any comparison except ``is_null``
returns ``False``. This is deliberate: rules referencing features that
aren't yet wired don't trigger, they just don't match.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

LOGICAL_OPERATORS: tuple[str, ...] = ("AND", "OR", "NOT")


def _resolve(field: str, context: dict[str, Any]) -> Any:
    """Walk a dotted path into ``context``. Returns ``None`` if any segment
    is missing or hits a non-mapping."""

    cur: Any = context
    for part in field.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
        if cur is None:
            return None
    return cur


def _coerce_pair(actual: Any, expected: Any) -> tuple[Any, Any]:
    """Best-effort cast so ``"5"`` compares to ``5`` correctly."""

    if isinstance(actual, bool) or isinstance(expected, bool):
        return actual, expected
    if isinstance(actual, (int, float)) and isinstance(expected, str):
        try:
            return actual, type(actual)(expected)
        except (TypeError, ValueError):
            return actual, expected
    if isinstance(expected, (int, float)) and isinstance(actual, str):
        try:
            return type(expected)(actual), expected
        except (TypeError, ValueError):
            return actual, expected
    return actual, expected


def _safe_lt(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return False
    a, b = _coerce_pair(a, b)
    try:
        return bool(a < b)
    except TypeError:
        return False


def _safe_gt(a: Any, b: Any) -> bool:
    if a is None or b is None:
        return False
    a, b = _coerce_pair(a, b)
    try:
        return bool(a > b)
    except TypeError:
        return False


def _eq(a: Any, b: Any) -> bool:
    a, b = _coerce_pair(a, b)
    return bool(a == b)


OPERATORS: dict[str, Callable[[Any, Any], bool]] = {
    "equals": _eq,
    "not_equals": lambda a, b: not _eq(a, b),
    "greater_than": _safe_gt,
    "less_than": _safe_lt,
    "greater_or_equal": lambda a, b: _safe_gt(a, b) or _eq(a, b),
    "less_or_equal": lambda a, b: _safe_lt(a, b) or _eq(a, b),
    "in": lambda a, b: a in b if isinstance(b, (list, tuple, set)) else False,
    "not_in": lambda a, b: a not in b if isinstance(b, (list, tuple, set)) else True,
    "contains": (
        lambda a, b: (b in a) if isinstance(a, (str, list, tuple, set, dict)) and a is not None else False
    ),
    "is_null": lambda a, _b: a is None,
    "is_not_null": lambda a, _b: a is not None,
    "between": (
        lambda a, b: (
            isinstance(b, (list, tuple))
            and len(b) == 2
            and (_safe_gt(a, b[0]) or _eq(a, b[0]))
            and (_safe_lt(a, b[1]) or _eq(a, b[1]))
        )
    ),
}


class RuleSyntaxError(ValueError):
    """Raised when a condition tree is malformed."""


def evaluate(tree: dict[str, Any], context: dict[str, Any]) -> bool:
    """Recursively evaluate a condition tree against ``context``."""

    if not isinstance(tree, dict):
        raise RuleSyntaxError(f"condition node must be a dict, got {type(tree).__name__}")

    op = tree.get("operator")
    if op in LOGICAL_OPERATORS:
        children = tree.get("conditions") or []
        if not isinstance(children, list):
            raise RuleSyntaxError("'conditions' must be a list")
        if op == "AND":
            return all(evaluate(c, context) for c in children)
        if op == "OR":
            return any(evaluate(c, context) for c in children)
        # NOT — single child
        if len(children) != 1:
            raise RuleSyntaxError("'NOT' takes exactly one child condition")
        return not evaluate(children[0], context)

    # Leaf node: {field, op, value}
    field = tree.get("field")
    leaf_op = tree.get("op")
    expected = tree.get("value")
    if not isinstance(field, str):
        raise RuleSyntaxError(f"leaf condition missing 'field' string: {tree!r}")
    if leaf_op not in OPERATORS:
        raise RuleSyntaxError(f"unknown leaf operator: {leaf_op!r}")
    actual = _resolve(field, context)
    return OPERATORS[leaf_op](actual, expected)


def explain(tree: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    """Return a parallel tree of evaluation results — useful for the
    rule-detail UI's "why did this match" panel."""

    if not isinstance(tree, dict):
        return {"matched": False, "error": f"bad node: {tree!r}"}
    op = tree.get("operator")
    if op in LOGICAL_OPERATORS:
        children = [explain(c, context) for c in (tree.get("conditions") or [])]
        if op == "AND":
            matched = all(c.get("matched") for c in children)
        elif op == "OR":
            matched = any(c.get("matched") for c in children)
        else:
            matched = not children[0].get("matched") if children else False
        return {"operator": op, "matched": matched, "children": children}
    field = tree.get("field")
    leaf_op = tree.get("op")
    expected = tree.get("value")
    actual = _resolve(field, context) if isinstance(field, str) else None
    return {
        "field": field,
        "op": leaf_op,
        "value": expected,
        "actual": actual,
        "matched": leaf_op in OPERATORS and OPERATORS[leaf_op](actual, expected),
    }
