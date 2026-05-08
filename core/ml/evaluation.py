"""Offline model evaluation.

Two helpers:

- :func:`evaluate_current` — load the current model, pull a fresh sample
  from the graph, compute precision/recall/F1/AUC against the
  cluster-membership labels.
- :func:`backtest_score_threshold` — sweep candidate decision thresholds
  and return the precision/recall pair at each, so analysts can pick
  one that matches the operator's tolerance.
"""

from __future__ import annotations

from typing import Any

from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score

from config.logging import get_logger

from .features import fetch_population
from .training import load_current

logger = get_logger(__name__)


async def evaluate_current(*, sample_size: int = 500) -> dict[str, Any]:
    model = load_current()
    if model is None:
        return {"status": "no_model"}

    samples = await fetch_population(limit=sample_size)
    if not samples:
        return {"status": "no_data"}

    y = [s.label for s in samples]
    proba = model.predict_proba([s.vector for s in samples])
    pred = [1 if p >= 0.5 else 0 for p in proba]

    metrics = {
        "precision": float(precision_score(y, pred, zero_division=0)),
        "recall": float(recall_score(y, pred, zero_division=0)),
        "f1": float(f1_score(y, pred, zero_division=0)),
        "auc": float(roc_auc_score(y, proba)) if len(set(y)) > 1 else 0.0,
        "sample_size": len(samples),
        "positives": sum(y),
        "negatives": len(y) - sum(y),
    }
    logger.info("ml.eval.current", **metrics)
    return {"status": "ok", "metrics": metrics, "trained_at": model.trained_at}


async def backtest_score_threshold(
    *, sample_size: int = 1000, thresholds: list[float] | None = None
) -> list[dict[str, Any]]:
    model = load_current()
    if model is None:
        return []
    samples = await fetch_population(limit=sample_size)
    if not samples:
        return []
    thresholds = thresholds or [0.10, 0.25, 0.40, 0.50, 0.65, 0.80, 0.90]
    y = [s.label for s in samples]
    proba = model.predict_proba([s.vector for s in samples])

    out = []
    for t in thresholds:
        pred = [1 if p >= t else 0 for p in proba]
        out.append(
            {
                "threshold": t,
                "precision": float(precision_score(y, pred, zero_division=0)),
                "recall": float(recall_score(y, pred, zero_division=0)),
                "f1": float(f1_score(y, pred, zero_division=0)),
                "predicted_positive": sum(pred),
            }
        )
    return out
