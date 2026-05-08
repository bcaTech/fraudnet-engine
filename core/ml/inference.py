"""Inference: load the current model and score wallets.

Exposes one helper, :func:`score_wallets`, that takes a list of wallet
ids, fetches features, runs the current behavioural model, and writes
``predictive_score`` back to the graph. Designed for both ad-hoc API
calls and the batch Celery task.
"""

from __future__ import annotations

from typing import Any

from config.logging import get_logger
from core.graph.client import get_neo4j_client

from .features import fetch_batch, fetch_population
from .training import TrainedModel, load_current

logger = get_logger(__name__)


async def score_wallets(
    wallet_ids: list[str] | None = None,
    *,
    persist: bool = True,
    sample_size: int = 500,
) -> dict[str, Any]:
    """Score either the named ``wallet_ids`` or a population sample.

    Returns ``{model_id, scored, persisted, scores}`` where ``scores`` is
    a list of ``(wallet_id, score)`` tuples in the same order as the
    fetched features.
    """

    model = load_current()
    if model is None:
        return {
            "scored": 0,
            "persisted": 0,
            "model_id": None,
            "note": "no model trained yet — call core.ml.training.train()",
        }

    if wallet_ids:
        features = await fetch_batch(wallet_ids)
    else:
        features = await fetch_population(limit=sample_size)

    if not features:
        return {"scored": 0, "persisted": 0, "model_id": "behavioural-current"}

    vectors = [f.vector for f in features]
    probas = model.predict_proba(vectors)
    scores = list(zip([f.wallet_id for f in features], probas, strict=True))

    persisted = 0
    if persist:
        client = get_neo4j_client()
        rows = [{"wallet_id": w, "score": float(s)} for w, s in scores]
        await client.execute_write(
            """
            UNWIND $rows AS row
            MATCH (w:Wallet {wallet_id: row.wallet_id})
            SET w.predictive_score = row.score,
                w.predictive_score_at = datetime()
            """,
            {"rows": rows},
        )
        persisted = len(rows)
        logger.info("ml.inference.persisted", count=persisted)

    return {
        "scored": len(scores),
        "persisted": persisted,
        "model_id": "behavioural-current",
        "metrics": model.metrics,
        "scores": [{"wallet_id": w, "score": float(s)} for w, s in scores],
    }
