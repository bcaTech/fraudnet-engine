"""ML pipeline async tasks (training + batch inference + evaluation).

Wired to :mod:`core.ml.training`, :mod:`core.ml.inference`,
:mod:`core.ml.evaluation`, and :mod:`core.ml.gnn_model`. The behavioural
baseline (logistic regression on graph-derived features) is real; the
GNN entry point falls through to the baseline until torch + torch-
geometric are installed.
"""

from __future__ import annotations

from config.logging import configure_logging, get_logger

from .celery_app import app

configure_logging()
logger = get_logger(__name__)


def _run_async(coro):
    from .periodic import _run_async as _shared_run_async

    return _shared_run_async(coro)


async def _ensure_neo4j_connected():
    """Workers don't have FastAPI's lifespan; the per-task client
    teardown also resets the singleton, so each ML task connects before
    using the driver."""

    from core.graph.client import get_neo4j_client

    client = get_neo4j_client()
    try:
        if client._driver is None:  # type: ignore[attr-defined]
            await client.connect()
    except AttributeError:
        await client.connect()
    return client


@app.task(name="tasks.ml_tasks.retrain_anomaly_baselines")
def retrain_anomaly_baselines(sample_size: int = 1000) -> dict:
    """Re-fit the behavioural model from current graph state.

    Returns the training-result summary (sample size, class balance,
    precision/recall/F1/AUC, model id). The new model is symlinked as
    ``current`` so the next inference call picks it up.
    """

    async def _go():
        await _ensure_neo4j_connected()
        from core.ml.training import train

        result = await train(sample_size=sample_size, save=True)
        return {
            "model_id": result.model_id,
            "sample_size": result.sample_size,
            "positives": result.positives,
            "negatives": result.negatives,
            **result.metrics,
            "saved_path": result.saved_path,
        }

    try:
        result = _run_async(_go())
        logger.info("celery.ml.train.complete", **result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.ml.train.error", error=str(exc))
        raise


@app.task(name="tasks.ml_tasks.evaluate_gnn_performance")
def evaluate_gnn_performance() -> dict:
    """Score the current model against a fresh sample. Reports
    precision/recall/F1/AUC. Used by the weekly model-review cadence."""

    async def _go():
        await _ensure_neo4j_connected()
        from core.ml.gnn_model import evaluate_performance

        return await evaluate_performance()

    try:
        result = _run_async(_go())
        logger.info("celery.ml.evaluate.complete", status=result.get("status"))
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.ml.evaluate.error", error=str(exc))
        raise


@app.task(name="tasks.ml_tasks.batch_score_wallets")
def batch_score_wallets(limit: int = 500) -> dict:
    """Score a sample of wallets and persist ``predictive_score`` back
    to the graph. Trims the score payload before returning so the
    Celery result backend doesn't bloat."""

    async def _go():
        await _ensure_neo4j_connected()
        from core.ml.inference import score_wallets

        out = await score_wallets(sample_size=limit, persist=True)
        return {
            "scored": out.get("scored"),
            "persisted": out.get("persisted"),
            "model_id": out.get("model_id"),
            "metrics": out.get("metrics"),
            "note": out.get("note"),
        }

    try:
        result = _run_async(_go())
        logger.info("celery.ml.score.complete", **{k: v for k, v in result.items() if k != "metrics"})
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("celery.ml.score.error", error=str(exc))
        raise
