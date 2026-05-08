"""ML pipeline async tasks (training + batch inference)."""

from __future__ import annotations

from config.logging import configure_logging, get_logger

from .celery_app import app

configure_logging()
logger = get_logger(__name__)


@app.task(name="tasks.ml_tasks.retrain_anomaly_baselines")
def retrain_anomaly_baselines() -> dict:
    logger.info("celery.ml.retrain_anomaly_baselines", status="stub")
    return {"status": "stub"}


@app.task(name="tasks.ml_tasks.evaluate_gnn_performance")
def evaluate_gnn_performance() -> dict:
    logger.info("celery.ml.evaluate_gnn_performance", status="stub")
    return {"status": "stub"}


@app.task(name="tasks.ml_tasks.batch_score_wallets")
def batch_score_wallets(limit: int = 1000) -> dict:
    logger.info("celery.ml.batch_score_wallets", status="stub", limit=limit)
    return {"status": "stub", "limit": limit}
