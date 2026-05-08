"""Reporting tasks (analytics snapshots, evidence package generation)."""

from __future__ import annotations

from config.logging import configure_logging, get_logger

from .celery_app import app

configure_logging()
logger = get_logger(__name__)


@app.task(name="tasks.report_tasks.generate_analytics_snapshot")
def generate_analytics_snapshot() -> dict:
    logger.info("celery.report.generate_analytics_snapshot", status="stub")
    return {"status": "stub"}


@app.task(name="tasks.report_tasks.build_evidence_package")
def build_evidence_package(cluster_id: str, case_id: str | None = None) -> dict:
    logger.info(
        "celery.report.build_evidence_package",
        status="stub",
        cluster_id=cluster_id,
        case_id=case_id,
    )
    return {"status": "stub", "cluster_id": cluster_id}
