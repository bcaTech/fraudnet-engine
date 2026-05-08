"""Scheduled (Celery beat) task stubs.

Each task here is a placeholder that logs a structured heartbeat. They keep
beat from raising ``KeyError`` while the real implementations land in
``core/mesh``, ``rules/scheduler``, ``integration``, and ``law_enforcement``.

When implementing the real version, import the engine module and call its
async entry point via ``asyncio.run`` (Celery workers are sync by default).
"""

from __future__ import annotations

from config.logging import configure_logging, get_logger

from .celery_app import app

configure_logging()
logger = get_logger(__name__)


def _heartbeat(name: str) -> dict[str, str]:
    logger.info("celery.beat.heartbeat", task=name, status="stub")
    return {"task": name, "status": "stub"}


@app.task(name="tasks.periodic.apply_temporal_decay")
def apply_temporal_decay() -> dict[str, str]:
    return _heartbeat("apply_temporal_decay")


@app.task(name="tasks.periodic.rescore_active_clusters")
def rescore_active_clusters() -> dict[str, str]:
    return _heartbeat("rescore_active_clusters")


@app.task(name="tasks.periodic.scancom_batch_import")
def scancom_batch_import() -> dict[str, str]:
    return _heartbeat("scancom_batch_import")


@app.task(name="tasks.periodic.sleeper_wallet_scan")
def sleeper_wallet_scan() -> dict[str, str]:
    return _heartbeat("sleeper_wallet_scan")


@app.task(name="tasks.periodic.campaign_detection")
def campaign_detection() -> dict[str, str]:
    return _heartbeat("campaign_detection")


@app.task(name="tasks.periodic.evaluate_scheduled_rules")
def evaluate_scheduled_rules() -> dict[str, str]:
    return _heartbeat("evaluate_scheduled_rules")


@app.task(name="tasks.periodic.process_inbound_integration")
def process_inbound_integration() -> dict[str, str]:
    return _heartbeat("process_inbound_integration")


@app.task(name="tasks.periodic.process_outbound_integration")
def process_outbound_integration() -> dict[str, str]:
    return _heartbeat("process_outbound_integration")


@app.task(name="tasks.periodic.rules_performance_aggregation")
def rules_performance_aggregation() -> dict[str, str]:
    return _heartbeat("rules_performance_aggregation")


@app.task(name="tasks.periodic.external_operator_health_check")
def external_operator_health_check() -> dict[str, str]:
    return _heartbeat("external_operator_health_check")


@app.task(name="tasks.periodic.law_enforcement_case_reminders")
def law_enforcement_case_reminders() -> dict[str, str]:
    return _heartbeat("law_enforcement_case_reminders")
