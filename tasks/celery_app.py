"""Celery application for FraudNet's async / periodic workloads.

Two processes consume this app:

- ``celery -A tasks.celery_app worker`` — runs queued jobs (mesh expansion,
  batch scoring, model inference, report generation).
- ``celery -A tasks.celery_app beat`` — fires the scheduled tasks declared in
  :data:`beat_schedule`.

The schedule mirrors the cadence documented in CLAUDE.md. Individual task
implementations live in sibling modules (``tasks.periodic``, ``tasks.mesh_tasks``,
etc.) and are auto-discovered via ``include``. Until the full implementations
land they are no-op stubs that log a structured event — that's enough to keep
beat happy and gives operators a heartbeat in the worker log.
"""

from __future__ import annotations

from celery import Celery
from celery.schedules import crontab, schedule

from config.settings import get_settings

_settings = get_settings()

app = Celery(
    "fraudnet",
    broker=_settings.celery_broker_url,
    backend=_settings.celery_result_backend,
    include=[
        "tasks.periodic",
        "tasks.mesh_tasks",
        "tasks.ml_tasks",
        "tasks.report_tasks",
    ],
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    broker_connection_retry_on_startup=True,
    result_expires=3600,
)

# Beat schedule — see CLAUDE.md "Celery Periodic Tasks". Cadences are ints
# (seconds) where simple, crontabs where the schedule is wall-clock-aligned.
app.conf.beat_schedule = {
    "apply-temporal-decay": {
        "task": "tasks.periodic.apply_temporal_decay",
        "schedule": schedule(run_every=3600),  # every 60 minutes
    },
    "rescore-active-clusters": {
        "task": "tasks.periodic.rescore_active_clusters",
        "schedule": schedule(run_every=6 * 3600),  # every 6 hours
    },
    "scancom-batch-import": {
        "task": "tasks.periodic.scancom_batch_import",
        "schedule": schedule(run_every=6 * 3600),
    },
    "sleeper-wallet-scan": {
        "task": "tasks.periodic.sleeper_wallet_scan",
        "schedule": schedule(run_every=30 * 60),  # 30 min
    },
    "campaign-detection": {
        "task": "tasks.periodic.campaign_detection",
        "schedule": schedule(run_every=15 * 60),  # 15 min
    },
    "evaluate-scheduled-rules": {
        "task": "tasks.periodic.evaluate_scheduled_rules",
        "schedule": schedule(run_every=5 * 60),  # 5 min
    },
    "process-inbound-integration": {
        "task": "tasks.periodic.process_inbound_integration",
        "schedule": schedule(run_every=15 * 60),
    },
    "process-outbound-integration": {
        "task": "tasks.periodic.process_outbound_integration",
        "schedule": schedule(run_every=15 * 60),
    },
    "rules-performance-aggregation": {
        "task": "tasks.periodic.rules_performance_aggregation",
        "schedule": schedule(run_every=3600),
    },
    "external-operator-health-check": {
        "task": "tasks.periodic.external_operator_health_check",
        "schedule": schedule(run_every=6 * 3600),
    },
    # Wall-clock-aligned dailies/weeklies.
    "retrain-anomaly-baselines": {
        "task": "tasks.ml_tasks.retrain_anomaly_baselines",
        "schedule": crontab(hour=2, minute=15),
    },
    "evaluate-gnn-performance": {
        "task": "tasks.ml_tasks.evaluate_gnn_performance",
        "schedule": crontab(hour=3, minute=0, day_of_week="sun"),
    },
    "generate-analytics-snapshot": {
        "task": "tasks.report_tasks.generate_analytics_snapshot",
        "schedule": crontab(hour=4, minute=0),
    },
    "law-enforcement-case-reminders": {
        "task": "tasks.periodic.law_enforcement_case_reminders",
        "schedule": crontab(hour=8, minute=0),
    },
    "cleanup-stale-rule-state": {
        "task": "tasks.periodic.cleanup_stale_rule_state",
        "schedule": crontab(hour=3, minute=30),  # nightly
    },
    "refresh-campaigns-cache": {
        "task": "tasks.periodic.refresh_campaigns_cache",
        "schedule": schedule(run_every=15 * 60),  # every 15 min
    },
}


__all__ = ["app"]
