# Runbook — Celery worker (+ beat)

Two services share this runbook because they're tightly coupled: beat
emits scheduled task IDs onto the Redis broker; worker consumes and
runs them.

## Health

- `docker compose ps worker` should show `(healthy)` (the celery
  inspect-ping probe).
- `docker compose ps beat` should show plain `Up` (no probe).

## Symptoms ↔ likely cause ↔ first action

| Symptom | Likely cause | First action |
|---|---|---|
| `worker` is `(unhealthy)` | Broker unreachable, or worker process can't reach `celery@<hostname>` | `docker compose logs worker` for tracebacks; usually a Redis blip |
| Tasks never run | beat not pushing, OR a stale `celerybeat-schedule` | `docker compose restart beat`. The schedule lives in the `celerybeat-data` named volume |
| Same task fires twice | beat restarted with overlapping schedule, OR two beat processes | Confirm only one `fraudnet-beat` container is running |
| `Task got Future attached to a different loop` | A task didn't tear down its async clients in `_run_async`'s finally | Check `tasks/periodic.py:_dispose_clients` is called; every async task must use `_run_async` from `periodic` (not its own loop manager) |
| `evaluate_scheduled_rules` fails with import errors | New import path in `rules/` not picked up | `docker compose restart worker beat` (modules don't auto-reload) |
| Memory creeps up over hours | Connection pool leaking | Restart worker; investigate driver/engine teardown in long-running tasks |

## Common one-liners

```bash
# Stream worker output
docker compose logs --no-color -f worker

# Run any task on demand
docker compose exec -T worker python -c \
  "from tasks.celery_app import app; r = app.send_task('tasks.periodic.sleeper_wallet_scan'); print(r.get(timeout=60))"

# Reset the beat schedule (rare; usually a beat restart suffices)
docker compose exec beat sh -c 'rm -f /var/run/celery/celerybeat-schedule*'
docker compose restart beat

# Check what's queued / active
docker compose exec -T worker celery -A tasks.celery_app inspect active
```

## Escalation

- **Worker goes unhealthy and stays unhealthy** after restart: page
  Backend on-call. Don't disable celery — many compliance side effects
  (KYC re-verification flag clearing, LE case reminders) live there.
- **A task fails repeatedly** but the worker stays up: that's
  acceptable; investigate during business hours unless the task is on
  the critical evidence/takedown path.

## SLOs

- Beat tick lag < 60s (a 5-minute schedule should fire within 5m+1m).
- Worker task success rate > 99% (excludes intentional dedup skips).
- ML training task duration < 60s on the demo dataset.
