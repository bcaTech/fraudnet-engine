# Runbook — API service

The FastAPI service in front of Neo4j and Postgres.

## Health

- Liveness: `curl http://<host>:8000/health` → `{"data": {"status": "ok", ...}}`.
- The healthcheck in compose hits `/health` every 30s.
- A degraded Neo4j flips `data.status` to `"degraded"` but the API stays
  up; investigators can still hit Postgres-backed endpoints.

## Symptoms ↔ likely cause ↔ first action

| Symptom | Likely cause | First action |
|---|---|---|
| `/health` 5xx, container restarts | Neo4j unreachable at boot | `docker compose logs neo4j`. Wait for healthy, then restart api. |
| `/health` 200 but `node_counts` empty | Seed never ran or Neo4j was wiped | `docker compose exec -T api python -m scripts.seed_demo_data --reset` |
| All endpoints return 401 | `AUTH_REQUIRED=true` and no token sent | Confirm caller's token; or re-set `AUTH_REQUIRED=false` for dev |
| Frontend gets CORS error | Frontend origin not in `CORS_ORIGINS` | Add the origin to `.env`, restart api |
| `/api/clusters/.../graph` 500 | New Neo4j temporal type leaks into a property dict | `core/graph/client.py:_coerce` should be the only place that handles this — extend if a new type appears |
| `audit_logs` table doesn't exist | First boot before `Base.metadata.create_all` | API auto-creates in non-prod environments. In prod run `alembic upgrade head` |
| WS clients see no events | `ws.bridge.started` missing in logs / Redis down | Restart api after Redis recovers; bridge re-subscribes on lifespan |

## Common one-liners

```bash
# Tail the structured log
docker compose logs --no-color -f api

# Re-seed with deterministic demo data
docker compose exec -T api python -m scripts.seed_demo_data --reset

# Run unit tests
docker compose exec -T api pytest -m 'not integration' -q

# Force a model retrain (writes to /app/models on the host bind mount)
docker compose exec -T worker python -c \
  "from tasks.celery_app import app; \
   print(app.send_task('tasks.ml_tasks.retrain_anomaly_baselines').get(timeout=60))"

# Inspect the most recent audit events
docker compose exec -T api python -c "
import asyncio; from sqlalchemy import desc, select
from db.models import AuditLog; from db.session import get_async_session
async def m():
    async with get_async_session() as db:
        for a in (await db.execute(select(AuditLog).order_by(desc(AuditLog.timestamp)).limit(10))).scalars():
            print(a.timestamp, a.actor_id, a.action, a.status_code)
asyncio.run(m())
"
```

## Escalation

- **Sustained 5xx > 5 minutes**: declare incident, page Backend on-call.
- **Authentication outage** (login 5xx): bypass with `AUTH_REQUIRED=false`
  *only* for read-only investigation; never run a write workload with
  auth disabled in prod.
- **Audit log writer failing**: don't block traffic; the middleware
  swallows audit-write errors. Investigate Postgres availability or
  schema drift.

## SLOs

- p99 latency < 500ms for read endpoints; < 1500ms for cluster
  graph endpoints (heavy Cypher).
- Availability target: 99.5% (MoMo BSS-aligned).
- Audit event write success rate: > 99.9% (silent failure means
  compliance loss, not user impact).
