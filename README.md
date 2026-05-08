# FraudNet Intelligence Engine

AI-native fraud network intelligence backend for mobile money. Built for
the MoMo / Scancom / law-enforcement triangle in Ghana, designed to scale
across MTN Group opcos under the FraudNet 2.0 plan in `docs/`.

> **The build doc is `CLAUDE.md`.** It is the canonical source of truth
> for module structure, schemas, scoring math, and the long list of rules
> that the engine enforces. Read it before changing behaviour.

---

## What's in the box

The engine is a single FastAPI service plus three sibling services
(Celery worker, Celery beat, Kafka consumer) and the standard data
plane (Neo4j + Postgres + Redis + Kafka + MinIO). One repo, one Docker
Compose file, one seeder, one CLI. From a fresh checkout, `make` your
own way through these end-to-end:

| Subsystem | Path | Notes |
|---|---|---|
| **Mesh expansion** | `core/mesh/` | BFS from a seed, distance discount, convergence bonus, decay, persistence |
| **Analytics** | `core/analytics/` | Community detection (Louvain + label-prop), centrality, anomaly + campaign + sleeper detectors, fund flow |
| **Rules engine** | `rules/` | Condition tree evaluator, 25+ action handlers, Redis dedup, Celery scheduler |
| **ML pipeline** | `core/ml/` | sklearn behavioural baseline; PyTorch GNN scaffold; per-feature extraction + training + inference + evaluation |
| **Evidence** | `core/evidence/` | Multi-page PDF packages with timeline + fund traces; MinIO-backed |
| **Auth** | `api/auth/` | bcrypt + JWT, role hierarchy, TOTP two-factor, Supabase-shape session |
| **Encryption** | `core/security/` | Fernet wrapper for TOTP secrets + audit-log PII fields |
| **Kafka consumers** | `ingestion/kafka_consumers/` | Transaction, SafeGuard, SIM-swap, device events |
| **WebSocket feeds** | `api/websocket/` | 6 feeds — 3 over Redis Streams (replay via `?since=`), 3 over pub/sub |
| **REST API** | `api/routes/` | 90+ endpoints across dashboard, clusters, nodes, agents, alerts, takedowns, rules, analytics, campaigns, LE, integration |
| **Migrations** | `alembic/` | Schema is owned by Alembic; API runs `upgrade head` on startup |

---

## Quickstart

### Prerequisites

- Docker Desktop (or compatible daemon) with Compose v2
- ~6 GB free RAM for the full stack
- Free ports: 8000 (API), 7474/7687 (Neo4j), 5432 (Postgres), 9000 (MinIO)

### Bring it up

```bash
cp .env.example .env             # adjust secrets if needed
docker compose up -d              # boots the entire stack
# Tables are created by Alembic on API startup. The seeder owns rows
# only — it'll fail-fast with a clear error if migrations haven't run.
docker compose exec -T api python -m scripts.seed_demo_data --reset
```

The seeder writes 500 wallets, 200 handsets, 350 SIMs, 80 agents,
~5000 transactions, 15 clusters, and the Postgres workflow state
(rules, takedowns, alerts, LE cases, operator integrations) into a
deterministic graph. It takes ~5 seconds.

#### Manual migration runs

The API runs `alembic upgrade head` on startup by default. To run it
manually (or in a separate deploy job) set
`RUN_MIGRATIONS_ON_STARTUP=false` and:

```bash
docker compose exec -T api alembic upgrade head    # apply pending
docker compose exec -T api alembic current         # show current rev
docker compose exec -T api alembic history --verbose
```

Generate a new migration after editing `db/models.py`:

```bash
docker compose exec -T api alembic revision --autogenerate -m "..."
```

### Smoke check

```bash
curl -s http://localhost:8000/health | jq
curl -s 'http://localhost:8000/api/dashboard/metrics' | jq
open http://localhost:8000/docs   # OpenAPI explorer
```

---

## Local development

The API container mounts the host directory at `/app`, so edits trigger
Uvicorn auto-reload. The worker, beat, and consumer containers do
**not** auto-reload — restart them when you change rules / Celery /
consumer code:

```bash
docker compose restart worker beat consumer
```

### Tests

Unit suite (no infra needed):

```bash
docker compose exec -T api pytest -m 'not integration' -q
```

Integration suite (requires the live stack + seeded data):

```bash
docker compose exec -T api pytest -m integration -q
```

### Lint / format / typecheck

```bash
docker compose exec -T api ruff check .
docker compose exec -T api ruff format --check .
docker compose exec -T api mypy api core rules ingestion db tasks config scripts
```

CI gates on all three — see `.github/workflows/ci.yml`. mypy runs in
strict mode (`strict = true` in pyproject) with a small allowlist for
intractable third-party untyped calls (Celery decorators, redis async
client). New code is expected to type-check from day one.

### Demo accounts

The seeder creates seven users, all with password `demo123`:

| username | role |
|---|---|
| `admin` | admin |
| `noc-lead` | senior_investigator |
| `inv-1`, `inv-2` | investigator |
| `analyst-1`, `analyst-2` | analyst |
| `viewer-1` | viewer |

Get a token:

```bash
curl -s -X POST http://localhost:8000/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"demo123"}' | jq -r .data.access_token
```

In dev (`AUTH_REQUIRED=false`) all routes still work without a token —
the `require_role()` dependency resolves an unauthenticated caller to
an "anon admin". Set `AUTH_REQUIRED=true` to enforce auth everywhere.

### Two-factor (TOTP)

Once authenticated, any user can enrol a TOTP factor. The flow:

```bash
TOKEN=...   # from /auth/login
# 1. Mint a fresh secret + QR code (returns plaintext once)
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
     http://localhost:8000/auth/totp/setup
# 2. Verify a 6-digit code from the authenticator app — flips totp_enabled
curl -s -X POST -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' -d '{"code":"123456"}' \
     http://localhost:8000/auth/totp/verify
```

Once enabled, `/auth/login` requires `totp_code` in the body. Without
it the response is `401 {"detail":{"error":"totp_required",...}}` so
the frontend can pivot to a TOTP-prompt screen without re-entering
the password. `/auth/totp/disable` clears the secret (also requires
a current valid code).

Secrets are encrypted at rest using `core.security.encryption.Fernet`
under `ENCRYPTION_KEY`. Same key encrypts MSISDN/IMEI fields in
`audit_logs.extra` per the data-governance policy.

### Frontend session

`/auth/session` returns the current session in **Supabase-compatible**
shape — `{ session: { access_token, expires_in, expires_at, user: {
id, email, role, user_metadata, app_metadata, ... } } }`. `/auth/refresh`
issues a fresh token. The shape is verbatim Supabase so frontends can
swap their auth provider without refactoring `session.user.id` /
`session.access_token` reads.

### Data retention

`audit_logs`, `alerts`, and `rule_triggers` are pruned nightly by
`tasks.periodic.purge_expired_records`. Windows are config-driven via
`AUDIT_RETENTION_DAYS=730`, `ALERT_RETENTION_DAYS=365`,
`TRIGGER_RETENTION_DAYS=180` (defaults shown). Unacknowledged alerts
are NEVER auto-purged — they represent open work that must be closed
by an analyst before retention applies.

---

## Architecture (one paragraph)

A single FastAPI service exposes the REST + WebSocket surface. Reads
go straight to Neo4j (graph state) and Postgres (workflow state); the
async Neo4j client coerces vendor temporals to native Python types at
the query boundary, so endpoints don't deal with vendor types.
Mutations either land synchronously (freeze, ack, takedown initiate)
or are dispatched through Kafka (transaction events, SafeGuard
signals, SIM swaps); a Kafka consumer service translates each into
Cypher writes + alert rows. The rules engine runs every 5 minutes via
Celery beat, iterating over the graph, evaluating condition trees,
and dispatching actions through the registry; trigger rows + Redis-
backed 15-minute dedup keys keep it idempotent. Real-time UX is driven
by three core WS feeds (alerts, cluster updates, metrics) over Redis
**Streams** with a `?since=<stream_id>` resume parameter for backfill
on reconnect, plus three derived feeds (rules, integration,
per-takedown) over Redis pub/sub. Any producer (route, Celery task,
consumer) can broadcast without knowing who's listening.

For the full architecture and the FraudNet 2.0 evolution, see
`CLAUDE.md` and `docs/FraudNet_2.0_backend_spec.md`.

---

## API overview

All responses use the envelope `{"data": ..., "meta": {...}, "errors": []}`.

| Group | Routes |
|---|---|
| Auth | `POST /auth/login` (+ TOTP), `GET /auth/me`, `GET /auth/session`, `POST /auth/refresh`, `POST /auth/users`, `GET /auth/users`, `PUT /auth/users/{id}`, `DELETE /auth/users/{id}` |
| TOTP | `POST /auth/totp/{setup,verify,validate,disable}`, `POST /auth/step-up` |
| Dashboard | `/api/dashboard/{metrics,cluster-overview,alert-feed,activity-timeline,recent-takedowns}` |
| Clusters | `/api/clusters` (list / detail / `/graph` / `/fund-flow` / `/nodes` / `/evidence` / `/expand`) |
| Nodes | `/api/nodes/search`, `/api/nodes/{type}/{id}`, freeze / unfreeze / flag / watchlist / connections |
| Agents | `/api/agents`, `/api/agents/map`, `/api/agents/{id}/cashout-patterns`, suspend / warn |
| Alerts | `/api/alerts`, `/stats`, acknowledge / dismiss |
| Takedowns | `/api/takedowns`, initiate / approve / complete / readiness / evidence-package |
| Rules | `/api/rules`, `/templates`, `/{id}/triggers`, `/{id}/performance` |
| Analytics | `/api/analytics/{kpis,clusters-over-time,fraud-value,seed-sources,agent-classification,top-nodes}` |
| Campaigns | `/api/campaigns` (cached detection list), `/api/campaigns/{id}` (detail + timeline) |
| Law enforcement | `/api/law-enforcement/{agencies,cases,cases/{id}/messages,cases/{id}/evidence,cases/{id}/outcomes,inbound-intel}` |
| Integration | `/api/integration/{operators,shared/inbound,shared/outbound,chamber/...}` |
| External (operator) API | `/api/external/v1/{flags,flags/query,intelligence,health}` (X-API-Key auth) |
| WebSocket | `/ws/alerts?since=<id>`, `/ws/cluster-updates?since=<id>`, `/ws/metrics?since=<id>` (Streams + replay); `/ws/rules`, `/ws/integration`, `/ws/takedown/{id}` (pub/sub) |

Full schemas at <http://localhost:8000/docs> and
<http://localhost:8000/openapi.json>.

---

## Project layout

The detailed module tree is in `CLAUDE.md` §3. Key entry points:

- `api/main.py` — ASGI app + lifespan (Neo4j connect, schema init, WS bridge, metrics loop)
- `api/routes/` — REST routers (one per resource)
- `api/auth/` — JWT + bcrypt + RBAC dependencies
- `api/websocket/` — connection manager, Redis bridge, publisher, six feeds
- `core/mesh/expansion.py` — BFS + scoring + persistence
- `core/analytics/` — community / centrality / anomaly / campaign / sleeper / fund-flow
- `core/evidence/` — package builder + ReportLab PDF exporter + MinIO upload
- `rules/engine.py` — scheduled-mode entry + per-context evaluator dispatch
- `rules/actions/` — action registry + handlers
- `ingestion/kafka_consumers/` — transaction, SafeGuard, scancom (SIM + device)
- `tasks/` — Celery app + beat schedule + periodic + mesh + ML + report tasks
- `db/models.py` — SQLAlchemy declarative models
- `db/session.py` — sync + async engine factories
- `scripts/seed_demo_data.py` — deterministic demo seed

---

## CI / CD

GitHub Actions in `.github/workflows/`:

- **ci.yml** — ruff check, ruff format --check, mypy (advisory),
  pytest (unit) on every push + PR. Integration tests fire only on
  pushes to main and bring up the full Docker stack.
- **docker.yml** — builds and pushes the API image to GHCR on every
  main push. Tagged with `sha-<short>`, branch name, and `latest`.

---

## Known gaps

See `TODO.md` for the running list of work-not-yet-done — things
spec'd in `CLAUDE.md` that haven't been wired, hot paths that are
stubs, and issues that surfaced during the last sprint.
