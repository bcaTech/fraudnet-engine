# TODO

Running list of work that's spec'd in `CLAUDE.md` but not yet implemented,
hot paths that are stubs, and issues we hit but moved past in the last
sprint. Issues should land here rather than rotting in commit messages.

## Stubs to flesh out

- **`tasks/periodic.py` heartbeats** — only `evaluate_scheduled_rules` and
  `rescore_active_clusters` are real. The others (`apply_temporal_decay`,
  `scancom_batch_import`, `sleeper_wallet_scan`, `campaign_detection`,
  `process_inbound_integration`, `process_outbound_integration`,
  `rules_performance_aggregation`, `external_operator_health_check`,
  `law_enforcement_case_reminders`) log a heartbeat and return `stub`.
  Most have a real implementation already in `core/analytics/` or
  `core/mesh/decay.py` — they just need wiring.
- **`tasks/ml_tasks.py`** — all three tasks (`retrain_anomaly_baselines`,
  `evaluate_gnn_performance`, `batch_score_wallets`) are stubs. The GNN
  model in `core/ml/gnn_model.py` doesn't exist yet — entire `core/ml/`
  module needs to be built.
- **`tasks/report_tasks.py`** — `generate_analytics_snapshot` and
  `build_evidence_package` are stubs. Evidence build is now real (in
  `core/evidence/builder.py`); the Celery wrapper just needs to call it.
- **`rules/scheduler.py`, `rules/lifecycle.py`, `rules/backtest.py`,
  `rules/shadow.py`, `rules/templates.py`, `rules/parser.py`,
  `rules/models.py`** — `CLAUDE.md` lists these but they don't exist.
  The engine works without them (templates are inline in
  `api/routes/rules.py`); life-cycle transitions are direct status
  updates.
- **`rules/actions/custom_webhook`** — recorded-intent stub. Needs HTTP
  POST + HMAC + retry + allow-list before production use.
- **`rules/actions/notify_external_operator`** — sets a graph property
  but doesn't actually push to the operator's external API. Should
  enqueue an outbound `SharedFlag` row.
- **`core/agents/scoring.py`, `core/agents/classification.py`,
  `core/agents/geographic.py`** — the agent risk scoring functions
  documented in CLAUDE.md don't exist as modules. The seeded values
  approximate them; production must compute from live data.
- **`core/takedown/executor.py`** — the coordinated-takedown executor
  is a placeholder. The current flow is "mark all steps completed in
  one transaction"; real implementation runs each step against its
  actuator (MoMo wallet freeze API, Scancom SIM flag API, agent SMS).
- **`core/takedown/wallet_freeze.py`, `sim_flag.py`, `agent_alert.py`,
  `restitution.py`** — actuator integrations not yet implemented.
- **`api/middleware/audit.py`** — referenced in CLAUDE.md as required
  for every protected action. Audit events are emitted ad-hoc by
  individual routes, not via the centralised middleware.

## Auth tightening

- The `User` foreign key is missing on most workflow tables (alert
  acknowledgements, takedowns, evidence packages all hard-code
  `"system"`). Once auth is enforced everywhere, populate
  `acknowledged_by` / `initiated_by` / `generated_by` / etc. from the
  authenticated principal.
- `/auth/users` POST exists; PUT / DELETE / disable endpoints are not
  implemented.
- Step-up auth (CLAUDE.md §7.1) for "high-risk" operations
  (model promotion, user role changes, data export, takedown filing)
  is not implemented.
- API keys for external operators are issued but never rotated. Need
  `POST /api/integration/operators/{id}/rotate-key` + an expiry
  policy.

## Data model / migrations

- **No Alembic migrations.** Tables are created via
  `Base.metadata.create_all()` in the seeder. First migration should
  capture the current schema snapshot.
- **Cluster `seed_date` stored as ISO string.** Should be a Neo4j
  datetime. The query layer wraps it in `datetime(...)` to compensate;
  fix this in the seeder + any other writers.

## Operational

- **Worker / beat container healthchecks fail** because the shared
  Dockerfile defines a HEALTHCHECK that probes port 8000. Either drop
  the healthcheck for those services in compose or expose a small HTTP
  health server in the Celery process.
- **`Base.metadata.create_all()` runs are manual** when adding new
  models. The first time the API hits a table that doesn't exist, it
  500s. Either run `create_all` at API startup (dev-only) or commit to
  Alembic for prod.
- **`celerybeat-schedule` SQLite file** ships in the volume mount.
  Acceptable for dev; in prod use the Redis scheduler or a real
  persistent volume.

## Frontend integration

- The dashboard project (`fraudnet-dashboard`) is in a sibling repo
  and shares CORS origins. Once it lands in production, lock down
  `CORS_ORIGINS` to the allowed list.
- WebSocket reconnect / backfill on disconnect is a frontend concern;
  backend doesn't currently keep a per-connection cursor. If the
  frontend needs to fill in events missed during a brief disconnect,
  add an offset-aware "since" parameter on each feed and back it with
  Redis Streams instead of pub/sub.

## Tests

- Integration tests are happy-path only. Need negative cases on the
  freeze / takedown / evidence flows.
- The fake Neo4j in `tests/test_mesh/test_expansion.py` only covers
  seed-not-found and zero-neighbours. Expand to verify a 3-node
  cluster's confidence math.
- No Kafka-consumer test coverage yet. The consumer base is a good
  candidate for a `Testcontainers`-driven integration test.
- mypy is currently advisory in CI (`continue-on-error: true`). Drive
  it to zero errors and flip the gate.

## Known issues / regressions

- Rule R07 ("KYC tier mismatch") fires on every scheduled run against
  the same wallet pool until the 15-minute Redis dedup window expires.
  After many runs this leaves a long tail of wallets carrying
  `kyc_pending_reverification=true` in dev. Consider a daily cleanup
  task.
- `passlib` 1.7.4 + `bcrypt` 4.1+ are incompatible — we bypass passlib
  by calling `bcrypt` directly in `api/auth/passwords.py`. If passlib
  releases a fix, simplify and switch back.
- `EmailStr` from Pydantic needs the `email-validator` package which
  isn't a project dep — `api/auth/routes.py` uses a plain regex
  pattern instead. Acceptable for now; switch to `EmailStr` once we
  add the dep.

## Documentation

- Per-service runbooks (`docs/runbooks/*.md`) referenced in CLAUDE.md
  haven't been written. At minimum add ones for the API, the worker,
  and the consumer.
- ADRs (`docs/adrs/`) — none yet. The decisions to use Neo4j over
  Memgraph, Redis over Aerospike, and a single FastAPI service over
  the FraudNet 2.0 microservice topology should be captured.
