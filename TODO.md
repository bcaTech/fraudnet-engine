# TODO

The running list of unfinished work. Items are added when discovered
and removed when shipped.

Last sweep: 2026-05-08 (post-data-governance sprint).

## Still open

### Telco / actuator integration (waiting on partners)

- **`scancom_batch_import`** — heartbeat-only Celery stub. Real
  implementation pulls SIM/IMEI/cell-tower deltas from the Scancom
  feed. Blocked on Scancom API contract.
- **MoMo BSS freeze API** (`core/takedown/wallet_freeze.py:apply_external_freeze`) —
  recorded-intent only. Real implementation HTTP-POSTs to the BSS
  with HMAC-signed body + idempotency key from `takedown_id`. Blocked
  on BSS endpoint spec.
- **Scancom registry SIM flag**
  (`core/takedown/sim_flag.py:apply_external_sim_flag`) — same
  pattern, same blocker.
- **Agent SMS dispatch**
  (`core/takedown/agent_alert.py:send_agent_warning`) — needs the
  operator notification service endpoint.
- **Outbound shared-flag delivery**
  (`tasks/periodic.py:process_outbound_integration`) — currently
  stamps `action_taken='sent'` without an actual HTTP call. Real
  delivery hits `/external/v1/flags` on the receiving operator with
  the same HMAC pattern as `custom_webhook`.

### ML / GNN

- **`core/ml/gnn_model.py`** is a placeholder that falls through to
  the behavioural baseline. Real GraphSAGE / GAT implementation
  blocked on:
  - PyTorch + PyTorch Geometric in the default container (currently
    in the optional `[ml]` extras, ~3 GB image bloat).
  - A GPU host for training (or 8+ CPU cores + 16 GB RAM minimum).
  - Labelled training data — current cluster_id-based labels are
    leaky for any reasonable feature set; need analyst-confirmed
    fraud examples.
- **Model performance over time** — `evaluate_gnn_performance` runs
  weekly. Add drift detection (population stability index, prediction
  histogram) before relying on the score for any auto-action.

### Auth tightening (last mile)

- **Step-up auth's second factor** — `/auth/step-up` mints a 5-min
  elevated token after an admin-role check. TOTP is now wired and
  could be required at this gate too (currently `/auth/step-up`
  doesn't enforce TOTP even when the user has it enabled). Decide
  the policy and wire.
- **API key rotation cadence** — endpoint exists; no scheduler
  enforces the 90-day rotation policy. Either add a beat task that
  expires keys past their rotation deadline, or rely on operators
  to rotate.
- **PII format detection in `X-Audit-Extra`** — the new field-level
  encryption catches MSISDN / IMEI / IMSI patterns. Extend with IBAN /
  bank-account formats if those start showing up in audit extras.

### Rules / ML interaction

- **`rules/scheduler.py`, `rules/lifecycle.py`, `rules/backtest.py`,
  `rules/shadow.py`, `rules/templates.py`, `rules/parser.py`,
  `rules/models.py`** — listed in CLAUDE.md but the engine works
  without them. Templates are inline in `api/routes/rules.py`;
  lifecycle transitions are direct status updates. Worth pulling
  into dedicated modules when we ship the rule-create UI.

### Operability

- **Worker queue split** — single Celery queue today. When ML
  training jobs grow past 30s, split into `tasks.heavy` (training,
  evidence builds) and `tasks.light` (decay, dedup) so heavy work
  doesn't starve fast tasks.
- **Lakehouse-backed audit archive** — current `audit_logs` lives
  only in Postgres with a 730-day retention sweep. CLAUDE.md §6.1
  calls for monthly Iceberg archive after 6 months. Blocked on the
  lakehouse rollout.
- **Encryption key management** — `ENCRYPTION_KEY` is a settings
  scalar today (defaulted in `config/settings.py`). Production must
  source it from a KMS / Vault rather than env. The seam is in
  `core.security.encryption._fernet`; swap that and call sites stay
  unchanged.

### Frontend integration

- **CORS hardening in production** — startup logs an error if
  `CORS_ORIGINS` still has localhost when `ENVIRONMENT=production`,
  but the API still boots. Decide whether to fail-fast instead.
- **WS Streams cap** — three core feeds use Redis Streams capped at
  `STREAM_MAXLEN=1000` entries via XADD MAXLEN ~. If a client is
  offline longer than ~1000 events of activity, oldest entries are
  trimmed and the replay returns a partial history. Document this
  to frontend.

### Documentation

- **Per-service runbook depth** — runbooks shipped (`api.md`,
  `worker.md`, `consumer.md`) but each is shallow. Add escalation
  contact details and incident-response timelines as the on-call
  rotation gets defined.
- **More ADRs** — three are written (single-service, Neo4j vs
  Memgraph, Redis vs Aerospike). Future candidates: bcrypt-direct vs
  passlib, the `_run_async` per-task client teardown pattern, the
  audit middleware vs decorator approach, the Streams-vs-pub/sub
  split for WS feeds.

## Known issues / regressions

- **passlib + bcrypt 4.1+ incompatibility** — bypassed by calling
  `bcrypt` directly in `api/auth/passwords.py`. Switch back to
  passlib if upstream releases a fix.
- **`EmailStr` requires `email-validator`** which isn't a project
  dep. `api/auth/routes.py` uses a plain regex pattern. Acceptable;
  switch to `EmailStr` once we add the dep.
- **R07 ("KYC tier mismatch") fires repeatedly** against the same
  wallet pool. `cleanup_stale_rule_state` (nightly Celery beat task)
  clears the `kyc_pending_reverification` flag after 7 days so the
  demo doesn't accumulate a long tail.

## Resolved this sprint

For commit-history reference; remove on next sweep.

- ✅ **Alembic baseline + autogenerate** (item 1) — replaced the
  `Base.metadata.create_all` shortcut with autogenerated migrations.
  API runs `alembic upgrade head` on startup.
- ✅ **mypy strict gate** (item 2) — 180 errors → 0; CI typecheck
  is now a hard gate.
- ✅ **TOTP second factor** (item 3) — setup / verify / validate /
  disable + login flow update.
- ✅ **Redis Streams backfill** (item 4) — 3 core feeds use Streams
  with `?since=` replay; rules / integration / takedown stay on
  pub/sub.
- ✅ **Campaigns API + cache** (item 5) — `/api/campaigns` reads
  from a 15-min Redis cache populated by Celery.
- ✅ **Frontend auth compat** (item 6) — `/auth/session` returns
  Supabase-shape, `/auth/refresh` issues sliding-window tokens.
- ✅ **Field encryption + retention** (item 7) — Fernet wrapper
  encrypts MSISDN/IMEI in audit logs; nightly purge enforces
  audit/alert/trigger retention windows.
