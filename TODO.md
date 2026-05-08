# TODO

The running list of unfinished work. Items are added when discovered
and removed when shipped, so the diff history of this file maps
roughly to outstanding work over time.

Last sweep: 2026-05-08.

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

- **Step-up auth's second factor** — `/auth/step-up` currently issues
  the elevated token after only an admin-role check. Production
  needs a real WebAuthn / TOTP factor here. Also needs route-level
  `step_up=True` claim enforcement on the high-risk endpoints
  (model promotion, takedown filing, data export — none of which
  exist yet, so this is preparatory).
- **API key rotation cadence** — endpoint exists; no scheduler
  enforces the 90-day rotation policy. Either add a beat task that
  expires keys past their rotation deadline, or rely on the operator
  to rotate.
- **PII redaction in `X-Audit-Extra`** — current redaction is name-
  based (`password`, `api_key`, etc.). Add format-based detection
  for free-text fields that might contain MSISDNs / wallet IDs.

### Rules / ML interaction

- **`rules/scheduler.py`, `rules/lifecycle.py`, `rules/backtest.py`,
  `rules/shadow.py`, `rules/templates.py`, `rules/parser.py`,
  `rules/models.py`** — listed in CLAUDE.md but the engine works
  without them. Templates are inline in `api/routes/rules.py`;
  lifecycle transitions are direct status updates. Worth pulling
  into dedicated modules when we ship the rule-create UI.

### Operability

- **mypy gate** — currently advisory in CI (`continue-on-error: true`).
  Drive errors to zero and flip the gate. Estimated 1–2 days of
  focused work.
- **Worker queue split** — single Celery queue today. When ML
  training jobs grow past 30s, split into `tasks.heavy` (training,
  evidence builds) and `tasks.light` (decay, dedup) so heavy work
  doesn't starve fast tasks.
- **Lakehouse-backed audit archive** — current `audit_logs` lives
  only in Postgres. CLAUDE.md §6.1 calls for monthly Iceberg
  archive after 6 months. Blocked on the lakehouse rollout.

### Frontend integration

- **WS reconnect / backfill** — backend doesn't keep a per-connection
  cursor. If the dashboard needs to fill in events missed during a
  brief disconnect, switch the bridge from Redis pub/sub to Redis
  Streams and add an `?since=` parameter on each feed.
- **CORS hardening in production** — startup logs an error if
  `CORS_ORIGINS` still has localhost when `ENVIRONMENT=production`,
  but the API still boots. Decide whether to fail-fast instead.

### Documentation

- **Per-service runbook depth** — runbooks shipped (`api.md`,
  `worker.md`, `consumer.md`) but each is shallow. Add escalation
  contact details and incident-response timelines as the on-call
  rotation gets defined.
- **More ADRs** — three are written (single-service, Neo4j vs
  Memgraph, Redis vs Aerospike). Future candidates: bcrypt-direct vs
  passlib (compatibility decision), the `_run_async` per-task client
  teardown pattern, the audit middleware vs decorator approach.

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
