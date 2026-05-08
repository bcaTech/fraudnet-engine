# ADR 0001 — Ship as a single FastAPI service (not microservices)

- **Status:** Accepted (initial v1).
- **Date:** 2026-05-08.
- **Deciders:** Backend engineering.
- **Related:** `docs/FraudNet_2.0_backend_spec.md` (the steady-state
  microservice topology this ADR explicitly defers).

## Context

CLAUDE.md and the FraudNet 2.0 spec both describe a polyrepo-style
monorepo with ~20 deployable services (`api-noc`, `ingest-voice`,
`brain-graph`, `decisions`, `action-tier{1,2,3}`, etc.). That topology
matches telco-scale operability needs — different latency tiers,
different infrastructure profiles, independent deploy cadences.

We're shipping the v1 of the engine for MoMo Ghana, against an
existing Scancom integration, with one analyst team and one operator.
Building 20 services up front buys complexity we can't yet pay for in
operability dollars (no service mesh, no tier-1 SRE, no on-call
rotation per service).

## Decision

Ship the v1 as a **single FastAPI service** (`api/`) plus its
unavoidable siblings:

- one Celery worker
- one Celery beat scheduler
- one Kafka consumer service (multiplexed across four topics)

All of them share one `Dockerfile`, one `pyproject.toml`, one
deployment cadence. Modules within the repo (`api/`, `core/`,
`rules/`, `tasks/`, `ingestion/`) preserve the eventual service
boundaries, so the migration to the FraudNet 2.0 topology is a
"split the package, don't rewrite the logic" exercise.

## Consequences

**Positive**

- One deploy, one health endpoint, one log stream — operationally
  cheap for the team we have.
- Module boundaries align with future services, so refactor is
  mechanical rather than architectural.
- All cross-cutting concerns (auth, audit, WS publishing) live in
  shared in-process modules, not negotiated over the wire.

**Negative**

- Inline-tier latency budget (CLAUDE.md §1) won't be met by a
  monolith deployed alongside the rest. When VoLTE in-call tagging
  arrives, `action-tier1` must split out.
- Independent scaling decisions (the graph backend wants different
  hardware than the dashboard API) get harder. We rely on
  vertical scaling and read-replicas instead.
- A bug in the rules engine can OOM the whole API. Defence in depth
  via Celery sandboxing (rules run in beat/worker, not API) and the
  audit middleware's swallow-all error path.

## Migration path

The FraudNet 2.0 spec describes the target. Earliest split candidates,
in order:

1. **Kafka consumer** → already its own container; promote to its own
   image + manifest.
2. **Celery worker (rules + ML)** → its own image; introduce
   `tasks.heavy` and `tasks.light` queues.
3. **`action-tier1`** → first true microservice when MTN's network
   probe vendor is selected.

Each split lands behind its own ADR.
