# FraudNet 2.0 — Backend

> **CLAUDE.md / engineering specification**
> **For:** MTN Ghana / brAIn Consult
> **Companion:** `FraudNet_2.0_Frontend.md`
> **Status:** v1.0 — covers Phases 1 + 2 in build, scaffolds Phases 3–4

This document is the working reference for building the FraudNet 2.0 backend. Drop it in the repo root as `CLAUDE.md`; it tells Claude Code (and any human engineer) what the system is, how it is laid out, what the conventions are, and where to look for things.

This is not a marketing document. It is a build document. Skip it at your peril.

---

## 1. Mission

FraudNet 2.0 is MTN Ghana's network-native, AI-driven fraud intelligence platform. It ingests real-time signals from voice signaling (SS7, Diameter, IMS), messaging (SMSC), data (DNS, IPDR), and MoMo events; fuses them on a single fraud graph; runs detection across behavioural, content, and graph models; and dispatches actions across three latency tiers (real-time inline, near-real-time, investigation). The full strategic context lives in the companion `Strategy_and_Build_Plan.md` — read it first if you have not.

The backend's defining constraints:

- **Telco-scale throughput.** Order of magnitude: 100M events/day across voice + SMS + MoMo at MTN Ghana scale; up to 10× that under federation (Phase 4).
- **Sub-200ms inline latency** for VoLTE in-call tagging (Tier 1 actions).
- **Graph fusion is the IP.** The architecture is designed so the graph is the integrating substrate, not a side artefact. Every signal becomes a node or edge.
- **Purpose limitation by design.** Data accessed for fraud prevention is engineered to be inaccessible to marketing, ARPU optimisation, or any other use. This is enforced in code and infra, not policy.
- **Multi-tenant.** Three audiences — internal NOC, MTN customers, B2B enterprise — must be served from the same engine without leakage between tenants.
- **Ghana legal posture.** Aligned with the Electronic Communications Bill 2025, Data Protection Bill 2025, and Data Harmonisation Bill 2025. Data residency in-country.

---

## 2. Architecture

The system is a four-layer pipeline with two cross-cutting bands. Layers map directly to the reference architecture in the strategy doc:

```
Layer 1  Signal Ingestion       — probes, SMSC, DNS, MoMo, external intel
Layer 2  Stream Processing      — Kafka, Flink, in-memory feature store, lakehouse
Layer 3  Detection & Intelligence — behavioural, content, GRAPH (the moat)
Layer 4  Decision & Action      — three-tier dispatcher + downstream actuators

Cross-cutting   Governance      — purpose limitation, audit, regulatory reporting
Cross-cutting   Feedback        — labels, retraining, champion/challenger
```

The codebase is a polyrepo-style **monorepo** (one git repository, many independently-deployable services) using a Turborepo-style build orchestrator. Services share types and libraries through workspace packages, but each service is a deployable artefact with its own container image, its own deploy cadence, and its own SLOs.

**Why microservices and not a monolith.** The latency tiers operate on different infrastructure profiles. Tier 1 inline scoring needs co-location with the network path and aggressive resource isolation; the investigator-facing API runs on commodity Kubernetes; the graph backend needs specialised hardware. Forcing these into a single deployable artefact ties scaling decisions together that should be independent.

**Why not pure event-driven.** The investigator workbench, customer self-service, and B2B portal need synchronous query semantics over the graph and over MoMo. A pure event-driven design pushes complexity onto the read side. The chosen split is: ingest, score, and act event-driven; query and investigate request-response.

---

## 3. Service map

```
fraudnet-backend/
├── services/
│   ├── ingest-voice/          # SS7/Diameter/SIP probe sink → Kafka
│   ├── ingest-sms/            # SMSC integration → Kafka
│   ├── ingest-data/           # DNS/IPDR feeds → Kafka
│   ├── ingest-momo/           # MoMo event listener → Kafka (extends existing FraudNet)
│   ├── ingest-intel/          # External intel adapters (GSMA, peer telco, customer reports)
│   ├── stream-features/       # Flink jobs: windowed feature computation
│   ├── stream-graph/          # Flink jobs: real-time graph mutations
│   ├── brain-behavioural/     # Behavioural model serving (gRPC + REST)
│   ├── brain-content/         # Content / URL classification serving
│   ├── brain-graph/           # Graph model serving (GNN inference + community detection)
│   ├── decisions/             # Decision/action plane orchestrator
│   ├── action-tier1/          # Inline action dispatcher (VoLTE tag, URL block, MoMo friction)
│   ├── action-tier2/          # Near-real-time customer alerts, prompts, SOC tickets
│   ├── action-tier3/          # NOC investigation tooling, takedown workflows
│   ├── api-noc/               # NOC investigator API
│   ├── api-customer/          # Customer self-service API
│   ├── api-enterprise/        # B2B tenant API (Phase 4)
│   ├── api-admin/             # System admin API
│   ├── api-public/            # API gateway (auth, rate limit, routing)
│   ├── compliance/            # Audit log, purpose-limitation enforcement, regulator reports
│   └── feedback/              # Label ingestion, retraining triggers
├── packages/
│   ├── schemas/               # Pydantic models, Avro schemas, OpenAPI types — single source of truth
│   ├── graph-client/          # Memgraph/Neo4j client wrapper with FraudNet semantics
│   ├── feature-client/        # Aerospike/Redis client with feature-store conventions
│   ├── kafka-client/          # Producers, consumers with retry/dead-letter wiring
│   ├── auth-lib/              # JWT validation, RBAC, tenant scoping
│   ├── audit-lib/             # Audit log primitives (every protected action goes through this)
│   ├── obs/                   # Observability — structured logging, tracing, metrics
│   └── testing/               # Test fixtures, factories, integration test helpers
├── infra/
│   ├── k8s/                   # Manifests (Kustomize)
│   ├── terraform/             # Cloud infra (GCP/AWS — pick one at deploy time)
│   ├── kafka-topics/          # Topic definitions (declarative)
│   └── flink-jobs/            # Flink job definitions, deployments
├── docs/
│   ├── runbooks/              # On-call runbooks per service
│   ├── adrs/                  # Architecture Decision Records
│   └── data-contracts/        # Inter-service data contracts
├── tools/
│   ├── load-gen/              # Synthetic event generator for load testing
│   ├── replay/                # Replay events from lakehouse for debugging
│   └── data-quality/          # Pipeline data-quality checks
├── pyproject.toml             # Workspace config
├── turbo.json
├── docker-compose.dev.yml     # Local dev environment
└── README.md
```

---

## 4. Tech stack

The stack is opinionated and chosen for telco-scale operability, not novelty.

### 4.1 Languages and frameworks

- **Python 3.12 + FastAPI** for all REST/gRPC service layers (consistency with existing FraudNet team competencies).
- **PyTorch + PyTorch Geometric** for graph models. **scikit-learn + LightGBM** for behavioural models. **Sentence-transformers + a small fine-tuned classifier** for content models.
- **Apache Flink (Java/Scala)** for stream processing. Use SQL-first where feasible; PyFlink only for prototyping. Production jobs are Java/Scala.
- **TypeScript** for build tooling, IaC scripts. The frontend is its own repo (see `FraudNet_2.0_Frontend.md`).

### 4.2 Data infrastructure

- **Apache Kafka** as the event spine. Confluent Platform recommended for operational tooling; Strimzi on Kubernetes acceptable for cost-sensitive deployments.
- **Apache Flink** for stream processing. Long-running jobs operated via Flink Kubernetes Operator.
- **Aerospike** as the in-memory feature store for the inline tier (Tier 1). Aerospike chosen over Redis for its sub-millisecond p99 at telco-relevant volumes and its native cross-DC replication for the eventual pan-African federation.
- **PostgreSQL 16** for relational state (alerts, takedowns, users, tenants, model registry). Use **TimescaleDB** extension for time-series alert telemetry. Run via managed Postgres (CloudSQL / RDS / equivalent).
- **Memgraph** as the production graph database. Memgraph chosen over Neo4j for streaming graph workloads (it is designed for in-memory graphs with continuous mutation), better real-time query performance, and Cypher compatibility for tooling familiarity. Replication and backup via Memgraph Enterprise.
- **Apache Iceberg on S3-compatible object storage** for the lakehouse — training data, replay logs, audit archive. Trino as the query engine over Iceberg.
- **MinIO** as the S3 layer for sovereign-cloud deployments where required by data residency.

### 4.3 Serving and ops

- **Kubernetes (1.30+)** as the runtime. EKS / GKE / on-prem rke2 — abstracted via Terraform.
- **Istio** as service mesh (mTLS between services, traffic policies, retries, circuit breakers).
- **OpenTelemetry** for tracing, metrics, structured logs. Backends: **Prometheus + Grafana** for metrics, **Tempo** for traces, **Loki** for logs. Or vendor (Datadog, New Relic) if the operating model prefers.
- **ArgoCD** for GitOps deployment.
- **Vault** (HashiCorp) for secrets.
- **Keycloak** for service-to-service identity and for the NOC operator SSO bridge.

### 4.4 What we explicitly do not use

- **No Supabase** for the FraudNet 2.0 backend. Supabase is excellent for product backends; it is not the right fit for telco-scale streaming, graph, and inline serving. (The frontend uses Supabase Auth — that's separate.)
- **No serverless functions** for production paths. Cold-start latency is incompatible with Tier 1 budgets.
- **No managed Kafka-as-a-Service** unless data residency exceptions are obtained from MTN Group and DPC. Kafka holds pre-redaction PII for short windows and must run in-country.

---

## 5. Service-by-service specification

This section is the detailed contract for each service. Read carefully before modifying.

### 5.1 Ingestion services (`services/ingest-*`)

All ingestion services share a common pattern: they translate vendor / external formats into FraudNet's canonical event schemas (defined in `packages/schemas`), produce to Kafka, and emit health metrics. They do not enrich, score, or persist; that is downstream.

**`ingest-voice`** consumes from network probe vendors (Polystar, Subex, NetScout, EXFO — the vendor-selection RFI happens in the first 45 days of the programme; the integration layer is built to be vendor-neutral via a `VoiceProbeAdapter` interface). Inputs: SS7/Diameter/SIP signaling events, CDRs. Outputs to topic `voice.events.v1`. Throughput target: 30k events/sec sustained, 100k peak. Critical: this service is on the inline path. Latency budget end-to-end (probe → Kafka): 30ms p99.

**`ingest-sms`** integrates with the SMSC. It receives SMS metadata and, where regulatorily authorised under Phase 1 scope, the message body (for URL extraction and template clustering). Output: `sms.events.v1`. Content scanning is gated by an explicit `purpose=fraud_prevention` claim in the request context (enforced by `audit-lib`).

**`ingest-data`** receives DNS resolver logs and IPDR feeds. Output: `data.events.v1`.

**`ingest-momo`** is an extension of the existing FraudNet MoMo integration — the most stable component. Output: `momo.events.v1`.

**`ingest-intel`** has separate adapters (one per source: GSMA T-ISAC, each peer telco share, customer reports endpoint, internal SOC). Output: `intel.events.v1`. Customer reports flow in from the `api-customer` service.

Common patterns for all ingestion services:

- Idempotent producers (Kafka exactly-once semantics where supported by the broker; at-least-once + downstream dedup otherwise).
- Schema-registry-backed Avro for all topic payloads.
- Dead-letter topic per source (`*.dlq.v1`).
- Lag-aware health checks: a service is unhealthy if its consumer lag exceeds threshold.
- No PII in logs. Ever. Use the `audit-lib` `redact()` helper when in doubt.

### 5.2 Stream processing (`services/stream-*`)

**`stream-features`** runs Flink jobs that compute windowed features over the four input topics:

- Caller velocity (calls/sec, calls/min, calls/hr) per number.
- Fan-out (unique callees) per number, per window.
- IMEI churn per number.
- Geographic motion entropy.
- Inter-call duration distribution.
- SMS template hash + frequency.
- URL extraction + reputation lookup.
- MoMo transaction velocity, counterparty diversity, value distribution.

Features are written to Aerospike (the hot store, used by inline scoring) and to Iceberg (the cold store, used for training and replay).

**`stream-graph`** runs Flink jobs that mutate the production graph in real time. Inputs: the four event topics. Outputs: `graph.mutations.v1` (a control topic that other services subscribe to for graph-aware logic). The job creates or updates nodes (Number, Wallet, Device, Bank Account) and adds edges (Call, SMS, Money Flow, Shared Device, Co-located) via a buffered batch writer to Memgraph. Buffer size and flush cadence are tuned for sub-minute consistency; never enable individual writes on the hot path.

### 5.3 Brain services (`services/brain-*`)

These are the inference services. Each exposes:

- A **gRPC** endpoint (used by the inline tier — lower overhead).
- A **REST** endpoint (used by the NOC API for ad-hoc scoring).
- A **batch** Cron job for retraining (driven by `feedback`).

**`brain-behavioural`** loads the current champion behavioural model (LightGBM + a small sequence model for temporal patterns) and serves scoring requests. Inputs: a number or wallet ID + a feature snapshot from Aerospike. Output: a score `[0.0, 1.0]` plus model version and a feature-attribution vector. Latency budget: 5ms p99 (the inline tier has a 200ms total budget across feature fetch + score + decision + actuator).

**`brain-content`** scores SMS content and URLs. Two sub-paths: a fast lookup against the malicious URL database (microseconds) and a model-driven path for novel content (a fine-tuned classifier on top of a small sentence-transformer). Latency budget: 30ms p99 for model path; <1ms for lookup path.

**`brain-graph`** is the strategic IP. It serves three distinct operations:

- **Node scoring**: given a node, compute its risk score by running a GNN over its k-hop neighbourhood. Cached with short TTL because GNN inference is expensive.
- **Community detection**: scheduled batch job (every 5 min) that runs Leiden community detection over the active subgraph and stores ring memberships. Results land in Postgres `rings` table.
- **Motif detection**: streaming job that watches `graph.mutations.v1` for known fraud motifs (mule chains, fan-out + collapse patterns, voice-then-MoMo sequences) and emits `motifs.detected.v1`.

### 5.4 Decisions and actions (`services/decisions`, `services/action-tier{1,2,3}`)

**`decisions`** is the orchestrator. It subscribes to `motifs.detected.v1`, `graph.mutations.v1`, and to scoring outputs from the brain services. It applies decision policy — which tier to dispatch to, which action to recommend, suppression rules (don't alert the same customer twice in 6 hours about the same number, etc.) — and emits `decisions.dispatched.v1` per tier.

Decision policy is **codified in YAML** (`services/decisions/policies/*.yaml`), versioned, and deployable independently. This is critical: regulatory-relevant decisions must be auditable and reviewable without code changes.

**`action-tier1`** consumes `decisions.dispatched.v1` filtered to Tier 1, and dispatches to the actuators:

- VoLTE handset tag — sends a SIP header rewrite to the IMS core.
- URL block — pushes to the DNS sinkhole for matched URLs.
- MoMo transaction friction — invokes the MoMo BSS API to inject a Send-with-Care prompt.

This service is on the inline path and is the most operationally sensitive. It runs in a dedicated K8s namespace with strict resource limits, dedicated nodes, and a tight rollout window.

**`action-tier2`** dispatches near-real-time actions: customer SMS / push alerts (via the customer notification service), Do I Know You prompts, Ask Me First flows, SOC ticket creation. Tolerates seconds-to-minutes latency.

**`action-tier3`** writes to the NOC investigation queue (Postgres + a Kafka notification for live-update of the NOC frontend) and prepares evidence packs.

### 5.5 API services (`services/api-*`)

Each API is a FastAPI service. They share `auth-lib` for token validation and `audit-lib` for action logging.

**`api-public`** is the gateway. Routes `/api/v1/me/*` to `api-customer`, `/api/v1/noc/*` to `api-noc`, `/api/v1/enterprise/*` to `api-enterprise`, `/api/v1/admin/*` to `api-admin`. Owns: rate limiting (per token, per tenant), CORS, request ID injection, OpenTelemetry tracing root span, top-level auth (JWT signature, basic claim validation). Does not own business logic.

**`api-noc`** serves the investigator workbench. Endpoints listed in the frontend spec under Reference §2.4. Implementation notes:

- Alert listing is backed by a denormalised Postgres view, refreshed by a trigger on alert state changes. Direct querying of the live `alerts` table at the volumes seen during incident spikes is too slow.
- Ring detail composes data from Postgres (`rings`, `ring_members`) and Memgraph (the graph and timeline). Two parallel queries, joined in the API. Time budget 250ms p95.
- Graph endpoint `GET /rings/{id}/graph` supports `depth`, `min_score`, `max_nodes` query params. Default `max_nodes=200`. Cap at 1000.
- The takedown workflow is a state machine; transitions are guarded and audit-logged.

**`api-customer`** serves the customer self-service surface. Tenant-of-one (each customer is their own tenant). Auth is the customer's MSISDN via a one-time-password flow, exchanged for a session JWT. Customer reports submitted via `POST /me/report` are forwarded to `ingest-intel`.

**`api-enterprise`** (Phase 4) serves B2B tenants. Strict tenant scoping is enforced at the database layer (every query carries a `tenant_id` clause; row-level security in Postgres provides defence in depth). API keys are tenant-scoped; webhook signatures use HMAC.

**`api-admin`** serves system administrators. Step-up authentication required for sensitive operations (model promotion, user role changes, data export). Audit log is the most heavily-written table and lives on its own Postgres instance with WORM retention semantics.

### 5.6 Compliance and feedback

**`compliance`** is the cross-cutting service for governance. It hosts:

- The **audit log writer** — every protected action across the platform writes to `audit.events.v1`, which is consumed by this service into an append-only Postgres table with monthly Iceberg archive.
- The **purpose limitation enforcer** — a sidecar that intercepts queries to PII-bearing tables and verifies the `purpose=fraud_prevention` claim is present, valid, and unexpired. Queries without it fail. This is the engineered version of the policy commitment.
- The **regulator export builder** — generates structured submission packs for NCA, DPC, BoG, CSA on schedule and on demand.

**`feedback`** consumes confirmed labels (from MoMo reversals, from `tier3` takedown closures, from customer reports marked verified) and triggers retraining. Three model families have separate retraining cadences: behavioural (weekly), content (daily for blacklist deltas, monthly for the model), graph (monthly with continuous online updates to community detection).

---

## 6. Data plane

### 6.1 Postgres schema (key tables)

Use lowercase `snake_case` for all tables and columns. UUIDs (v7, time-ordered) for all primary keys. Every table has `created_at`, `updated_at` (TIMESTAMPTZ), and where appropriate a `version` column for optimistic concurrency.

```sql
-- Core entities (partial)
CREATE TABLE numbers (
  id            UUID PRIMARY KEY,
  msisdn        TEXT UNIQUE NOT NULL,             -- E.164 format
  imsi          TEXT,
  imei_history  TEXT[] DEFAULT '{}',
  registered_at TIMESTAMPTZ,
  status        TEXT NOT NULL DEFAULT 'active',
  risk_score    NUMERIC(4,3),                     -- last computed score
  risk_score_at TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE wallets (
  id              UUID PRIMARY KEY,
  wallet_id       TEXT UNIQUE NOT NULL,
  msisdn_id       UUID REFERENCES numbers(id),
  kyc_status      TEXT,
  risk_score      NUMERIC(4,3),
  risk_score_at   TIMESTAMPTZ,
  -- ...
);

CREATE TABLE alerts (
  id              UUID PRIMARY KEY,
  type            TEXT NOT NULL,                  -- voice | sms | momo | ott
  severity        TEXT NOT NULL,                  -- critical | high | medium | low
  subject_kind    TEXT NOT NULL,                  -- number | wallet | device
  subject_id      UUID NOT NULL,
  score           NUMERIC(4,3) NOT NULL,
  ring_id         UUID REFERENCES rings(id),
  status          TEXT NOT NULL DEFAULT 'new',    -- new | claimed | reviewing | closed | fp
  assignee_id     UUID REFERENCES users(id),
  closed_at       TIMESTAMPTZ,
  closed_reason   TEXT,
  details         JSONB NOT NULL,                 -- model attribution, evidence pointers
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX alerts_status_severity_created_idx ON alerts (status, severity, created_at DESC);
CREATE INDEX alerts_assignee_status_idx        ON alerts (assignee_id, status) WHERE status IN ('claimed', 'reviewing');

CREATE TABLE rings (
  id              UUID PRIMARY KEY,
  type            TEXT NOT NULL,                  -- voice_scam | smishing | mule | mixed
  status          TEXT NOT NULL DEFAULT 'monitoring', -- monitoring | takedown | dismantled | dismissed
  composite_score NUMERIC(4,3),
  active_since    TIMESTAMPTZ NOT NULL,
  last_activity   TIMESTAMPTZ NOT NULL,
  member_count    INT NOT NULL DEFAULT 0,
  metadata        JSONB NOT NULL DEFAULT '{}',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ring_members (
  ring_id         UUID NOT NULL REFERENCES rings(id) ON DELETE CASCADE,
  member_kind     TEXT NOT NULL,                  -- number | wallet | device
  member_id       UUID NOT NULL,
  role            TEXT,                           -- originator | mule | recipient | coordinator
  confidence      NUMERIC(4,3),
  first_seen      TIMESTAMPTZ,
  last_seen       TIMESTAMPTZ,
  PRIMARY KEY (ring_id, member_kind, member_id)
);

CREATE TABLE takedowns (
  id              UUID PRIMARY KEY,
  ring_id         UUID NOT NULL REFERENCES rings(id),
  status          TEXT NOT NULL DEFAULT 'drafted',-- drafted | approved | filed | acknowledged | executed | closed
  filed_with      TEXT,                           -- nca | police | bog | dpc
  filed_at        TIMESTAMPTZ,
  evidence_hash   TEXT,
  metadata        JSONB NOT NULL DEFAULT '{}',
  created_by      UUID NOT NULL REFERENCES users(id),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE audit_events (
  id              UUID PRIMARY KEY,
  actor_id        UUID,
  actor_kind      TEXT NOT NULL,                  -- user | service | system
  action          TEXT NOT NULL,                  -- e.g., 'alerts.claim'
  resource_kind   TEXT NOT NULL,
  resource_id     UUID,
  purpose         TEXT NOT NULL,                  -- fraud_prevention | regulatory_export | audit
  request_id      TEXT,
  metadata        JSONB NOT NULL DEFAULT '{}',
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- audit_events is partitioned monthly, archived to Iceberg after 6 months
```

Multi-tenant isolation for B2B (Phase 4) uses Postgres row-level security:

```sql
ALTER TABLE enterprise_campaigns ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON enterprise_campaigns
  USING (tenant_id = current_setting('fraudnet.tenant_id')::UUID);
```

Every API connection sets `fraudnet.tenant_id` from the authenticated tenant claim before any query.

### 6.2 Memgraph schema

The graph is the integrating substrate. Nodes and edges are typed; properties are minimal (high-cardinality data lives in Postgres, the graph holds structure).

**Node types**

```cypher
(:Number   {msisdn: STRING, risk_score: FLOAT, created_at: DATETIME})
(:Wallet   {wallet_id: STRING, risk_score: FLOAT, kyc_status: STRING})
(:Device   {imei: STRING, first_seen: DATETIME})
(:Account  {bank: STRING, account_hash: STRING})  // hashed bank account, not plaintext
(:Ring     {ring_id: STRING, type: STRING, status: STRING})
```

**Edge types**

```cypher
(:Number)-[:CALLED   {ts: DATETIME, duration: INT}]->(:Number)
(:Number)-[:SMSED    {ts: DATETIME, template_hash: STRING}]->(:Number)
(:Wallet)-[:SENT     {ts: DATETIME, amount: FLOAT}]->(:Wallet)
(:Number)-[:OWNS]    ->(:Wallet)
(:Number)-[:USED     {since: DATETIME}]->(:Device)
(:Wallet)-[:CASHED_OUT_TO {ts: DATETIME, amount: FLOAT}]->(:Account)
(:Number)-[:MEMBER_OF {role: STRING, confidence: FLOAT}]->(:Ring)
(:Wallet)-[:MEMBER_OF {role: STRING, confidence: FLOAT}]->(:Ring)
```

**Indexes** are mandatory on `Number.msisdn`, `Wallet.wallet_id`, `Device.imei`, `Ring.ring_id` and on edge `ts` properties for time-windowed queries.

**Pattern: ring identification (Cypher)**

```cypher
// A coordinated voice → SMS → MoMo cash-out pattern
MATCH (a:Number)-[c:CALLED]->(b:Number)
MATCH (a)-[s:SMSED]->(b) WHERE s.ts > c.ts AND s.ts < c.ts + duration('PT1H')
MATCH (b)-[:OWNS]->(w:Wallet)-[t:SENT]->(:Wallet)-[:CASHED_OUT_TO]->()
WHERE t.ts > s.ts AND t.ts < s.ts + duration('PT24H')
RETURN a, b, w, count(*) AS pattern_count
ORDER BY pattern_count DESC
LIMIT 100
```

This pattern — voice contact, then SMS lure, then MoMo extraction within a 24-hour window — is the fingerprint of the threat profile that defines the moat. It is invisible to a signal-only architecture.

### 6.3 Kafka topics

Versioned by suffix (`.v1`); breaking schema changes require a new topic and a dual-publish migration.

| Topic | Producer | Consumer(s) | Retention | Partitions |
|---|---|---|---|---|
| `voice.events.v1` | `ingest-voice` | `stream-features`, `stream-graph` | 7d | 100 |
| `sms.events.v1` | `ingest-sms` | `stream-features`, `stream-graph`, `brain-content` | 7d | 50 |
| `data.events.v1` | `ingest-data` | `stream-features` | 3d | 50 |
| `momo.events.v1` | `ingest-momo` | `stream-features`, `stream-graph` | 30d | 50 |
| `intel.events.v1` | `ingest-intel` | `stream-graph`, `decisions` | 90d | 10 |
| `graph.mutations.v1` | `stream-graph` | `decisions`, `brain-graph`, `api-noc` | 30d | 50 |
| `motifs.detected.v1` | `brain-graph` | `decisions` | 90d | 20 |
| `decisions.dispatched.v1` | `decisions` | `action-tier{1,2,3}` | 30d | 50 |
| `actions.taken.v1` | `action-tier*` | `feedback`, `compliance` | 90d | 20 |
| `audit.events.v1` | all | `compliance` | 30d | 20 |
| `*.dlq.v1` | each ingest | manual replay tooling | 30d | 5 |

Schema registry is mandatory; producers fail-closed if the registry is unreachable.

### 6.4 Aerospike feature schema

Namespace `fraudnet`; sets per entity. Keys are entity IDs. TTL per record matches the longest feature window.

```
Set:     numbers
Key:     msisdn
Bins:
  vel_1m       INT      // calls in last 1 minute
  vel_5m       INT
  vel_1h       INT
  fanout_1h    INT      // unique callees in last hour
  imei_count   INT      // distinct IMEIs seen in last 30d
  geo_entropy  FLOAT
  sms_freq_1h  INT
  smshash_top  STRING   // most-frequent SMS template hash
  last_score   FLOAT
  last_score_at INT
  ttl: 86400  // seconds
```

Read pattern: single-key reads with a 1ms p99 budget. Avoid scans.

### 6.5 Iceberg / lakehouse

Tables under `s3://fraudnet-lake/`:

- `events_voice` — partitioned by date, hour. Source of truth for replay and training.
- `events_sms`, `events_data`, `events_momo` — same structure.
- `features_snapshots` — periodic snapshots of feature-store state for training.
- `audit_archive` — immutable audit log archive (rotated from Postgres monthly).
- `model_predictions` — every score the brain services emit, for offline analysis.

Trino is the query engine. dbt for transformations. Metabase for ad-hoc exploration (no PII columns exposed).

---

## 7. Cross-cutting concerns

### 7.1 Authentication and authorisation

- All service-to-service traffic is mTLS via Istio. Identity is workload identity (SPIFFE), not API keys.
- All user-facing API traffic uses JWT bearer tokens issued by Keycloak. Tokens are short-lived (5 min); refresh via the gateway.
- RBAC is enforced in `auth-lib` at the route level using a decorator: `@require_role('FRAUD_LEAD')`. Tenant scoping is enforced at the data layer (RLS).
- Step-up auth for high-risk operations uses a separate short-lived token obtained via WebAuthn / second factor. Required for: model promotion, user role changes, data export, takedown filing.

### 7.2 Purpose limitation enforcement

This is the engineered version of the regulatory commitment and is non-negotiable. Implementation:

- Every database connection from a service carries a session GUC `fraudnet.purpose`. Services set this from their declared purpose at connection-init time.
- PII-bearing tables have row-level security policies that require `current_setting('fraudnet.purpose') IN (allowed_purposes)`. A service that has declared purpose `fraud_prevention` cannot read tables marked as `purpose=marketing_only`.
- The `compliance` service's purpose-limitation sidecar audits every cross-purpose read; an unexpected access pattern triggers an immediate alert and a forced re-authentication for the originating actor.
- Code review for any change touching PII tables requires sign-off from the DPO liaison.

### 7.3 Audit logging

Every protected action — claiming an alert, viewing a customer profile, promoting a model, filing a takedown, exporting data — writes an audit event via `audit-lib.record(action, resource, purpose, metadata)`. Audit events flow through Kafka to Postgres and are archived to Iceberg with WORM semantics. The audit log is the single source of truth for regulator inquiries.

### 7.4 Observability

Three-pillar observability is mandatory.

- **Metrics** via OpenTelemetry; conventions in `packages/obs`. Every service exposes RED metrics (Rate, Errors, Duration) per endpoint and USE metrics (Utilization, Saturation, Errors) per resource. Custom metrics for business KPIs (alerts/min, ring-confirmation rate, MoMo loss prevented).
- **Tracing** via OpenTelemetry; sampled at 1% in production, 100% on error. Every request is traced from `api-public` through to all downstream calls.
- **Logs** are structured JSON via `obs.log()`. PII is redacted at the logging layer; the `redact()` function is automatic for known field names (msisdn, imei, wallet_id, etc.) and explicit elsewhere. Log levels: DEBUG (dev only), INFO (default), WARN, ERROR. ERRORs page on-call.

Dashboards are version-controlled JSON in `infra/grafana/`. SLOs per service are documented in `docs/runbooks/{service}.md`.

### 7.5 Data residency

All production data — Kafka topics, Postgres, Memgraph, Aerospike, Iceberg — resides within Ghana. The pan-African federation (Phase 4) operates on a federated-learning model: model updates and ring signatures travel between opcos; raw data does not. This is the architectural enforcement of the Data Protection Bill.

### 7.6 Security

- Container images built from distroless or Alpine bases; no shell in production images.
- All secrets via Vault — no environment variables for sensitive values.
- Network policies (Calico / Cilium) restrict egress per service.
- Quarterly penetration testing. Annual independent security review tied to NCA / CSA reporting requirements.
- Dependency scanning in CI (Snyk / Trivy).

---

## 8. Local development

### 8.1 Setup

```bash
git clone git@github.com:mtn-ghana/fraudnet-backend.git
cd fraudnet-backend
make bootstrap                # Installs Python venv, Node tooling, pre-commit hooks
docker compose up -d kafka postgres memgraph aerospike minio
make seed                     # Populates sample data
make dev                      # Runs all services in dev mode
```

The `docker-compose.dev.yml` brings up the full data plane locally with sample data. Services run via `uvicorn --reload`. Flink jobs run in mini-cluster mode for development.

### 8.2 Working on a single service

```bash
make dev SERVICE=api-noc      # Runs just that service against shared infra
```

Each service has its own `pyproject.toml` and can be run independently. Use `pytest -k <test>` for fast iteration.

### 8.3 Running tests

- Unit tests: `make test-unit` — run per-service in isolation, fast (<30s total).
- Integration tests: `make test-integration` — bring up a per-service test Docker compose, run against real Kafka/Postgres/Memgraph in test mode.
- Contract tests: `make test-contracts` — verify cross-service Avro / OpenAPI compatibility.
- Load tests: `make load-gen` — synthetic event generator for capacity planning.

CI runs unit + integration + contract on every PR. Load and security scans run nightly.

### 8.4 Synthetic load generator

`tools/load-gen` produces realistic event streams against any environment. Configurable RPS per topic, ring-injection patterns (it can inject a known fraud ring with controllable parameters for end-to-end testing), and replay from Iceberg. Use this for any change that touches the inline path.

---

## 9. Deployment

### 9.1 Environments

- `dev` — engineer laptops + an always-on dev cluster for shared services.
- `staging` — full cluster with anonymised production data subset; mirrors production topology.
- `prod` — production. Single region in Ghana for Phase 1; federated multi-region for Phase 4.

### 9.2 Promotion flow

GitOps via ArgoCD. The deployment pipeline:

1. PR merged → CI builds container image, pushes to registry, generates manifest changes in `infra/k8s/`.
2. ArgoCD auto-syncs to `staging`.
3. After staging soak (24h or on-demand promotion), a release-bot opens a PR to bump the `prod` overlay.
4. Manual approval (reviewer + on-call) merges the PR; ArgoCD syncs to `prod`.
5. Canary rollout via Argo Rollouts: 5% → 25% → 50% → 100%, with automated rollback on metric regression.

Inline-tier services (action-tier1) have an additional gate: a 30-minute soak at 5% traffic before any wider rollout.

### 9.3 Rollback

- Application-level: `argocd app rollback {app} {revision}`.
- Database migrations: every migration is paired with a rollback script. Application code must tolerate both the pre- and post-migration schema for one release cycle.
- Models: champion/challenger framework supports instant rollback via `POST /admin/models/{id}/rollback`.

### 9.4 Disaster recovery

- Kafka: cross-DC replication via MirrorMaker 2 to a warm secondary cluster.
- Postgres: streaming replication + point-in-time recovery (PITR) with 7-day window.
- Memgraph: snapshots every 6h, replication to standby.
- Aerospike: cross-DC replication for the inline tier (Phase 2 onward).
- RTO: 1 hour for Tier 1 services; 4 hours for the rest. RPO: 15 minutes.

---

## 10. Conventions

### 10.1 Python style

- `ruff` for linting + formatting (replaces black, isort, flake8).
- `mypy` strict mode for type checking. Public APIs are fully typed.
- Pydantic for I/O validation; dataclasses for internal value objects.
- FastAPI with explicit dependency injection — no globals, no module-level singletons.
- Async by default for I/O paths; `asyncio.TaskGroup` for fan-out.

### 10.2 Repo conventions

- One PR = one logical change. Commits are `type(scope): summary` (Conventional Commits).
- Every PR includes: tests, docs delta if applicable, ADR if architectural.
- Code review by at least two engineers, one of whom is in the affected service team.
- ADRs in `docs/adrs/` — numbered, immutable once merged. Significant decisions get ADRs.

### 10.3 Error handling

- Every service exposes errors via the standard error envelope (defined in `packages/schemas`):
  ```json
  { "error": { "code": "alert_not_found", "message": "...", "details": {...} },
    "request_id": "..." }
  ```
- HTTP codes are semantic; 4xx for client errors, 5xx only for genuinely server-side failures.
- Internal exceptions use a typed hierarchy in `packages/schemas/errors.py`; do not raise bare `Exception`.

### 10.4 Logging conventions

- Structured JSON. Required fields: `timestamp`, `level`, `service`, `request_id`, `message`. Optional: `actor_id`, `resource_id`, `duration_ms`.
- Never log raw PII. Use `redact()` from `obs`. CI has a linter rule that fails the build on suspicious logging patterns.
- One log line per externally-observable event. Avoid intermediate progress logs in hot paths.

### 10.5 Testing conventions

- Test files alongside source (`foo.py` → `foo_test.py`).
- Factories (in `packages/testing`) for all domain objects. Factories use realistic Ghanaian data.
- No test depends on another test's state. Each test sets up and tears down its own fixtures.
- Integration tests use real services (Kafka, Postgres, etc.) via Testcontainers, not mocks.

---

## 11. Operational runbook (top-level)

Per-service runbooks live in `docs/runbooks/`. Common patterns:

### 11.1 Alert: ingest-voice consumer lag rising

1. Check probe-vendor health (status page, vendor dashboard).
2. If vendor healthy, scale the service: `kubectl scale deploy ingest-voice --replicas=N`.
3. If lag persists, check Kafka broker health; partition skew is the typical cause.
4. If sustained >30 min, declare incident, page network team.

### 11.2 Alert: brain-graph p99 latency >100ms

1. Check Memgraph cluster health; query latency at the source.
2. Check feature-store availability (Aerospike).
3. Roll back to the previous model version: `make rollback MODEL=brain-graph`.
4. If model is not the cause, scale the service vertically (graph queries are CPU-bound).

### 11.3 Routine: weekly model performance review

Run `make model-report` every Monday. Flags recall regressions, drift, false-positive spikes. Threshold breaches open Jira tickets to the data science team.

### 11.4 Routine: monthly auto-untag review

Per Airtel-style pattern, the monthly auto-untag job (a Flink job in `feedback`) reviews all numbers tagged Suspected SPAM in the prior month and lifts the tag for those with no further qualifying signals. Output is reviewed by the FRAUD_LEAD before commit.

---

## 12. Key implementation gotchas

Things that have already cost time and will cost more if forgotten.

- **Schema evolution.** Avro schema changes are insidious. Always add fields with defaults. Never reorder. Never repurpose. New required fields require a topic version bump.
- **Aerospike and Postgres are not transactional together.** Inline scoring reads from Aerospike; alert persistence writes to Postgres. The two can drift briefly. Reconciliation jobs in `compliance` close the gap.
- **Memgraph is in-memory.** Restart loses transient state; rely on the lakehouse + replay tooling for recovery, not on Memgraph durability alone. Snapshots are on a 6-hour cadence; events between snapshots are replayed from Kafka.
- **Kafka exactly-once is not free.** Use it on the critical paths (decisions, action-tier1) and accept at-least-once with downstream dedup elsewhere.
- **Probe vendor APIs are flaky.** Build retries, dead letters, and a manual replay path from day one. Vendors will fail; the platform must not.
- **Time zones bite.** Everything internal is UTC. Display layer formats. Tests use frozen clocks.
- **PII in error messages.** Easy to leak. Sanitise in the error envelope at the gateway.
- **Multi-tenant boundary in graph queries.** Memgraph does not have row-level security. Tenant boundaries in B2B graph queries are enforced in the API layer; do not query the graph directly from `api-enterprise` without going through the tenant-scoping wrapper in `graph-client`.
- **Stream-features and stream-graph share inputs but have different time semantics.** Features are window-aggregated (event-time, with a 30-second lateness allowance). Graph is per-event. A bug in either job's watermarking corrupts everything downstream. Watermarks are sacred.
- **Costs.** Aerospike and Memgraph are the biggest line items. Right-size early and re-evaluate quarterly. Iceberg storage grows fast; archival policy from day one.

---

## 13. Phased build sequencing

This document covers the steady-state architecture. The build sequences as follows.

**Phase 1 (months 0–6):** All ingestion services scaffolded; `ingest-voice`, `ingest-sms`, `ingest-momo` production-ready. `stream-features` and `stream-graph` running. `brain-behavioural` and `brain-content` serving. `decisions` + `action-tier1` (VoLTE tag, URL block) and `action-tier2` (customer alerts) on the inline + near-real-time paths. `api-noc` and `api-customer` shipping. `compliance` operational. No `brain-graph` yet (graph runs only as substrate for queries; no GNN scoring).

**Phase 2 (months 6–12):** `brain-graph` ships. Community detection, motif detection, GNN scoring all live. `api-noc` ring detail and graph view become first-class. The fusion proposition becomes real.

**Phase 3 (months 12–18):** `ingest-data` (DNS/IPDR) production-ready. OTT URL blocking actuator. Smishing pattern detection in `brain-content` extended. Customer self-service surface adds OTT scope.

**Phase 4 (months 18–24):** `api-enterprise` and the B2B portal scope. Federated graph across MTN Group opcos. Group-level platform service.

---

## 14. Reading order for new engineers

1. This document end-to-end.
2. The strategy and architecture documents (`Strategy_and_Build_Plan.md`, the reference architecture diagram).
3. `docs/adrs/0001-monorepo-with-microservices.md` and the rest of the ADR sequence.
4. The service-specific READMEs for whichever service you're working on.
5. The relevant runbook.
6. The data contracts in `docs/data-contracts/`.

If you are looking at this and any of these documents is missing or stale, that is a defect. File it and either fix it yourself or assign it to the service team.

---

**End of backend specification.**
