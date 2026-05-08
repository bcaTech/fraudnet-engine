# FraudNet Intelligence Engine — Claude Code Mega-Prompt

## Project Overview
Build the complete backend for **FraudNet**, an AI-native fraud network intelligence platform for mobile money. This engine ingests device, SIM, wallet, and transaction data, constructs a graph of interconnected fraud identities, applies AI/ML analytics to detect fraud clusters, scores nodes for confidence, and exposes a REST + WebSocket API consumed by the FraudNet NOC Dashboard (a separate React frontend).

The platform operates in the MoMo (Mobile Money) ecosystem in Ghana, in partnership with Scancom (the telco), which provides IMEI, IMSI, and cell tower data. The architecture is designed for cross-industry evolution: it can operate as a single-operator tool, integrate with peer platforms from other mobile money operators, serve as a shared industry intelligence hub, and provide secure interfaces for law enforcement collaboration.

## Tech Stack
- **Language:** Python 3.11+
- **Web Framework:** FastAPI (REST API + WebSocket support)
- **Graph Database:** Neo4j (via neo4j Python driver)
- **Message Queue:** Apache Kafka (via confluent-kafka or aiokafka)
- **Task Queue:** Celery + Redis (for async ML jobs and batch processing)
- **ML/AI:** PyTorch Geometric (Graph Neural Networks), scikit-learn, NetworkX (graph algorithms)
- **Cache:** Redis (for session state, real-time counters, alert deduplication)
- **Relational DB:** PostgreSQL (via SQLAlchemy + Alembic) for audit logs, user management, configuration, evidence packages
- **Object Storage:** MinIO or S3-compatible (for evidence package PDFs and exports)
- **Containerization:** Docker + Docker Compose for local dev; Kubernetes-ready Dockerfiles
- **Testing:** pytest + pytest-asyncio

## Project Structure

```
fraudnet-engine/
├── docker-compose.yml          # Neo4j, Kafka, Redis, PostgreSQL, MinIO, API
├── Dockerfile
├── pyproject.toml
├── alembic/                    # Database migrations
├── config/
│   ├── settings.py             # Pydantic settings (env-based config)
│   ├── constants.py            # Thresholds, decay rates, scoring weights
│   └── logging.py
├── api/
│   ├── main.py                 # FastAPI app factory
│   ├── auth/
│   │   ├── models.py           # User, Role, Session
│   │   ├── jwt.py              # JWT token handling
│   │   ├── rbac.py             # Role-based access control decorators
│   │   └── routes.py           # /auth/login, /auth/me, /auth/users
│   ├── routes/
│   │   ├── dashboard.py        # /api/dashboard — NOC overview metrics
│   │   ├── clusters.py         # /api/clusters — CRUD, detail, graph data
│   │   ├── nodes.py            # /api/nodes — search, detail, actions (freeze, flag)
│   │   ├── agents.py           # /api/agents — list, detail, risk scores, map data
│   │   ├── alerts.py           # /api/alerts — list, acknowledge, filter
│   │   ├── takedowns.py        # /api/takedowns — initiate, status, execute steps
│   │   ├── analytics.py        # /api/analytics — KPIs, time series, distributions
│   │   ├── campaigns.py        # /api/campaigns — detected campaigns, timeline
│   │   ├── crossnetwork.py     # /api/cross-network — fund flow analysis
│   │   └── config.py           # /api/config — threshold management
│   ├── websocket/
│   │   └── feeds.py            # WebSocket: /ws/alerts, /ws/cluster-updates, /ws/metrics
│   └── middleware/
│       ├── audit.py            # Immutable audit logging for all actions
│       └── ratelimit.py
├── core/
│   ├── graph/
│   │   ├── client.py           # Neo4j connection pool and query helpers
│   │   ├── models.py           # Node and Edge dataclasses/Pydantic models
│   │   ├── queries.py          # Cypher query library (parameterized)
│   │   └── schema.py           # Graph schema initialization (constraints, indexes)
│   ├── mesh/
│   │   ├── seed.py             # Seed identification from fraud events
│   │   ├── expansion.py        # Breadth-first graph expansion algorithm
│   │   ├── scoring.py          # Node and edge confidence scoring
│   │   ├── decay.py            # Temporal decay engine
│   │   ├── clustering.py       # Cluster formation, merging, dissolution
│   │   └── maintenance.py      # Continuous mesh maintenance (prune, re-score)
│   ├── analytics/
│   │   ├── community.py        # Community detection (Louvain + Label Propagation)
│   │   ├── centrality.py       # Betweenness, degree, eigenvector, PageRank
│   │   ├── anomaly.py          # Device, transaction, temporal, velocity anomalies
│   │   ├── campaign.py         # Campaign pattern detection
│   │   ├── sleeper.py          # Sleeper wallet detection
│   │   └── fund_flow.py        # Fund flow tracing and Sankey data generation
│   ├── ml/
│   │   ├── gnn_model.py        # Graph Neural Network (PyTorch Geometric)
│   │   ├── training.py         # Model training pipeline
│   │   ├── inference.py        # Real-time and batch inference
│   │   ├── features.py         # Feature extraction from graph
│   │   └── evaluation.py       # Precision/recall/backtesting
│   ├── agents/
│   │   ├── scoring.py          # Agent risk scoring model
│   │   ├── classification.py   # Complicit/exploited/incidental classification
│   │   └── geographic.py       # Area-adjusted baseline calculation
│   ├── evidence/
│   │   ├── builder.py          # Evidence package assembly
│   │   ├── fund_trace.py       # Victim-to-cashout fund tracing
│   │   ├── timeline.py         # Event timeline generation
│   │   └── export.py           # PDF generation for law enforcement
│   └── takedown/
│       ├── readiness.py        # Pre-takedown readiness assessment
│       ├── executor.py         # Coordinated takedown execution engine
│       ├── wallet_freeze.py    # MoMo API integration for wallet freeze
│       ├── sim_flag.py         # Scancom API integration for SIM/IMEI flagging
│       ├── agent_alert.py      # Agent notification system
│       └── restitution.py      # Victim restitution tracking
├── rules/
│   ├── engine.py               # Core rules evaluation engine
│   ├── models.py               # Rule, Condition, Action Pydantic models
│   ├── parser.py               # Condition expression parser (nested AND/OR groups)
│   ├── evaluator.py            # Real-time event evaluation against active rules
│   ├── scheduler.py            # Scheduled rule evaluation (batch mode)
│   ├── actions/
│   │   ├── registry.py         # Action type registry and dispatcher
│   │   ├── wallet_actions.py   # Freeze, limit, restrict, force KYC, warn
│   │   ├── agent_actions.py    # Suspend, downgrade float, warn
│   │   ├── safeguard_actions.py # Apply Send with Care, Ask Me First
│   │   ├── escalation_actions.py # Escalate, watchlist, flag for law enforcement
│   │   ├── network_actions.py  # Block cross-network, notify external operator
│   │   └── webhook_actions.py  # Custom webhook for external integrations
│   ├── backtest.py             # Run rule against historical data
│   ├── shadow.py               # Shadow mode logging (evaluate but don't execute)
│   ├── lifecycle.py            # Rule state machine (draft→backtest→shadow→live→paused→retired)
│   └── templates.py            # Pre-built rule templates
├── law_enforcement/
│   ├── cases.py                # Case CRUD and lifecycle management
│   ├── evidence.py             # Evidence package management for cases
│   ├── messaging.py            # Secure message thread per case
│   ├── inbound_intel.py        # Receive intelligence from law enforcement → seed pipeline
│   ├── portal_auth.py          # Separate auth flow for law enforcement users
│   └── outcome_tracking.py     # Arrests, prosecutions, convictions, recovered funds
├── integration/
│   ├── operator_registry.py    # Connected operator management
│   ├── api_gateway.py          # External API gateway (inbound from other operators)
│   ├── outbound_sharing.py     # Push flags and intelligence to connected operators
│   ├── inbound_processing.py   # Receive and process flags from other operators
│   ├── identifier_masking.py   # Configurable masking rules (full vs hashed vs partial)
│   ├── vocabulary_mapping.py   # Map between operator-specific fraud categories
│   ├── onboarding.py           # Operator onboarding workflow
│   ├── health_monitoring.py    # Monitor external API connections
│   └── telecoms_chamber.py     # Telecoms Chamber registry integration
├── ingestion/
│   ├── kafka_consumers/
│   │   ├── transaction_consumer.py   # Real-time transaction events
│   │   ├── safeguard_consumer.py     # SafeGuard feature events
│   │   ├── auth_consumer.py          # Authentication and session events
│   │   └── scancom_consumer.py       # SIM swap, device events
│   ├── batch/
│   │   ├── scancom_import.py         # Bulk IMEI/IMSI/cell tower import
│   │   ├── historical_load.py        # Historical transaction data load
│   │   └── registry_sync.py          # Wallet and agent registry sync
│   └── enrichment/
│       ├── identity_resolver.py      # MSISDN → IMSI → IMEI → Wallet resolution
│       ├── geo_enrichment.py         # Cell tower → geographic coordinates
│       └── event_enrichment.py       # Enrich raw events with graph context
├── tasks/
│   ├── celery_app.py                 # Celery configuration
│   ├── periodic.py                   # Scheduled tasks (decay, maintenance, batch scoring)
│   ├── mesh_tasks.py                 # Async mesh expansion and re-scoring
│   ├── ml_tasks.py                   # Model training and batch inference
│   └── report_tasks.py              # Scheduled analytics and report generation
├── db/
│   ├── models.py                     # SQLAlchemy models (see PostgreSQL Models below)
│   ├── crud.py                       # Database CRUD operations
│   └── session.py                    # Database session management
├── tests/
│   ├── conftest.py                   # Fixtures (test Neo4j, test Kafka, sample data)
│   ├── test_mesh/
│   │   ├── test_expansion.py
│   │   ├── test_scoring.py
│   │   └── test_decay.py
│   ├── test_analytics/
│   │   ├── test_community.py
│   │   ├── test_centrality.py
│   │   └── test_anomaly.py
│   ├── test_api/
│   │   ├── test_clusters.py
│   │   ├── test_takedowns.py
│   │   └── test_auth.py
│   └── test_ingestion/
│       └── test_enrichment.py
└── scripts/
    ├── seed_demo_data.py             # Generate realistic demo data in Neo4j
    ├── train_gnn.py                  # Standalone GNN training script
    └── benchmark_queries.py          # Graph query performance benchmarks
```

## Neo4j Graph Schema

### Node Labels and Properties

```cypher
// Constraints and indexes
CREATE CONSTRAINT wallet_id IF NOT EXISTS FOR (w:Wallet) REQUIRE w.wallet_id IS UNIQUE;
CREATE CONSTRAINT msisdn IF NOT EXISTS FOR (p:PhoneNumber) REQUIRE p.msisdn IS UNIQUE;
CREATE CONSTRAINT imei IF NOT EXISTS FOR (h:Handset) REQUIRE h.imei IS UNIQUE;
CREATE CONSTRAINT imsi IF NOT EXISTS FOR (s:SIM) REQUIRE s.imsi IS UNIQUE;
CREATE CONSTRAINT agent_id IF NOT EXISTS FOR (a:Agent) REQUIRE a.agent_id IS UNIQUE;
CREATE CONSTRAINT tx_id IF NOT EXISTS FOR (t:Transaction) REQUIRE t.tx_id IS UNIQUE;

CREATE INDEX wallet_risk IF NOT EXISTS FOR (w:Wallet) ON (w.risk_score);
CREATE INDEX wallet_cluster IF NOT EXISTS FOR (w:Wallet) ON (w.cluster_id);
CREATE INDEX wallet_status IF NOT EXISTS FOR (w:Wallet) ON (w.status);
CREATE INDEX handset_flagged IF NOT EXISTS FOR (h:Handset) ON (h.flagged);
CREATE INDEX agent_risk IF NOT EXISTS FOR (a:Agent) ON (a.risk_score);

// Node: Wallet
// Properties: wallet_id, msisdn, name, kyc_tier, creation_date, balance,
//             status, risk_score, cluster_id, confidence_score, behavioral_score,
//             predictive_score, is_sleeper, last_activity, freeze_date

// Node: Handset
// Properties: imei, make, model, first_seen, last_seen, sim_count,
//             flagged, flag_reason, flag_date

// Node: SIM
// Properties: imsi, registration_date, msisdn, status, swap_count,
//             last_swap_date, flagged

// Node: PhoneNumber
// Properties: msisdn, registration_status, kyc_tier, account_age

// Node: Agent
// Properties: agent_id, name, lat, lng, area_name, registration_date,
//             risk_score, classification, monthly_volume, fraud_cashout_rate,
//             float_avg, suspended, suspension_date

// Node: Transaction
// Properties: tx_id, type, amount, timestamp, status, flagged, flag_reason

// Node: CellTower
// Properties: cell_id, lac, lat, lng, coverage_radius_m

// Node: Cluster
// Properties: cluster_id, name, seed_type, seed_date, seed_node_id,
//             node_count, confidence_score, status, estimated_fraud_value,
//             density, isolation_score
```

### Relationship Types

```cypher
// Device-identity links
(:SIM)-[:INSERTED_IN {strength: float, first_seen: datetime, last_seen: datetime, duration_days: int}]->(:Handset)
(:SIM)-[:HAS_NUMBER {active: boolean, start_date: datetime, end_date: datetime}]->(:PhoneNumber)
(:PhoneNumber)-[:OWNS_WALLET {registration_date: datetime, kyc_verified: boolean}]->(:Wallet)

// Transaction links
(:Wallet)-[:SENT_TO {tx_id: string, amount: float, timestamp: datetime, type: string, strength: float}]->(:Wallet)
(:Wallet)-[:CASHED_OUT_AT {tx_id: string, amount: float, timestamp: datetime, strength: float}]->(:Agent)
(:Wallet)-[:CASHED_IN_AT {tx_id: string, amount: float, timestamp: datetime}]->(:Agent)

// Geographic links
(:Handset)-[:CONNECTED_TO {timestamp: datetime, duration_s: int}]->(:CellTower)
(:Handset)-[:CO_LOCATED_WITH {tower_id: string, timestamp: datetime, window_minutes: int, strength: float}]->(:Handset)

// Cluster membership
(:Wallet)-[:BELONGS_TO {confidence: float, joined_date: datetime, role: string}]->(:Cluster)
(:Handset)-[:BELONGS_TO]->(:Cluster)
(:SIM)-[:BELONGS_TO]->(:Cluster)
(:Agent)-[:LINKED_TO {fraud_cashout_count: int, strength: float}]->(:Cluster)
```

## API Specification

### Authentication
```
POST   /auth/login          — {username, password} → {access_token, role}
GET    /auth/me              — Current user profile
POST   /auth/users           — Create user (Admin only)
```

### Dashboard
```
GET    /api/dashboard/metrics              — KPI summary (active clusters, wallets under review, fraud value, takedowns)
GET    /api/dashboard/alert-feed           — Recent alerts (paginated, filterable by severity)
GET    /api/dashboard/cluster-overview     — Mini graph data for all active clusters
GET    /api/dashboard/activity-timeline    — 24h transaction volume + fraud overlay
GET    /api/dashboard/recent-takedowns     — Last 10 takedowns with status
```

### Clusters
```
GET    /api/clusters                       — List clusters (filterable by status, confidence range, date range)
GET    /api/clusters/:id                   — Cluster detail (metadata + summary stats)
GET    /api/clusters/:id/graph             — Full graph data (nodes + edges) for visualization
GET    /api/clusters/:id/evidence          — Evidence chain timeline
GET    /api/clusters/:id/fund-flow         — Sankey-format fund flow data
GET    /api/clusters/:id/nodes             — Paginated node table
POST   /api/clusters/:id/expand            — Trigger manual mesh expansion from this cluster
```

### Nodes
```
GET    /api/nodes/search                   — Search by MSISDN, IMEI, IMSI, wallet_id, agent_id
GET    /api/nodes/:type/:id                — Node detail (type = wallet|handset|sim|agent)
GET    /api/nodes/:type/:id/connections    — All edges for this node
GET    /api/nodes/:type/:id/timeline       — Event timeline for this node
POST   /api/nodes/wallet/:id/freeze        — Freeze wallet (Investigator+)
POST   /api/nodes/wallet/:id/unfreeze      — Unfreeze wallet (Investigator+)
POST   /api/nodes/:type/:id/flag           — Flag node for review
POST   /api/nodes/:type/:id/watchlist      — Add to watch list
```

### Agents
```
GET    /api/agents                         — List agents (filterable, sortable)
GET    /api/agents/map                     — GeoJSON for agent map with risk coloring
GET    /api/agents/:id                     — Agent detail + risk breakdown
GET    /api/agents/:id/cashout-patterns    — Time-of-day heatmap data
POST   /api/agents/:id/suspend             — Suspend agent (Investigator+)
POST   /api/agents/:id/warn               — Issue warning
```

### Alerts
```
GET    /api/alerts                         — Paginated alert list (filterable by type, severity, acknowledged)
POST   /api/alerts/:id/acknowledge         — Mark alert as acknowledged
POST   /api/alerts/:id/dismiss             — Dismiss alert with reason
GET    /api/alerts/stats                   — Alert volume by type and severity (for charts)
```

### Takedowns
```
GET    /api/takedowns                      — List all takedowns
GET    /api/takedowns/:id                  — Takedown detail with step status
POST   /api/takedowns                      — Initiate takedown {cluster_id} (Investigator+)
GET    /api/takedowns/:id/readiness        — Pre-takedown readiness assessment
POST   /api/takedowns/:id/approve          — Approve takedown (Senior Investigator+)
POST   /api/takedowns/:id/execute          — Execute takedown steps
GET    /api/takedowns/:id/evidence-package — Download evidence package PDF
```

### Analytics
```
GET    /api/analytics/kpis                 — Aggregated KPIs for date range
GET    /api/analytics/clusters-over-time   — Time series: clusters detected
GET    /api/analytics/fraud-value           — Time series: fraud value prevented
GET    /api/analytics/seed-sources         — Distribution of seed types
GET    /api/analytics/model-performance    — Precision, recall, F1 over time
GET    /api/analytics/false-positives      — False positive tracking
GET    /api/analytics/top-nodes            — Most connected / highest risk nodes
```

### Campaigns
```
GET    /api/campaigns                      — List detected campaigns
GET    /api/campaigns/:id                  — Campaign detail with temporal data
GET    /api/campaigns/:id/timeline         — Multi-series time data for visualization
```

### Cross-Network
```
GET    /api/cross-network/fund-flows       — Sankey data: outbound fund flows by destination network
GET    /api/cross-network/blind-spot       — Estimated fraud value exiting network
GET    /api/cross-network/registry-status  — Telecoms Chamber registry status (placeholder)
```

### Configuration
```
GET    /api/config                         — All configurable parameters
PUT    /api/config/:key                    — Update parameter (Admin only)
GET    /api/config/system-health           — Data feed status, DB stats, model status
```

### Rules Engine
```
GET    /api/rules                          — List rules (filterable by status, creator)
POST   /api/rules                          — Create rule (Analyst+)
GET    /api/rules/:id                      — Rule detail with trigger history
PUT    /api/rules/:id                      — Update rule definition (draft/paused only)
POST   /api/rules/:id/backtest             — Run rule against historical data {days: 30|60|90}
POST   /api/rules/:id/shadow               — Activate shadow mode
POST   /api/rules/:id/promote              — Promote to live (Investigator+ approval)
POST   /api/rules/:id/pause                — Pause live rule
POST   /api/rules/:id/retire               — Retire rule permanently
GET    /api/rules/:id/triggers             — Paginated trigger history with outcomes
GET    /api/rules/:id/performance          — Trigger rate, false positive rate, override rate
POST   /api/rules/:id/simulate             — "What-if" simulation with modified conditions
GET    /api/rules/templates                — List pre-built rule templates
POST   /api/rules/from-template/:template_id — Clone template as new draft rule
GET    /api/rules/actions/registry         — List all available action types with parameter schemas
```

### Law Enforcement
```
GET    /api/law-enforcement/cases          — List cases (filterable by agency, status)
POST   /api/law-enforcement/cases          — Create case/referral from cluster (Investigator+)
GET    /api/law-enforcement/cases/:id      — Case detail
PUT    /api/law-enforcement/cases/:id      — Update case status/metadata
GET    /api/law-enforcement/cases/:id/messages — Secure message thread
POST   /api/law-enforcement/cases/:id/messages — Post message to thread
GET    /api/law-enforcement/cases/:id/evidence — List evidence packages for case
POST   /api/law-enforcement/cases/:id/evidence/generate — Generate/regenerate evidence package
GET    /api/law-enforcement/cases/:id/evidence/:pkg_id/download — Download evidence PDF
GET    /api/law-enforcement/cases/:id/outcomes — Outcome tracking (arrests, prosecutions, etc.)
PUT    /api/law-enforcement/cases/:id/outcomes — Update outcomes
GET    /api/law-enforcement/agencies       — List registered agencies
POST   /api/law-enforcement/agencies       — Register agency (Admin only)
POST   /api/law-enforcement/inbound-intel  — Receive intelligence from law enforcement (seeds)
```

### Operator Integration
```
GET    /api/integration/operators           — List connected operators with health status
POST   /api/integration/operators           — Register new operator (Admin, initiates onboarding)
GET    /api/integration/operators/:id       — Operator detail + configuration
PUT    /api/integration/operators/:id/config — Update sharing rules, masking, auto-integration
GET    /api/integration/operators/:id/health — API health metrics
POST   /api/integration/operators/:id/test  — Send test flag for integration verification

GET    /api/integration/shared/outbound     — Flags and intelligence shared with operators
GET    /api/integration/shared/inbound      — Flags and intelligence received from operators
POST   /api/integration/shared/inbound/:id/action — Accept/dismiss/integrate inbound flag

# External API (consumed by OTHER operators connecting to FraudNet)
POST   /api/external/v1/flags              — Receive flag from external operator (API-key auth)
GET    /api/external/v1/flags/query         — Query whether identifier is flagged (API-key auth)
POST   /api/external/v1/intelligence       — Push structured intelligence (API-key auth)
GET    /api/external/v1/health             — Connection health check

GET    /api/integration/chamber/status      — Telecoms Chamber initiative status
GET    /api/integration/chamber/metrics     — FraudNet contribution metrics
```

### WebSocket Feeds
```
WS     /ws/alerts            — Real-time alert stream (includes rule-triggered alerts)
WS     /ws/cluster-updates   — Cluster confidence changes, new nodes, status changes
WS     /ws/metrics           — Live metric updates (every 5s)
WS     /ws/takedown/:id      — Live takedown execution progress
WS     /ws/rules             — Rule trigger events, shadow mode logs, status changes
WS     /ws/integration       — Inbound/outbound flag events, operator health changes
```

## Core Algorithms

### Mesh Expansion (core/mesh/expansion.py)
```python
async def expand_from_seed(
    seed_node_id: str,
    seed_type: str,
    seed_confidence: float,
    max_depth: int = 4,
    expansion_threshold: float = 0.25,
    distance_discount: float = 0.7,
    convergence_bonus: float = 0.15,
    convergence_cap: float = 0.30,
) -> Cluster:
    """
    Breadth-first expansion from a seed node.
    
    1. Start with seed node at given confidence
    2. For each node, retrieve all edges sorted by strength
    3. For each connected node:
       a. Calculate preliminary confidence = parent_confidence * edge_strength * (distance_discount ^ depth)
       b. Check for convergence: count independent paths to other discovered nodes
       c. Add convergence bonus (capped)
       d. If confidence > expansion_threshold, add to cluster and queue for expansion
    4. Continue until depth limit reached or no new qualifying nodes
    5. Calculate cluster metrics (density, isolation, central nodes)
    6. Merge with existing clusters if cross-links detected
    """
```

### Confidence Scoring (core/mesh/scoring.py)
```python
def calculate_node_confidence(
    node_id: str,
    seed_proximity: float,       # Distance-discounted proximity to nearest seed
    edge_strength_sum: float,    # Sum of all edges to cluster members
    convergence_factor: float,   # Number of independent paths to fraud nodes
    behavioral_score: float,     # Transaction pattern analysis score
    predictive_score: float,     # GNN model output
    negative_evidence: float,    # Legitimate history discount
    weights: ScoringWeights,     # Configurable weights
) -> float:
    """
    Weighted combination normalized to [0, 1].
    
    score = (
        weights.seed_proximity * seed_proximity +
        weights.edge_strength * min(edge_strength_sum / weights.edge_norm, 1.0) +
        weights.convergence * min(convergence_factor / weights.conv_norm, 1.0) +
        weights.behavioral * behavioral_score +
        weights.predictive * predictive_score
    ) * (1.0 - weights.negative * negative_evidence)
    
    Return clamped to [0, 1]
    """
```

### Temporal Decay (core/mesh/decay.py)
```python
def apply_decay(strength: float, half_life_days: float, days_elapsed: float) -> float:
    """Exponential decay: strength * e^(-λt) where λ = ln(2) / half_life"""
    import math
    lambda_val = math.log(2) / half_life_days
    return strength * math.exp(-lambda_val * days_elapsed)
```

### Agent Risk Scoring (core/agents/scoring.py)
```python
def calculate_agent_risk(
    fraud_wallet_concentration: float,  # % of cashouts from fraud-linked wallets
    velocity_clustering: float,         # Multiple fraud cashouts in short windows
    amount_pattern_score: float,        # Structuring/round number patterns
    geographic_anomaly: float,          # Distant wallets using this agent
    float_anomaly: float,              # Unusual float preparation patterns
    historical_deviation: float,        # Deviation from own baseline
    area_baseline: float,              # Area-adjusted expected fraud rate
) -> Tuple[float, str]:
    """Returns (risk_score, classification)"""
```

### Rules Engine (rules/engine.py)
```python
class RuleEngine:
    """
    Core rules evaluation engine. Evaluates conditions against node/event
    context and dispatches actions.
    
    Supports two evaluation modes:
    1. Real-time: Called by Kafka consumers when events arrive.
       event_context includes the triggering event + enriched node data.
    2. Scheduled: Called by Celery periodic tasks.
       Evaluates all active scheduled rules against current graph state.
    
    Rule evaluation flow:
    1. Load active rules (cached in Redis, refreshed on rule state change)
    2. For each rule, evaluate condition tree against context
    3. If conditions match:
       a. If rule requires approval → create pending action + alert
       b. If auto-execute → dispatch actions via ActionRegistry
       c. Log trigger event with full context for audit
    4. Track trigger counts for rate limiting and analytics
    """
    
    async def evaluate_event(self, event: Event, context: NodeContext) -> List[RuleMatch]:
        """Evaluate all real-time rules against an incoming event."""
        
    async def evaluate_scheduled(self, rule: Rule) -> List[RuleMatch]:
        """Evaluate a scheduled rule against current graph state."""
        
    async def backtest(self, rule: Rule, days: int) -> BacktestResult:
        """Run rule against historical events. Returns match count,
        affected nodes, estimated false positive rate."""
        
    async def shadow_evaluate(self, event: Event, context: NodeContext) -> List[ShadowLog]:
        """Evaluate shadow-mode rules. Log matches but don't execute."""


class ConditionEvaluator:
    """
    Parses and evaluates nested condition trees.
    
    Condition tree structure:
    {
        "operator": "AND",  // or "OR"
        "conditions": [
            {"field": "node.risk_score", "op": "greater_than", "value": 0.7},
            {"field": "node.cluster_confidence", "op": "greater_than", "value": 0.5},
            {
                "operator": "OR",
                "conditions": [
                    {"field": "alert.severity", "op": "equals", "value": "critical"},
                    {"field": "node.cross_network_transfer_count_24h", "op": "greater_than", "value": 3}
                ]
            }
        ]
    }
    """
    
    def evaluate(self, condition_tree: dict, context: dict) -> bool:
        """Recursively evaluate condition tree against context."""


class ActionRegistry:
    """
    Registry of all available action types. Each action type implements:
    - validate(params): Check parameters are valid
    - execute(target, params): Execute the action
    - rollback(target, params): Reverse the action (where possible)
    - describe(params): Human-readable description for audit log
    
    Action types:
    - freeze_wallet, unfreeze_wallet
    - reduce_transaction_limit, restore_transaction_limit
    - block_cross_network, unblock_cross_network
    - apply_send_with_care, remove_send_with_care
    - apply_ask_me_first, remove_ask_me_first
    - force_kyc_reverification
    - restrict_cashout, unrestrict_cashout
    - downgrade_agent_float, restore_agent_float
    - suspend_agent, unsuspend_agent
    - issue_customer_warning
    - issue_agent_warning
    - escalate_to_investigator
    - add_to_watchlist, remove_from_watchlist
    - flag_for_law_enforcement
    - notify_external_operator
    - custom_webhook
    """
    
    def register(self, action_type: str, handler: ActionHandler): ...
    async def execute(self, action_type: str, target: str, params: dict, rule_id: str, trigger_context: dict): ...
```

### External Integration Protocol (integration/api_gateway.py)
```python
class ExternalAPIGateway:
    """
    Handles inbound API requests from connected operators.
    
    Authentication: API key per operator (stored in PostgreSQL, 
    validated via middleware). Rate-limited per operator.
    
    Endpoints served:
    - POST /api/external/v1/flags
      Receive a fraud flag: {identifier_type, identifier, risk_score, 
      context, source_operator, timestamp}
      Depending on operator config:
        - Auto-create seed in mesh pipeline, OR
        - Queue for analyst review
    
    - GET /api/external/v1/flags/query
      Query: {identifier_type, identifier}
      Returns: {flagged: bool, risk_level: str, flagged_since: datetime}
      Respects masking rules per operator.
    
    - POST /api/external/v1/intelligence
      Receive structured intelligence package:
      {identifiers: [], relationships: [], context: str, confidence: float}
      Creates enriched seed with cross-network provenance.
    
    Data sovereignty: All outbound sharing passes through 
    identifier_masking.py which applies per-operator masking rules 
    before any data leaves the system.
    """
```

## Kafka Topics
```
fraudnet.transactions         — Real-time MoMo transaction events
fraudnet.safeguard.events     — Send with Care, Return to Sender, DIKY, Ask Me First events
fraudnet.auth.events          — Authentication, session, device fingerprint events
fraudnet.scancom.sim-swaps    — SIM swap notifications
fraudnet.scancom.device-events — Device registration, deregistration events
fraudnet.alerts               — Internal: generated alerts for WebSocket broadcast
fraudnet.cluster-updates      — Internal: cluster state changes for WebSocket broadcast
fraudnet.metric-updates       — Internal: metric snapshots for WebSocket broadcast
fraudnet.rules.triggers       — Internal: rule trigger events for audit and analytics
fraudnet.rules.actions        — Internal: action execution events
fraudnet.integration.inbound  — Flags and intelligence received from external operators
fraudnet.integration.outbound — Flags and intelligence to be shared with external operators
fraudnet.law-enforcement      — Case events, evidence generation requests
```

## Celery Periodic Tasks
```python
# Every 60 minutes: Apply temporal decay to all edges, prune weak edges
# Every 6 hours: Batch import Scancom registry data
# Every 6 hours: Re-score all active clusters
# Daily: Retrain anomaly detection baselines
# Weekly: Evaluate GNN model performance, trigger retraining if degraded
# Daily: Generate analytics snapshots for reporting
# Every 30 minutes: Run sleeper wallet detection scan
# Every 15 minutes: Run campaign pattern detection
# Every 5 minutes: Evaluate all scheduled-mode rules against current graph state
# Every 15 minutes: Process inbound integration queue (flags from external operators)
# Every 15 minutes: Process outbound integration queue (share flags with operators)
# Hourly: Rules performance metrics aggregation (trigger counts, false positive rates)
# Daily: Law enforcement case status check and reminder generation
# Every 6 hours: External operator health check and alert on degradation
```

## Docker Compose Services
```yaml
services:
  api:           # FastAPI application (port 8000)
  worker:        # Celery worker
  beat:          # Celery beat scheduler
  neo4j:         # Graph database (ports 7474, 7687)
  kafka:         # Message broker (port 9092)
  zookeeper:     # Kafka dependency
  redis:         # Cache + Celery broker (port 6379)
  postgres:      # Relational DB (port 5432)
  minio:         # Object storage (port 9000)
```

## Environment Variables
```
NEO4J_URI=bolt://neo4j:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=<secret>
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
REDIS_URL=redis://redis:6379/0
DATABASE_URL=postgresql://fraudnet:<secret>@postgres:5432/fraudnet
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=<secret>
MINIO_SECRET_KEY=<secret>
JWT_SECRET=<secret>
JWT_ALGORITHM=HS256
JWT_EXPIRE_MINUTES=480
LOG_LEVEL=INFO
ENVIRONMENT=development
```

## Demo Data Seeding Script (scripts/seed_demo_data.py)
Generate realistic demo data that matches the frontend's synthetic data expectations:
- 500 wallets, 200 handsets, 350 SIMs, 80 agents, 5000 transactions
- 15 clusters with realistic graph structures
- 100 alerts of various types
- 8 takedowns at various stages
- 20 rules: 10 live (mix of templates and custom), 3 in shadow mode, 3 in backtest, 4 drafts. Include trigger history for live rules.
- 4 external operators (AirtelTigo Money, Telecel Cash, G-Money, Zeepay) at various connection states. Include shared flag history for connected operators.
- 6 law enforcement cases (Ghana Police CID, EOCO, NCA) at various stages. Include message threads and evidence packages.
- All interconnected with realistic edge strengths and temporal data
- Geographic data centered on Accra, Kumasi, Tamale, Cape Coast, Takoradi

## PostgreSQL Models (db/models.py)
```python
# Users & Auth
User:           id, username, email, password_hash, role, created_at, last_login, active
APIKey:         id, operator_id, key_hash, permissions[], created_at, expires_at, active

# Rules Engine
Rule:           id, name, description, created_by, created_at, updated_at, status (draft/backtesting/shadow/live/paused/retired), conditions_json, actions_json, scope_json, evaluation_mode (realtime/scheduled/onetime), schedule_interval, expiry_date, expiry_triggers, approved_by, approved_at
RuleTrigger:    id, rule_id, triggered_at, event_id, node_id, node_type, context_json, actions_executed[], outcome (success/failed/overridden/pending_approval), overridden_by, override_reason
RuleBacktest:   id, rule_id, days, started_at, completed_at, match_count, affected_nodes_json, estimated_fp_rate

# Takedowns
Takedown:       id, cluster_id, initiated_by, initiated_at, approved_by, approved_at, status, wallets_frozen, sims_flagged, agents_alerted, evidence_package_id, completed_at
TakedownStep:   id, takedown_id, step_type, status, started_at, completed_at, detail_json

# Law Enforcement
LECase:         id, agency_id, status, cluster_ids[], created_by, created_at, assigned_officer, officer_contact, notes
LECaseMessage:  id, case_id, sender_id, sender_role, content, timestamp, attachments[]
LEAgency:       id, name, type, contact_name, contact_email, contact_phone, api_key_id, active
LEOutcome:      id, case_id, outcome_type (arrest/prosecution/conviction/acquittal/funds_recovered), detail, amount_recovered, date, reported_by

# Evidence
EvidencePackage: id, cluster_id, case_id, generated_at, generated_by, version, file_hash, file_path (MinIO), page_count, file_size
EvidenceAccess:  id, package_id, accessed_by, accessed_at, user_role

# Operator Integration
ExternalOperator: id, name, contact_name, contact_email, technical_contact, api_key_id, status (connected/pending/disconnected), integration_type, data_sharing_level, masking_rules_json, auto_integrate, onboarding_step, created_at
SharedFlag:       id, direction (inbound/outbound), operator_id, identifier_type, identifier_masked, identifier_hash, risk_score, context, shared_at, action_taken, actioned_at
IntelligencePackage: id, direction, operator_id, identifiers_json, relationships_json, confidence, context, received_at, processed, processed_at

# Configuration
ConfigParam:    id, key, value, value_type, description, updated_by, updated_at, requires_restart

# Audit
AuditLog:       id, user_id, action, target_type, target_id, detail_json, ip_address, timestamp, rule_id (nullable, if triggered by rule)
```

## Key Implementation Notes
1. **All Neo4j queries must be parameterized** — never interpolate values into Cypher strings
2. **Audit logging is mandatory** — every write operation (freeze, flag, takedown, config change, rule trigger, evidence access) must be logged with user, timestamp, action, affected entities, and triggering rule (if applicable)
3. **WebSocket feeds must be efficient** — use Redis pub/sub to broadcast from Kafka consumers to WebSocket connections
4. **Evidence packages are immutable** — once generated, stored in MinIO and referenced by hash
5. **All API responses follow consistent schema** — `{data: T, meta: {total, page, per_page}, errors: []}`
6. **Health checks** — `/health` endpoint checking Neo4j, Kafka, Redis, PostgreSQL, MinIO, and external operator API connectivity
7. **CORS configured** for the Lovable frontend origin
8. **Rate limiting** on all mutation endpoints and per-operator on external API
9. **Graph queries optimized** — all traversals use parameterized depth limits and early termination
10. **Confidence scores cached in Redis** with 5-minute TTL for dashboard performance
11. **Rules engine must be performant** — active rules cached in Redis, condition evaluation is hot-path code, actions dispatched asynchronously via Kafka
12. **Rules must be idempotent** — re-evaluating a rule against the same context must not trigger duplicate actions. Use trigger deduplication with Redis keys (rule_id + node_id + 15-min window)
13. **External API authentication** — API keys per operator, validated via middleware, all requests logged. Separate from internal JWT auth.
14. **Identifier masking is non-negotiable** — outbound shared data ALWAYS passes through masking layer. Default: hash all identifiers unless operator config explicitly permits clear-text for specific fields.
15. **Law enforcement portal uses separate auth domain** — law enforcement users are NOT in the same user table as internal users. Separate authentication flow, separate session management, restricted API surface.
16. **Multi-tenant readiness** — although initially single-operator, all data models include operator_id fields and all queries are tenant-scoped. This enables future multi-tenant deployment without schema migration.
17. **Rule action rollback** — all reversible actions (freeze, limit, restrict, suspend) must support rollback. When a rule is paused or retired, pending actions from that rule are flagged for review. Completed actions require manual rollback.
18. **Integration resilience** — external operator API calls use circuit breaker pattern (3 failures → open circuit → retry after 60s). Failed outbound shares are queued for retry.

## Build this as a production-grade, well-tested, fully documented Python backend. Start with docker-compose, schema initialization, core mesh expansion and scoring, then the API layer, then ingestion, then ML pipeline. Every module should have comprehensive docstrings and type hints.
