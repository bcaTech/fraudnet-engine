"""Parameterised Cypher query library.

Centralising Cypher here keeps the query surface auditable and makes it easy
to optimise hot paths. Every query name is documented with its parameter
contract — never inline values into the strings, always pass via ``params``.
"""

from __future__ import annotations

from typing import Final

# ---------------------------------------------------------------------------
# Health / metadata
# ---------------------------------------------------------------------------

PING: Final[str] = "RETURN 1 AS ok"

COUNT_BY_LABEL: Final[str] = """
CALL {
    MATCH (w:Wallet)    RETURN 'Wallet' AS label, count(w) AS n
    UNION ALL
    MATCH (h:Handset)   RETURN 'Handset' AS label, count(h) AS n
    UNION ALL
    MATCH (s:SIM)       RETURN 'SIM' AS label, count(s) AS n
    UNION ALL
    MATCH (a:Agent)     RETURN 'Agent' AS label, count(a) AS n
    UNION ALL
    MATCH (c:Cluster)   RETURN 'Cluster' AS label, count(c) AS n
    UNION ALL
    MATCH (t:Transaction) RETURN 'Transaction' AS label, count(t) AS n
}
RETURN label, n
"""


# ---------------------------------------------------------------------------
# Mesh expansion: fetch all neighbours of a node ordered by edge strength.
# Parameters: { id: str, label: str (e.g. 'Wallet'), key: str (e.g. 'wallet_id') }
# ---------------------------------------------------------------------------

EXPAND_NEIGHBOURS: Final[str] = """
MATCH (n)
WHERE n[$key] = $id
MATCH (n)-[r]-(m)
WITH n, r, m, coalesce(r.strength, 0.0) AS strength
RETURN
    elementId(n)        AS source_eid,
    elementId(m)        AS target_eid,
    labels(m)           AS target_labels,
    properties(m)       AS target_props,
    type(r)             AS rel_type,
    properties(r)       AS rel_props,
    strength
ORDER BY strength DESC
LIMIT $limit
"""


# ---------------------------------------------------------------------------
# Cluster persistence
# ---------------------------------------------------------------------------

UPSERT_CLUSTER: Final[str] = """
MERGE (c:Cluster {cluster_id: $cluster_id})
SET c.name = coalesce($name, c.name),
    c.seed_type = $seed_type,
    c.seed_date = datetime($seed_date),
    c.seed_node_id = $seed_node_id,
    c.node_count = $node_count,
    c.confidence_score = $confidence_score,
    c.status = $status,
    c.estimated_fraud_value = $estimated_fraud_value,
    c.density = $density,
    c.isolation_score = $isolation_score
RETURN c
"""

# Generic membership upsert. Parameters: cluster_id, node_label, node_key,
# node_id, confidence, joined_date, role.
ATTACH_MEMBER: Final[str] = """
MATCH (c:Cluster {cluster_id: $cluster_id})
CALL {
    WITH c
    MATCH (n)
    WHERE any(l IN labels(n) WHERE l = $node_label) AND n[$node_key] = $node_id
    MERGE (n)-[r:BELONGS_TO]->(c)
    SET r.confidence = $confidence,
        r.joined_date = $joined_date,
        r.role = coalesce($role, r.role)
    SET n.cluster_id = $cluster_id,
        n.confidence_score = coalesce($confidence, n.confidence_score)
    RETURN count(*) AS attached
}
RETURN attached
"""


# ---------------------------------------------------------------------------
# Cluster queries
# ---------------------------------------------------------------------------

LIST_CLUSTERS: Final[str] = """
MATCH (c:Cluster)
WHERE ($status IS NULL OR c.status = $status)
  AND ($min_confidence IS NULL OR c.confidence_score >= $min_confidence)
  AND ($max_confidence IS NULL OR c.confidence_score <= $max_confidence)
  AND ($since IS NULL OR datetime(c.seed_date) >= datetime($since))
RETURN c {
    .cluster_id, .name, .seed_type, .seed_date, .seed_node_id,
    .node_count, .confidence_score, .status, .estimated_fraud_value,
    .density, .isolation_score
} AS cluster
ORDER BY c.confidence_score DESC, c.seed_date DESC
SKIP $skip
LIMIT $limit
"""

COUNT_CLUSTERS: Final[str] = """
MATCH (c:Cluster)
WHERE ($status IS NULL OR c.status = $status)
  AND ($min_confidence IS NULL OR c.confidence_score >= $min_confidence)
  AND ($max_confidence IS NULL OR c.confidence_score <= $max_confidence)
  AND ($since IS NULL OR datetime(c.seed_date) >= datetime($since))
RETURN count(c) AS n
"""

GET_CLUSTER: Final[str] = """
MATCH (c:Cluster {cluster_id: $cluster_id})
OPTIONAL MATCH (n)-[:BELONGS_TO]->(c)
WITH c, count(DISTINCT n) AS member_count, collect(DISTINCT labels(n)[0]) AS member_labels
RETURN c {
    .cluster_id, .name, .seed_type, .seed_date, .seed_node_id,
    .node_count, .confidence_score, .status, .estimated_fraud_value,
    .density, .isolation_score
} AS cluster,
member_count,
member_labels
"""

GET_CLUSTER_GRAPH: Final[str] = """
MATCH (c:Cluster {cluster_id: $cluster_id})
MATCH (n)-[:BELONGS_TO]->(c)
WITH collect(DISTINCT n) AS nodes
UNWIND nodes AS n
OPTIONAL MATCH (n)-[r]-(m)
WHERE m IN nodes AND type(r) <> 'BELONGS_TO'
RETURN
    [x IN nodes | {
        eid: elementId(x),
        labels: labels(x),
        props: properties(x)
    }] AS node_records,
    collect(DISTINCT {
        source_eid: elementId(startNode(r)),
        target_eid: elementId(endNode(r)),
        type: type(r),
        props: properties(r)
    }) AS edge_records
"""

GET_CLUSTER_NODES: Final[str] = """
MATCH (c:Cluster {cluster_id: $cluster_id})
MATCH (n)-[r:BELONGS_TO]->(c)
RETURN
    elementId(n)             AS eid,
    labels(n)                AS labels,
    properties(n)            AS props,
    coalesce(r.confidence, 0.0) AS confidence,
    r.role                   AS role
ORDER BY confidence DESC
SKIP $skip
LIMIT $limit
"""


# ---------------------------------------------------------------------------
# Dashboard / KPI queries
# ---------------------------------------------------------------------------

DASHBOARD_METRICS: Final[str] = """
CALL {
    MATCH (c:Cluster) WHERE c.status IN ['active', 'investigating']
    RETURN count(c) AS active_clusters,
           sum(coalesce(c.estimated_fraud_value, 0.0)) AS estimated_fraud_value
}
CALL {
    MATCH (w:Wallet) WHERE w.status IN ['flagged', 'frozen']
    RETURN count(w) AS wallets_under_review
}
CALL {
    MATCH (a:Agent) WHERE a.classification IN ['exploited', 'complicit']
    RETURN count(a) AS high_risk_agents
}
CALL {
    MATCH (c:Cluster) WHERE c.status = 'takedown_complete'
    RETURN count(c) AS takedowns_completed
}
RETURN
    active_clusters,
    wallets_under_review,
    high_risk_agents,
    takedowns_completed,
    estimated_fraud_value
"""

CLUSTER_OVERVIEW: Final[str] = """
MATCH (c:Cluster)
WHERE c.status IN ['active', 'investigating', 'takedown_pending']
WITH c
ORDER BY c.confidence_score DESC, c.estimated_fraud_value DESC
LIMIT $limit
OPTIONAL MATCH (n)-[:BELONGS_TO]->(c)
WITH c, count(DISTINCT n) AS member_count
RETURN c {
    .cluster_id, .name, .seed_type, .confidence_score, .status,
    .estimated_fraud_value, .density, .isolation_score
} AS cluster, member_count
"""


# ---------------------------------------------------------------------------
# Node lookup
# ---------------------------------------------------------------------------

GET_WALLET: Final[str] = """
MATCH (w:Wallet {wallet_id: $wallet_id})
OPTIONAL MATCH (w)-[:BELONGS_TO]->(c:Cluster)
RETURN w {
    .wallet_id, .msisdn, .name, .kyc_tier, .creation_date, .balance,
    .status, .risk_score, .cluster_id, .confidence_score, .behavioral_score,
    .predictive_score, .is_sleeper, .last_activity, .freeze_date
} AS wallet,
c.cluster_id AS cluster_id
"""

GET_HANDSET: Final[str] = """
MATCH (h:Handset {imei: $imei})
RETURN h {
    .imei, .make, .model, .first_seen, .last_seen, .sim_count,
    .flagged, .flag_reason, .flag_date
} AS handset
"""

GET_SIM: Final[str] = """
MATCH (s:SIM {imsi: $imsi})
RETURN s {
    .imsi, .msisdn, .registration_date, .status, .swap_count,
    .last_swap_date, .flagged
} AS sim
"""

GET_AGENT: Final[str] = """
MATCH (a:Agent {agent_id: $agent_id})
OPTIONAL MATCH (a)-[r:LINKED_TO]->(c:Cluster)
WITH a, collect(DISTINCT c.cluster_id) AS linked_clusters
RETURN a {
    .agent_id, .name, .lat, .lng, .area_name, .registration_date,
    .risk_score, .classification, .monthly_volume, .fraud_cashout_rate,
    .float_avg, .suspended, .suspension_date
} AS agent,
linked_clusters
"""

GET_PHONE: Final[str] = """
MATCH (p:PhoneNumber {msisdn: $msisdn})
OPTIONAL MATCH (p)-[:OWNS_WALLET]->(w:Wallet)
WITH p, collect(DISTINCT w.wallet_id) AS wallet_ids
RETURN p {
    .msisdn, .registration_status, .kyc_tier, .account_age
} AS phone,
wallet_ids
"""


# Top-N connections for a node, used by /api/nodes/:type/:id/connections.
GET_NODE_CONNECTIONS: Final[str] = """
MATCH (n)
WHERE any(l IN labels(n) WHERE l = $label) AND n[$key] = $id
MATCH (n)-[r]-(m)
RETURN
    type(r) AS rel_type,
    properties(r) AS rel_props,
    labels(m) AS target_labels,
    properties(m) AS target_props,
    coalesce(r.strength, 0.0) AS strength
ORDER BY strength DESC
LIMIT $limit
"""


# Universal search by free-text fragment. Each branch limits results so a
# common substring (e.g. "+233") doesn't return everything.
SEARCH_NODES: Final[str] = """
CALL {
    MATCH (w:Wallet)
    WHERE toLower(w.wallet_id) CONTAINS toLower($q)
       OR (w.msisdn IS NOT NULL AND w.msisdn CONTAINS $q)
       OR (w.name IS NOT NULL AND toLower(w.name) CONTAINS toLower($q))
    RETURN
        'wallet' AS type,
        w.wallet_id AS id,
        coalesce(w.name, w.wallet_id) AS label,
        coalesce(w.msisdn, '') AS subtitle,
        coalesce(w.risk_score, 0.0) AS risk_score,
        w.status AS status,
        w.cluster_id AS cluster_id
    LIMIT $branch_limit

    UNION

    MATCH (h:Handset)
    WHERE h.imei CONTAINS $q
       OR (h.make IS NOT NULL AND toLower(h.make) CONTAINS toLower($q))
       OR (h.model IS NOT NULL AND toLower(h.model) CONTAINS toLower($q))
    RETURN
        'handset' AS type,
        h.imei AS id,
        coalesce(h.make, '') + ' ' + coalesce(h.model, '') AS label,
        h.imei AS subtitle,
        CASE WHEN h.flagged = true THEN 0.85 ELSE 0.0 END AS risk_score,
        CASE WHEN h.flagged = true THEN 'flagged' ELSE 'active' END AS status,
        null AS cluster_id
    LIMIT $branch_limit

    UNION

    MATCH (s:SIM)
    WHERE s.imsi CONTAINS $q
       OR (s.msisdn IS NOT NULL AND s.msisdn CONTAINS $q)
    RETURN
        'sim' AS type,
        s.imsi AS id,
        coalesce(s.msisdn, s.imsi) AS label,
        s.imsi AS subtitle,
        CASE WHEN s.flagged = true THEN 0.85 ELSE 0.0 END AS risk_score,
        s.status AS status,
        null AS cluster_id
    LIMIT $branch_limit

    UNION

    MATCH (a:Agent)
    WHERE toLower(a.agent_id) CONTAINS toLower($q)
       OR (a.name IS NOT NULL AND toLower(a.name) CONTAINS toLower($q))
    RETURN
        'agent' AS type,
        a.agent_id AS id,
        coalesce(a.name, a.agent_id) AS label,
        coalesce(a.area_name, '') AS subtitle,
        coalesce(a.risk_score, 0.0) AS risk_score,
        a.classification AS status,
        null AS cluster_id
    LIMIT $branch_limit

    UNION

    MATCH (p:PhoneNumber)
    WHERE p.msisdn CONTAINS $q
    RETURN
        'phone' AS type,
        p.msisdn AS id,
        p.msisdn AS label,
        coalesce(p.registration_status, '') AS subtitle,
        0.0 AS risk_score,
        p.registration_status AS status,
        null AS cluster_id
    LIMIT $branch_limit
}
RETURN type, id, label, subtitle, risk_score, status, cluster_id
ORDER BY risk_score DESC, type
LIMIT $limit
"""

FREEZE_WALLET: Final[str] = """
MATCH (w:Wallet {wallet_id: $wallet_id})
SET w.status = 'frozen',
    w.freeze_date = datetime()
RETURN w {
    .wallet_id, .msisdn, .name, .status, .risk_score, .cluster_id,
    .confidence_score, .freeze_date
} AS wallet
"""

UNFREEZE_WALLET: Final[str] = """
MATCH (w:Wallet {wallet_id: $wallet_id})
SET w.status = CASE
        WHEN coalesce(w.cluster_id, '') = '' THEN 'active'
        ELSE 'flagged'
    END,
    w.freeze_date = null
RETURN w {
    .wallet_id, .msisdn, .name, .status, .risk_score, .cluster_id,
    .confidence_score, .freeze_date
} AS wallet
"""

FLAG_NODE: Final[str] = """
MATCH (n)
WHERE any(l IN labels(n) WHERE l = $label) AND n[$key] = $id
SET n.flagged = true,
    n.flag_reason = $reason,
    n.flag_date = datetime()
RETURN labels(n) AS labels, properties(n) AS props
"""
