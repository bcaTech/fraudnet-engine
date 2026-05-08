"""Generate realistic FraudNet demo data.

Populates Neo4j with the full graph (wallets, handsets, SIMs, phone numbers,
agents, transactions, cell towers, clusters and their relationships) and
PostgreSQL with the workflow state (alerts, takedowns, rules, external
operators, law-enforcement cases) so the API and frontend have something to
render before real ingestion comes online.

Usage (from inside the Docker network)::

    docker compose exec api python -m scripts.seed_demo_data --reset

Volumes / counts mirror the spec in ``CLAUDE.md``: 500 wallets, 200 handsets,
350 SIMs, 80 agents, 5000 transactions, 15 clusters, 100 alerts, 8 takedowns,
20 rules, 4 external operators, 6 LE cases. Geographic data is centred on
Accra, Kumasi, Tamale, Cape Coast and Takoradi.

The seeder is intentionally deterministic: it seeds Python's ``random`` with a
fixed value so successive runs produce the same shapes. Pass ``--seed`` to
override.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import math
import random
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

from sqlalchemy.orm import Session

from config.logging import configure_logging, get_logger
from core.graph.client import Neo4jClient, get_neo4j_client
from core.graph.queries import ATTACH_MEMBER, UPSERT_CLUSTER
from core.graph.schema import initialize_schema
from db.models import (
    Alert,
    Base,
    ExternalOperator,
    LEAgency,
    LECase,
    LECaseMessage,
    Rule,
    RuleTrigger,
    SharedFlag,
    Takedown,
    TakedownStep,
    User,
)
from db.session import get_sync_engine, get_sync_session

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Counts / geography
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Counts:
    wallets: int = 500
    handsets: int = 200
    sims: int = 350
    phone_numbers: int = 500  # 1:1 with wallets
    agents: int = 80
    transactions: int = 5_000
    cell_towers: int = 60
    clusters: int = 15
    alerts: int = 100
    takedowns: int = 8
    rules: int = 20
    operators: int = 4
    le_cases: int = 6


COUNTS = Counts()

# (city_name, lat, lng, weight, radius_km)
CITIES: list[tuple[str, float, float, float, float]] = [
    ("Accra", 5.5563, -0.1969, 0.45, 18.0),
    ("Kumasi", 6.6885, -1.6244, 0.22, 14.0),
    ("Tamale", 9.4035, -0.8423, 0.10, 10.0),
    ("Cape Coast", 5.1054, -1.2466, 0.10, 8.0),
    ("Takoradi", 4.8845, -1.7554, 0.13, 9.0),
]

GHANA_MCC_MNC = "62001"  # MCC 620 (Ghana) + MNC 01 (MTN)
GHANA_MSISDN_PREFIX = "+23324"  # MTN-style mobile prefix

HANDSET_MAKES = [
    ("Tecno", ["Camon 19", "Spark 9", "Pop 5", "Pova Neo 2"]),
    ("Infinix", ["Hot 12", "Smart 6", "Note 12", "Zero X Pro"]),
    ("Samsung", ["Galaxy A04", "Galaxy A14", "Galaxy A24", "Galaxy M14"]),
    ("Itel", ["A26", "A48", "Vision 3", "S17"]),
    ("Nokia", ["G10", "C20", "C32", "G21"]),
    ("Apple", ["iPhone 11", "iPhone 12", "iPhone 13"]),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def _luhn_check_digit(digits: str) -> str:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = int(ch)
        if i % 2 == 0:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return str((10 - total % 10) % 10)


def _gen_imei(rng: random.Random) -> str:
    body = "".join(str(rng.randint(0, 9)) for _ in range(14))
    return body + _luhn_check_digit(body)


def _gen_imsi(rng: random.Random) -> str:
    return GHANA_MCC_MNC + "".join(str(rng.randint(0, 9)) for _ in range(10))


def _gen_msisdn(rng: random.Random) -> str:
    return GHANA_MSISDN_PREFIX + "".join(str(rng.randint(0, 9)) for _ in range(7))


def _pick_city(rng: random.Random) -> tuple[str, float, float, float]:
    weights = [c[3] for c in CITIES]
    name, lat, lng, _, radius = rng.choices(CITIES, weights=weights, k=1)[0]
    return name, lat, lng, radius


def _jitter_coord(rng: random.Random, lat: float, lng: float, radius_km: float) -> tuple[float, float]:
    """Return a point within ``radius_km`` of (lat, lng), uniformly distributed."""

    # 1 deg latitude ~ 111 km; longitude scales by cos(lat).
    r = radius_km * math.sqrt(rng.random()) / 111.0
    theta = rng.uniform(0, 2 * math.pi)
    return (
        lat + r * math.cos(theta),
        lng + r * math.sin(theta) / max(math.cos(math.radians(lat)), 0.01),
    )


def _rand_dt(rng: random.Random, *, days_back: int = 180) -> datetime:
    delta = timedelta(seconds=rng.randint(0, days_back * 86_400))
    return datetime.now(UTC) - delta


def _hash_id(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


# ---------------------------------------------------------------------------
# In-memory representations (used to wire relationships between phases)
# ---------------------------------------------------------------------------


@dataclass
class GraphState:
    rng: random.Random
    wallets: list[dict] = field(default_factory=list)
    handsets: list[dict] = field(default_factory=list)
    sims: list[dict] = field(default_factory=list)
    phones: list[dict] = field(default_factory=list)
    agents: list[dict] = field(default_factory=list)
    cell_towers: list[dict] = field(default_factory=list)
    clusters: list[dict] = field(default_factory=list)
    cluster_membership: dict[str, list[tuple[str, str, str, float]]] = field(default_factory=dict)
    """cluster_id → list of (label, key_field, key_value, confidence)."""


# ---------------------------------------------------------------------------
# Neo4j seeding
# ---------------------------------------------------------------------------


async def _wipe_graph(client: Neo4jClient) -> None:
    logger.info("seed.neo4j.wipe.start")
    await client.execute_write("MATCH (n) DETACH DELETE n")
    logger.info("seed.neo4j.wipe.complete")


async def _seed_cell_towers(client: Neo4jClient, state: GraphState) -> None:
    rng = state.rng
    for i in range(COUNTS.cell_towers):
        city_name, lat, lng, radius = _pick_city(rng)
        cell_lat, cell_lng = _jitter_coord(rng, lat, lng, radius)
        state.cell_towers.append(
            {
                "cell_id": f"CELL-{i:05d}",
                "lac": rng.randint(1000, 9999),
                "lat": round(cell_lat, 6),
                "lng": round(cell_lng, 6),
                "coverage_radius_m": float(rng.randint(500, 3500)),
                "area_name": city_name,
            }
        )

    await client.execute_write(
        """
        UNWIND $rows AS row
        MERGE (t:CellTower {cell_id: row.cell_id})
        SET t += row
        """,
        {"rows": state.cell_towers},
    )
    logger.info("seed.neo4j.cell_towers", count=len(state.cell_towers))


async def _seed_agents(client: Neo4jClient, state: GraphState) -> None:
    rng = state.rng
    for i in range(COUNTS.agents):
        city_name, lat, lng, radius = _pick_city(rng)
        a_lat, a_lng = _jitter_coord(rng, lat, lng, radius)
        # Most agents are clean; a tail are exploited / complicit.
        roll = rng.random()
        if roll < 0.78:
            classification, risk_lo, risk_hi = "clean", 0.02, 0.20
        elif roll < 0.92:
            classification, risk_lo, risk_hi = "incidental", 0.15, 0.45
        elif roll < 0.985:
            classification, risk_lo, risk_hi = "exploited", 0.55, 0.78
        else:
            classification, risk_lo, risk_hi = "complicit", 0.80, 0.95
        state.agents.append(
            {
                "agent_id": f"AGT-{i:05d}",
                "name": f"Agent {i:03d} ({city_name})",
                "lat": round(a_lat, 6),
                "lng": round(a_lng, 6),
                "area_name": city_name,
                "registration_date": _rand_dt(rng, days_back=720).isoformat(),
                "risk_score": round(rng.uniform(risk_lo, risk_hi), 3),
                "classification": classification,
                "monthly_volume": round(rng.uniform(40_000, 800_000), 2),
                "fraud_cashout_rate": round(
                    {"clean": 0.0, "incidental": 0.03, "exploited": 0.18, "complicit": 0.42}[classification]
                    * rng.uniform(0.7, 1.3),
                    4,
                ),
                "float_avg": round(rng.uniform(2_000, 25_000), 2),
                "suspended": classification == "complicit" and rng.random() < 0.4,
            }
        )

    await client.execute_write(
        """
        UNWIND $rows AS row
        MERGE (a:Agent {agent_id: row.agent_id})
        SET a += row
        """,
        {"rows": state.agents},
    )
    logger.info("seed.neo4j.agents", count=len(state.agents))


async def _seed_handsets(client: Neo4jClient, state: GraphState) -> None:
    rng = state.rng
    seen: set[str] = set()
    while len(state.handsets) < COUNTS.handsets:
        imei = _gen_imei(rng)
        if imei in seen:
            continue
        seen.add(imei)
        make, models = rng.choice(HANDSET_MAKES)
        first = _rand_dt(rng, days_back=540)
        last = first + timedelta(days=rng.randint(7, 540))
        state.handsets.append(
            {
                "imei": imei,
                "make": make,
                "model": rng.choice(models),
                "first_seen": first.isoformat(),
                "last_seen": last.isoformat(),
                "sim_count": 0,  # filled in once SIMs are wired
                "flagged": False,
                "flag_reason": None,
            }
        )

    await client.execute_write(
        """
        UNWIND $rows AS row
        MERGE (h:Handset {imei: row.imei})
        SET h += row
        """,
        {"rows": state.handsets},
    )
    logger.info("seed.neo4j.handsets", count=len(state.handsets))


async def _seed_sims_and_phones(client: Neo4jClient, state: GraphState) -> None:
    rng = state.rng
    # Each SIM ↔ exactly one MSISDN (HAS_NUMBER). 350 SIMs map to 350 of the 500 phone numbers.
    used_msisdns: set[str] = set()
    used_imsis: set[str] = set()

    # Build phone numbers first — we need 500 (one per wallet).
    while len(state.phones) < COUNTS.phone_numbers:
        msisdn = _gen_msisdn(rng)
        if msisdn in used_msisdns:
            continue
        used_msisdns.add(msisdn)
        state.phones.append(
            {
                "msisdn": msisdn,
                "registration_status": rng.choices(
                    ["fully_kyc", "partial_kyc", "unverified"], weights=[0.7, 0.22, 0.08]
                )[0],
                "kyc_tier": rng.choices([1, 2, 3], weights=[0.15, 0.55, 0.30])[0],
                "account_age": rng.randint(30, 1825),
            }
        )

    await client.execute_write(
        """
        UNWIND $rows AS row
        MERGE (p:PhoneNumber {msisdn: row.msisdn})
        SET p += row
        """,
        {"rows": state.phones},
    )
    logger.info("seed.neo4j.phones", count=len(state.phones))

    # Assign SIMs: 350 of them, each pointing at one phone number.
    phones_for_sim = rng.sample(state.phones, COUNTS.sims)
    while len(state.sims) < COUNTS.sims:
        imsi = _gen_imsi(rng)
        if imsi in used_imsis:
            continue
        used_imsis.add(imsi)
        msisdn = phones_for_sim[len(state.sims)]["msisdn"]
        swap_count = rng.choices([0, 1, 2, 3], weights=[0.85, 0.10, 0.04, 0.01])[0]
        last_swap = _rand_dt(rng, days_back=180) if swap_count else None
        state.sims.append(
            {
                "imsi": imsi,
                "msisdn": msisdn,
                "registration_date": _rand_dt(rng, days_back=540).isoformat(),
                "status": "active",
                "swap_count": swap_count,
                "last_swap_date": last_swap.isoformat() if last_swap else None,
                "flagged": False,
            }
        )

    await client.execute_write(
        """
        UNWIND $rows AS row
        MERGE (s:SIM {imsi: row.imsi})
        SET s += row
        """,
        {"rows": state.sims},
    )
    logger.info("seed.neo4j.sims", count=len(state.sims))

    # Wire SIM -> PhoneNumber (HAS_NUMBER).
    await client.execute_write(
        """
        UNWIND $rows AS row
        MATCH (s:SIM {imsi: row.imsi})
        MATCH (p:PhoneNumber {msisdn: row.msisdn})
        MERGE (s)-[r:HAS_NUMBER]->(p)
        SET r.active = true,
            r.start_date = datetime(row.registration_date)
        """,
        {"rows": state.sims},
    )

    # Wire SIM -> Handset (INSERTED_IN). Most SIMs fit one handset; a few hop
    # between two — that's the strong-edge fraud signal.
    sim_to_handsets: list[dict] = []
    for sim in state.sims:
        # Each SIM has 1 main handset, 12% have a second one.
        primary = rng.choice(state.handsets)
        first = _rand_dt(rng, days_back=540)
        last = first + timedelta(days=rng.randint(15, 480))
        sim_to_handsets.append(
            {
                "imsi": sim["imsi"],
                "imei": primary["imei"],
                "first_seen": first.isoformat(),
                "last_seen": last.isoformat(),
                "duration_days": (last - first).days,
                "strength": round(rng.uniform(0.65, 0.95), 3),
            }
        )
        if rng.random() < 0.12:
            secondary = rng.choice(state.handsets)
            if secondary["imei"] != primary["imei"]:
                f2 = _rand_dt(rng, days_back=200)
                l2 = f2 + timedelta(days=rng.randint(5, 90))
                sim_to_handsets.append(
                    {
                        "imsi": sim["imsi"],
                        "imei": secondary["imei"],
                        "first_seen": f2.isoformat(),
                        "last_seen": l2.isoformat(),
                        "duration_days": (l2 - f2).days,
                        "strength": round(rng.uniform(0.40, 0.70), 3),
                    }
                )

    await client.execute_write(
        """
        UNWIND $rows AS row
        MATCH (s:SIM {imsi: row.imsi})
        MATCH (h:Handset {imei: row.imei})
        MERGE (s)-[r:INSERTED_IN]->(h)
        SET r.first_seen = datetime(row.first_seen),
            r.last_seen = datetime(row.last_seen),
            r.duration_days = row.duration_days,
            r.strength = row.strength
        """,
        {"rows": sim_to_handsets},
    )

    # Update handset.sim_count.
    sim_counts: dict[str, int] = defaultdict(int)
    for row in sim_to_handsets:
        sim_counts[row["imei"]] += 1
    for h in state.handsets:
        h["sim_count"] = sim_counts.get(h["imei"], 0)

    await client.execute_write(
        """
        UNWIND $rows AS row
        MATCH (h:Handset {imei: row.imei})
        SET h.sim_count = row.sim_count
        """,
        {"rows": [{"imei": h["imei"], "sim_count": h["sim_count"]} for h in state.handsets]},
    )

    # Handset -> CellTower (CONNECTED_TO).
    connections: list[dict] = []
    for h in state.handsets:
        for _ in range(rng.randint(1, 3)):
            tower = rng.choice(state.cell_towers)
            connections.append(
                {
                    "imei": h["imei"],
                    "cell_id": tower["cell_id"],
                    "timestamp": _rand_dt(rng, days_back=60).isoformat(),
                    "duration_s": rng.randint(30, 4 * 3600),
                }
            )
    await client.execute_write(
        """
        UNWIND $rows AS row
        MATCH (h:Handset {imei: row.imei})
        MATCH (c:CellTower {cell_id: row.cell_id})
        MERGE (h)-[r:CONNECTED_TO {timestamp: datetime(row.timestamp)}]->(c)
        SET r.duration_s = row.duration_s
        """,
        {"rows": connections},
    )
    logger.info("seed.neo4j.handset_links", sim_handset=len(sim_to_handsets), tower=len(connections))


async def _seed_wallets(client: Neo4jClient, state: GraphState) -> None:
    rng = state.rng
    for i in range(COUNTS.wallets):
        msisdn = state.phones[i]["msisdn"]
        creation = _rand_dt(rng, days_back=720)
        balance = round(rng.uniform(0, 4_500), 2)
        wallet_id = f"MOMO-{i:06d}"
        state.wallets.append(
            {
                "wallet_id": wallet_id,
                "msisdn": msisdn,
                "name": f"Customer {i:04d}",
                "kyc_tier": state.phones[i]["kyc_tier"],
                "creation_date": creation.isoformat(),
                "balance": balance,
                "status": "active",
                "risk_score": round(rng.uniform(0.0, 0.18), 3),
                "confidence_score": 0.0,
                "behavioral_score": round(rng.uniform(0.0, 0.20), 3),
                "predictive_score": round(rng.uniform(0.0, 0.15), 3),
                "is_sleeper": False,
                "last_activity": _rand_dt(rng, days_back=30).isoformat(),
            }
        )

    await client.execute_write(
        """
        UNWIND $rows AS row
        MERGE (w:Wallet {wallet_id: row.wallet_id})
        SET w += row
        """,
        {"rows": state.wallets},
    )

    # Wire PhoneNumber -> Wallet (OWNS_WALLET).
    await client.execute_write(
        """
        UNWIND $rows AS row
        MATCH (p:PhoneNumber {msisdn: row.msisdn})
        MATCH (w:Wallet {wallet_id: row.wallet_id})
        MERGE (p)-[r:OWNS_WALLET]->(w)
        SET r.registration_date = datetime(row.creation_date),
            r.kyc_verified = true
        """,
        {"rows": state.wallets},
    )
    logger.info("seed.neo4j.wallets", count=len(state.wallets))


async def _seed_clusters(client: Neo4jClient, state: GraphState) -> None:
    """Carve out 15 clusters and elevate their members' risk."""

    rng = state.rng
    # Reserve ~30% of wallets as fraud-linked across all clusters.
    fraud_wallet_pool = rng.sample(state.wallets, k=int(COUNTS.wallets * 0.32))
    rng.shuffle(fraud_wallet_pool)

    # Sleeper agents are the cashout sinks. Bias toward exploited/complicit ones.
    bad_agents = [a for a in state.agents if a["classification"] in ("exploited", "complicit")]
    if len(bad_agents) < COUNTS.clusters:
        backfill_pool = [a for a in state.agents if a["classification"] == "incidental"]
        needed = COUNTS.clusters - len(bad_agents)
        bad_agents = bad_agents + rng.sample(backfill_pool, k=min(needed, len(backfill_pool)))
    if len(bad_agents) < COUNTS.clusters:
        # Last resort — borrow random clean agents so we always have enough sinks.
        backfill_pool = [a for a in state.agents if a not in bad_agents]
        needed = COUNTS.clusters - len(bad_agents)
        bad_agents = bad_agents + rng.sample(backfill_pool, k=min(needed, len(backfill_pool)))

    seed_types_pool = ["wallet", "handset", "sim", "agent"]
    statuses = [
        ("active", 0.45),
        ("investigating", 0.25),
        ("takedown_pending", 0.10),
        ("takedown_complete", 0.12),
        ("dissolved", 0.08),
    ]
    status_keys, status_weights = zip(*statuses, strict=True)

    next_wallet_idx = 0
    for c_idx in range(COUNTS.clusters):
        cluster_id = f"CLUSTER-{c_idx + 1:04d}"
        size = rng.randint(8, 38)
        # Cluster wallets — slice from the fraud pool.
        cluster_wallets = fraud_wallet_pool[next_wallet_idx : next_wallet_idx + size]
        next_wallet_idx += size
        if len(cluster_wallets) < 4:  # ran out of fraud pool — borrow a few more.
            cluster_wallets = cluster_wallets + rng.sample(state.wallets, 4)
        seed_type = rng.choice(seed_types_pool)
        seed_wallet = cluster_wallets[0]
        confidence = round(rng.uniform(0.55, 0.93), 3)
        seed_date = _rand_dt(rng, days_back=90)
        status = rng.choices(status_keys, weights=status_weights)[0]
        est_value = round(rng.uniform(8_000, 180_000) * (confidence + 0.3), 2)
        density = round(rng.uniform(0.20, 0.65), 3)
        isolation = round(rng.uniform(0.30, 0.80), 3)

        # Pick a seed_node_id from the right entity type.
        if seed_type == "wallet":
            seed_node_id = seed_wallet["wallet_id"]
        elif seed_type == "handset":
            seed_node_id = rng.choice(state.handsets)["imei"]
        elif seed_type == "sim":
            seed_node_id = rng.choice(state.sims)["imsi"]
        else:
            seed_node_id = rng.choice(bad_agents)["agent_id"]

        cluster_record = {
            "cluster_id": cluster_id,
            "name": f"Ring {c_idx + 1:02d} — {rng.choice(['Accra', 'Kumasi', 'Tamale', 'Cape Coast', 'Takoradi'])}",
            "seed_type": seed_type,
            "seed_date": seed_date.isoformat(),
            "seed_node_id": seed_node_id,
            "node_count": len(cluster_wallets),
            "confidence_score": confidence,
            "status": status,
            "estimated_fraud_value": est_value,
            "density": density,
            "isolation_score": isolation,
        }
        state.clusters.append(cluster_record)

        await client.execute_write(UPSERT_CLUSTER, cluster_record)

        # Mark cluster wallets: set status, risk, cluster_id and BELONGS_TO.
        membership = state.cluster_membership.setdefault(cluster_id, [])
        for j, w in enumerate(cluster_wallets):
            role = "central" if j == 0 else ("accomplice" if j <= 3 else "node")
            member_conf = max(
                0.20, round(confidence * (0.5 + 0.5 * (1.0 - j / max(1, len(cluster_wallets)))), 3)
            )
            await client.execute_write(
                ATTACH_MEMBER,
                {
                    "cluster_id": cluster_id,
                    "node_label": "Wallet",
                    "node_key": "wallet_id",
                    "node_id": w["wallet_id"],
                    "confidence": member_conf,
                    "joined_date": seed_date.isoformat(),
                    "role": role,
                },
            )
            # Elevate this wallet's risk + status (mutate state too so later phases see it).
            new_status = (
                "frozen" if status == "takedown_complete" else ("flagged" if rng.random() < 0.7 else "active")
            )
            new_risk = round(min(0.97, member_conf + rng.uniform(-0.05, 0.10)), 3)
            w["status"] = new_status
            w["risk_score"] = new_risk
            w["confidence_score"] = member_conf
            w["cluster_id"] = cluster_id
            w["is_sleeper"] = role == "node" and rng.random() < 0.18
            await client.execute_write(
                """
                MATCH (w:Wallet {wallet_id: $wallet_id})
                SET w.status = $status,
                    w.risk_score = $risk_score,
                    w.confidence_score = $confidence_score,
                    w.cluster_id = $cluster_id,
                    w.is_sleeper = $is_sleeper
                """,
                {
                    "wallet_id": w["wallet_id"],
                    "status": new_status,
                    "risk_score": new_risk,
                    "confidence_score": member_conf,
                    "cluster_id": cluster_id,
                    "is_sleeper": w["is_sleeper"],
                },
            )
            membership.append(("Wallet", "wallet_id", w["wallet_id"], member_conf))

        # Pick 1-3 cashout agents per cluster and link them to the cluster.
        cashout_agents = rng.sample(bad_agents, k=min(rng.randint(1, 3), len(bad_agents)))
        for a in cashout_agents:
            await client.execute_write(
                """
                MATCH (a:Agent {agent_id: $agent_id})
                MATCH (c:Cluster {cluster_id: $cluster_id})
                MERGE (a)-[r:LINKED_TO]->(c)
                SET r.fraud_cashout_count = coalesce(r.fraud_cashout_count, 0) + $count,
                    r.strength = $strength
                """,
                {
                    "agent_id": a["agent_id"],
                    "cluster_id": cluster_id,
                    "count": rng.randint(3, 18),
                    "strength": round(rng.uniform(0.45, 0.92), 3),
                },
            )
            membership.append(("Agent", "agent_id", a["agent_id"], 0.0))

    logger.info("seed.neo4j.clusters", count=len(state.clusters))


async def _seed_transactions(client: Neo4jClient, state: GraphState) -> None:
    """Generate 5000 transactions. Wallets in clusters trade with each other and
    cash out to a small set of agents; clean wallets follow more uniform patterns."""

    rng = state.rng
    cluster_wallets_by_cluster: dict[str, list[dict]] = {
        c["cluster_id"]: [w for w in state.wallets if w.get("cluster_id") == c["cluster_id"]]
        for c in state.clusters
    }
    clean_wallets = [w for w in state.wallets if not w.get("cluster_id")]

    txns: list[dict] = []
    sent_to: list[dict] = []
    cashouts: list[dict] = []
    cashins: list[dict] = []

    # Pre-pick 1-3 cashout agents per cluster (the fraud sinks).
    cluster_sink_agents: dict[str, list[str]] = {}
    bad_agent_ids = [
        a["agent_id"] for a in state.agents if a["classification"] in ("exploited", "complicit")
    ] or [a["agent_id"] for a in state.agents]
    for c in state.clusters:
        cluster_sink_agents[c["cluster_id"]] = rng.sample(bad_agent_ids, k=min(2, len(bad_agent_ids)))

    # 60% of transactions involve cluster wallets, 40% are clean traffic.
    for i in range(COUNTS.transactions):
        is_fraud = rng.random() < 0.60 and state.clusters
        ts = _rand_dt(rng, days_back=45)
        tx_id = f"TX-{i:08d}"
        if is_fraud:
            cluster = rng.choice(state.clusters)
            members = cluster_wallets_by_cluster.get(cluster["cluster_id"]) or []
            if len(members) < 2:
                continue
            # Pick an internal-transfer or cashout — mix is roughly 70/30.
            if rng.random() < 0.70:
                src, dst = rng.sample(members, 2)
                amount = round(
                    rng.choices(
                        [rng.uniform(20, 200), rng.uniform(200, 800), rng.uniform(800, 2_500)],
                        weights=[0.55, 0.35, 0.10],
                    )[0],
                    2,
                )
                flagged = rng.random() < 0.30
                txns.append(
                    {
                        "tx_id": tx_id,
                        "type": "p2p",
                        "amount": amount,
                        "timestamp": ts.isoformat(),
                        "status": "completed",
                        "flagged": flagged,
                        "flag_reason": (
                            "structuring"
                            if flagged and rng.random() < 0.5
                            else ("velocity" if flagged else None)
                        ),
                    }
                )
                sent_to.append(
                    {
                        "tx_id": tx_id,
                        "src": src["wallet_id"],
                        "dst": dst["wallet_id"],
                        "amount": amount,
                        "timestamp": ts.isoformat(),
                        "type": "p2p",
                        "strength": round(rng.uniform(0.40, 0.85), 3),
                    }
                )
            else:
                src = rng.choice(members)
                agent_id = rng.choice(cluster_sink_agents[cluster["cluster_id"]])
                amount = round(rng.uniform(300, 2_500), 2)
                txns.append(
                    {
                        "tx_id": tx_id,
                        "type": "cashout",
                        "amount": amount,
                        "timestamp": ts.isoformat(),
                        "status": "completed",
                        "flagged": rng.random() < 0.45,
                        "flag_reason": "high_risk_agent" if rng.random() < 0.4 else None,
                    }
                )
                cashouts.append(
                    {
                        "tx_id": tx_id,
                        "wallet_id": src["wallet_id"],
                        "agent_id": agent_id,
                        "amount": amount,
                        "timestamp": ts.isoformat(),
                        "strength": round(rng.uniform(0.55, 0.95), 3),
                    }
                )
        # Clean traffic: random wallet pair, modest amounts, rarely flagged.
        elif rng.random() < 0.5 or len(clean_wallets) < 2:
            src = rng.choice(state.wallets)
            dst = rng.choice(state.wallets)
            if src["wallet_id"] == dst["wallet_id"]:
                continue
            amount = round(rng.uniform(5, 600), 2)
            txns.append(
                {
                    "tx_id": tx_id,
                    "type": "p2p",
                    "amount": amount,
                    "timestamp": ts.isoformat(),
                    "status": "completed",
                    "flagged": False,
                    "flag_reason": None,
                }
            )
            sent_to.append(
                {
                    "tx_id": tx_id,
                    "src": src["wallet_id"],
                    "dst": dst["wallet_id"],
                    "amount": amount,
                    "timestamp": ts.isoformat(),
                    "type": "p2p",
                    "strength": round(rng.uniform(0.10, 0.35), 3),
                }
            )
        else:
            wallet = rng.choice(state.wallets)
            agent = rng.choice(state.agents)
            amount = round(rng.uniform(20, 1_000), 2)
            if rng.random() < 0.5:
                txns.append(
                    {
                        "tx_id": tx_id,
                        "type": "cashin",
                        "amount": amount,
                        "timestamp": ts.isoformat(),
                        "status": "completed",
                        "flagged": False,
                        "flag_reason": None,
                    }
                )
                cashins.append(
                    {
                        "tx_id": tx_id,
                        "wallet_id": wallet["wallet_id"],
                        "agent_id": agent["agent_id"],
                        "amount": amount,
                        "timestamp": ts.isoformat(),
                    }
                )
            else:
                txns.append(
                    {
                        "tx_id": tx_id,
                        "type": "cashout",
                        "amount": amount,
                        "timestamp": ts.isoformat(),
                        "status": "completed",
                        "flagged": False,
                        "flag_reason": None,
                    }
                )
                cashouts.append(
                    {
                        "tx_id": tx_id,
                        "wallet_id": wallet["wallet_id"],
                        "agent_id": agent["agent_id"],
                        "amount": amount,
                        "timestamp": ts.isoformat(),
                        "strength": round(rng.uniform(0.10, 0.30), 3),
                    }
                )

    # Bulk insert transaction nodes.
    for i in range(0, len(txns), 500):
        chunk = txns[i : i + 500]
        await client.execute_write(
            """
            UNWIND $rows AS row
            MERGE (t:Transaction {tx_id: row.tx_id})
            SET t.type = row.type,
                t.amount = row.amount,
                t.timestamp = datetime(row.timestamp),
                t.status = row.status,
                t.flagged = row.flagged,
                t.flag_reason = row.flag_reason
            """,
            {"rows": chunk},
        )

    # SENT_TO edges.
    for i in range(0, len(sent_to), 500):
        chunk = sent_to[i : i + 500]
        await client.execute_write(
            """
            UNWIND $rows AS row
            MATCH (a:Wallet {wallet_id: row.src})
            MATCH (b:Wallet {wallet_id: row.dst})
            MERGE (a)-[r:SENT_TO {tx_id: row.tx_id}]->(b)
            SET r.amount = row.amount,
                r.timestamp = datetime(row.timestamp),
                r.type = row.type,
                r.strength = row.strength
            """,
            {"rows": chunk},
        )

    # CASHED_OUT_AT edges.
    for i in range(0, len(cashouts), 500):
        chunk = cashouts[i : i + 500]
        await client.execute_write(
            """
            UNWIND $rows AS row
            MATCH (w:Wallet {wallet_id: row.wallet_id})
            MATCH (a:Agent {agent_id: row.agent_id})
            MERGE (w)-[r:CASHED_OUT_AT {tx_id: row.tx_id}]->(a)
            SET r.amount = row.amount,
                r.timestamp = datetime(row.timestamp),
                r.strength = row.strength
            """,
            {"rows": chunk},
        )

    # CASHED_IN_AT edges.
    for i in range(0, len(cashins), 500):
        chunk = cashins[i : i + 500]
        await client.execute_write(
            """
            UNWIND $rows AS row
            MATCH (w:Wallet {wallet_id: row.wallet_id})
            MATCH (a:Agent {agent_id: row.agent_id})
            MERGE (w)-[r:CASHED_IN_AT {tx_id: row.tx_id}]->(a)
            SET r.amount = row.amount,
                r.timestamp = datetime(row.timestamp)
            """,
            {"rows": chunk},
        )

    logger.info(
        "seed.neo4j.transactions",
        txns=len(txns),
        sent_to=len(sent_to),
        cashouts=len(cashouts),
        cashins=len(cashins),
    )


# ---------------------------------------------------------------------------
# Postgres seeding
# ---------------------------------------------------------------------------


def _seed_postgres(state: GraphState, *, reset: bool) -> None:
    engine = get_sync_engine()
    if reset:
        logger.info("seed.postgres.wipe.start")
        Base.metadata.drop_all(engine)
        logger.info("seed.postgres.wipe.complete")
    Base.metadata.create_all(engine)

    with get_sync_session() as session:
        _seed_users(session, state)
        operators = _seed_operators(session, state)
        agencies = _seed_agencies(session)
        rules = _seed_rules(session, state)
        _seed_takedowns(session, state)
        _seed_le_cases(session, state, agencies)
        _seed_alerts(session, state, rules)
        _seed_shared_flags(session, state, operators)
        session.commit()


def _seed_users(session: Session, state: GraphState) -> None:
    rng = state.rng
    fixtures = [
        ("admin", "admin@fraudnet.local", "admin"),
        ("noc-lead", "lead@fraudnet.local", "senior_investigator"),
        ("inv-1", "inv1@fraudnet.local", "investigator"),
        ("inv-2", "inv2@fraudnet.local", "investigator"),
        ("analyst-1", "analyst1@fraudnet.local", "analyst"),
        ("analyst-2", "analyst2@fraudnet.local", "analyst"),
        ("viewer-1", "viewer1@fraudnet.local", "viewer"),
    ]
    for username, email, role in fixtures:
        session.merge(
            User(
                id=_new_id("user"),
                username=username,
                email=email,
                # Demo only — bcrypt hash of "demo123" generated once and pinned.
                password_hash="$2b$12$qwIYMUN3L.1Zv.TNQFZKiOe23RGf3wkqVRxNNiyz6YzZjBI5EYqX.",
                role=role,
                last_login=_rand_dt(rng, days_back=14),
            )
        )
    logger.info("seed.postgres.users", count=len(fixtures))


def _seed_operators(session: Session, state: GraphState) -> list[ExternalOperator]:
    rng = state.rng
    operators_data = [
        ("AirtelTigo Money", "Kwame Boateng", "kwame.boateng@airteltigo.gh", "connected", True),
        ("Telecel Cash", "Ama Owusu", "ama.owusu@telecel.gh", "connected", True),
        ("G-Money", "Yaw Mensah", "yaw.mensah@gtbank.com.gh", "pending", False),
        ("Zeepay", "Nana Adjei", "nana.adjei@zeepay.gh", "disconnected", False),
    ]
    operators: list[ExternalOperator] = []
    for name, contact, email, status, auto in operators_data:
        op = ExternalOperator(
            id=_new_id("op"),
            name=name,
            contact_name=contact,
            contact_email=email,
            technical_contact=email.replace("@", ".tech@"),
            status=status,
            integration_type="bidirectional" if status == "connected" else "inbound_only",
            data_sharing_level=rng.choice(["hashed", "partial", "clear_partial"]),
            masking_rules={"msisdn": "hash", "imei": "hash", "wallet_id": "partial"},
            auto_integrate=auto,
            onboarding_step="complete" if status == "connected" else "awaiting_credentials",
            last_health_check=_rand_dt(rng, days_back=2) if status != "pending" else None,
            last_health_status="healthy"
            if status == "connected"
            else ("degraded" if status == "disconnected" else None),
        )
        session.add(op)
        operators.append(op)
    session.flush()
    logger.info("seed.postgres.operators", count=len(operators))
    return operators


def _seed_agencies(session: Session) -> dict[str, LEAgency]:
    agencies = {
        "CID": LEAgency(
            id=_new_id("agency"),
            name="Ghana Police CID",
            type="police",
            contact_name="DSP Akua Sarpong",
            contact_email="cid.cybercrime@police.gov.gh",
            contact_phone="+233302773906",
        ),
        "EOCO": LEAgency(
            id=_new_id("agency"),
            name="Economic and Organised Crime Office",
            type="financial_crime",
            contact_name="Director K. Asante",
            contact_email="director@eoco.gov.gh",
            contact_phone="+233302667700",
        ),
        "NCA": LEAgency(
            id=_new_id("agency"),
            name="National Communications Authority",
            type="regulator",
            contact_name="J. Owusu-Ansah",
            contact_email="enforcement@nca.org.gh",
            contact_phone="+233302771701",
        ),
    }
    for a in agencies.values():
        session.add(a)
    session.flush()
    logger.info("seed.postgres.agencies", count=len(agencies))
    return agencies


def _seed_rules(session: Session, state: GraphState) -> list[Rule]:
    rng = state.rng
    rules: list[Rule] = []
    statuses = ["live"] * 10 + ["shadow"] * 3 + ["backtesting"] * 3 + ["draft"] * 4
    rng.shuffle(statuses)
    templates = [
        ("High-velocity P2P transfers", "node.tx_count_5m", "greater_than", 8, "freeze_wallet"),
        ("Cross-network bursting", "node.cross_network_24h", "greater_than", 3, "block_cross_network"),
        ("Sleeper wallet awakening", "node.dormant_days", "greater_than", 90, "apply_send_with_care"),
        ("Structured cashouts", "node.round_amount_streak", "greater_than", 5, "freeze_wallet"),
        ("Risk score critical", "node.risk_score", "greater_than", 0.85, "escalate_to_investigator"),
        (
            "Agent fraud concentration",
            "agent.fraud_cashout_rate",
            "greater_than",
            0.30,
            "downgrade_agent_float",
        ),
        ("KYC tier mismatch", "node.kyc_tier", "less_than", 2, "force_kyc_reverification"),
        ("SIM swap chain", "node.sim_swap_count_30d", "greater_than", 2, "apply_ask_me_first"),
        ("Inbound op flag", "alert.source", "equals", "external_operator", "add_to_watchlist"),
        (
            "Cluster confidence high",
            "node.cluster_confidence",
            "greater_than",
            0.80,
            "escalate_to_investigator",
        ),
        ("Burst payouts", "node.payout_count_15m", "greater_than", 6, "reduce_transaction_limit"),
        ("Geo anomaly", "node.cell_distance_km", "greater_than", 250, "issue_customer_warning"),
        ("Recurring fraudster cashout", "agent.fraud_cashouts_24h", "greater_than", 4, "suspend_agent"),
        ("New wallet large outflow", "node.account_age_days", "less_than", 7, "apply_send_with_care"),
        (
            "Round amount cluster",
            "node.round_amount_pct_24h",
            "greater_than",
            0.85,
            "escalate_to_investigator",
        ),
        ("Inactive then high-value", "node.idle_days", "greater_than", 60, "apply_ask_me_first"),
        ("Watchlisted MSISDN", "node.on_watchlist", "equals", True, "block_cross_network"),
        ("Failed KYC velocity", "node.kyc_failures_24h", "greater_than", 3, "force_kyc_reverification"),
        ("Mass victim refund", "alert.type", "equals", "victim_complaint", "freeze_wallet"),
        (
            "Foreign IP login burst",
            "auth.foreign_ip_rate_24h",
            "greater_than",
            0.6,
            "force_kyc_reverification",
        ),
    ]
    for i in range(COUNTS.rules):
        name, field_path, op, value, action = templates[i % len(templates)]
        status = statuses[i]
        rule = Rule(
            id=_new_id("rule"),
            name=f"R{i + 1:02d}: {name}",
            description=f"Auto-generated demo rule. Triggers when {field_path} {op} {value}.",
            created_by="analyst-1",
            status=status,
            conditions={
                "operator": "AND",
                "conditions": [{"field": field_path, "op": op, "value": value}],
            },
            actions=[{"type": action, "params": {}}],
            scope={"node_types": ["wallet"]},
            evaluation_mode="realtime" if i % 3 != 0 else "scheduled",
            schedule_interval="5m" if i % 3 == 0 else None,
            approved_by="noc-lead" if status in ("live", "shadow") else None,
            approved_at=_rand_dt(rng, days_back=30) if status in ("live", "shadow") else None,
            trigger_count=rng.randint(0, 800) if status == "live" else 0,
            false_positive_count=rng.randint(0, 60) if status == "live" else 0,
        )
        session.add(rule)
        rules.append(rule)

    session.flush()

    # A scattering of trigger history for live rules.
    live_rules = [r for r in rules if r.status == "live"]
    triggers_added = 0
    for rule in live_rules:
        for _ in range(min(rule.trigger_count, rng.randint(2, 12))):
            wallet = rng.choice(state.wallets)
            session.add(
                RuleTrigger(
                    id=_new_id("trig"),
                    rule_id=rule.id,
                    triggered_at=_rand_dt(rng, days_back=14),
                    event_id=f"EVT-{uuid.uuid4().hex[:10]}",
                    node_id=wallet["wallet_id"],
                    node_type="wallet",
                    context={"risk_score": wallet["risk_score"], "status": wallet["status"]},
                    actions_executed=[{"type": rule.actions[0]["type"], "status": "ok"}],
                    outcome=rng.choices(["success", "overridden", "failed"], weights=[0.8, 0.15, 0.05])[0],
                )
            )
            triggers_added += 1

    logger.info("seed.postgres.rules", count=len(rules), triggers=triggers_added)
    return rules


def _seed_takedowns(session: Session, state: GraphState) -> None:
    rng = state.rng
    eligible_clusters = [
        c for c in state.clusters if c["status"] in ("takedown_pending", "takedown_complete", "investigating")
    ]
    if len(eligible_clusters) < COUNTS.takedowns:
        eligible_clusters = eligible_clusters + rng.sample(state.clusters, COUNTS.takedowns)

    statuses = [
        "pending",
        "approved",
        "in_progress",
        "completed",
        "completed",
        "completed",
        "completed",
        "in_progress",
    ]
    rng.shuffle(statuses)
    step_types = [
        "freeze_wallets",
        "flag_sims",
        "alert_agents",
        "notify_law_enforcement",
        "generate_evidence_package",
    ]
    for i in range(COUNTS.takedowns):
        cluster = eligible_clusters[i]
        status = statuses[i]
        initiated = _rand_dt(rng, days_back=20)
        approved = initiated + timedelta(hours=rng.randint(2, 36)) if status != "pending" else None
        completed = approved + timedelta(hours=rng.randint(6, 96)) if status == "completed" else None
        td = Takedown(
            id=_new_id("td"),
            cluster_id=cluster["cluster_id"],
            initiated_by="inv-1",
            initiated_at=initiated,
            approved_by="noc-lead" if approved else None,
            approved_at=approved,
            status=status,
            wallets_frozen=rng.randint(4, 18) if status != "pending" else 0,
            sims_flagged=rng.randint(2, 12) if status != "pending" else 0,
            agents_alerted=rng.randint(0, 3),
            completed_at=completed,
            summary=f"Coordinated takedown for {cluster['name']} (confidence "
            f"{cluster['confidence_score']:.2f}, est. value GHS "
            f"{cluster['estimated_fraud_value']:.0f}).",
        )
        session.add(td)
        for j, step_type in enumerate(step_types):
            if status == "pending":
                step_status = "pending"
                started = None
                step_completed = None
            elif status == "approved":
                step_status = "pending" if j > 0 else "ready"
                started = None
                step_completed = None
            elif status == "in_progress":
                # Earlier steps done, later still pending.
                threshold = rng.randint(1, len(step_types) - 1)
                if j < threshold:
                    step_status = "completed"
                elif j == threshold:
                    step_status = "in_progress"
                else:
                    step_status = "pending"
                started = approved + timedelta(hours=j) if step_status != "pending" else None
                step_completed = approved + timedelta(hours=j + 1) if step_status == "completed" else None
            else:  # completed
                step_status = "completed"
                started = approved + timedelta(hours=j)
                step_completed = approved + timedelta(hours=j + 1)
            session.add(
                TakedownStep(
                    id=_new_id("tdstep"),
                    takedown_id=td.id,
                    step_type=step_type,
                    status=step_status,
                    started_at=started,
                    completed_at=step_completed,
                    detail={"step_index": j},
                )
            )
    logger.info("seed.postgres.takedowns", count=COUNTS.takedowns)


def _seed_le_cases(session: Session, state: GraphState, agencies: dict[str, LEAgency]) -> None:
    rng = state.rng
    case_states = [
        ("under_review", "CID"),
        ("active_investigation", "EOCO"),
        ("evidence_requested", "CID"),
        ("active_investigation", "CID"),
        ("closed_no_action", "NCA"),
        ("active_investigation", "EOCO"),
    ]
    for _i, (status, agency_key) in enumerate(case_states):
        agency = agencies[agency_key]
        clusters_picked = rng.sample(state.clusters, k=min(rng.randint(1, 2), len(state.clusters)))
        case = LECase(
            id=_new_id("case"),
            agency_id=agency.id,
            status=status,
            cluster_ids=[c["cluster_id"] for c in clusters_picked],
            created_by="inv-1",
            created_at=_rand_dt(rng, days_back=45),
            assigned_officer=agency.contact_name,
            officer_contact=agency.contact_email,
            notes=f"Case package referencing {len(clusters_picked)} cluster(s).",
        )
        session.add(case)
        # Two-three message thread per case.
        for j in range(rng.randint(2, 4)):
            session.add(
                LECaseMessage(
                    id=_new_id("msg"),
                    case_id=case.id,
                    sender_id="inv-1" if j % 2 == 0 else agency.id,
                    sender_role="investigator" if j % 2 == 0 else "law_enforcement",
                    content=(
                        "Initial referral with full evidence package attached."
                        if j == 0
                        else "Acknowledged. Officer assigned, awaiting subpoena."
                        if j == 1
                        else "Subpoena returned, please share latest fund-flow chart."
                    ),
                    timestamp=case.created_at + timedelta(hours=j * 18),
                )
            )
    logger.info("seed.postgres.le_cases", count=len(case_states))


def _seed_alerts(session: Session, state: GraphState, rules: list[Rule]) -> None:
    rng = state.rng
    severities = ["low", "medium", "high", "critical"]
    severity_weights = [0.30, 0.40, 0.22, 0.08]
    types = [
        "rule_trigger",
        "cluster_expansion",
        "cluster_confidence_change",
        "agent_risk_change",
        "sleeper_awakening",
        "campaign_detected",
        "sim_swap_burst",
        "victim_complaint",
        "external_inbound_flag",
    ]
    for _i in range(COUNTS.alerts):
        severity = rng.choices(severities, weights=severity_weights)[0]
        a_type = rng.choice(types)
        target_wallet = rng.choice(state.wallets)
        rule = rng.choice(rules) if a_type == "rule_trigger" else None
        cluster_id = target_wallet.get("cluster_id") or rng.choice(state.clusters)["cluster_id"]
        acknowledged = rng.random() < 0.45
        session.add(
            Alert(
                id=_new_id("alert"),
                created_at=_rand_dt(rng, days_back=7),
                type=a_type,
                severity=severity,
                title={
                    "rule_trigger": f"Rule fired: {rule.name if rule else 'unknown'}",
                    "cluster_expansion": "Cluster expanded with 4 new high-confidence members",
                    "cluster_confidence_change": "Cluster confidence crossed critical threshold",
                    "agent_risk_change": "Agent risk classification escalated",
                    "sleeper_awakening": "Dormant wallet activated with high-value outflow",
                    "campaign_detected": "Repeating cashout pattern detected across 3 clusters",
                    "sim_swap_burst": "Multiple SIM swaps observed in 24h window",
                    "victim_complaint": "Customer complaint filed via SafeGuard",
                    "external_inbound_flag": "Flag received from connected operator",
                }[a_type],
                description=f"Automated alert generated for wallet {target_wallet['wallet_id']}.",
                target_type="wallet",
                target_id=target_wallet["wallet_id"],
                cluster_id=cluster_id,
                acknowledged=acknowledged,
                acknowledged_at=_rand_dt(rng, days_back=2) if acknowledged else None,
                acknowledged_by="inv-2" if acknowledged else None,
                rule_id=rule.id if rule else None,
                extra={"score_delta": round(rng.uniform(-0.2, 0.4), 3)},
            )
        )
    logger.info("seed.postgres.alerts", count=COUNTS.alerts)


def _seed_shared_flags(session: Session, state: GraphState, operators: list[ExternalOperator]) -> None:
    rng = state.rng
    connected = [o for o in operators if o.status == "connected"]
    if not connected:
        return
    for _ in range(28):
        op = rng.choice(connected)
        wallet = rng.choice(state.wallets)
        direction = rng.choice(["inbound", "outbound"])
        session.add(
            SharedFlag(
                id=_new_id("flag"),
                direction=direction,
                operator_id=op.id,
                identifier_type="msisdn",
                identifier_masked=f"+23324***{wallet['msisdn'][-4:]}",
                identifier_hash=_hash_id(wallet["msisdn"]),
                risk_score=round(rng.uniform(0.55, 0.95), 3),
                context=rng.choice(
                    [
                        "Customer reported as scammer in P2P fraud.",
                        "Multiple structuring patterns observed.",
                        "Linked to confirmed cashout ring.",
                        "Inbound complaint chain (3 victims).",
                    ]
                ),
                shared_at=_rand_dt(rng, days_back=10),
                action_taken=rng.choice(["accepted", "dismissed", "integrated", None, None]),
            )
        )
    logger.info("seed.postgres.shared_flags", count=28)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


async def main() -> None:
    parser = argparse.ArgumentParser(description="Seed FraudNet with realistic demo data.")
    parser.add_argument(
        "--reset", action="store_true", help="Wipe all existing data in Neo4j and Postgres before seeding."
    )
    parser.add_argument("--seed", type=int, default=20260507, help="RNG seed.")
    args = parser.parse_args()

    configure_logging()
    rng = random.Random(args.seed)
    state = GraphState(rng=rng)

    client = get_neo4j_client()
    await client.connect()

    try:
        if args.reset:
            await _wipe_graph(client)

        await initialize_schema(client)

        await _seed_cell_towers(client, state)
        await _seed_agents(client, state)
        await _seed_handsets(client, state)
        await _seed_sims_and_phones(client, state)
        await _seed_wallets(client, state)
        await _seed_clusters(client, state)
        await _seed_transactions(client, state)

        _seed_postgres(state, reset=args.reset)

        logger.info("seed.complete")
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
