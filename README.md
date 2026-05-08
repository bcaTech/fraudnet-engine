# FraudNet Intelligence Engine

AI-native fraud network intelligence backend for mobile money. See `CLAUDE.md` for the full
specification.

## Quickstart

```bash
cp .env.example .env
docker compose up -d neo4j postgres redis minio kafka
docker compose up api
```

The API serves at <http://localhost:8000>. OpenAPI docs at `/docs`.

## Local development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e .[dev]
pytest -m "not integration"
```

## Project layout

The full module tree is documented in `CLAUDE.md`. Key entry points:

- `api/main.py` — FastAPI application factory
- `core/mesh/expansion.py` — Breadth-first mesh expansion from a fraud seed
- `core/mesh/scoring.py` — Node confidence scoring
- `core/graph/schema.py` — Neo4j constraints and indexes (run on startup)
- `tasks/celery_app.py` — Periodic mesh maintenance, decay, batch scoring
