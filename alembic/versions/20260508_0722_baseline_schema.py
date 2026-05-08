"""baseline schema — captures the table set as of 2026-05-08.

This baseline uses ``Base.metadata.create_all`` rather than the usual
hand-written ``op.create_table`` calls. We started shipping with the
schema bootstrapped through SQLAlchemy directly (and an
``Base.metadata.create_all`` step in the API lifespan for dev), so by
the time Alembic landed every table already existed in production.
This migration:

- Lets a *fresh* database catch up by recreating every table from
  ``Base.metadata`` in one shot.
- Lets an *existing* database be marked as already-applied via
  ``alembic stamp 20260508_0001`` without any DDL changes.

Subsequent migrations must use proper ``op.create_table`` /
``op.add_column`` calls so that the delta is reviewable and
reversible.

Revision ID: 20260508_0001
Revises:
Create Date: 2026-05-08
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

from db.models import Base

# revision identifiers, used by Alembic.
revision: str = "20260508_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create every table that exists in the current Base.metadata.

    Idempotent against partially-bootstrapped databases — uses
    SQLAlchemy's checkfirst=True implicitly via create_all.
    """

    Base.metadata.create_all(op.get_bind())


def downgrade() -> None:
    """Drop every table that the baseline created.

    Order matters because of foreign keys; metadata.drop_all sorts
    automatically.
    """

    Base.metadata.drop_all(op.get_bind())
