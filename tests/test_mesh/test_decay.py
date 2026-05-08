"""Unit tests for the temporal decay engine."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from core.mesh.decay import (
    apply_decay,
    days_between,
    decayed_strength,
    half_life_for,
    should_prune,
)


def test_half_life_halves_strength() -> None:
    s = apply_decay(strength=1.0, half_life_days=30, days_elapsed=30)
    assert abs(s - 0.5) < 1e-6


def test_two_half_lives_quarters_strength() -> None:
    s = apply_decay(strength=1.0, half_life_days=30, days_elapsed=60)
    assert abs(s - 0.25) < 1e-6


def test_zero_elapsed_preserves_strength() -> None:
    assert apply_decay(0.7, 30, 0) == 0.7


def test_decayed_strength_uses_edge_type_half_life() -> None:
    now = datetime(2024, 1, 31, tzinfo=timezone.utc)
    last = datetime(2024, 1, 1, tzinfo=timezone.utc)
    s = decayed_strength("SENT_TO", base_strength=0.8, last_seen=last, now=now)
    expected = 0.8 * 0.5  # 30 days at 30-day half-life
    assert abs(s - expected) < 1e-6


def test_days_between_handles_naive_datetimes() -> None:
    a = datetime(2024, 1, 1)
    b = datetime(2024, 1, 11)
    assert days_between(a, b) == 10.0


def test_should_prune_below_threshold() -> None:
    assert should_prune(0.01) is True
    assert should_prune(0.99) is False


def test_half_life_for_unknown_returns_default() -> None:
    assert half_life_for("WHATEVER") == 30.0


def test_decay_with_negative_half_life_is_noop() -> None:
    assert apply_decay(0.5, -1, 30) == 0.5


def test_decay_after_long_elapsed_approaches_zero() -> None:
    s = apply_decay(strength=1.0, half_life_days=14, days_elapsed=365)
    assert s < 1e-6
