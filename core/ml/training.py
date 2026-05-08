"""Behavioural-model training.

Trains a logistic-regression baseline on per-wallet features pulled
from Neo4j. The model is small, fast to retrain, and serves as the
champion until a richer LightGBM / GNN model overtakes it. Models are
versioned and stored under :data:`MODEL_DIR`; the most recent is
symlinked as ``current``.

This is intentionally simple — it's a real model, not a stub — so the
inference path has something concrete to load. Production training
moves to a managed pipeline once the lakehouse is wired up.
"""

from __future__ import annotations

import os
import pickle
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from config.logging import get_logger

from .features import FEATURE_NAMES, WalletFeatures, fetch_population

logger = get_logger(__name__)


MODEL_DIR = Path(os.environ.get("FRAUDNET_MODEL_DIR", "/app/models"))
CURRENT_MODEL = "behavioural-current.pkl"


@dataclass
class TrainingResult:
    model_id: str
    sample_size: int
    positives: int
    negatives: int
    metrics: dict[str, float]
    feature_names: tuple[str, ...]
    saved_path: str


@dataclass
class TrainedModel:
    """Pickle-friendly container holding the scaler + classifier."""

    scaler: StandardScaler
    clf: LogisticRegression
    feature_names: tuple[str, ...]
    trained_at: str
    metrics: dict[str, float]

    def predict_proba(self, vectors: list[list[float]]) -> list[float]:
        if not vectors:
            return []
        X = self.scaler.transform(vectors)
        # column 1 = probability of class 1 (fraud-linked)
        return [float(p) for p in self.clf.predict_proba(X)[:, 1]]


async def train(*, sample_size: int = 1000, save: bool = True) -> TrainingResult:
    """Pull a sample, fit a logistic regression, persist, return metrics."""

    samples: list[WalletFeatures] = await fetch_population(limit=sample_size)
    if not samples:
        raise ValueError("training failed: no wallets in the graph")

    X = [s.vector for s in samples]
    y = [s.label for s in samples]
    positives = sum(y)
    negatives = len(y) - positives
    if positives < 5 or negatives < 5:
        raise ValueError(
            f"training failed: not enough class diversity (positives={positives}, negatives={negatives})"
        )

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)

    scaler = StandardScaler().fit(X_train)
    Xs = scaler.transform(X_train)
    Xt = scaler.transform(X_test)

    clf = LogisticRegression(max_iter=500, class_weight="balanced", random_state=42)
    clf.fit(Xs, y_train)

    y_pred = clf.predict(Xt)
    y_proba = clf.predict_proba(Xt)[:, 1]

    metrics = {
        "precision": float(precision_score(y_test, y_pred, zero_division=0)),
        "recall": float(recall_score(y_test, y_pred, zero_division=0)),
        "f1": float(f1_score(y_test, y_pred, zero_division=0)),
        "auc": float(roc_auc_score(y_test, y_proba)) if len(set(y_test)) > 1 else 0.0,
    }

    model = TrainedModel(
        scaler=scaler,
        clf=clf,
        feature_names=FEATURE_NAMES,
        trained_at=datetime.now(UTC).isoformat(),
        metrics=metrics,
    )

    saved_path = ""
    if save:
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        model_id = "behavioural-" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S") + ".pkl"
        path = MODEL_DIR / model_id
        with path.open("wb") as fh:
            pickle.dump(model, fh)
        # Update the "current" symlink atomically.
        current = MODEL_DIR / CURRENT_MODEL
        tmp = MODEL_DIR / (CURRENT_MODEL + ".tmp")
        if tmp.exists():
            tmp.unlink()
        tmp.symlink_to(model_id)
        os.replace(tmp, current)
        saved_path = str(path)
        logger.info("ml.training.saved", path=saved_path, **metrics)

    return TrainingResult(
        model_id=Path(saved_path).name if saved_path else "in-memory",
        sample_size=len(samples),
        positives=positives,
        negatives=negatives,
        metrics=metrics,
        feature_names=FEATURE_NAMES,
        saved_path=saved_path,
    )


def load_current() -> TrainedModel | None:
    path = MODEL_DIR / CURRENT_MODEL
    if not path.exists():
        return None
    with path.open("rb") as fh:
        loaded: TrainedModel = pickle.load(fh)
        return loaded


__all__ = ["train", "load_current", "TrainedModel", "TrainingResult", "MODEL_DIR"]
