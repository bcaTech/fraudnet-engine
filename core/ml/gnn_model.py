"""Graph neural network model — k-hop scoring over the cluster subgraph.

This module is intentionally a clean placeholder. PyTorch + PyTorch
Geometric live in the optional ``[ml]`` extras (heavy + platform-
specific) and aren't installed in the default container, so the GNN
training and inference paths are stubs that fall back to the
behavioural baseline. The shape of the public API is fixed so that
swapping in a real GNN later doesn't require call-site changes.

Design (when implemented):

- **Architecture:** GraphSAGE or GAT, 2-3 layers, hidden dim 64.
- **Inputs:** node features from :mod:`core.ml.features` plus edge type
  embeddings.
- **Training:** semi-supervised — labels are cluster membership for
  known fraud nodes; unlabelled nodes contribute via neighbourhood
  aggregation only.
- **Inference:** ``score_node(wallet_id)`` runs a 2-hop subgraph through
  the model and returns a `[0, 1]` risk probability.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from config.logging import get_logger

logger = get_logger(__name__)


@dataclass
class GNNScore:
    wallet_id: str
    score: float
    model: str
    note: str | None = None


def _torch_available() -> bool:
    try:
        import torch  # noqa: F401
        import torch_geometric  # noqa: F401

        return True
    except Exception:  # noqa: BLE001
        return False


async def score_node(wallet_id: str) -> GNNScore:
    """Score a single wallet via the GNN. Falls back to the behavioural
    baseline when torch / torch-geometric aren't available."""

    if _torch_available():
        # Hook for the real implementation. Until the GNN is trained,
        # log + fall through.
        logger.info("ml.gnn.placeholder", wallet_id=wallet_id)

    from .inference import score_wallets

    out = await score_wallets([wallet_id], persist=False)
    if not out.get("scores"):
        return GNNScore(
            wallet_id=wallet_id,
            score=0.0,
            model="behavioural-baseline",
            note="no score available",
        )
    score = float(out["scores"][0]["score"])
    return GNNScore(
        wallet_id=wallet_id,
        score=score,
        model="behavioural-baseline",
        note="GNN not trained yet — using behavioural fallback",
    )


async def evaluate_performance() -> dict[str, Any]:
    """Run the offline evaluation against the current model. When the
    GNN is in place this will return GNN-specific drift / calibration
    metrics; for now it forwards to the behavioural evaluator."""

    from .evaluation import evaluate_current

    return await evaluate_current()
