"""Deterministic fingerprint for asset DAG definitions.

The fingerprint captures all asset attributes that affect DAG behavior.
A mismatch between the fingerprint embedded in a generated DAG file and
the current asset definition signals drift (stale DAG file or package
version skew between Airflow scheduler and workers).
"""

from __future__ import annotations

import hashlib
import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from data_assets.core.asset import Asset


def compute_fingerprint(asset_cls: type[Asset]) -> str:
    """Compute a deterministic hash of the DAG-relevant asset attributes.

    Returns the first 16 hex characters of a SHA-256 digest.
    """
    asset = asset_cls()

    canonical = {
        "name": asset.name,
        "default_run_mode": str(asset.default_run_mode),
        "source_name": asset.source_name,
        "parent_asset_name": getattr(asset, "parent_asset_name", None),
        "source_tables": getattr(asset, "source_tables", []),
        "dag_config": asset.dag_config,
        "version": _get_version(),
    }

    payload = json.dumps(canonical, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def _get_version() -> str:
    from data_assets import __version__

    return __version__
