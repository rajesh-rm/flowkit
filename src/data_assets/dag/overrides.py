"""Load admin overrides from TOML and merge with package defaults."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from data_assets.core.asset import Asset

DEFAULT_CONFIG: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay_minutes": 5,
    "max_active_runs": 1,
    "secrets_source": "env",
}

SCHEDULE_BY_MODE: dict[str, str | None] = {
    "full": "0 5 * * *",
    "forward": "@hourly",
    "backfill": None,
    "transform": "0 8 * * *",
}


def load_overrides(output_dir: Path) -> dict[str, Any]:
    """Read dag_overrides.toml from *output_dir*. Returns {} if missing."""
    path = output_dir / "dag_overrides.toml"
    if not path.exists():
        return {}
    try:
        with open(path, "rb") as f:
            return tomllib.load(f)
    except tomllib.TOMLDecodeError as exc:
        raise ValueError(f"Invalid TOML in {path}: {exc}") from exc


def merge_config(asset_cls: type[Asset], overrides: dict[str, Any]) -> dict[str, Any]:
    """Merge package defaults, asset dag_config, and admin overrides.

    Priority (highest wins): admin overrides > dag_config > package defaults.
    """
    asset = asset_cls()
    mode = str(asset.default_run_mode)
    source = asset.source_name or "transform"

    # Layer 1: package defaults
    config: dict[str, Any] = {
        **DEFAULT_CONFIG,
        "schedule": SCHEDULE_BY_MODE.get(mode, "0 5 * * *"),
        "tags": [source],
        "description": asset.description,
        "run_mode": mode,
    }

    # Layer 2: developer overrides from asset class
    config.update(asset.dag_config)

    # Layer 3: admin overrides from TOML
    asset_overrides = overrides.get(asset.name, {})
    config.update(asset_overrides)

    # Validate orgs structure if present (consumed by generator)
    orgs = config.get("orgs")
    if orgs is not None:
        if not isinstance(orgs, list) or not all(isinstance(o, dict) for o in orgs):
            raise ValueError(
                f"dag_overrides.toml: '{asset.name}.orgs' must be a list of "
                f"tables (e.g., [[{asset.name}.orgs]]), got {type(orgs).__name__}"
            )
        for i, entry in enumerate(orgs):
            if "org" not in entry:
                raise ValueError(
                    f"dag_overrides.toml: '{asset.name}.orgs[{i}]' is missing "
                    f"required key 'org'"
                )

    return config
