"""Load admin overrides from TOML and merge with package defaults."""

from __future__ import annotations

import logging
import os
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from data_assets.core.asset import Asset

logger = logging.getLogger(__name__)

DEFAULT_CONFIG: dict[str, Any] = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay_minutes": 5,
    "max_active_runs": 1,
    "secrets_source": "env",
    "enabled": False,
}

SCHEDULE_BY_MODE: dict[str, str | None] = {
    "full": "0 5 * * *",
    "forward": "@hourly",
    "backfill": None,
    "transform": "0 8 * * *",
}


def load_overrides(output_dir: Path) -> tuple[dict[str, Any], bool]:
    """Read dag_overrides.toml from *output_dir*.

    Returns (overrides_dict, file_existed). When the file is missing,
    returns ({}, False) so callers can distinguish a fresh run from
    an empty override file.
    """
    path = output_dir / "dag_overrides.toml"
    if not path.exists():
        return {}, False
    try:
        with open(path, "rb") as f:
            return tomllib.load(f), True
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


# ---------------------------------------------------------------------------
# TOML auto-registration helpers
# ---------------------------------------------------------------------------

def _default_schedule(asset_cls: type[Asset]) -> str | None:
    """Return the default schedule string for an asset class."""
    mode = str(asset_cls().default_run_mode)
    return SCHEDULE_BY_MODE.get(mode, "0 5 * * *")


def _build_entry(asset_cls: type[Asset], timestamp: str) -> str:
    """Build a TOML section for one asset (disabled, with commented schedule)."""
    schedule = _default_schedule(asset_cls)
    schedule_line = f"# schedule = \"{schedule}\"" if schedule else "# schedule = None"
    return (
        f"# Added {timestamp} by data-assets sync\n"
        f"[{asset_cls.name}]\n"
        f"enabled = false\n"
        f"{schedule_line}\n"
    )


def ensure_toml_entries(
    output_dir: Path,
    assets: dict[str, type[Asset]],
    toml_existed: bool,
) -> list[str]:
    """Ensure every registered asset has an entry in dag_overrides.toml.

    On a fresh run (toml_existed=False), creates the file with ALL assets.
    On subsequent runs, appends only newly discovered assets.

    Returns a list of asset names that were added.
    """
    path = output_dir / "dag_overrides.toml"
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    if toml_existed:
        existing_text = path.read_text()
        # Parse existing TOML to find which assets already have entries
        existing_keys = set(tomllib.loads(existing_text).keys())
    else:
        existing_text = ""
        existing_keys = set()

    new_assets = sorted(
        name for name in assets if name not in existing_keys
    )

    if not new_assets:
        return []

    lines: list[str] = []
    for name in new_assets:
        lines.append(_build_entry(assets[name], timestamp))

    # Build full content and write atomically (temp file + rename)
    new_block = "\n".join(lines) + "\n"
    if existing_text:
        separator = "" if existing_text.endswith("\n") else "\n"
        full_content = existing_text + separator + new_block
    else:
        full_content = new_block

    tmp_path = path.with_suffix(".toml.tmp")
    tmp_path.write_text(full_content)
    os.replace(tmp_path, path)

    logger.info(
        "Added %d new asset(s) to dag_overrides.toml: %s",
        len(new_assets), ", ".join(new_assets),
    )
    return new_assets
