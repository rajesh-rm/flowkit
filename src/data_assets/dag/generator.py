"""DAG file generator — the core of ``data-assets sync``."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from data_assets.core.asset import Asset
from data_assets.core.registry import all_assets, discover
from data_assets.dag.fingerprint import compute_fingerprint
from data_assets.dag.overrides import ensure_toml_entries, load_overrides, merge_config
from data_assets.dag.templates import (
    DAG_TEMPLATE,
    DISABLED_TEMPLATE,
    MANAGED_MARKER,
    SOURCE_SECRETS_MAP,
)

logger = logging.getLogger(__name__)


@dataclass
class SyncResult:
    """Summary of a sync run."""

    created: list[str] = field(default_factory=list)
    updated: list[str] = field(default_factory=list)
    disabled: list[str] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)
    inactive: list[str] = field(default_factory=list)


def sync(output_dir: Path) -> SyncResult:
    """Synchronise DAG files in *output_dir* with registered assets.

    1. Discovers all assets from the installed package.
    2. Ensures every asset has an entry in ``dag_overrides.toml``
       (creates the file on a fresh run, appends new assets otherwise).
    3. Reads admin overrides from ``dag_overrides.toml``.
    4. Generates (or regenerates) a DAG file per asset.
       Assets with ``enabled = false`` get ``schedule=None``.
    5. Disables orphan managed files whose asset is no longer registered.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    discover()
    assets = all_assets()

    # Ensure TOML has entries for all assets (append-only)
    overrides, toml_existed = load_overrides(output_dir)
    ensure_toml_entries(output_dir, assets, toml_existed)

    # Reload overrides after potential TOML changes
    overrides, _ = load_overrides(output_dir)

    result = SyncResult()
    expected_files: set[str] = set()

    for name, asset_cls in sorted(assets.items()):
        config = merge_config(asset_cls, overrides)
        enabled = config.get("enabled", False)

        # Disabled assets get schedule=None (DAG exists but won't auto-run)
        if not enabled:
            config["schedule"] = None
            result.inactive.append(name)

        fingerprint = compute_fingerprint(asset_cls)
        _generate_asset_dags(
            asset_cls, name, config, fingerprint, output_dir, expected_files, result,
        )

    _warn_orphan_overrides(overrides, set(assets.keys()))
    _disable_orphan_files(output_dir, expected_files, result)
    return result


def _generate_asset_dags(
    asset_cls: type[Asset], name: str, config: dict[str, Any],
    fingerprint: str, output_dir: Path, expected_files: set[str],
    result: SyncResult,
) -> None:
    """Generate DAG file(s) for one asset — multi-org or single-tenant."""
    orgs = config.get("orgs")
    if orgs and isinstance(orgs, list):
        for org_cfg in orgs:
            org_name = org_cfg["org"]
            org_slug = org_name.lower().replace("-", "_").replace("/", "_")
            filename = f"dag_{name}_{org_slug}.py"
            expected_files.add(filename)
            content = _render_multi_org(asset_cls, config, fingerprint, org_cfg)
            _write_dag(output_dir, filename, content, result)
    else:
        filename = f"dag_{name}.py"
        expected_files.add(filename)
        content = _render_dag(asset_cls, config, fingerprint)
        _write_dag(output_dir, filename, content, result)


def _warn_orphan_overrides(
    overrides: dict[str, Any], registered_names: set[str],
) -> None:
    """Warn about override entries for unregistered assets."""
    for key in overrides:
        if key not in registered_names and key != "defaults":
            logger.warning(
                "dag_overrides.toml has entry for '%s' which is no longer "
                "registered in the data-assets package. Consider removing it.",
                key,
            )


def _disable_orphan_files(
    output_dir: Path, expected_files: set[str], result: SyncResult,
) -> None:
    """Disable managed DAG files whose asset is no longer registered."""
    for path in sorted(output_dir.glob("dag_*.py")):
        if path.name in expected_files or path.name.startswith("z_"):
            continue
        if _is_managed(path):
            _disable_orphan(path, result)


def _write_dag(
    output_dir: Path, filename: str, content: str, result: SyncResult,
) -> None:
    """Write a DAG file, tracking whether it was created or updated."""
    path = output_dir / filename
    if path.exists():
        old = path.read_text()
        if old == content:
            result.skipped.append(filename)
            return
        result.updated.append(filename)
    else:
        result.created.append(filename)
    path.write_text(content)


def _render_dag(
    asset_cls: type[Asset], config: dict[str, Any], fingerprint: str,
) -> str:
    """Render a single-tenant DAG file."""
    secrets_source = config.get("secrets_source", "env")
    source_name = asset_cls.source_name or ""

    if secrets_source == "airflow_connection" and source_name in SOURCE_SECRETS_MAP:
        mapping = SOURCE_SECRETS_MAP[source_name]
        connection_id = config.get("connection_id", mapping["default_connection_id"])
        run_fn = _build_run_connection(asset_cls.name, config, connection_id, mapping)
        subtitle = " (Airflow Connection secrets)"
    else:
        run_fn = _build_run_env(asset_cls.name, config)
        subtitle = ""

    return DAG_TEMPLATE.substitute(
        marker=MANAGED_MARKER,
        asset_name=asset_cls.name,
        dag_id=asset_cls.name,
        fingerprint=fingerprint,
        subtitle=subtitle,
        run_function=run_fn,
        **_common_vars(config),
    )


def _render_multi_org(
    asset_cls: type[Asset],
    config: dict[str, Any],
    fingerprint: str,
    org_cfg: dict[str, str],
) -> str:
    """Render a per-org DAG file for multi-org assets."""
    source_name = asset_cls.source_name or ""
    mapping = SOURCE_SECRETS_MAP.get(source_name, {})
    connection_id = config.get("connection_id", mapping.get("default_connection_id", ""))
    org_name = org_cfg["org"]
    org_slug = org_name.lower().replace("-", "_").replace("/", "_")

    run_fn = _build_run_multi_org(
        asset_cls.name, config, connection_id, mapping, org_cfg,
    )

    return DAG_TEMPLATE.substitute(
        marker=MANAGED_MARKER,
        asset_name=asset_cls.name,
        dag_id=f"{asset_cls.name}_{org_slug}",
        fingerprint=fingerprint,
        subtitle=f" (org: {org_name})",
        run_function=run_fn,
        **_common_vars(config),
    )


def _common_vars(config: dict[str, Any]) -> dict[str, str]:
    """Extract template variables common to all DAG templates."""
    schedule = config.get("schedule")
    schedule_str = f'"{schedule}"' if schedule is not None else "None"

    tags = sorted(set(config.get("tags", [])))

    return {
        "run_mode": config.get("run_mode", "full"),
        "schedule": schedule_str,
        "owner": config.get("owner", "data-engineering"),
        "retries": str(config.get("retries", 3)),
        "retry_delay_minutes": str(config.get("retry_delay_minutes", 5)),
        "max_active_runs": str(config.get("max_active_runs", 1)),
        "tags": repr(tags),
        "description": _escape_description(config.get("description", "")),
    }


def _build_run_env(asset_name: str, config: dict[str, Any]) -> str:
    """Build a _run() function body that uses env-var-based secrets."""
    mode = config.get("run_mode", "full")
    return (
        'def _run(**context):\n'
        '    from data_assets import run_asset\n'
        '\n'
        '    return run_asset(\n'
        f'        asset_name="{asset_name}",\n'
        f'        run_mode="{mode}",\n'
        '        asset_fingerprint=_ASSET_FINGERPRINT,\n'
        '        airflow_run_id=context.get("run_id"),\n'
        '    )'
    )


def _build_run_connection(
    asset_name: str, config: dict[str, Any],
    connection_id: str, mapping: dict,
) -> str:
    """Build a _run() function body that reads from an Airflow Connection."""
    mode = config.get("run_mode", "full")
    secrets_block = _build_secrets_block(mapping)
    return (
        'def _run(**context):\n'
        '    from airflow.sdk import BaseHook\n'
        '    from data_assets import run_asset\n'
        '\n'
        f'    conn = BaseHook.get_connection("{connection_id}")\n'
        '    extra = conn.extra_dejson or {}\n'
        '    secrets = {\n'
        f'{secrets_block}\n'
        '    }\n'
        '    return run_asset(\n'
        f'        asset_name="{asset_name}",\n'
        f'        run_mode="{mode}",\n'
        '        secrets=secrets,\n'
        '        asset_fingerprint=_ASSET_FINGERPRINT,\n'
        '        airflow_run_id=context.get("run_id"),\n'
        '    )'
    )


def _build_run_multi_org(
    asset_name: str, config: dict[str, Any],
    connection_id: str, mapping: dict, org_cfg: dict[str, str],
) -> str:
    """Build a _run() function body for a specific org."""
    mode = config.get("run_mode", "full")
    org_name = org_cfg["org"]

    # Build secrets from field_map (shared creds) + org-specific overrides
    lines: list[str] = []
    for env_var, conn_attr in mapping.get("field_map", {}).items():
        lines.append(f'        "{env_var}": conn.{conn_attr} or "",')
    for env_var, extra_key in mapping.get("extra_map", {}).items():
        # Use org-specific value if available, else fall back to Connection extra
        org_value = org_cfg.get(extra_key)
        if org_value is not None:
            lines.append(f'        "{env_var}": "{org_value}",')
        else:
            lines.append(f'        "{env_var}": extra.get("{extra_key}", ""),')
    secrets_block = "\n".join(lines)

    return (
        'def _run(**context):\n'
        '    from airflow.sdk import BaseHook\n'
        '    from data_assets import run_asset\n'
        '\n'
        f'    conn = BaseHook.get_connection("{connection_id}")\n'
        '    extra = conn.extra_dejson or {}\n'
        '    secrets = {\n'
        f'{secrets_block}\n'
        '    }\n'
        '    return run_asset(\n'
        f'        asset_name="{asset_name}",\n'
        f'        run_mode="{mode}",\n'
        f'        partition_key="{org_name}",\n'
        '        secrets=secrets,\n'
        '        asset_fingerprint=_ASSET_FINGERPRINT,\n'
        '        airflow_run_id=context.get("run_id"),\n'
        '    )'
    )


def _build_secrets_block(mapping: dict) -> str:
    """Build the secrets dict lines for Airflow Connection templates."""
    lines: list[str] = []
    for env_var, conn_attr in mapping.get("field_map", {}).items():
        lines.append(f'        "{env_var}": conn.{conn_attr} or "",')

    host_env = mapping.get("host_env")
    if host_env:
        lines.append(f'        "{host_env}": f"https://{{conn.host}}" if conn.host else "",')

    for env_var, extra_key in mapping.get("extra_map", {}).items():
        lines.append(f'        "{env_var}": extra.get("{extra_key}", ""),')

    return "\n".join(lines)


def _escape_description(desc: str) -> str:
    """Escape a description string for safe embedding in a Python string literal."""
    return desc.replace("\\", "\\\\").replace('"', '\\"')


def _is_managed(path: Path) -> bool:
    """Check whether a file was generated by data-assets sync."""
    try:
        first_line = path.read_text().split("\n", 1)[0]
        return first_line.strip() == MANAGED_MARKER
    except OSError:
        return False


def _disable_orphan(path: Path, result: SyncResult) -> None:
    """Rewrite a managed DAG file as a disabled stub."""
    # Extract dag_id from filename: dag_<name>.py → <name>
    stem = path.stem
    asset_name = stem.removeprefix("dag_")

    content = DISABLED_TEMPLATE.substitute(
        marker=MANAGED_MARKER,
        asset_name=asset_name,
        dag_id=asset_name,
    )
    path.write_text(content)
    result.disabled.append(path.name)
    logger.info("Disabled orphan DAG file: %s", path.name)
