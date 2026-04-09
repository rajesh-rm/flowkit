"""Command-line interface for data-assets.

Usage:
    data-assets list [--json] [--source <name>]
    data-assets sync --output-dir <path>
    data-assets fingerprint <asset_name>
    data-assets setup-systemd --output-dir <path> --dag-dir <path> [options]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="data-assets",
        description="DAG generation and lifecycle management for data-assets.",
    )
    sub = parser.add_subparsers(dest="command")

    # --- list ---
    p_list = sub.add_parser("list", help="List all registered assets")
    p_list.add_argument("--json", action="store_true", dest="as_json", help="JSON output")
    p_list.add_argument("--source", help="Filter by source_name")

    # --- sync ---
    p_sync = sub.add_parser("sync", help="Generate/update DAG files")
    p_sync.add_argument(
        "--output-dir", required=True, type=Path,
        help="Directory to write DAG files to",
    )

    # --- fingerprint ---
    p_fp = sub.add_parser("fingerprint", help="Print fingerprint for an asset")
    p_fp.add_argument("asset_name", help="Registered asset name")

    # --- setup-systemd ---
    p_sd = sub.add_parser("setup-systemd", help="Generate systemd unit files")
    p_sd.add_argument(
        "--output-dir", required=True, type=Path,
        help="Directory to write unit files to",
    )
    p_sd.add_argument("--dag-dir", required=True, help="Airflow DAGs directory")
    p_sd.add_argument("--venv-path", default="/opt/airflow/venv", help="Python venv path")
    p_sd.add_argument("--pip-index-url", default=None, help="Custom PyPI index URL")
    p_sd.add_argument("--interval", type=int, default=15, help="Sync interval in minutes")
    p_sd.add_argument("--user", default="airflow", help="System user")

    args = parser.parse_args(argv)

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "list":
        _cmd_list(args)
    elif args.command == "sync":
        _cmd_sync(args)
    elif args.command == "fingerprint":
        _cmd_fingerprint(args)
    elif args.command == "setup-systemd":
        _cmd_setup_systemd(args)


def _cmd_list(args: argparse.Namespace) -> None:
    from data_assets.core.registry import all_assets, discover

    discover()
    assets = all_assets()

    rows: list[dict[str, str]] = []
    for name in sorted(assets):
        asset = assets[name]()
        source = asset.source_name or "transform"
        if args.source and source != args.source:
            continue
        rows.append({
            "name": name,
            "source": source,
            "mode": str(asset.default_run_mode),
            "parent": getattr(asset, "parent_asset_name", "") or "",
        })

    if args.as_json:
        print(json.dumps(rows, indent=2))
        return

    if not rows:
        print("No assets found.")
        return

    # Simple tabular output
    widths = {k: max(len(k), max(len(r[k]) for r in rows)) for k in rows[0]}
    header = "  ".join(k.upper().ljust(widths[k]) for k in rows[0])
    print(header)
    print("  ".join("-" * widths[k] for k in rows[0]))
    for row in rows:
        print("  ".join(row[k].ljust(widths[k]) for k in row))


def _cmd_sync(args: argparse.Namespace) -> None:
    from data_assets.dag.generator import sync

    result = sync(args.output_dir)
    print(
        f"Sync complete: "
        f"{len(result.created)} created, "
        f"{len(result.updated)} updated, "
        f"{len(result.disabled)} disabled, "
        f"{len(result.skipped)} unchanged"
    )
    for f in result.created:
        print(f"  + {f}")
    for f in result.updated:
        print(f"  ~ {f}")
    for f in result.disabled:
        print(f"  x {f}")


def _cmd_fingerprint(args: argparse.Namespace) -> None:
    from data_assets.core.registry import discover, get
    from data_assets.dag.fingerprint import compute_fingerprint

    discover()
    asset_cls = get(args.asset_name)
    print(compute_fingerprint(asset_cls))


def _cmd_setup_systemd(args: argparse.Namespace) -> None:
    from data_assets.dag.systemd import generate_systemd_units

    service, timer, setup = generate_systemd_units(
        dag_dir=args.dag_dir,
        venv_path=args.venv_path,
        pip_index_url=args.pip_index_url,
        interval_minutes=args.interval,
        user=args.user,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    svc_path = args.output_dir / "data-assets-sync.service"
    tmr_path = args.output_dir / "data-assets-sync.timer"
    setup_path = args.output_dir / "data-assets-setup.sh"
    svc_path.write_text(service)
    tmr_path.write_text(timer)
    setup_path.write_text(setup)

    print(f"Generated: {svc_path}")
    print(f"Generated: {tmr_path}")
    print(f"Generated: {setup_path}")
    print()
    print("Review the files, then run the setup script:")
    print(f"  cd {args.output_dir}")
    print(f"  sudo bash {setup_path.name}")
