"""Asset discovery and auto-registration."""

from __future__ import annotations

import importlib
import logging
import pkgutil
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from data_assets.core.asset import Asset

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

_registry: dict[str, type[Asset]] = {}


def register(asset_cls: type[Asset]) -> type[Asset]:
    """Register an asset class by its name. Can be used as a decorator."""
    name = asset_cls.name
    if name in _registry:
        logger.warning("Asset '%s' is already registered; overwriting.", name)
    _registry[name] = asset_cls
    logger.debug("Registered asset '%s'", name)
    return asset_cls


def get(name: str) -> type[Asset]:
    """Look up an asset class by name."""
    if name not in _registry:
        raise KeyError(
            f"Asset '{name}' not found in registry. "
            f"Known assets: {sorted(_registry.keys())}"
        )
    return _registry[name]


def all_assets() -> dict[str, type[Asset]]:
    """Return a copy of the full registry."""
    return dict(_registry)


def discover() -> None:
    """Auto-discover and import all asset modules under data_assets.assets.

    Importing each module triggers @register decorators on asset classes.
    After discovery, validates that all declared dependencies exist.
    """
    import data_assets.assets as assets_pkg

    for importer, modname, ispkg in pkgutil.walk_packages(
        assets_pkg.__path__, prefix=assets_pkg.__name__ + "."
    ):
        try:
            importlib.import_module(modname)
        except Exception:
            logger.exception("Failed to import asset module '%s'", modname)

    _validate_dependencies()


def _validate_dependencies() -> None:
    """Check that all declared asset dependencies reference registered assets.

    Validates:
    - parent_asset_name on entity-parallel APIAssets
    - source_tables entries on TransformAssets (matched by target_table)
    """
    known_names = set(_registry.keys())
    known_tables = {cls().target_table for cls in _registry.values()}

    for name, cls in _registry.items():
        asset = cls()

        # Check parent_asset_name (entity-parallel)
        parent = getattr(asset, "parent_asset_name", None)
        if parent and parent not in known_names:
            logger.error(
                "Asset '%s' declares parent_asset_name='%s' which is not registered. "
                "Known assets: %s",
                name, parent, sorted(known_names),
            )

        # Check source_tables (transform assets)
        source_tables = getattr(asset, "source_tables", [])
        for table in source_tables:
            if table not in known_tables:
                logger.warning(
                    "Asset '%s' declares source_table '%s' which doesn't match "
                    "any registered asset's target_table.",
                    name, table,
                )


def sync_to_db(engine: Engine) -> None:
    """Write/update asset_registry rows in data_ops for all registered assets."""
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from sqlalchemy.orm import Session

    from data_assets.db.models import AssetRegistry

    now = datetime.now(UTC)
    with Session(engine) as session:
        for name, cls in _registry.items():
            asset = cls()
            values = {
                "asset_name": name,
                "asset_type": getattr(asset, "asset_type", "api").value
                if hasattr(getattr(asset, "asset_type", None), "value")
                else str(getattr(asset, "asset_type", "api")),
                "source_name": getattr(asset, "source_name", None),
                "target_schema": asset.target_schema,
                "target_table": asset.target_table,
                "load_strategy": asset.load_strategy.value,
                "registered_at": now,
                "config": {},
            }
            stmt = pg_insert(AssetRegistry).values(**values).on_conflict_do_update(
                index_elements=["asset_name"],
                set_={
                    "asset_type": values["asset_type"],
                    "source_name": values["source_name"],
                    "target_schema": values["target_schema"],
                    "target_table": values["target_table"],
                    "load_strategy": values["load_strategy"],
                    "config": values["config"],
                },
            )
            session.execute(stmt)
        session.commit()
