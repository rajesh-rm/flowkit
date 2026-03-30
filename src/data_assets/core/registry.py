"""Asset discovery and auto-registration."""

from __future__ import annotations

import importlib
import logging
import pkgutil

from data_assets.core.asset import Asset

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
            raise ValueError(
                f"Asset '{name}' declares parent_asset_name='{parent}' which is "
                f"not registered. Known assets: {sorted(known_names)}"
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


