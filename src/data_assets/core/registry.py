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

    # Instantiate each asset once; build known_tables in the same pass
    instances: dict[str, Asset] = {}
    known_tables: set[str] = set()
    for name, cls in _registry.items():
        asset = cls()
        instances[name] = asset
        known_tables.add(asset.target_table)

    for name, asset in instances.items():
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
                raise ValueError(
                    f"Asset '{name}' declares source_table '{table}' which "
                    f"doesn't match any registered asset's target_table. "
                    f"Known tables: {sorted(known_tables)}"
                )

        _validate_indexes(name, asset)

    _validate_no_cycles(instances)


def _build_dependency_graph(instances: dict[str, Asset]) -> dict[str, list[str]]:
    """Build a directed graph of asset dependencies from source_tables."""
    table_to_name = {inst.target_table: name for name, inst in instances.items()}
    graph: dict[str, list[str]] = {}
    for name, asset in instances.items():
        deps = [
            table_to_name[table]
            for table in getattr(asset, "source_tables", [])
            if table in table_to_name
        ]
        if deps:
            graph[name] = deps
    return graph


def _validate_no_cycles(instances: dict[str, Asset]) -> None:
    """Detect circular dependencies among transform source_tables."""
    graph = _build_dependency_graph(instances)

    UNVISITED, IN_PROGRESS, DONE = 0, 1, 2
    state: dict[str, int] = dict.fromkeys(graph, UNVISITED)

    def _visit(node: str, path: list[str]) -> None:
        state[node] = IN_PROGRESS
        path.append(node)
        for dep in graph.get(node, []):
            if state.get(dep) == IN_PROGRESS:
                cycle = path[path.index(dep):] + [dep]
                raise ValueError(
                    f"Circular dependency detected: {' -> '.join(cycle)}"
                )
            if state.get(dep, DONE) == UNVISITED:
                _visit(dep, path)
        path.pop()
        state[node] = DONE

    for node in graph:
        if state[node] == UNVISITED:
            _visit(node, [])


def _validate_indexes(name: str, asset) -> None:
    """Check that every asset has at least one index with valid columns."""
    if not asset.indexes:
        raise ValueError(
            f"Asset '{name}' has no indexes defined. Every asset must "
            f"declare at least one Index in its 'indexes' class attribute."
        )

    column_names = {c.name for c in asset.columns}
    for idx in asset.indexes:
        bad = [c for c in idx.columns if c not in column_names]
        if bad:
            raise ValueError(
                f"Asset '{name}' has index referencing non-existent "
                f"columns: {bad}. Known columns: {sorted(column_names)}"
            )
        if idx.include:
            bad_inc = [c for c in idx.include if c not in column_names]
            if bad_inc:
                raise ValueError(
                    f"Asset '{name}' has index with INCLUDE referencing "
                    f"non-existent columns: {bad_inc}."
                )


