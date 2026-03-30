"""AssetRegistry — collects and provides access to asset definitions.

A module-level registry instance. Source modules register their
AssetDefinition instances at import time. The DAG factory queries
the registry to discover which assets to generate DAGs for.
"""
