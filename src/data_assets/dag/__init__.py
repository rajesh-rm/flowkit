"""DAG generation and lifecycle management for Airflow integration.

Submodules:
    fingerprint — deterministic hash of asset DAG definitions for drift detection
    generator   — core sync engine: generates, updates, and disables DAG files
    overrides   — loads admin overrides from dag_overrides.toml
    templates   — DAG file template and source-to-Connection mappings
    systemd     — generates systemd unit files for automated sync
"""
