"""Pydantic configuration models for engmx_flowkit.

FlowkitConfig controls package behavior: which sources are enabled,
schedule overrides, DAG prefix, and tags. Loaded from (in priority order):
1. Programmatic FlowkitConfig passed to generate_dags()
2. Config file at FLOWKIT_CONFIG_PATH environment variable
3. Airflow Variable named 'flowkit_config'
4. Defaults baked into each AssetDefinition
"""
