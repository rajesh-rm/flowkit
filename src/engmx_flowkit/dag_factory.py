"""DAG factory — generates Airflow DAGs from registered asset definitions.

Reads from the AssetRegistry and FlowkitConfig, then produces Airflow DAG
objects using the Airflow 3.x SDK. Each AssetDefinition becomes one DAG
with a single @task that extracts data via the configured API client.

Usage from a DAG file::

    from engmx_flowkit import generate_dags
    globals().update(generate_dags())
"""
