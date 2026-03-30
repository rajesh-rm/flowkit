"""Asset definitions for SDLC data sources.

Each source module (sonarqube, servicenow, github) defines AssetDefinition
instances and registers them with the module-level registry. The DAG factory
queries this registry to generate Airflow DAGs.
"""
