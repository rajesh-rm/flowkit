"""Optional DAG factory: auto-generate Airflow DAGs for all registered assets.

Usage:
    # In your DAGs folder, create a file with:
    from data_assets_dag_factory import create_dags
    globals().update(create_dags())
"""

from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


DEFAULT_SCHEDULES = {
    "full": "0 5 * * *",
    "forward": "@hourly",
    "backfill": None,
    "transform": "0 8 * * *",
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


def _run_asset(asset_name: str, run_mode: str, **kwargs):
    from data_assets import run_asset

    return run_asset(
        asset_name=asset_name,
        run_mode=run_mode,
        airflow_run_id=kwargs.get("run_id"),
    )


def create_dags(
    schedule_overrides: dict[str, str] | None = None,
    tag_prefix: str = "data_assets",
) -> dict[str, DAG]:
    """Generate DAGs for all registered assets.

    Returns a dict of dag_id -> DAG suitable for injection into globals().
    """
    from data_assets.core.registry import all_assets, discover

    discover()
    dags = {}
    overrides = schedule_overrides or {}

    for name, asset_cls in all_assets().items():
        asset = asset_cls()
        mode = asset.default_run_mode.value
        schedule = overrides.get(name, DEFAULT_SCHEDULES.get(mode))
        source = getattr(asset, "source_name", "transform") or "transform"

        dag = DAG(
            dag_id=name,
            schedule=schedule,
            default_args=DEFAULT_ARGS,
            max_active_runs=1,
            catchup=False,
            tags=[tag_prefix, source],
            description=asset.description,
        )

        PythonOperator(
            task_id="run",
            python_callable=_run_asset,
            op_kwargs={"asset_name": name, "run_mode": mode},
            dag=dag,
        )

        dags[name] = dag

    return dags
