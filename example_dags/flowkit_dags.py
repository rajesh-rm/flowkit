"""Example Airflow DAGs for data_assets.

Each DAG runs a single asset via run_asset(). Place this file in your
Airflow DAGs folder.

Secrets can be passed in three ways (in priority order):
1. Explicit `secrets` dict to run_asset() — from Airflow Connections/Variables
2. Environment variables — set on the worker (e.g., via K8s secrets)
3. .env file — for local development

The examples below show option 1 (Airflow Connections) and option 2 (env vars).
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}


# ---------------------------------------------------------------------------
# Option A: Pass secrets from Airflow Connection (recommended)
# ---------------------------------------------------------------------------


def _run_github_with_connection(**kwargs):
    """Fetch GitHub credentials from Airflow Connection 'github_app'."""
    from airflow.hooks.base import BaseHook

    from data_assets import run_asset

    conn = BaseHook.get_connection("github_app")
    extra = conn.extra_dejson

    return run_asset(
        "github_repos",
        run_mode="full",
        secrets={
            "GITHUB_APP_ID": conn.login,
            "GITHUB_PRIVATE_KEY": conn.password,
            "GITHUB_INSTALLATION_ID": extra["installation_id"],
            "GITHUB_ORGS": extra["orgs"],
        },
        airflow_run_id=kwargs.get("run_id"),
    )


with DAG(
    dag_id="github_repos",
    schedule="0 5 * * *",
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=["data_assets", "github"],
) as dag_gh_repos:
    PythonOperator(
        task_id="run",
        python_callable=_run_github_with_connection,
    )


# ---------------------------------------------------------------------------
# Option B: Rely on env vars already set on the worker
# ---------------------------------------------------------------------------


def _run_asset_simple(asset_name: str, run_mode: str, **kwargs):
    """No explicit secrets — reads from env vars or .env file."""
    from data_assets import run_asset

    return run_asset(
        asset_name=asset_name,
        run_mode=run_mode,
        airflow_run_id=kwargs.get("run_id"),
    )


with DAG(
    dag_id="sonarqube_projects",
    schedule="0 6 * * *",
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=["data_assets", "sonarqube"],
) as dag_sq_projects:
    PythonOperator(
        task_id="run",
        python_callable=_run_asset_simple,
        op_kwargs={"asset_name": "sonarqube_projects", "run_mode": "full"},
    )

with DAG(
    dag_id="sonarqube_issues",
    schedule="0 7 * * *",
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=["data_assets", "sonarqube"],
) as dag_sq_issues:
    PythonOperator(
        task_id="run",
        python_callable=_run_asset_simple,
        op_kwargs={"asset_name": "sonarqube_issues", "run_mode": "forward"},
    )

with DAG(
    dag_id="servicenow_incidents",
    schedule="@hourly",
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=["data_assets", "servicenow"],
) as dag_sn_incidents:
    PythonOperator(
        task_id="run",
        python_callable=_run_asset_simple,
        op_kwargs={"asset_name": "servicenow_incidents", "run_mode": "forward"},
    )

with DAG(
    dag_id="servicenow_changes",
    schedule="0 */4 * * *",
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=["data_assets", "servicenow"],
) as dag_sn_changes:
    PythonOperator(
        task_id="run",
        python_callable=_run_asset_simple,
        op_kwargs={"asset_name": "servicenow_changes", "run_mode": "forward"},
    )

with DAG(
    dag_id="github_pull_requests",
    schedule="0 6 * * *",
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=["data_assets", "github"],
) as dag_gh_prs:
    PythonOperator(
        task_id="run",
        python_callable=_run_asset_simple,
        op_kwargs={"asset_name": "github_pull_requests", "run_mode": "forward"},
    )

with DAG(
    dag_id="jira_projects",
    schedule="0 5 * * *",
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=["data_assets", "jira"],
) as dag_jira_projects:
    PythonOperator(
        task_id="run",
        python_callable=_run_asset_simple,
        op_kwargs={"asset_name": "jira_projects", "run_mode": "full"},
    )

with DAG(
    dag_id="jira_issues",
    schedule="0 6 * * *",
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=["data_assets", "jira"],
) as dag_jira_issues:
    PythonOperator(
        task_id="run",
        python_callable=_run_asset_simple,
        op_kwargs={"asset_name": "jira_issues", "run_mode": "forward"},
    )

with DAG(
    dag_id="incident_summary",
    schedule="0 8 * * *",
    default_args=default_args,
    max_active_runs=1,
    catchup=False,
    tags=["data_assets", "transform"],
) as dag_incident_summary:
    PythonOperator(
        task_id="run",
        python_callable=_run_asset_simple,
        op_kwargs={"asset_name": "incident_summary", "run_mode": "transform"},
    )
