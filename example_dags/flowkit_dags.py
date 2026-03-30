"""Example Airflow DAGs for data_assets.

Each DAG runs a single asset via run_asset(). Place this file in your
Airflow DAGs folder.
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


def _run_asset(asset_name: str, run_mode: str, **kwargs):
    """Wrapper that calls data_assets.run_asset and passes Airflow context."""
    from data_assets import run_asset

    return run_asset(
        asset_name=asset_name,
        run_mode=run_mode,
        airflow_run_id=kwargs.get("run_id"),
    )


# --- SonarQube ---

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
        python_callable=_run_asset,
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
        python_callable=_run_asset,
        op_kwargs={"asset_name": "sonarqube_issues", "run_mode": "forward"},
    )


# --- ServiceNow ---

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
        python_callable=_run_asset,
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
        python_callable=_run_asset,
        op_kwargs={"asset_name": "servicenow_changes", "run_mode": "forward"},
    )


# --- GitHub ---

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
        python_callable=_run_asset,
        op_kwargs={"asset_name": "github_repos", "run_mode": "full"},
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
        python_callable=_run_asset,
        op_kwargs={"asset_name": "github_pull_requests", "run_mode": "forward"},
    )


# --- Jira ---

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
        python_callable=_run_asset,
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
        python_callable=_run_asset,
        op_kwargs={"asset_name": "jira_issues", "run_mode": "forward"},
    )


# --- Transforms ---

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
        python_callable=_run_asset,
        op_kwargs={"asset_name": "incident_summary", "run_mode": "transform"},
    )
