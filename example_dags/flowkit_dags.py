"""Example Airflow DAGs for data_assets.

Each DAG has one task and is responsible for one asset. Place this file
in your Airflow DAGs folder.

GitHub multi-org: each org gets its own DAG (github_repos_org_one,
github_pull_requests_org_one, etc.) because each org has its own
GitHub App credentials (.pem file).

Airflow Connection setup for GitHub multi-org:
    Connection ID: github_app
    Login: <GitHub App ID>
    Password: <Private key PEM contents>
    Extra (JSON): {
        "orgs": [
            {"org": "org-one", "installation_id": "111"},
            {"org": "org-two", "installation_id": "222"}
        ]
    }
"""

from datetime import timedelta

from airflow.sdk import DAG, BaseHook
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
}

# Cron schedules — staggered so upstream assets finish before downstream.
SCHEDULE_TIER_1 = "0 5 * * *"  # repos, org-level assets, jira_projects
SCHEDULE_TIER_2 = "0 6 * * *"  # repo-scoped assets, sonarqube, jira_issues


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run_asset_simple(asset_name: str, run_mode: str, **kwargs):
    """No explicit secrets — reads from env vars or .env file."""
    from data_assets import run_asset

    return run_asset(
        asset_name=asset_name,
        run_mode=run_mode,
        airflow_run_id=kwargs.get("run_id"),
    )


def _run_github(asset_name: str, run_mode: str, org_config: dict, **kwargs):
    """Run a GitHub asset with per-org credentials from Airflow Connection."""
    from data_assets import run_asset

    conn = BaseHook.get_connection("github_app")
    return run_asset(
        asset_name=asset_name,
        run_mode=run_mode,
        partition_key=org_config["org"],
        secrets={
            "GITHUB_APP_ID": conn.login,
            "GITHUB_PRIVATE_KEY": conn.password,
            "GITHUB_INSTALLATION_ID": org_config["installation_id"],
            "GITHUB_ORGS": org_config["org"],
        },
        airflow_run_id=kwargs.get("run_id"),
    )


# ---------------------------------------------------------------------------
# GitHub — one DAG per org per asset
# ---------------------------------------------------------------------------

# Read org configs from the Airflow Connection.
# Each org has its own installation_id (GitHub App is installed per org).
try:
    _gh_conn = BaseHook.get_connection("github_app")
    _gh_orgs = _gh_conn.extra_dejson.get("orgs", [])
except Exception:
    _gh_orgs = []

for _org_cfg in _gh_orgs:
    _org = _org_cfg["org"]
    _slug = _org.replace("-", "_")

    with DAG(
        dag_id=f"github_repos_{_slug}",
        schedule=SCHEDULE_TIER_1,
        default_args=default_args,
        max_active_runs=1,
        catchup=False,
        tags=["data_assets", "github", _org],
    ) as _dag:
        PythonOperator(
            task_id="run",
            python_callable=_run_github,
            op_kwargs={
                "asset_name": "github_repos",
                "run_mode": "full",
                "org_config": _org_cfg,
            },
        )

    # All repo-scoped GitHub assets — one DAG per org per asset.
    # Assets that fan out by repo (entity-parallel) depend on github_repos
    # being loaded first. Schedule them after repos.
    _gh_repo_assets = {
        "github_pull_requests": "forward",
        "github_branches": "full",
        "github_commits": "forward",
        "github_workflows": "full",
        "github_workflow_runs": "forward",
        "github_repo_properties": "full",
    }
    for _asset_name, _mode in _gh_repo_assets.items():
        with DAG(
            dag_id=f"{_asset_name}_{_slug}",
            schedule=SCHEDULE_TIER_2,
            default_args=default_args,
            max_active_runs=1,
            catchup=False,
            tags=["data_assets", "github", _org],
        ) as _dag:
            PythonOperator(
                task_id="run",
                python_callable=_run_github,
                op_kwargs={
                    "asset_name": _asset_name,
                    "run_mode": _mode,
                    "org_config": _org_cfg,
                },
            )

    # Org-level GitHub assets (not repo-scoped)
    for _asset_name, _mode in [("github_members", "full"), ("github_runner_groups", "full")]:
        with DAG(
            dag_id=f"{_asset_name}_{_slug}",
            schedule=SCHEDULE_TIER_1,
            default_args=default_args,
            max_active_runs=1,
            catchup=False,
            tags=["data_assets", "github", _org],
        ) as _dag:
            PythonOperator(
                task_id="run",
                python_callable=_run_github,
                op_kwargs={
                    "asset_name": _asset_name,
                    "run_mode": _mode,
                    "org_config": _org_cfg,
                },
            )

    # Deeper nested GitHub assets (depend on parent assets above)
    _gh_nested_assets = {
        "github_user_details": "full",       # depends on github_members
        "github_workflow_jobs": "forward",    # depends on github_workflow_runs
        "github_runner_group_repos": "full",  # depends on github_runner_groups
    }
    for _asset_name, _mode in _gh_nested_assets.items():
        with DAG(
            dag_id=f"{_asset_name}_{_slug}",
            schedule="0 7 * * *",  # Later schedule — parents must run first
            default_args=default_args,
            max_active_runs=1,
            catchup=False,
            tags=["data_assets", "github", _org],
        ) as _dag:
            PythonOperator(
                task_id="run",
                python_callable=_run_github,
                op_kwargs={
                    "asset_name": _asset_name,
                    "run_mode": _mode,
                    "org_config": _org_cfg,
                },
            )


# ---------------------------------------------------------------------------
# SonarQube
# ---------------------------------------------------------------------------

with DAG(
    dag_id="sonarqube_projects",
    schedule=SCHEDULE_TIER_2,
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


# ---------------------------------------------------------------------------
# ServiceNow
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Jira
# ---------------------------------------------------------------------------

with DAG(
    dag_id="jira_projects",
    schedule=SCHEDULE_TIER_1,
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
    schedule=SCHEDULE_TIER_2,
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


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------

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
