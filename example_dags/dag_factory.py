"""Optional DAG factory: auto-generate Airflow DAGs for all registered assets.

Usage:
    # In your DAGs folder, create a file with:
    from data_assets_dag_factory import create_dags
    globals().update(create_dags())

Secrets:
    The factory reads secrets from Airflow Connections and passes them
    via the `secrets` parameter to run_asset(). Each source has a
    corresponding Airflow Connection ID:

    - github_app:     login=APP_ID, password=PRIVATE_KEY, extra={"installation_id", "orgs"}
    - jira:           login=EMAIL, password=API_TOKEN, host=JIRA_URL
    - sonarqube:      password=TOKEN, host=SONARQUBE_URL
    - servicenow:     login=USERNAME, password=PASSWORD, host=INSTANCE_URL
    - data_assets_db: standard Postgres connection for DATABASE_URL
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

# Maps source_name → Airflow Connection ID → env var extraction logic
SOURCE_CONNECTIONS = {
    "github": "github_app",
    "jira": "jira",
    "sonarqube": "sonarqube",
    "servicenow": "servicenow",
}


def _resolve_secrets(source_name: str, org_override: dict | None = None) -> dict[str, str]:
    """Pull secrets from Airflow Connections for the given source.

    For GitHub multi-org, org_override provides per-org installation_id and org name.
    The GitHub App credentials (app_id, private_key) are shared across orgs.
    """
    from airflow.hooks.base import BaseHook

    conn_id = SOURCE_CONNECTIONS.get(source_name)
    if not conn_id:
        return {}

    try:
        conn = BaseHook.get_connection(conn_id)
    except Exception:
        return {}  # Connection not configured — fall back to env vars

    extra = conn.extra_dejson or {}

    if source_name == "github":
        secrets = {
            "GITHUB_APP_ID": conn.login or "",
            "GITHUB_PRIVATE_KEY": conn.password or "",
        }
        if org_override:
            secrets["GITHUB_INSTALLATION_ID"] = org_override["installation_id"]
            secrets["GITHUB_ORGS"] = org_override["org"]
        else:
            secrets["GITHUB_INSTALLATION_ID"] = extra.get("installation_id", "")
            secrets["GITHUB_ORGS"] = extra.get("orgs", "")
        return secrets
    if source_name == "jira":
        return {
            "JIRA_URL": f"https://{conn.host}" if conn.host else "",
            "JIRA_EMAIL": conn.login or "",
            "JIRA_API_TOKEN": conn.password or "",
        }
    if source_name == "sonarqube":
        return {
            "SONARQUBE_URL": f"https://{conn.host}" if conn.host else "",
            "SONARQUBE_TOKEN": conn.password or "",
        }
    if source_name == "servicenow":
        return {
            "SERVICENOW_INSTANCE": f"https://{conn.host}" if conn.host else "",
            "SERVICENOW_USERNAME": conn.login or "",
            "SERVICENOW_PASSWORD": conn.password or "",
        }
    return {}


def _run_asset(
    asset_name: str, run_mode: str, source_name: str,
    org_override: dict | None = None, **kwargs,
):
    from data_assets import run_asset

    secrets = _resolve_secrets(source_name, org_override=org_override)
    return run_asset(
        asset_name=asset_name,
        run_mode=run_mode,
        secrets=secrets or None,
        airflow_run_id=kwargs.get("run_id"),
    )


def _get_github_orgs() -> list[dict]:
    """Read per-org GitHub config from Airflow Connection extra field.

    Expected Connection extra format:
        {"orgs": [{"org": "my-org", "installation_id": "12345"}, ...]}

    Falls back to single-org if the legacy format is used:
        {"orgs": "my-org", "installation_id": "12345"}
    """
    from airflow.hooks.base import BaseHook

    try:
        conn = BaseHook.get_connection("github_app")
    except Exception:
        return []

    extra = conn.extra_dejson or {}
    orgs_config = extra.get("orgs", [])

    # Multi-org format: list of dicts
    if isinstance(orgs_config, list) and orgs_config and isinstance(orgs_config[0], dict):
        return orgs_config

    # Legacy single-org format: orgs is a string, installation_id at top level
    if isinstance(orgs_config, str) and orgs_config:
        return [{"org": orgs_config, "installation_id": extra.get("installation_id", "")}]

    return []


def create_dags(
    schedule_overrides: dict[str, str] | None = None,
    tag_prefix: str = "data_assets",
) -> dict[str, DAG]:
    """Generate DAGs for all registered assets.

    For GitHub assets, creates one DAG per org (e.g., github_repos_org_one,
    github_pull_requests_org_one) so each org uses its own credentials.

    Returns a dict of dag_id -> DAG suitable for injection into globals().
    """
    from data_assets.core.registry import all_assets, discover

    discover()
    dags = {}
    overrides = schedule_overrides or {}
    github_orgs = _get_github_orgs()

    for name, asset_cls in all_assets().items():
        asset = asset_cls()
        mode = asset.default_run_mode.value
        schedule = overrides.get(name, DEFAULT_SCHEDULES.get(mode))
        source = getattr(asset, "source_name", "transform") or "transform"

        # GitHub assets: one DAG per org
        if source == "github" and github_orgs:
            for org_cfg in github_orgs:
                org_slug = org_cfg["org"].replace("/", "_").replace("-", "_")
                dag_id = f"{name}_{org_slug}"

                dag = DAG(
                    dag_id=dag_id,
                    schedule=overrides.get(dag_id, schedule),
                    default_args=DEFAULT_ARGS,
                    max_active_runs=1,
                    catchup=False,
                    tags=[tag_prefix, source, org_cfg["org"]],
                    description=f"{asset.description} ({org_cfg['org']})",
                )

                PythonOperator(
                    task_id="run",
                    python_callable=_run_asset,
                    op_kwargs={
                        "asset_name": name,
                        "run_mode": mode,
                        "source_name": source,
                        "org_override": org_cfg,
                    },
                    dag=dag,
                )

                dags[dag_id] = dag
            continue

        # All other assets: one DAG
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
            op_kwargs={
                "asset_name": name,
                "run_mode": mode,
                "source_name": source,
            },
            dag=dag,
        )

        dags[name] = dag

    return dags
