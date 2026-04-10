"""DAG file templates and source-to-Connection field mappings."""

from __future__ import annotations

from string import Template

# ---------------------------------------------------------------------------
# Managed file marker — sync only touches files whose first line matches this
# ---------------------------------------------------------------------------
MANAGED_MARKER = "# data-assets-managed"

# ---------------------------------------------------------------------------
# Source → Airflow Connection field mapping
# ---------------------------------------------------------------------------
SOURCE_SECRETS_MAP: dict[str, dict] = {
    "github": {
        "default_connection_id": "github_app",
        "field_map": {
            "GITHUB_APP_ID": "login",
            "GITHUB_PRIVATE_KEY": "password",
        },
        "extra_map": {
            "GITHUB_INSTALLATION_ID": "installation_id",
            "GITHUB_ORGS": "orgs",
        },
    },
    "jira": {
        "default_connection_id": "jira",
        "field_map": {
            "JIRA_EMAIL": "login",
            "JIRA_API_TOKEN": "password",
        },
        "host_env": "JIRA_URL",
    },
    "sonarqube": {
        "default_connection_id": "sonarqube",
        "field_map": {
            "SONARQUBE_TOKEN": "password",
        },
        "host_env": "SONARQUBE_URL",
    },
    "servicenow": {
        "default_connection_id": "servicenow",
        "field_map": {
            "SERVICENOW_USERNAME": "login",
            "SERVICENOW_PASSWORD": "password",
        },
        "host_env": "SERVICENOW_INSTANCE",
    },
}

# ---------------------------------------------------------------------------
# Templates (string.Template — $var syntax avoids conflicts with Python {})
# ---------------------------------------------------------------------------

DAG_TEMPLATE = Template("""\
$marker
\"\"\"Auto-generated DAG for asset: $asset_name$subtitle

Fingerprint: $fingerprint
Do not edit — customise via dag_overrides.toml, then re-run: data-assets sync
\"\"\"
from datetime import timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

_ASSET_FINGERPRINT = "$fingerprint"


$run_function


with DAG(
    dag_id="$dag_id",
    schedule=$schedule,
    default_args={
        "owner": "$owner",
        "retries": $retries,
        "retry_delay": timedelta(minutes=$retry_delay_minutes),
        "retry_exponential_backoff": True,
    },
    max_active_runs=$max_active_runs,
    catchup=False,
    tags=$tags,
    description="$description",
) as dag:
    PythonOperator(task_id="run", python_callable=_run)
""")

DISABLED_TEMPLATE = Template("""\
$marker
\"\"\"DISABLED: Asset '$asset_name' is no longer registered in the data-assets package.

This DAG was automatically disabled by: data-assets sync
Remove this file manually once you no longer need the Airflow run history.
\"\"\"
from airflow.sdk import DAG

DAG(dag_id="$dag_id", schedule=None, catchup=False, is_paused_upon_creation=True)
""")
