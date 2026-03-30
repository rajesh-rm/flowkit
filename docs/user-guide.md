# User Guide

## Prerequisites

- Apache Airflow 3.1+
- Python 3.11+

## Installation

```bash
pip install engmx-flowkit
```

Or install with dev dependencies:

```bash
pip install engmx-flowkit[dev]
```

## Quick Start

### 1. Configure Airflow Connections

Set up connections for each source you want to use. See [configuration.md](configuration.md) for details.

```bash
airflow connections add sonarqube_default \
  --conn-type http \
  --conn-host https://sonar.example.com \
  --conn-password "sqa_xxxxx"
```

### 2. Create the Stub DAG File

Place this file in your Airflow DAG folder:

```python
# dags/flowkit_dags.py
from engmx_flowkit import generate_dags

# Generate all configured DAGs — Airflow discovers them from globals()
globals().update(generate_dags())
```

That's it. DAGs will appear in the Airflow UI tagged with `flowkit` and the source name.

### 3. (Optional) Customize via Configuration

To control which sources are enabled, override schedules, or pass source-specific parameters:

**Option A: Airflow Variable**

```bash
airflow variables set flowkit_config '{"dag_prefix": "engmx", "sources": {"sonarqube": {"enabled": true, "schedule": "0 6 * * *"}}}'
```

**Option B: Config file**

```bash
export FLOWKIT_CONFIG_PATH=/path/to/flowkit_config.yaml
```

**Option C: Programmatic**

```python
from engmx_flowkit import generate_dags
from engmx_flowkit.config import FlowkitConfig, SourceConfig

config = FlowkitConfig(
    dag_prefix="myteam",
    sources={
        "sonarqube": SourceConfig(connection_id="my_sonar"),
        "github": SourceConfig(enabled=False),
    },
)
globals().update(generate_dags(config=config))
```

## Selective Asset Loading

```python
# Only SonarQube and GitHub assets
globals().update(generate_dags(include=["sonarqube_*"]))

# Everything except ServiceNow
globals().update(generate_dags(exclude=["servicenow_*"]))

# Specific assets by name
globals().update(generate_dags(include=["sonarqube_issues", "github_pull_requests"]))
```

## Verifying in Airflow UI

After placing the stub DAG file:

1. Navigate to the Airflow UI
2. DAGs appear with IDs like `flowkit_sonarqube_issues`, `flowkit_github_pull_requests`
3. Each DAG is tagged with `flowkit` and its source name
4. Assets are visible in the Airflow Assets view with `engmx://` URIs
5. Enable the DAGs and trigger them manually or wait for the schedule
