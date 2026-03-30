# Architecture

## Guiding Principle

**Airflow is the orchestrator.** This package does not reimplement scheduling, retries, transforms, or error handling. It provides two things:

1. **Asset definitions** — what data exists and where it comes from
2. **API clients** — how to extract data from each source

Airflow handles everything else: scheduling, retries, task dependencies, observability, and secrets management.

## Layered Design

```
User's DAG folder              engmx_flowkit (installed package)
┌──────────────────┐           ┌──────────────────────────────────┐
│ flowkit_dags.py  │ ───────>  │  DAG Factory                     │
│ (one-line stub)  │           │    ├── reads AssetRegistry        │
└──────────────────┘           │    ├── reads FlowkitConfig        │
                               │    └── produces Airflow DAGs      │
                               │                                    │
                               │  Asset Definitions                 │
                               │    ├── sonarqube (issues, metrics) │
                               │    ├── servicenow (incidents, ...)│
                               │    └── github (repos, PRs, ...)   │
                               │                                    │
                               │  API Clients                       │
                               │    ├── SonarQubeClient             │
                               │    ├── ServiceNowClient            │
                               │    └── GitHubClient                │
                               │                                    │
                               │  Config (Pydantic models)          │
                               └──────────────────────────────────┘
```

## Components

### Asset Definitions (`assets/`)

Each source module (e.g., `assets/sonarqube.py`) defines one or more `AssetDefinition` instances. An `AssetDefinition` is a dataclass that captures:

- **name** — unique identifier (e.g., `sonarqube_issues`)
- **source** — which SDLC tool (e.g., `sonarqube`)
- **uri** — Airflow Asset URI using `engmx://` scheme
- **description** — human-readable purpose
- **schedule** — default cron expression
- **connection_id** — default Airflow Connection ID
- **client_class** — which API client to use
- **endpoint_config** — API-specific parameters (endpoint, filters)

Asset definitions self-register with the `AssetRegistry` at import time.

### Asset Registry (`assets/_registry.py`)

A module-level registry that collects all `AssetDefinition` instances. The DAG factory queries it for registered assets. Third-party code can also register custom assets.

### API Clients (`clients/`)

Each client implements the `BaseAPIClient` protocol:

- Accepts an Airflow `Connection` (host, credentials) and endpoint config
- Provides an `extract()` method that returns an iterator of dicts
- Handles pagination internally
- Does NOT handle retries — Airflow's task retries cover transient failures

Clients use `httpx` for HTTP requests.

### DAG Factory (`dag_factory.py`)

The core engine. Given the registry and configuration:

1. Iterates registered asset definitions
2. Filters by enabled sources and include/exclude lists
3. For each asset, creates an Airflow `DAG` with:
   - An `Asset` object (name, URI, metadata)
   - A `@task` that instantiates the client, calls `extract()`, and yields `Metadata`
   - Schedule, tags, and default_args from config

### Configuration (`config.py`)

Pydantic models for package behavior. Two layers:

- **Airflow Connections** — store credentials (API tokens, URLs). The package never manages secrets.
- **FlowkitConfig** — controls which sources are enabled, schedule overrides, DAG prefix, tags. Loaded from Airflow Variable, config file, or passed programmatically.

## What This Package Does NOT Do

- **Scheduling** — Airflow's scheduler handles cron and asset-triggered schedules
- **Retries** — Airflow's `retries` and `retry_delay` in `default_args`
- **Error handling framework** — Airflow's task failure handling and alerting
- **Data transformation pipelines** — any field mapping is trivial logic in the task
- **Data storage** — the package extracts data; where it lands is the DAG author's choice
- **Secrets management** — Airflow Connections and secrets backends
