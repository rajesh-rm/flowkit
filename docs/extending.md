# Extending engmx_flowkit

## Adding a New Source

To add a new SDLC tool (e.g., Jira), create two files:

### 1. API Client (`clients/jira.py`)

Implement the `BaseAPIClient` protocol:

```python
from engmx_flowkit.clients._base import BaseAPIClient

class JiraClient(BaseAPIClient):
    """Jira REST API client."""

    def __init__(self, connection, config):
        # connection: Airflow Connection with host + credentials
        # config: endpoint-specific parameters from AssetDefinition
        ...

    def extract(self, **params):
        # Yield dicts of raw data, handling pagination internally
        ...
```

### 2. Asset Definitions (`assets/jira.py`)

Define assets and register them:

```python
from engmx_flowkit.assets._base import AssetDefinition
from engmx_flowkit.assets._registry import registry
from engmx_flowkit.clients.jira import JiraClient

jira_issues = AssetDefinition(
    name="jira_issues",
    source="jira",
    uri="engmx://jira/issues",
    description="Jira issues and their status",
    schedule="@daily",
    connection_id="jira_default",
    client_class=JiraClient,
    endpoint_config={"resource": "search", "jql": "project = ENG"},
)

registry.register(jira_issues)
```

### 3. Wire It Up

Import the new asset module in `assets/__init__.py` so the registry picks it up at import time.

## Custom Assets at Runtime

Users can define custom assets without modifying the package:

```python
# In your DAG file, before generate_dags()
from engmx_flowkit.assets._base import AssetDefinition
from engmx_flowkit.assets._registry import registry
from engmx_flowkit.clients.sonarqube import SonarQubeClient

custom_asset = AssetDefinition(
    name="sonarqube_hotspots",
    source="sonarqube",
    uri="engmx://sonarqube/hotspots",
    description="Security hotspots from SonarQube",
    schedule="@daily",
    connection_id="sonarqube_default",
    client_class=SonarQubeClient,
    endpoint_config={"resource": "hotspots"},
)
registry.register(custom_asset)

from engmx_flowkit import generate_dags
globals().update(generate_dags())
```

## Entry Point Plugins (Future)

Third-party packages can register sources via `pyproject.toml` entry points:

```toml
# In a third-party package:
[project.entry-points."engmx_flowkit.sources"]
jira = "my_jira_plugin.assets:register"
```

The registry can discover these via `importlib.metadata.entry_points()`.
