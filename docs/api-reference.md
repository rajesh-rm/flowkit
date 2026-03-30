# API Reference

## Public API

### `generate_dags()`

```python
from engmx_flowkit import generate_dags

def generate_dags(
    config: FlowkitConfig | None = None,
    include: list[str] | None = None,
    exclude: list[str] | None = None,
) -> dict[str, DAG]:
    """Generate Airflow DAG objects for configured assets.

    Args:
        config: Package configuration. If None, loaded from Airflow Variable
                or config file.
        include: Asset name patterns to include (supports glob-style wildcards).
                 If None, all registered assets are included.
        exclude: Asset name patterns to exclude. Applied after include filter.

    Returns:
        Dict mapping dag_id -> DAG. Inject into module globals() for Airflow
        discovery.
    """
```

### `AssetDefinition`

```python
from engmx_flowkit.assets import AssetDefinition

@dataclass
class AssetDefinition:
    name: str               # Unique identifier (e.g., "sonarqube_issues")
    source: str             # Source tool name (e.g., "sonarqube")
    uri: str                # Airflow Asset URI (e.g., "engmx://sonarqube/issues")
    description: str        # Human-readable description
    schedule: str | None    # Default cron expression or Airflow preset
    connection_id: str      # Default Airflow Connection ID
    client_class: type      # API client class implementing BaseAPIClient
    endpoint_config: dict   # API-specific parameters
```

### `AssetRegistry`

```python
from engmx_flowkit.assets import registry

class AssetRegistry:
    def register(self, definition: AssetDefinition) -> None:
        """Register an asset definition."""

    def get_all(self) -> list[AssetDefinition]:
        """Return all registered asset definitions."""

    def get_by_source(self, source: str) -> list[AssetDefinition]:
        """Return asset definitions for a specific source."""

    def get_by_name(self, name: str) -> AssetDefinition:
        """Return a specific asset definition by name."""

# Module-level instance:
registry = AssetRegistry()
```

### `BaseAPIClient`

```python
from engmx_flowkit.clients import BaseAPIClient

class BaseAPIClient(Protocol):
    def __init__(self, connection: Connection, config: dict) -> None:
        """Initialize with Airflow Connection and endpoint config."""

    def extract(self, **params) -> Iterator[dict]:
        """Extract data from the source API.

        Handles pagination internally. Yields dicts of raw data.
        """
```

### `FlowkitConfig`

```python
from engmx_flowkit.config import FlowkitConfig, SourceConfig

class SourceConfig(BaseModel):
    enabled: bool = True
    connection_id: str | None = None    # Override default
    schedule: str | None = None         # Override default
    extra_params: dict = {}

class FlowkitConfig(BaseModel):
    dag_prefix: str = "flowkit"
    tags: list[str] = ["flowkit", "engmx"]
    sources: dict[str, SourceConfig] = {}
```

## Exceptions

```python
from engmx_flowkit.exceptions import FlowkitError, ConfigurationError, ExtractionError

class FlowkitError(Exception):
    """Base exception for all engmx_flowkit errors."""

class ConfigurationError(FlowkitError):
    """Invalid or missing configuration."""

class ExtractionError(FlowkitError):
    """Failed to extract data from a source API."""
```
