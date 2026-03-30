"""Example: Selective asset loading — cherry-pick specific assets.

Demonstrates how to include/exclude specific assets or configure
the package programmatically.
"""

from engmx_flowkit import generate_dags
from engmx_flowkit.config import FlowkitConfig, SourceConfig

# Option 1: Include only specific assets by name pattern
# globals().update(generate_dags(include=["sonarqube_*", "github_pull_requests"]))

# Option 2: Exclude specific sources
# globals().update(generate_dags(exclude=["servicenow_*"]))

# Option 3: Full programmatic configuration
config = FlowkitConfig(
    dag_prefix="myteam",
    tags=["myteam", "data-asset"],
    sources={
        "sonarqube": SourceConfig(
            connection_id="my_sonar",
            schedule="0 6 * * *",
        ),
        "github": SourceConfig(
            extra_params={"orgs": ["my-org"]},
        ),
        "servicenow": SourceConfig(enabled=False),
    },
)
globals().update(generate_dags(config=config))
