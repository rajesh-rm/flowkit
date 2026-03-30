"""Concrete asset definitions for all supported SDLC data sources.

Importing this package triggers auto-registration of all asset classes
via the @register decorator.
"""

import data_assets.assets.github  # noqa: F401
import data_assets.assets.jira  # noqa: F401
import data_assets.assets.servicenow  # noqa: F401
import data_assets.assets.sonarqube  # noqa: F401
import data_assets.assets.transforms  # noqa: F401
