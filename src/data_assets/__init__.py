"""data_assets — Self-contained ETL engine for data assets.

Apache Airflow calls run_asset() and this package handles everything else:
extraction, rate limiting, checkpointing, schema management, loading,
validation, and promotion.
"""

__version__ = "0.1.0"

from data_assets.runner import run_asset

__all__ = ["run_asset", "__version__"]
