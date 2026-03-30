"""Configured stdlib logger for stdout output consumed by Airflow."""

from __future__ import annotations

import logging
import sys


def setup_logging(level: int = logging.INFO) -> None:
    """Configure the data_assets logger hierarchy.

    Output goes to stdout so Airflow task logs capture it.
    """
    root_logger = logging.getLogger("data_assets")
    if root_logger.handlers:
        return  # Already configured

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    root_logger.addHandler(handler)
    root_logger.setLevel(level)
