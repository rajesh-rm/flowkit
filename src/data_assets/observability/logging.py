"""Configured stdlib logger for stdout output consumed by Airflow."""

from __future__ import annotations

import logging
import sys


def setup_logging(level: int = logging.INFO) -> None:
    """Configure the data_assets logger hierarchy.

    In Airflow (or pytest), the root logger already has handlers that
    capture task output, so we only set the level and let propagation
    carry logs upward.  In standalone/CLI usage the root logger is bare,
    so we attach a StreamHandler to stdout ourselves.
    """
    pkg_logger = logging.getLogger("data_assets")
    if pkg_logger.handlers:
        return  # Already configured

    # Only add our own handler when no parent handler would capture logs.
    # Airflow's task runner and pytest both install root-level handlers;
    # adding a second one causes every message to appear twice.
    if not logging.getLogger().handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )
        pkg_logger.addHandler(handler)

    pkg_logger.setLevel(level)
