"""Enumerations used throughout the package."""

from enum import Enum


class RunMode(str, Enum):
    FULL = "full"
    FORWARD = "forward"
    BACKFILL = "backfill"
    TRANSFORM = "transform"


class LoadStrategy(str, Enum):
    FULL_REPLACE = "full_replace"
    UPSERT = "upsert"
    APPEND = "append"


class AssetType(str, Enum):
    API = "api"
    TRANSFORM = "transform"


class ParallelMode(str, Enum):
    NONE = "none"
    PAGE_PARALLEL = "page_parallel"
    ENTITY_PARALLEL = "entity_parallel"
