"""Enumerations used throughout the package."""

from enum import StrEnum


class RunMode(StrEnum):
    FULL = "full"
    FORWARD = "forward"
    BACKFILL = "backfill"
    TRANSFORM = "transform"


class LoadStrategy(StrEnum):
    FULL_REPLACE = "full_replace"
    UPSERT = "upsert"
    APPEND = "append"


class AssetType(StrEnum):
    API = "api"
    TRANSFORM = "transform"


class ParallelMode(StrEnum):
    NONE = "none"
    PAGE_PARALLEL = "page_parallel"
    ENTITY_PARALLEL = "entity_parallel"
