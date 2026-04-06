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


class SchemaContract(StrEnum):
    EVOLVE = "evolve"
    FREEZE = "freeze"
    DISCARD = "discard"


class PaginationStrategy(StrEnum):
    PAGE_NUMBER = "page_number"
    OFFSET = "offset"
    CURSOR = "cursor"
    KEYSET = "keyset"
    NONE = "none"


class IndexMethod(StrEnum):
    BTREE = "btree"
    GIN = "gin"
    GIST = "gist"
    HASH = "hash"
    BRIN = "brin"
    SPGIST = "spgist"


class CheckpointType(StrEnum):
    """Describes which extraction mode wrote a checkpoint.

    Separate from ParallelMode because sequential uses NONE for config
    but "sequential" for checkpoint provenance.
    """

    SEQUENTIAL = "sequential"
    PAGE_PARALLEL = "page_parallel"
    ENTITY_PARALLEL = "entity_parallel"
