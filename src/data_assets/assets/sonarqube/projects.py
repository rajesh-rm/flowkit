"""SonarQube projects via /api/components/search.

Lists all projects (qualifier=TRK) from the SonarQube instance.

SonarQube's Elasticsearch backend caps ``/api/components/search`` at 10,000
results. When total projects exceed that limit, this asset shards queries
using the ``q`` (name-substring search) parameter to partition the result
space into sub-queries each within the safe limit. For instances with fewer
than ~9,900 projects the standard page-number pagination is used directly.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.rest_asset import RestAsset
from data_assets.extract.api_client import APIClient
from data_assets.extract.rate_limiter import RateLimiter
from data_assets.extract.token_manager import SonarQubeTokenManager
from data_assets.load.loader import write_to_temp

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from data_assets.core.run_context import RunContext

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Sharding constants
# ---------------------------------------------------------------------------

# SonarQube ES backend hard-caps results at 10,000.  We trigger sharding
# below that so there is a one-page (100 result) safety margin.
_SAFE_LIMIT = 9_900

# Characters used to build q-param prefixes.  SonarQube search is
# case-insensitive so lowercase + digits covers all alphanumeric matches.
_SEARCH_CHARS = "abcdefghijklmnopqrstuvwxyz0123456789"


@register
class SonarQubeProjects(RestAsset):
    """SonarQube projects — full catalog, refreshed each run.

    Overrides ``extract()`` to handle the SonarQube 10k result limit.
    When total projects exceed ``_SAFE_LIMIT`` the extraction fans out
    into sub-queries using two-character (and, if needed, longer) ``q``
    prefixes and deduplicates results by project key.
    """

    name = "sonarqube_projects"
    source_name = "sonarqube"
    target_schema = "raw"
    target_table = "sonarqube_projects"

    # --- Source config ---
    token_manager_class = SonarQubeTokenManager
    base_url_env = "SONARQUBE_URL"
    endpoint = "/api/components/search"
    rate_limit_per_second = 5.0

    # --- Response parsing ---
    response_path = "components"  # JSON path to the records array
    pagination = {
        "strategy": "page_number",
        "page_size": 100,
        "total_path": "paging.total",
        "page_index_path": "paging.pageIndex",
    }

    # --- Parallelism ---
    # extract() handles its own pagination/sharding; the standard parallel
    # dispatch in the runner is bypassed.
    parallel_mode = ParallelMode.NONE

    # --- Load behavior ---
    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    # --- Schema ---
    # /api/components/search returns: key, name, qualifier, project
    columns = [
        Column("key", "TEXT", nullable=False),
        Column("name", "TEXT"),
        Column("qualifier", "TEXT"),
    ]
    primary_key = ["key"]
    indexes = [
        Index(columns=("name",)),
    ]

    def build_request(self, context, checkpoint=None):
        spec = super().build_request(context, checkpoint)
        # Filter to projects only (TRK = project qualifier in SonarQube)
        spec.params["qualifiers"] = "TRK"
        return spec

    # ------------------------------------------------------------------
    # Custom extraction — sharding for the 10k limit
    # ------------------------------------------------------------------

    def extract(self, engine: Engine, temp_table: str, context: RunContext) -> int:
        """Extract projects, sharding by ``q`` param if total exceeds the safe limit."""
        user_max_pages = context.params.get("max_pages")
        client = self._create_client()
        try:
            initial_total = self._probe(client, context)
            logger.info(
                "SonarQube projects: total=%d, safe_limit=%d",
                initial_total, _SAFE_LIMIT,
            )

            if initial_total <= _SAFE_LIMIT:
                seen_keys: set[str] = set()
                self._paginate_shard(
                    client, engine, temp_table, context,
                    q_param=None, seen_keys=seen_keys,
                    user_max_pages=user_max_pages,
                )
                return len(seen_keys)

            return self._extract_sharded(
                client, engine, temp_table, context, initial_total,
                user_max_pages=user_max_pages,
            )
        finally:
            logger.info("SonarQube API stats: %s", client.stats)
            client.close()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _create_client(self) -> APIClient:
        """Build an APIClient matching the runner's ``_extract_api`` setup."""
        token_mgr = self.token_manager_class()
        rate_limiter = RateLimiter(self.rate_limit_per_second)
        return APIClient(
            token_mgr,
            rate_limiter,
            timeout=self.request_timeout,
            max_retries=self.max_retries,
            error_classifier=self.classify_error,
        )

    def _probe(
        self, client: APIClient, context: RunContext, q: str | None = None,
    ) -> int:
        """Lightweight probe (ps=1) to read the total count for a query."""
        spec = self.build_request(context)
        spec.params["ps"] = 1  # minimal payload
        if q is not None:
            spec.params["q"] = q
        data = client.request(spec)
        return data.get("paging", {}).get("total", 0)

    # -- pagination / shard helpers ------------------------------------

    def _paginate_shard(
        self,
        client: APIClient,
        engine: Engine,
        temp_table: str,
        context: RunContext,
        q_param: str | None,
        seen_keys: set[str],
        user_max_pages: int | None = None,
    ) -> set[str]:
        """Paginate all pages for a ``q`` value, dedup, write to temp.

        Returns the set of project *names* found (used for early-termination
        tracking in ``_extend_shard``).
        """
        names_found: set[str] = set()
        page = 1
        # Safety limit matching _fetch_pages; ceil(_SAFE_LIMIT / page_size).
        internal_cap = (_SAFE_LIMIT // self.pagination_config.page_size) + 1
        max_pages = user_max_pages if user_max_pages is not None else internal_cap

        while True:
            if page > max_pages:
                logger.warning(
                    "Shard q='%s': reached max_pages limit (%d). Stopping.",
                    q_param, max_pages,
                )
                break

            spec = self.build_request(context, checkpoint={"next_page": page})
            if q_param is not None:
                spec.params["q"] = q_param
            data = client.request(spec)
            df, state = self.parse_response(data)

            if not df.empty:
                # Dedup: keep only rows whose key we haven't already written
                new_mask = ~df["key"].isin(seen_keys)
                new_df = df[new_mask]
                if not new_df.empty:
                    write_to_temp(engine, temp_table, new_df)
                    seen_keys.update(new_df["key"])
                # Track ALL names (even dupes) for early-termination counting
                names_found.update(df["name"])

            if not state.has_more:
                break
            page += 1

        return names_found

    # -- sharded extraction --------------------------------------------

    def _extract_sharded(
        self,
        client: APIClient,
        engine: Engine,
        temp_table: str,
        context: RunContext,
        initial_total: int,
        user_max_pages: int | None = None,
    ) -> int:
        """Shard the query space into 2-char ``q`` prefixes and paginate each."""
        seen_keys: set[str] = set()
        total_prefixes = len(_SEARCH_CHARS) ** 2  # 1,296
        prefixes_done = 0
        log_every = 200

        for c1 in _SEARCH_CHARS:
            for c2 in _SEARCH_CHARS:
                prefix = c1 + c2
                shard_total = self._probe(client, context, q=prefix)

                if shard_total > _SAFE_LIMIT:
                    self._extend_shard(
                        client, engine, temp_table, context,
                        prefix, shard_total, seen_keys,
                        user_max_pages=user_max_pages,
                    )
                elif shard_total > 0:
                    self._paginate_shard(
                        client, engine, temp_table, context,
                        q_param=prefix, seen_keys=seen_keys,
                        user_max_pages=user_max_pages,
                    )

                prefixes_done += 1
                if prefixes_done % log_every == 0:
                    logger.info(
                        "Shard progress: %d/%d prefixes, %d unique projects so far",
                        prefixes_done, total_prefixes, len(seen_keys),
                    )

        # Skip reconciliation when max_pages is set — partial fetch is intentional.
        if user_max_pages is not None:
            logger.info(
                "SonarQube: max_pages=%d override active — skipping reconciliation check",
                user_max_pages,
            )
            return len(seen_keys)

        # Reconciliation — abort if the shortfall is too large to be normal
        # churn.  FULL_REPLACE would overwrite the complete dataset with a
        # partial one, silently dropping projects from the database.
        collected = len(seen_keys)
        shortfall_pct = (1 - collected / initial_total) * 100 if initial_total else 0

        if shortfall_pct > 5:
            raise ValueError(
                f"SonarQube sharding: collected only {collected} of "
                f"{initial_total} expected projects ({shortfall_pct:.1f}% shortfall). "
                f"Aborting to prevent data loss via FULL_REPLACE."
            )

        if collected < initial_total:
            logger.warning(
                "SonarQube sharding: collected %d of %d expected projects "
                "(%.1f%% shortfall, within 5%% tolerance)",
                collected, initial_total, shortfall_pct,
            )
        else:
            logger.info(
                "SonarQube sharding complete: collected %d projects (expected %d)",
                collected, initial_total,
            )
        return collected

    _MAX_SHARD_DEPTH = 4  # 2-char base + 4 extensions = 6-char prefix (36^6 ≈ 2.2B)

    def _extend_shard(
        self,
        client: APIClient,
        engine: Engine,
        temp_table: str,
        context: RunContext,
        parent_prefix: str,
        parent_total: int,
        seen_keys: set[str],
        depth: int = 0,
        user_max_pages: int | None = None,
    ) -> set[str]:
        """Extend *parent_prefix* by one character, with early termination.

        Each call owns a ``local_names`` set counting unique project names
        found within this prefix's expansion only.  When *parent_total*
        unique names have been collected the remaining sub-prefixes are
        skipped — they would only yield duplicates.

        Returns the set of project names discovered.
        """
        if depth >= self._MAX_SHARD_DEPTH:
            logger.warning(
                "Shard '%s': reached max depth %d, stopping extension",
                parent_prefix, self._MAX_SHARD_DEPTH,
            )
            return set()

        local_names: set[str] = set()

        for char in _SEARCH_CHARS:
            sub_prefix = parent_prefix + char
            sub_total = self._probe(client, context, q=sub_prefix)
            if sub_total == 0:
                continue

            if sub_total > _SAFE_LIMIT:
                logger.info(
                    "Shard '%s' has %d results, extending to depth %d",
                    sub_prefix, sub_total, len(sub_prefix) + 1,
                )
                child_names = self._extend_shard(
                    client, engine, temp_table, context,
                    sub_prefix, sub_total, seen_keys,
                    depth=depth + 1,
                    user_max_pages=user_max_pages,
                )
                local_names.update(child_names)
            else:
                names = self._paginate_shard(
                    client, engine, temp_table, context,
                    q_param=sub_prefix, seen_keys=seen_keys,
                    user_max_pages=user_max_pages,
                )
                local_names.update(names)

            # Early termination
            if len(local_names) >= parent_total:
                logger.info(
                    "Shard '%s': early stop — collected %d/%d unique names",
                    parent_prefix, len(local_names), parent_total,
                )
                break

        return local_names
