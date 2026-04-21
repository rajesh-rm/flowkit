"""GitHub deployments — per-repository, entity-parallel, GraphQL transport.

First GraphQL asset in the codebase. GitHub's REST deployments endpoint can't
return `latestStatus` inline without an N+1 per-deployment call, so we use the
GraphQL deployments connection — one page = one request for id, env, state,
created/updated timestamps, creator, commit sha, and the latest status.

Pagination: cursor-based via `pageInfo.endCursor`; ordered by `CREATED_AT DESC`
so `should_stop()` can halt paging once we're older than the watermark or the
configured `pull_upto_days` cap (whichever is more recent).

Multi-org: follows the existing `GITHUB_ORGS`-per-run pattern. Each org is a
separate Airflow task with its own GitHub App creds, invoked with its own
`partition_key`. `filter_entity_keys` scopes the parent repo list to that org
and reshapes each `"org/repo"` string into the `{owner, name, full_name}` dict
the GraphQL variables and the entity-key injection both expect.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd
from sqlalchemy import BigInteger, DateTime, Text

from data_assets.assets.github.helpers import GitHubRepoAsset, get_github_base_url
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec

logger = logging.getLogger(__name__)

# Sort by CREATED_AT DESC so `should_stop` can halt paging once the page's
# oldest deployment predates the watermark / pull_upto_days cap. GitHub's
# `DeploymentOrder` enum only supports CREATED_AT; direction is inlined
# because ASC would invert should_stop's stop-when-older-than-threshold rule.
_DEPLOYMENTS_QUERY = """
query($owner: String!, $repo: String!, $pageSize: Int!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    deployments(first: $pageSize, after: $cursor, orderBy: {field: CREATED_AT, direction: DESC}) {
      pageInfo { endCursor hasNextPage }
      nodes {
        databaseId environment description state createdAt updatedAt
        creator { login }
        commit { oid }
        latestStatus { state }
      }
    }
  }
}
"""

_DESC_LIMIT = 4000
_DESC_HEAD = 2000
_DESC_TAIL = 2000


@register
class GitHubDeployments(GitHubRepoAsset):
    """Deployment history per repository via GitHub GraphQL, UPSERT on (databaseId, owner)."""

    name = "github_deployments"
    target_table = "github_deployments"

    # GraphQL point budget (5000/hr/token) dominates REST's 5000 calls/hr.
    rate_limit_per_second = 1.0
    max_workers = 3

    pagination_config = PaginationConfig(strategy="cursor", page_size=100)

    # Dict-shaped entity keys: owner/name feed GraphQL variables; full_name is
    # injected as org_repo_key by the framework. Populated by filter_entity_keys.
    entity_key_column = None
    entity_key_map = {
        "owner": "organization",
        "name": "repo_name",
        "full_name": "org_repo_key",
    }

    load_strategy = LoadStrategy.UPSERT
    default_run_mode = RunMode.FORWARD
    date_column = "created_at"

    # Max history depth (~2 years). FULL mode backfills to today - pull_upto_days.
    # FORWARD mode uses max(watermark, today - pull_upto_days) as its stop point.
    pull_upto_days: int = 720

    columns = [
        Column("deployment_id", BigInteger(), nullable=False),
        Column("organization", Text(), nullable=False),
        Column("repo_name", Text(), nullable=False),
        Column("org_repo_key", Text(), nullable=False),
        Column("environment", Text()),
        Column("description", Text(), nullable=True),
        Column("state", Text()),
        Column("latest_status", Text(), nullable=True),
        Column("creator_login", Text(), nullable=True),
        Column("sha", Text()),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("updated_at", DateTime(timezone=True)),
        Column("source_url", Text()),
    ]

    primary_key = ["deployment_id", "organization"]

    column_max_lengths = {
        "organization": 100,
        "repo_name": 100,
        "org_repo_key": 200,
        "environment": 256,
        # After truncation the worst case is 2000 + len("[truncated]") + 2000 = 4011.
        "description": 4100,
        "state": 30,
        "latest_status": 30,
        "creator_login": 100,
        "sha": 40,
        "source_url": 512,
    }

    indexes = [
        Index(columns=("deployment_id",)),
        Index(columns=("created_at",)),
        Index(columns=("org_repo_key",)),
        Index(columns=("environment",)),
    ]

    def filter_entity_keys(self, keys: list) -> list:
        """Scope to current org (via base) and reshape strings into dicts.

        Drops any parent entry that isn't a ``"owner/repo"`` string. The drop is
        logged (not silent) so operators can diagnose a repo going missing from
        downstream deployment data rather than discovering the gap weeks later.
        """
        result: list[dict[str, str]] = []
        for full_name in super().filter_entity_keys(keys):
            if isinstance(full_name, str) and "/" in full_name:
                owner, name = full_name.split("/", 1)
                result.append({"owner": owner, "name": name, "full_name": full_name})
            else:
                logger.warning(
                    "github_deployments: dropping malformed parent repo "
                    "full_name=%r (expected 'owner/repo' string)",
                    full_name,
                )
        return result

    def build_entity_request(
        self,
        entity_key: Any,
        context: RunContext,
        checkpoint: dict[str, Any] | None = None,
    ) -> RequestSpec:
        cursor = (checkpoint or {}).get("cursor")
        variables: dict[str, Any] = {
            "owner": entity_key["owner"],
            "repo": entity_key["name"],
            "pageSize": self.pagination_config.page_size,
            "cursor": cursor,
        }
        return RequestSpec(
            method="POST",
            url=f"{get_github_base_url()}/graphql",
            body={"query": _DEPLOYMENTS_QUERY, "variables": variables},
            headers={"Accept": "application/vnd.github+json"},
        )

    def parse_response(
        self, response: dict[str, Any],
    ) -> tuple[pd.DataFrame, PaginationState]:
        # Guard against non-dict top-level responses (proxy rewrites, maintenance
        # HTML pages deserialized as strings, schema regressions). The alternative
        # is a cryptic `AttributeError: 'list' object has no attribute 'get'`
        # surfacing at the runner boundary with no asset context.
        if not isinstance(response, dict):
            raise ValueError(
                f"GraphQL response for {self.name} is not a JSON object: "
                f"got {type(response).__name__}"
            )

        # GraphQL returns HTTP 200 even for query/permission errors — inspect the
        # body. These are not transient; fail fast and let the runner drop the temp.
        if errors := response.get("errors"):
            first = errors[0] if isinstance(errors, list) and errors else errors
            message = (
                first.get("message", str(errors))
                if isinstance(first, dict)
                else str(errors)
            )
            raise ValueError(f"GraphQL error from {self.name}: {message}")

        deployments = (
            ((response.get("data") or {}).get("repository") or {}).get("deployments")
            or {}
        )
        nodes = deployments.get("nodes") or []
        page_info = deployments.get("pageInfo") or {}

        # Missing-key check runs against each raw node before the DataFrame
        # collapse hides absent keys as NaN. Null parents (creator, commit,
        # latestStatus) are tolerated by _present_in's permissive-null rule.
        self._check_required_keys(
            nodes,
            {
                "databaseId": "deployment_id",
                "environment": "environment",
                "description": "description",
                "state": "state",
                "createdAt": "created_at",
                "updatedAt": "updated_at",
                "creator.login": "creator_login",
                "commit.oid": "sha",
                "latestStatus.state": "latest_status",
            },
        )

        # organization / repo_name / org_repo_key are filled by entity_key_map
        # injection after this method returns; source_url is filled by transform().
        records = [
            {
                "deployment_id": n["databaseId"],
                "organization": None,
                "repo_name": None,
                "org_repo_key": None,
                "environment": n.get("environment"),
                "description": n.get("description"),
                "state": n.get("state"),
                "latest_status": (n.get("latestStatus") or {}).get("state"),
                "creator_login": (n.get("creator") or {}).get("login"),
                "sha": (n.get("commit") or {}).get("oid"),
                "created_at": n.get("createdAt"),
                "updated_at": n.get("updatedAt"),
                "source_url": None,
            }
            for n in nodes
        ]
        df = pd.DataFrame(records, columns=[c.name for c in self.columns])
        return df, PaginationState(
            has_more=bool(page_info.get("hasNextPage")),
            cursor=page_info.get("endCursor"),
        )

    def should_stop(self, df: pd.DataFrame, context: RunContext) -> bool:
        """Stop paging when the page's oldest createdAt precedes the threshold.

        Threshold is the more recent of the forward watermark (FORWARD mode)
        and today - pull_upto_days — so runaway or missing watermarks never
        pull beyond the configured history cap.
        """
        if df.empty or "created_at" not in df.columns:
            return False
        oldest = pd.to_datetime(df["created_at"], utc=True, errors="coerce").min()
        if pd.isna(oldest):
            return False
        return oldest < self._history_threshold(context)

    def _history_threshold(self, context: RunContext) -> datetime:
        cap = datetime.now(UTC) - timedelta(days=self.pull_upto_days)
        watermark = context.start_date
        if watermark is None:
            return cap
        if watermark.tzinfo is None:
            watermark = watermark.replace(tzinfo=UTC)
        return max(cap, watermark)

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute source_url from the injected repo key and truncate long descriptions.

        Truncation emits a single INFO log per run listing the count of rewritten
        descriptions — without it, silent destructive rewrite of a user-visible
        field is impossible to audit post-hoc. Counting happens inline inside the
        map callable so there is no separate pre-computation pass over the column.
        """
        if df.empty:
            return df
        df["source_url"] = (
            "https://github.com/"
            + df["org_repo_key"].astype(str)
            + "/deployments/"
            + df["deployment_id"].astype(str)
        )
        truncated = [0]

        def _truncate_and_count(text: Any) -> str | None:
            if pd.isna(text):
                return None
            if len(text) <= _DESC_LIMIT:
                return text
            truncated[0] += 1
            return text[:_DESC_HEAD] + "[truncated]" + text[-_DESC_TAIL:]

        df["description"] = df["description"].map(_truncate_and_count)
        if truncated[0]:
            logger.info(
                "github_deployments: truncated %d description(s) over %d chars",
                truncated[0], _DESC_LIMIT,
            )
        return df
