"""GitHub user details — full profile for each org member."""

from __future__ import annotations

from typing import Any

import pandas as pd

from data_assets.assets.github.helpers import get_github_base_url
from data_assets.core.api_asset import APIAsset
from data_assets.core.column import Column, Index
from data_assets.core.enums import LoadStrategy, ParallelMode, RunMode
from data_assets.core.registry import register
from data_assets.core.run_context import RunContext
from data_assets.core.types import PaginationConfig, PaginationState, RequestSpec
from data_assets.extract.token_manager import GitHubAppTokenManager
from sqlalchemy import DateTime, Integer, Text


@register
class GitHubUserDetails(APIAsset):
    """Full user profile for each organization member."""

    name = "github_user_details"
    source_name = "github"
    target_schema = "raw"
    target_table = "github_user_details"

    token_manager_class = GitHubAppTokenManager
    base_url = "https://api.github.com"

    pagination_config = PaginationConfig(strategy="none")
    parallel_mode = ParallelMode.ENTITY_PARALLEL
    max_workers = 4

    parent_asset_name = "github_members"

    load_strategy = LoadStrategy.FULL_REPLACE
    default_run_mode = RunMode.FULL

    # Carries clear PII (name, email, bio, location). Declared False for now
    # to preserve current behavior; flip to True and mark the relevant
    # Column(sensitive=True) once the tokenization endpoint is live and the
    # rollout for this asset is planned.
    contains_sensitive_data = False

    columns = [
        Column("login", Text(), nullable=False),
        Column("name", Text()),
        Column("company", Text()),
        Column("location", Text()),
        Column("email", Text()),
        Column("bio", Text()),
        Column("public_repos", Integer()),
        Column("followers", Integer()),
        Column("created_at", DateTime(timezone=True)),
        Column("updated_at", DateTime(timezone=True)),
    ]
    primary_key = ["login"]
    indexes = [
        Index(columns=("company",)),
    ]

    def build_entity_request(self, entity_key, context: RunContext, checkpoint=None) -> RequestSpec:
        base = get_github_base_url()
        return RequestSpec(
            method="GET",
            url=f"{base}/users/{entity_key}",
            params={},
            headers={"Accept": "application/vnd.github+json"},
        )

    def parse_response(self, response: dict[str, Any]) -> tuple[pd.DataFrame, PaginationState]:
        if not response or "login" not in response:
            return pd.DataFrame(columns=[c.name for c in self.columns]), PaginationState(has_more=False)

        self._check_required_keys([response], {
            "login": "login",
            "name": "name",
            "company": "company",
            "location": "location",
            "email": "email",
            "bio": "bio",
            "public_repos": "public_repos",
            "followers": "followers",
            "created_at": "created_at",
            "updated_at": "updated_at",
        })

        row = {
            "login": response["login"],
            "name": response.get("name"),
            "company": response.get("company"),
            "location": response.get("location"),
            "email": response.get("email"),
            "bio": response.get("bio"),
            "public_repos": response.get("public_repos"),
            "followers": response.get("followers"),
            "created_at": response.get("created_at"),
            "updated_at": response.get("updated_at"),
        }
        return pd.DataFrame([row]), PaginationState(has_more=False)
