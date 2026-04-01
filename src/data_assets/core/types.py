"""Supporting dataclass types used across the package."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PaginationConfig:
    """Declares how an API endpoint is paginated.

    Strategies:
        "page_number" — ?page=N&per_page=M (GitHub, SonarQube)
        "offset"      — ?offset=N&limit=M (Jira, ServiceNow legacy)
        "keyset"      — filter by last-seen sort key (ServiceNow best practice)
        "cursor"      — opaque cursor token from previous response
        "none"        — single request, no pagination

    Param name overrides (for APIs that use non-standard param names):
        page_size_param:   query param for page size (default "ps")
        page_number_param: query param for page number (default "p")
        limit_param:       query param for limit in offset mode (default "limit")
        offset_param:      query param for offset in offset mode (default "offset")
        page_index_path:   dot-path to current page index in response JSON
                           (default None — uses result-count heuristic)
    """

    strategy: str
    page_size: int = 100
    cursor_field: str | None = None
    total_path: str | None = None
    page_size_param: str = "ps"
    page_number_param: str = "p"
    limit_param: str = "limit"
    offset_param: str = "offset"
    page_index_path: str | None = None


@dataclass(frozen=True)
class RequestSpec:
    """Describes an HTTP request to be issued by the API client."""

    method: str
    url: str
    params: dict | None = None
    headers: dict | None = None
    body: dict | None = None


@dataclass
class PaginationState:
    """Returned by parse_response to indicate pagination continuation."""

    has_more: bool
    cursor: str | None = None
    next_offset: int | None = None
    next_page: int | None = None
    total_pages: int | None = None
    total_records: int | None = None


@dataclass
class ValidationResult:
    """Result of running validation checks on extracted data."""

    passed: bool
    failures: list[str] = field(default_factory=list)


class SkippedRequestError(Exception):
    """Raised when an API request should be skipped (e.g., 404 for deleted entity)."""

    pass
