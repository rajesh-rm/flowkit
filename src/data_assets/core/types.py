"""Supporting dataclass types used across the package."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class PaginationConfig:
    """Declares how an API endpoint is paginated."""

    strategy: str  # "cursor", "offset", "page_number", "date_window", "none"
    page_size: int = 100
    cursor_field: str | None = None
    total_field: str | None = None


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
