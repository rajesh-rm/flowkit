# Inspired Ideas — Future Enhancements

> Patterns researched from Airbyte, Dagster, dlt, Prefect, Singer/Meltano, and Faros AI.
> Filtered to what's practical for our context: Python package → Postgres, orchestrated by Airflow, focused on SDLC tool APIs.
> Each idea includes estimated implementation effort and the pattern's origin.

---

## Tier 1: High Value, Low Effort

### 1. Error Classifier — retry / skip / fail by status code

**Pattern from:** Airbyte Composite Error Handler

Currently our `APIClient` handles 429 and 5xx with retry, 4xx with fail. But some 404s are expected (e.g., a deleted repo during entity-parallel extraction) and should be skipped, not fail the entire run.

**Idea:** Add an `error_policy` method to `APIAsset` that classifies responses:

```python
def classify_error(self, status_code: int, response: dict) -> str:
    """Return 'retry', 'skip', or 'fail'. Override per asset."""
    if status_code == 404:
        return "skip"  # Entity was deleted between runs
    if status_code == 429 or status_code >= 500:
        return "retry"
    return "fail"
```

The API client calls this instead of hardcoded status code checks. Entity-parallel workers that get "skip" log a warning and move to the next entity instead of killing the whole pool.

**Effort:** ~30 lines. Modify `api_client.py` + `api_asset.py`.

---

### 2. Rate Limit Header Extraction — proactive + reactive

**Pattern from:** Airbyte `WaitTimeFromHeader`, GitHub API docs

Our rate limiter is proactive (token bucket) but only reactively handles `Retry-After`. GitHub, Jira, and ServiceNow all return rate limit headers that we currently ignore:

| Source | Headers |
|--------|---------|
| GitHub | `X-RateLimit-Remaining`, `X-RateLimit-Reset` (unix timestamp) |
| Jira | `Retry-After` (seconds), `X-RateLimit-Remaining` |
| ServiceNow | `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset` |

**Idea:** After each API response, check `X-RateLimit-Remaining`. If it drops below a threshold (e.g., 10% of limit), preemptively slow down the rate limiter rather than waiting for a 429. This prevents rate limit errors entirely instead of reacting to them.

```python
def _check_rate_headers(self, response: httpx.Response) -> None:
    remaining = response.headers.get("X-RateLimit-Remaining")
    reset = response.headers.get("X-RateLimit-Reset")
    if remaining and int(remaining) < 50:
        wait = int(reset) - time.time() if reset else 30
        self._rate_limiter.pause_for(max(wait, 1))
```

**Effort:** ~25 lines in `api_client.py`.

---

### 3. Keyset Pagination for ServiceNow

**Pattern from:** ServiceNow best practices, Airbyte ServiceNow connector

Offset pagination (`sysparm_offset`) on large ServiceNow tables is unreliable — records inserted/updated during extraction cause rows to be skipped or duplicated. The correct approach is **keyset pagination**: sort by `sys_updated_on, sys_id` and filter with `WHERE (sys_updated_on, sys_id) > (last_seen_timestamp, last_seen_id)`.

**Idea:** Add a `"keyset"` pagination strategy. The asset declares the sort/filter columns, and `parse_response` returns the last record's values as the cursor:

```python
pagination_config = PaginationConfig(
    strategy="keyset",
    cursor_field="sys_updated_on,sys_id",
    page_size=1000,
)
```

**Effort:** ~40 lines. New strategy in `pagination.py` + update ServiceNow assets.

---

### 4. Run Metadata Enrichment

**Pattern from:** Dagster materialization metadata

Our `run_history.metadata` JSONB column exists but is barely used. Dagster records rich metadata per materialization: row counts, date ranges, data quality scores, even rendered charts.

**Idea:** Track and store per-run operational metrics automatically:

```json
{
  "date_range": ["2025-01-01", "2025-01-15"],
  "api_calls_made": 47,
  "rate_limit_waits": 2,
  "pages_fetched": 12,
  "extraction_seconds": 8.3,
  "promotion_seconds": 0.4,
  "retry_count": 1
}
```

Collect these counters during the run and write them to `metadata` on completion. Makes debugging much easier — "why did this run take 5 minutes?" is answered by checking `api_calls_made` and `rate_limit_waits`.

**Effort:** ~30 lines. Add counters to runner, pass through to `record_run_success()`.

---

## Tier 2: High Value, Medium Effort

### 5. Jira Custom Field Normalization

**Pattern from:** Faros AI, Jira API field metadata

Jira issues contain `customfield_10801`, `customfield_12345` etc. that are meaningless without the field metadata mapping. Different Jira instances have different custom fields.

**Idea:** Add a `jira_fields` asset that fetches `/rest/api/3/field` and stores the `id → name` mapping. Then `jira_issues` can either:
- Rename columns dynamically based on the field mapping
- Store custom fields as a JSONB column with human-readable keys

```python
@register
class JiraFields(APIAsset):
    name = "jira_fields"
    # Fetches /rest/api/3/field → stores id, name, type, schema
    # Other Jira assets can query this for custom field mapping
```

**Effort:** ~80 lines. New asset + field lookup utility.

---

### 6. Composite Cursor for Tie-Breaking

**Pattern from:** Airbyte `DatetimeBasedCursor`, ServiceNow keyset pagination

When two records have the same `updated_at` timestamp, a simple timestamp watermark can miss records or re-process them. The fix: use a composite cursor of `(timestamp, id)` as the watermark.

**Idea:** Extend `coverage_tracker` to support an optional `cursor_id` alongside the timestamp watermark. Assets that need tie-breaking declare `cursor_id_column` in addition to `date_column`.

```python
class CoverageTracker:
    forward_watermark: datetime
    forward_cursor_id: str | None  # Last processed ID at the watermark timestamp
```

**Effort:** ~50 lines. Extend ORM model + update `_compute_date_window` + update `_update_watermarks`.

---

### 7. JSON Flattening for Shallow Nesting

**Pattern from:** dlt auto-normalizer, Airbyte legacy normalization

Many API responses have 1-2 levels of nesting that we manually flatten in each `parse_response()`. Examples: `repo.owner.login`, `issue.fields.status.name`, `pr.user.login`.

**Idea:** Provide a `flatten_record(record, separator="__", max_depth=2)` utility that assets can call in `parse_response()`:

```python
# Before: manual extraction in every parse_response
{"user_login": pr.get("user", {}).get("login", "")}

# After: automatic flattening
flat = flatten_record(pr, max_depth=2)
# flat["user__login"] = "dev-alice"
# flat["head__ref"] = "feature/xyz"
```

Assets still declare their `columns` (static schema), but the flattening utility reduces boilerplate in `parse_response()`.

**Effort:** ~40 lines. New utility function in `core/` or `extract/`.

---

### 8. Schema Contract Modes — evolve / freeze / discard

**Pattern from:** dlt schema contracts

Our schema manager auto-adds new columns (`evolve` mode). But in production, you sometimes want to freeze the schema and reject unexpected fields, or silently discard them.

**Idea:** Add a `schema_contract` attribute to `Asset`:

```python
class Asset:
    schema_contract: str = "evolve"  # "evolve" | "freeze" | "discard"
```

- `evolve` (default): auto-add new columns (current behavior)
- `freeze`: raise error if asset definition has columns not in the table
- `discard`: silently ignore new columns in parse_response output

**Effort:** ~30 lines. Add attribute + branching in `ensure_columns()`.

---

## Tier 3: Medium Value, Higher Effort

### 9. Source Grouping — shared config across related assets

**Pattern from:** dlt `@source` decorator, Airbyte source specification

Assets for the same source (e.g., all GitHub assets) share `base_url`, `token_manager_class`, `rate_limit_per_second`. Currently each asset repeats these.

**Idea:** Introduce an optional `Source` class that groups shared config:

```python
class GitHubSource:
    base_url = "https://api.github.com"
    token_manager_class = GitHubAppTokenManager
    rate_limit_per_second = 10.0

@register
class GitHubRepos(APIAsset):
    source = GitHubSource
    # Inherits base_url, token_manager, rate_limit from source
```

This reduces repetition and ensures consistency across all assets for a source.

**Effort:** ~60 lines. New `Source` base class + runner integration.

---

### 10. Data Quality Checks as Separate Step

**Pattern from:** Dagster `@asset_check`, dbt tests

Our `validate()` runs inline and blocks promotion. Dagster separates "blocking checks" (must pass) from "warning checks" (log but continue). dbt has `tests:` that run post-materialization.

**Idea:** Split validation into two tiers:

```python
class Asset:
    def validate_blocking(self, df, context) -> ValidationResult:
        """Must pass — blocks promotion."""
        return super().validate(df, context)  # PK not null, row count > 0

    def validate_warning(self, df, context) -> list[str]:
        """Warnings only — logged but don't block."""
        warnings = []
        if len(df) < self._expected_min_rows:
            warnings.append(f"Unusually low row count: {len(df)}")
        return warnings
```

Warnings are stored in `run_history.metadata` for review.

**Effort:** ~40 lines. Extend `Asset` + runner.

---

### 11. Changelog / Audit Event Streaming

**Pattern from:** Faros AI, Jira changelog, ServiceNow audit

Both Jira (`/rest/api/3/issue/{id}/changelog`) and ServiceNow (`sys_audit`) provide change history as event streams. These are naturally append-only and ideal for tracking "what changed and when" — key for engineering metrics.

**Idea:** Add dedicated changelog assets:

- `jira_issue_changelog` — fetches changelogs per issue (entity-parallel)
- `servicenow_audit` — fetches audit records (sequential, incremental)

These use `APPEND` load strategy since they're immutable event logs. Enable use cases like: "average time an issue spends in each status", "who reassigned this incident".

**Effort:** ~100 lines per asset. Two new assets + test fixtures.

---

### 12. GraphQL Support for GitHub

**Pattern from:** Airbyte GitHub connector, GitHub API docs

GitHub's REST API requires many requests for nested data (PRs → reviews → comments is 3 endpoints). GraphQL can fetch all three in one request with nested pagination.

**Idea:** Add a `GraphQLAsset` variant of `APIAsset` that:
- Sends POST requests with GraphQL queries
- Handles Relay-style cursor pagination (`pageInfo.endCursor`, `pageInfo.hasNextPage`)
- Supports nested pagination (paginate inner lists within a single outer page)
- Respects GitHub's point-based rate limiting (different from REST)

This dramatically reduces API call count for deeply nested GitHub data.

**Effort:** ~120 lines. New asset subclass + pagination variant.

---

## Tier 4: Nice-to-Have, Lower Priority

### 13. Row-Count Anomaly Detection

**Pattern from:** Dagster freshness policies, Monte Carlo-style observability

Compare current run's row count against recent historical average. Alert if it drops significantly (possible API issue or filter bug).

```python
# In runner, after extraction:
avg_rows = get_avg_rows_last_n_runs(engine, asset_name, n=5)
if rows_extracted < avg_rows * 0.5:
    logger.warning("Row count anomaly: got %d, average is %d", rows_extracted, avg_rows)
```

**Effort:** ~20 lines.

---

### 14. Asset Dependency Graph for Transform Scheduling

**Pattern from:** Dagster asset dependencies, dbt DAG

Transform assets declare `source_tables` but there's no enforcement that those tables are fresh. Dagster blocks downstream assets if upstream hasn't materialized.

**Idea:** Before running a transform, check that all `source_tables` have a recent `last_success_at` in the asset registry. Warn or fail if a source is stale.

**Effort:** ~30 lines in runner.

---

### 15. Dry Run Mode

**Pattern from:** dlt `pipeline.run(write_disposition="skip")`

Run the full extraction pipeline but don't promote to the main table. Useful for testing new assets, validating API responses, and checking row counts before going live.

```python
run_asset("new_asset", run_mode="full", dry_run=True)
# Extracts to temp table, validates, but skips promotion
```

**Effort:** ~10 lines. Skip the promote step when `dry_run=True`.

---

## Implementation Priority Guide

For the next development cycle, I'd recommend this order:

1. **Error classifier** (#1) — prevents silent failures, easy win
2. **Rate limit headers** (#2) — required for production GitHub/Jira usage
3. **Run metadata** (#4) — essential for debugging production runs
4. **Keyset pagination** (#3) — correctness fix for ServiceNow
5. **Jira custom fields** (#5) — needed as soon as Jira is used in production
6. **JSON flattening utility** (#7) — reduces boilerplate across all assets
7. **Everything else** — as needed based on production experience
