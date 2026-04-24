# data_assets — documentation

Self-contained ETL engine for data assets, backed by PostgreSQL or MariaDB, orchestrated by Apache Airflow. The root [README.md](../README.md) has the project elevator pitch; this page is the map for everything else.

## Start here (first day on the job)

1. **[tutorial-dev-setup.md](tutorial-dev-setup.md)** — clone, `.venv`, a local Postgres or MariaDB container (≈5 min).
2. **[tutorial-first-asset.md](tutorial-first-asset.md)** — build a `RestAsset` (declarative), an `APIAsset` (custom), and a `TransformAsset` (SQL-to-SQL). Each section is runnable end-to-end (≈30 min).
3. **[how-to-guides.md](how-to-guides.md)** — recipes for specific tasks once you know the basics: choose a run mode, debug a failed run, add a source, pass secrets from Airflow.
4. **[extending-reference.md](extending-reference.md)** — the definitive reference for every class attribute, override point, and extension seam. Use as lookup, not reading order.

## By intent

| I want to… | Read |
|------------|------|
| Understand the system design | [architecture.md](architecture.md) |
| Browse built-in assets + source endpoints | [assets-catalog.md](assets-catalog.md) |
| Configure env vars, secrets, DB connection | [configuration.md](configuration.md) |
| Run via Apache Airflow | [airflow-deployment.md](airflow-deployment.md) · [local-airflow.md](local-airflow.md) |
| Write unit or integration tests | [testing.md](testing.md) |
| Follow contribution rules, commit style | [../CONTRIBUTING.md](../CONTRIBUTING.md) |
| Add a new data source (whole vendor) | [tutorial-first-asset.md](tutorial-first-asset.md) §3 or §4 + [extending-reference.md](extending-reference.md) |
| Add a new SQL transform | [tutorial-first-asset.md](tutorial-first-asset.md) §5 + [extending-reference.md](extending-reference.md) "Dialect Extensions" |

## Diataxis map

This project follows the [Diataxis](https://diataxis.fr/) documentation model:

- **Tutorials** — learn by doing. Files: `tutorial-*.md`.
- **How-to guides** — task-oriented recipes. Files: `how-to-guides.md`.
- **Reference** — lookup, not reading order. Files: `assets-catalog.md`, `extending-reference.md`, `configuration.md`.
- **Explanation** — understanding the "why". Files: `architecture.md`.

Use tutorials when learning. Use how-to when you know what you want to do but not how. Use reference for accurate technical detail. Use explanation when the design itself is confusing.

## Common pitfalls for new contributors

- **Don't hand-write dialect-specific SQL in tests** — use `read_rows` / `table_exists` from `tests/integration/_db_utils.py`. See [testing.md](testing.md) "Writing dialect-portable integration tests" for the golden rule + DO/DON'T table.
- **Don't hand-branch on backend name in transforms** — use the helpers on `Dialect` (`week_start_from_ts`, `date_add_days`, `cast_bigint`, `UTC_SESSION_SQL`). See [extending-reference.md](extending-reference.md) "Dialect Extensions".
- **Don't skip `optional_columns` for nullable API fields** — missing required fields raise `MissingKeyError` and abort the run. See [extending-reference.md](extending-reference.md) "Optional columns".
- **Don't commit to `main`** — every change goes through a feature branch + PR. See [../CONTRIBUTING.md](../CONTRIBUTING.md).
