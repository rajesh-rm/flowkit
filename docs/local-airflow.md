# Running DAGs Locally

Run the full Airflow + data-assets pipeline on your local machine. Useful for:

- Developers testing new assets end-to-end before merging
- Admins learning the DAG lifecycle before deploying to production

> **Production architecture** is different: Airflow scheduler and workers run on separate nodes using EdgeExecutor. This guide uses `airflow standalone` (single-node) for simplicity. See [How Production Differs](#9-how-production-differs) at the end.
>
> **Important**: This guide uses a separate Airflow venv (`~/airflow-venv`) from the dev venv (`.venv`). Do not install Airflow into your dev venv — it causes noisy warnings and credential resolution surprises during ad-hoc testing. See [section 8](#8-why-separate-venvs-matter) for details.

## 1. Prerequisites

Before starting, you need:

- **Python 3.11+** installed
- **PostgreSQL 14+** running locally (see [quickstart-dev.md](quickstart-dev.md) sections 3-6 for setup)
- **Source credentials** set as environment variables (see [configuration.md](configuration.md))
- **`DATABASE_URL`** exported:
  ```bash
  export DATABASE_URL="postgresql://flowkit:flowkit@localhost:5432/data_assets"
  ```

## 2. Install data-assets from the Local Repo

Create a venv for Airflow and install the package in editable mode directly from your local clone:

```bash
# Create a dedicated venv (keeps Airflow deps separate from dev tooling)
python3.11 -m venv ~/airflow-venv

# Install Airflow with the standard provider (includes PythonOperator)
~/airflow-venv/bin/pip install "apache-airflow[standard]"

# Install data-assets from your local clone (editable — changes take effect immediately)
~/airflow-venv/bin/pip install -e /path/to/flowkit
```

> **Using uv?** Replace `pip install` with `uv pip install` in the commands above.

Verify the install:

```bash
~/airflow-venv/bin/data-assets list
```

This should print all registered assets.

## 3. Install Airflow Locally

If you installed Airflow in step 2, initialise its metadata database and create an admin user:

```bash
export AIRFLOW_HOME=~/airflow

# Initialise the Airflow metadata DB (SQLite by default for local dev)
~/airflow-venv/bin/airflow db migrate

# Create an admin user
~/airflow-venv/bin/airflow users create \
    --username admin \
    --firstname Local \
    --lastname Dev \
    --role Admin \
    --email admin@localhost \
    --password admin
```

> For full Airflow installation options, see the [official Airflow installation guide](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html).

## 4. Generate DAGs

Point `data-assets sync` at Airflow's DAGs folder:

```bash
mkdir -p ~/airflow/dags/data_assets

~/airflow-venv/bin/data-assets sync --output-dir ~/airflow/dags/data_assets
```

On the first run, this creates:

- `dag_overrides.toml` with every asset listed (`enabled = false`)
- A DAG file per asset (all with `schedule=None` until activated)

Check the output:

```bash
ls ~/airflow/dags/data_assets/
```

## 5. Activate an Asset

Edit `dag_overrides.toml` to enable the asset you want to test:

```bash
vi ~/airflow/dags/data_assets/dag_overrides.toml
```

Set `enabled = true` and optionally uncomment the schedule:

```toml
[sonarqube_projects]
enabled = true
# schedule = "0 5 * * *"
```

Re-run sync to regenerate the DAG with its schedule:

```bash
~/airflow-venv/bin/data-assets sync --output-dir ~/airflow/dags/data_assets
```

> For local testing, you can leave `enabled = false` and manually trigger the DAG from the Airflow UI instead of enabling a schedule.

## 6. Start the Airflow Server

```bash
export AIRFLOW_HOME=~/airflow

~/airflow-venv/bin/airflow standalone
```

This starts the webserver, scheduler, and triggerer in a single process. Open the Airflow UI at **http://localhost:8080** and log in with `admin` / `admin`.

> `airflow standalone` is for local development only. See the [Airflow standalone reference](https://airflow.apache.org/docs/apache-airflow/stable/start.html) for details.

## 7. Trigger and Observe a DAG Run

1. In the Airflow UI, go to **DAGs**
2. Find your asset (e.g., `sonarqube_projects`). Filter by tag if needed.
3. If the DAG is paused (toggle is off), click the toggle to unpause it
4. Click the **play button** (right side) to trigger a manual run
5. Click the **DAG name** to open it, then **Grid** or **Graph** view to watch progress
6. Click the **run** task, then **Log** to see the `run_asset()` output

The task log will show extraction progress, row counts, and the final result.

## 8. Why Separate Venvs Matter

This guide creates `~/airflow-venv` separate from the dev `.venv`. If you install Airflow into the same venv you use for ad-hoc `run_asset()` testing, two things happen:

### Problem 1: Noisy warnings on every ad-hoc run

The package has optional Airflow imports — when Airflow is not installed, they silently fall back to environment variables. When Airflow IS installed (same venv), these imports succeed and the code tries to look up credentials from Airflow Connections:

- `engine.py` calls `BaseHook.get_connection("data_assets_db")` before checking `DATABASE_URL`
- `token_manager.py` calls `BaseHook.get_connection()` for every credential key (`SONARQUBE_TOKEN`, `GITHUB_APP_ID`, etc.)

Each call fails (no matching Airflow Connection exists) and logs a WARNING with a stack trace. A single `run_asset()` call can produce **12+ warning stack traces** before falling through to your env vars. The asset still runs correctly, but the output is very noisy.

### Problem 2: Airflow Connections silently override env vars

The credential resolution order is: Airflow Connection > environment variable > `.env` file.

If you configured Airflow Connections for local Airflow testing (e.g., a `data_assets_db` connection), then later switch to ad-hoc `run_asset()` testing with a different `DATABASE_URL`, the Airflow Connection takes priority. Your `DATABASE_URL` is silently ignored.

### How to detect

You are hitting this if you see warnings like:

```
WARNING - Airflow connection 'data_assets_db' lookup failed, falling back to env vars
WARNING - Airflow connection 'SONARQUBE_TOKEN' lookup failed
```

### How to fix

**Option A (recommended): Use separate venvs.** The dev `.venv` (no Airflow) for ad-hoc testing, and `~/airflow-venv` (with Airflow) for DAG testing. This is what this guide recommends.

**Option B: Same venv, suppress the lookups.** If you must use one venv, unset `AIRFLOW_HOME` before ad-hoc runs so Airflow's metadata DB is not reachable:

```bash
unset AIRFLOW_HOME
.venv/bin/python -c "from data_assets import run_asset; run_asset('sonarqube_projects', 'full')"
```

**Option C: Same venv, accept the noise.** The warnings are harmless — the asset runs correctly via env var fallback. If the noise doesn't bother you, ignore it.

## 9. How Production Differs

| Aspect | Local | Production |
|--------|-------|------------|
| Executor | `standalone` (single process) | EdgeExecutor (scheduler + remote workers) |
| Package install | `pip install -e .` from local clone | `pip install data-assets` from Nexus/Artifactory |
| DAG sync | Manual: `data-assets sync` | Automated: systemd timer every 15 min |
| Asset activation | Edit TOML manually | Ops edits TOML on server nodes |
| TOML backups | None | Automatic (4/day, 30-day retention) |
| Corruption guard | None | systemd validates TOML before sync |
| Metadata DB | SQLite | PostgreSQL |

For production deployment, see [Airflow Deployment Guide](airflow-deployment.md).

## 10. Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| DAG doesn't appear in UI | Airflow hasn't parsed the file yet | Wait 30 seconds or restart Airflow |
| DAG appears but shows "Import Error" | Missing dependency in Airflow venv | Check the error message. Run `~/airflow-venv/bin/pip install <missing>` |
| DAG shows `schedule=None` | Asset not activated | Set `enabled = true` in `dag_overrides.toml` and re-run sync |
| Task fails with `No database connection found` | `DATABASE_URL` not set | Export it before starting Airflow: `export DATABASE_URL="postgresql://..."` |
| Task fails with credential errors | Source env vars not set | Set the required variables (see [configuration.md](configuration.md)) before starting Airflow |
| Port 8080 already in use | Another process using the port | Set `AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8081` to use a different port |
| Many `Airflow connection 'X' lookup failed` warnings | Airflow is installed in the same venv as your ad-hoc testing | Use separate venvs, or `unset AIRFLOW_HOME` before running. See [section 8](#8-why-separate-venvs-matter) |
| `DATABASE_URL` is ignored, connects to wrong DB | Airflow Connection `data_assets_db` exists and takes priority | Remove the Airflow Connection, or use a separate dev venv without Airflow |

For Airflow-specific issues, see:

- [Airflow troubleshooting guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/index.html)
- [Airflow FAQ](https://airflow.apache.org/docs/apache-airflow/stable/faq.html)

## See Also

- [Local Dev Quickstart](quickstart-dev.md) -- package development setup
- [Airflow Deployment Guide](airflow-deployment.md) -- production deployment with systemd
- [Configuration](configuration.md) -- credentials and runtime overrides
