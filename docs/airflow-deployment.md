# Airflow Deployment Guide

Deploy data-assets DAGs to an Airflow cluster with zero-touch automated updates.

After a one-time setup, new assets published to your package registry (Nexus, Artifactory, etc.) are automatically picked up by your Airflow servers — no manual intervention per release.

## How It Works

```
Developer pushes code
    |
    v
CI builds + publishes data-assets package to Nexus
    |
    v
systemd timer on Airflow nodes (every 15 min)
    |
    ├── pip install --upgrade data-assets   (picks up new version)
    └── data-assets sync --output-dir ...   (generates/updates DAG files)
          |
          v
Airflow scheduler parses new DAG files on next heartbeat
```

The `data-assets sync` command:
- Ensures every registered asset has an entry in `dag_overrides.toml` (creates the file on a fresh run, appends new assets on subsequent runs)
- Creates a DAG file for every registered asset
- Assets with `enabled = false` get `schedule = None` (visible in Airflow but won't auto-run)
- Assets with `enabled = true` get their configured schedule
- Regenerates existing files when the package changes
- Disables orphan DAGs when assets are removed
- Never modifies existing entries in `dag_overrides.toml` or custom `z_*` files

## Prerequisites

- RHEL 8 or 9 (or compatible: CentOS Stream, Rocky, Alma)
- Python 3.11+ installed (AppStream or custom build)
- Apache Airflow 3.0+ with CeleryExecutor or EdgeExecutor
- `apache-airflow-providers-standard` package (included in the default `apache-airflow[standard]` install)
- A Python virtual environment for Airflow (e.g., `/opt/airflow/venv`)
- Network access to your package registry (Nexus, PyPI, etc.)

## One-Time Setup

### Step 1: Install the package

```bash
sudo -u airflow /opt/airflow/venv/bin/pip install data-assets \
    --index-url https://nexus.company.com/repository/pypi/simple
```

Replace the `--index-url` with your corporate registry URL.

### Step 2: Generate systemd files

Run as the airflow user (no sudo needed for this step):

```bash
/opt/airflow/venv/bin/data-assets setup-systemd \
    --output-dir /tmp/data-assets-systemd \
    --dag-dir /opt/airflow/dags/data_assets \
    --venv-path /opt/airflow/venv \
    --pip-index-url https://nexus.company.com/repository/pypi/simple \
    --interval 15 \
    --user airflow
```

This creates three files in `/tmp/data-assets-systemd/`:

| File | Purpose |
|------|---------|
| `data-assets-sync.service` | Validates + backs up `dag_overrides.toml`, then runs `pip install --upgrade` + `data-assets sync` |
| `data-assets-sync.timer` | Fires the service every 15 minutes |
| `data-assets-setup.sh` | One-time install script (the only file that needs sudo) |

The service includes two safeguards:
- **Corruption guard**: validates `dag_overrides.toml` before sync. If the TOML is corrupt, the service logs an error (`user.err` priority) and skips the sync entirely.
- **Automatic backups**: creates a timestamped backup of `dag_overrides.toml` before each sync (4 per day, 30-day retention) in `<dag-dir>/.toml_backups/`. To restore from a corrupt file, copy the most recent backup back.

### Step 3: Review and run the setup script

**Always review the script before running it:**

```bash
cat /tmp/data-assets-systemd/data-assets-setup.sh
```

It will:
- Create the DAG output directory with correct ownership
- Copy the unit files to `/etc/systemd/system/`
- Restore SELinux context (RHEL requirement)
- Enable and start the timer

When satisfied, run it:

```bash
cd /tmp/data-assets-systemd
sudo bash data-assets-setup.sh
```

### Step 4: Verify

```bash
# Timer should be active
systemctl status data-assets-sync.timer

# Trigger a manual run to verify everything works
sudo systemctl start data-assets-sync.service

# Check the output
journalctl -u data-assets-sync.service --no-pager -n 50

# DAG files should appear
ls /opt/airflow/dags/data_assets/
```

### Step 5: Run on every Airflow node

Repeat Steps 1-4 on every node (scheduler and workers). The generated DAG files are deterministic — all nodes produce identical output.

For shared filesystem setups (NFS/EFS), only one node needs to run `data-assets sync`. The others just need the package installed.

## Admin Overrides (dag_overrides.toml)

This file controls which assets are active in production and how their DAGs are configured.

### Auto-creation

You do not need to create this file manually. `data-assets sync` manages it:

- **Fresh run** (no file exists): creates `dag_overrides.toml` with every registered asset listed, all set to `enabled = false` with a commented-out default schedule. This gives ops a complete, syntax-correct template.
- **Subsequent runs**: appends entries for newly discovered assets only. Existing entries are never modified or deleted.

Each auto-generated entry looks like:

```toml
# Added 2026-04-09T14:30:00 by data-assets sync
[sonarqube_measures]
enabled = false
# schedule = "0 5 * * *"
```

### Activating an asset

To enable an asset for production, set `enabled = true`:

```toml
[sonarqube_measures]
enabled = true
# schedule = "0 5 * * *"     <-- uncomment to customise
```

On the next sync cycle (~15 min), the DAG will be regenerated with its configured schedule.

### Example overrides

```toml
# Enable and change schedule
[servicenow_incidents]
enabled = true
schedule = "*/30 * * * *"
retries = 5

# Enable with Airflow Connections for secrets
[jira_projects]
enabled = true
secrets_source = "airflow_connection"
connection_id = "jira"

# GitHub multi-org: one DAG per org
[github_repos]
enabled = true
secrets_source = "airflow_connection"
connection_id = "github_app"

[[github_repos.orgs]]
org = "org-one"
installation_id = "12345"

[[github_repos.orgs]]
org = "org-two"
installation_id = "67890"
```

**Existing entries are never overwritten by `data-assets sync`.** The file is read on every sync and merged with package defaults.

### Available override keys

| Key | Default | Description |
|-----|---------|-------------|
| `enabled` | `false` | Whether the asset runs on a schedule (`true` = active, `false` = `schedule=None`) |
| `schedule` | Depends on run mode | Cron expression or Airflow preset (`@hourly`, `@daily`) |
| `retries` | `3` | Number of retries on failure |
| `retry_delay_minutes` | `5` | Delay between retries |
| `max_active_runs` | `1` | Maximum concurrent DAG runs |
| `owner` | `data-engineering` | DAG owner shown in Airflow UI |
| `tags` | Auto from source | Tags for filtering in Airflow UI |
| `secrets_source` | `env` | `env` (environment variables) or `airflow_connection` |
| `connection_id` | Auto from source | Airflow Connection ID (only when `secrets_source = "airflow_connection"`) |

### Default schedules by run mode

| Run mode | Default schedule |
|----------|-----------------|
| `full` | `0 5 * * *` (daily at 05:00) |
| `forward` | `@hourly` |
| `backfill` | None (manual trigger only) |
| `transform` | `0 8 * * *` (daily at 08:00, after source extractions) |

## Custom Admin DAGs

If you create custom DAGs in the same directory, prefix them with `z_` to protect them:

```
/opt/airflow/dags/data_assets/
    dag_overrides.toml          <-- your overrides (existing entries never touched)
    .toml_backups/              <-- automatic backups (4/day, 30-day retention)
    z_custom_report.py          <-- your custom DAG (never touched)
    dag_github_repos.py         <-- generated (safe to regenerate)
    dag_servicenow_incidents.py <-- generated
```

Any file starting with `z_` is ignored by `data-assets sync`.

## Managing DAGs in the Airflow UI

### Finding your DAGs

All generated DAGs are tagged by source. In the Airflow UI:
1. Go to **DAGs** page
2. Filter by tag: `github`, `jira`, `servicenow`, `sonarqube`, or `transform`

### Pausing and unpausing

Click the toggle switch next to any DAG to pause/unpause it. This is safe — `data-assets sync` does not change pause state.

### Triggering a manual run

Click the "play" button on any DAG to trigger it immediately. Useful for:
- Testing after initial setup
- Running a `backfill` mode asset (which has no schedule)
- Re-running after a failure

### Viewing run history

Click on a DAG name, then the **Grid** or **Graph** view. Each run shows:
- Duration
- Status (success/failed/running)
- Log output (click a task to see logs)

### Checking for drift warnings

If the DAG file is out of date with the installed package, the task log will contain:

```
WARNING - DAG fingerprint mismatch for 'github_repos': DAG file has abc123,
current asset definition produces def456. Regenerate DAG files with: data-assets sync
```

This means the package was updated but `data-assets sync` hasn't run yet. The timer will fix it automatically on the next cycle, or trigger it manually:

```bash
sudo systemctl start data-assets-sync.service
```

## CLI Reference

All commands are available as `data-assets <command>` or `python -m data_assets <command>`.

### data-assets list

Show all registered assets:

```bash
data-assets list
data-assets list --json
data-assets list --source github
data-assets list --output-dir /opt/airflow/dags/data_assets   # includes enabled status
```

When `--output-dir` is provided, the output includes an `ENABLED` column showing the `enabled` status from `dag_overrides.toml`.

### data-assets sync

Generate/update DAG files:

```bash
data-assets sync --output-dir /opt/airflow/dags/data_assets/
```

Output example:
```
Sync complete: 2 created, 1 updated, 0 disabled, 28 unchanged, 2 inactive
  + dag_new_asset.py
  + dag_another_asset.py
  ~ dag_updated_asset.py
  . new_asset (inactive — set enabled = true in dag_overrides.toml)
  . another_asset (inactive — set enabled = true in dag_overrides.toml)
```

Inactive assets have DAG files with `schedule=None` — they are visible in Airflow but won't run automatically until enabled.

### data-assets fingerprint

Print the fingerprint hash for an asset (useful for debugging drift):

```bash
data-assets fingerprint github_repos
# a02d38c4dcca1852
```

### data-assets setup-systemd

Generate systemd unit files and setup script:

```bash
data-assets setup-systemd \
    --output-dir /tmp/systemd \
    --dag-dir /opt/airflow/dags/data_assets \
    --venv-path /opt/airflow/venv \
    --pip-index-url https://nexus.company.com/repository/pypi/simple \
    --interval 15 \
    --user airflow
```

| Flag | Default | Description |
|------|---------|-------------|
| `--output-dir` | (required) | Where to write the generated files |
| `--dag-dir` | (required) | Airflow DAGs directory |
| `--venv-path` | `/opt/airflow/venv` | Python virtual environment path |
| `--pip-index-url` | System default | Corporate package registry URL |
| `--interval` | `15` | Sync interval in minutes |
| `--user` | `airflow` | System user to run the service as |

## Troubleshooting

### Timer is not firing

```bash
# Check timer status
systemctl status data-assets-sync.timer

# Check timer list
systemctl list-timers | grep data-assets

# Check for SELinux denials
ausearch -m avc -ts recent | grep data-assets
```

### Service fails with permission errors

```bash
# Check journalctl for details
journalctl -u data-assets-sync.service -n 50

# Common fix: ensure airflow user owns the venv and DAG dir
sudo chown -R airflow:airflow /opt/airflow/venv
sudo chown -R airflow:airflow /opt/airflow/dags/data_assets
```

### pip install fails (network/proxy)

If your environment uses a corporate proxy, ensure the airflow user's environment has proxy settings. Add to the service file:

```ini
Environment="HTTPS_PROXY=http://proxy.company.com:8080"
Environment="NO_PROXY=localhost,127.0.0.1"
```

Then reload: `sudo systemctl daemon-reload`

### Sync fails with "dag_overrides.toml is corrupt"

The systemd service validates the TOML before sync. If corrupt, it logs an error and skips.

```bash
# Check error details
journalctl -u data-assets-sync.service -p err -n 10

# Restore from the most recent backup
ls /opt/airflow/dags/data_assets/.toml_backups/
cp /opt/airflow/dags/data_assets/.toml_backups/dag_overrides.2026-04-09_q2.toml.bak \
   /opt/airflow/dags/data_assets/dag_overrides.toml

# Trigger a manual sync to verify
sudo systemctl start data-assets-sync.service
```

### Task fails with `DatabaseRetryExhausted`

The framework retried a database operation (temp table write, promotion, or checkpoint save) up to the configured limit and gave up.

**Common causes:** database connection pool exhausted, network instability, deadlocks.

**Solutions:**
1. Check database logs for the underlying error
2. Increase `DATA_ASSETS_DB_RETRY_ATTEMPTS` in the task's environment if the issue is transient
3. Increase `DATA_ASSETS_DB_RETRY_BASE_DELAY` if the database needs more recovery time between attempts
4. Check database connection capacity: `SHOW max_connections` (PostgreSQL) or `SHOW VARIABLES LIKE 'max_connections'` (MariaDB)
5. Retry the task manually once the database recovers

### Task fails with column length validation error

A `ValueError` mentioning "exceeds max length" means the API returned a value longer than the asset's declared `column_max_lengths` limit. This is a data quality guard — the limit may need adjusting if the API legitimately returns longer values. Check the asset class definition in `src/data_assets/assets/`.

### DAGs don't appear in Airflow

1. Check the DAG files exist: `ls /opt/airflow/dags/data_assets/`
2. Check Airflow's DAG processing log for import errors
3. Verify the DAGs directory is in Airflow's `dags_folder` config
4. Check file permissions: files must be readable by the Airflow scheduler process

### Reverting to a previous package version

```bash
sudo -u airflow /opt/airflow/venv/bin/pip install data-assets==0.1.9
sudo -u airflow /opt/airflow/venv/bin/data-assets sync \
    --output-dir /opt/airflow/dags/data_assets/
```

The sync regenerates all DAGs from the downgraded package. Previously disabled assets reappear.

## See Also

- [Running DAGs Locally](local-airflow.md) — try the full DAG pipeline on your local machine first
- [User Guide](user-guide.md) — running assets, run modes, watermarks
- [Configuration](configuration.md) — credentials and Airflow Connection setup
- [Assets Catalog](assets-catalog.md) — all built-in assets
