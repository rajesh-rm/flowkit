"""BaseAPIClient — the protocol all source clients implement.

Accepts an Airflow Connection (host, credentials) and endpoint config.
Provides extract() which yields dicts of raw API response data.
Handles pagination internally. Does NOT handle retries — Airflow's
task-level retries cover transient failures.
"""
