"""Tests for db.engine: connection resolution, engine factory, schema creation."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy.engine import Engine

from data_assets.db.engine import _resolve_database_url, ensure_schemas, get_engine


# ---------------------------------------------------------------------------
# _resolve_database_url
# ---------------------------------------------------------------------------


class TestResolveDatabaseUrl:
    def test_returns_env_var_when_airflow_not_installed(self, monkeypatch):
        """When Airflow is not installed (ImportError), fall back to DATABASE_URL."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://user:pass@localhost/db")

        with patch(
            "data_assets.db.engine.load_dotenv"
        ):
            # Simulate Airflow not installed by making the import raise
            with patch.dict("sys.modules", {"airflow": None, "airflow.sdk": None}):
                url = _resolve_database_url()

        assert url == "postgresql://user:pass@localhost/db"

    def test_returns_env_var_when_set(self, monkeypatch):
        """DATABASE_URL env var should be returned when Airflow is unavailable."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://host/testdb")

        with patch(
            "data_assets.db.engine.load_dotenv"
        ):
            with patch.dict("sys.modules", {"airflow": None, "airflow.sdk": None}):
                url = _resolve_database_url()

        assert url == "postgresql://host/testdb"

    def test_raises_runtime_error_when_no_source_available(self, monkeypatch):
        """RuntimeError when neither Airflow nor DATABASE_URL is available."""
        monkeypatch.delenv("DATABASE_URL", raising=False)

        with patch("data_assets.db.engine.load_dotenv"):
            with patch.dict("sys.modules", {"airflow": None, "airflow.sdk": None}):
                with pytest.raises(RuntimeError, match="No database connection found"):
                    _resolve_database_url()

    def test_airflow_connection_returned_when_available(self):
        """When Airflow is installed and connection exists, return its URI."""
        mock_hook = MagicMock()
        mock_conn = MagicMock()
        mock_conn.get_uri.return_value = "postgresql://airflow@db:5432/prod"
        mock_hook.get_connection.return_value = mock_conn

        mock_module = MagicMock()
        mock_module.BaseHook = mock_hook

        with patch.dict("sys.modules", {
            "airflow": MagicMock(),
            "airflow.sdk": mock_module,
        }):
            url = _resolve_database_url("my_conn")

        mock_hook.get_connection.assert_called_once_with("my_conn")
        assert url == "postgresql://airflow@db:5432/prod"

    def test_falls_back_to_env_when_airflow_connection_fails(self, monkeypatch):
        """When Airflow is installed but connection lookup fails, fall back to env."""
        monkeypatch.setenv("DATABASE_URL", "postgresql://fallback/db")

        mock_hook = MagicMock()
        mock_hook.get_connection.side_effect = Exception("conn not found")

        mock_module = MagicMock()
        mock_module.BaseHook = mock_hook

        with patch("data_assets.db.engine.load_dotenv"):
            with patch.dict("sys.modules", {
                "airflow": MagicMock(),
                "airflow.sdk": mock_module,
            }):
                url = _resolve_database_url()

        assert url == "postgresql://fallback/db"


# ---------------------------------------------------------------------------
# get_engine
# ---------------------------------------------------------------------------


class TestGetEngine:
    def test_returns_engine_instance(self):
        """get_engine should return a SQLAlchemy Engine."""
        get_engine.cache_clear()
        mock_engine = MagicMock(spec=Engine)

        with patch(
            "data_assets.db.engine._resolve_database_url",
            return_value="postgresql://fake/db",
        ):
            with patch(
                "data_assets.db.engine.create_engine",
                return_value=mock_engine,
            ) as mock_create:
                with patch(
                    "data_assets.db.engine.attach_utc_session_hook",
                ):
                    engine = get_engine()

        mock_create.assert_called_once_with(
            "postgresql://fake/db",
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        assert engine is mock_engine
        get_engine.cache_clear()

    def test_caches_engine(self):
        """Subsequent calls should return the same cached engine."""
        get_engine.cache_clear()
        mock_engine = MagicMock(spec=Engine)

        with patch(
            "data_assets.db.engine._resolve_database_url",
            return_value="postgresql://fake/db",
        ) as mock_resolve:
            with patch(
                "data_assets.db.engine.create_engine",
                return_value=mock_engine,
            ):
                with patch(
                    "data_assets.db.engine.attach_utc_session_hook",
                ):
                    engine1 = get_engine()
                    engine2 = get_engine()

        assert engine1 is engine2
        mock_resolve.assert_called_once()
        get_engine.cache_clear()

    def test_attaches_utc_session_hook(self):
        """get_engine must attach the UTC session hook to the created engine."""
        get_engine.cache_clear()
        mock_engine = MagicMock(spec=Engine)

        with patch(
            "data_assets.db.engine._resolve_database_url",
            return_value="postgresql://fake/db",
        ):
            with patch(
                "data_assets.db.engine.create_engine",
                return_value=mock_engine,
            ):
                with patch(
                    "data_assets.db.engine.attach_utc_session_hook",
                ) as mock_hook:
                    get_engine()

        mock_hook.assert_called_once_with(mock_engine)
        get_engine.cache_clear()


# ---------------------------------------------------------------------------
# ensure_schemas
# ---------------------------------------------------------------------------


class TestEnsureSchemas:
    def test_creates_all_required_schemas(self):
        """ensure_schemas should execute CREATE SCHEMA for each required schema."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

        ensure_schemas(mock_engine)

        assert mock_conn.execute.call_count == 4
        executed_sql = [
            str(c.args[0].text) for c in mock_conn.execute.call_args_list
        ]
        assert "CREATE SCHEMA IF NOT EXISTS data_ops" in executed_sql
        assert "CREATE SCHEMA IF NOT EXISTS raw" in executed_sql
        assert "CREATE SCHEMA IF NOT EXISTS mart" in executed_sql
        assert "CREATE SCHEMA IF NOT EXISTS temp_store" in executed_sql


# ---------------------------------------------------------------------------
# attach_utc_session_hook
# ---------------------------------------------------------------------------


class TestUtcSessionHook:
    """Every new DB connection must force session timezone to UTC."""

    def _run_connect_hook(self, dialect_name: str) -> list[str]:
        """Create a mock engine, attach the hook, fire a fake 'connect' event,
        and return the SQL statements the hook asked the driver to execute.
        """
        from data_assets.db.engine import attach_utc_session_hook
        from sqlalchemy import event

        engine = MagicMock(spec=Engine)
        engine.dialect = MagicMock()
        engine.dialect.name = dialect_name

        captured: list[str] = []

        # Capture the listener instead of registering on a real engine.
        def _fake_listens_for(target, identifier):
            def decorator(fn):
                dbapi_conn = MagicMock()
                cursor = MagicMock()
                cursor.execute.side_effect = lambda sql: captured.append(sql)
                dbapi_conn.cursor.return_value = cursor
                fn(dbapi_conn, MagicMock())
                return fn
            return decorator

        with patch.object(event, "listens_for", side_effect=_fake_listens_for):
            attach_utc_session_hook(engine)

        return captured

    def test_postgres_sets_utc(self):
        assert self._run_connect_hook("postgresql") == ["SET TIME ZONE 'UTC'"]

    def test_mariadb_sets_utc(self):
        assert self._run_connect_hook("mariadb") == ["SET time_zone = '+00:00'"]

    def test_mysql_sets_utc(self):
        # SQLAlchemy uses 'mysql' as the dialect name even for MariaDB drivers.
        assert self._run_connect_hook("mysql") == ["SET time_zone = '+00:00'"]

    def test_unknown_dialect_is_noop(self):
        assert self._run_connect_hook("sqlite") == []
