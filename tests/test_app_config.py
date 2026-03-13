"""Tests for app_config.py — ported from AppConfigTests.cs (16 tests)."""

import json
import os
import tempfile

import pytest

from etl.app_config import (
    AppConfig,
    DatabaseSettings,
    PathSettings,
    TaskQueueSettings,
    load_config,
)
from etl import connection_helper


# --- AppConfig defaults ---


def test_appconfig_default_has_database_settings():
    config = AppConfig()
    assert config.database is not None


def test_appconfig_default_has_task_queue_settings():
    config = AppConfig()
    assert config.task_queue is not None


# --- DatabaseSettings defaults ---


def test_database_settings_default_host():
    db = DatabaseSettings()
    assert db.host == "localhost"


def test_database_settings_default_username():
    db = DatabaseSettings()
    assert db.username == "claude"


def test_database_settings_default_password_reads_env_var(monkeypatch):
    monkeypatch.setenv("ETL_DB_PASSWORD", "test_pw")
    config = load_config()
    assert config.database.password == "test_pw"


def test_database_settings_default_password_empty_when_env_var_missing(monkeypatch):
    monkeypatch.delenv("ETL_DB_PASSWORD", raising=False)
    config = load_config()
    assert config.database.password == ""


def test_database_settings_default_database_name():
    db = DatabaseSettings()
    assert db.database_name == "atc"


def test_database_settings_default_timeout():
    db = DatabaseSettings()
    assert db.timeout == 15


def test_database_settings_default_command_timeout():
    db = DatabaseSettings()
    assert db.command_timeout == 300


# --- TaskQueueSettings defaults ---


def test_task_queue_settings_default_thread_count():
    tq = TaskQueueSettings()
    assert tq.thread_count == 5


def test_task_queue_settings_default_poll_interval_ms():
    tq = TaskQueueSettings()
    assert tq.poll_interval_ms == 5000


def test_task_queue_settings_default_idle_shutdown_seconds():
    tq = TaskQueueSettings()
    assert tq.idle_shutdown_seconds == 28_800


def test_database_settings_password_ignores_appsettings_json(monkeypatch, tmp_path):
    monkeypatch.setenv("ETL_DB_PASSWORD", "from_env")
    settings_file = tmp_path / "appsettings.json"
    settings_file.write_text(json.dumps({"Database": {"Password": "from_json"}}))
    config = load_config(settings_file)
    assert config.database.password == "from_env"


# --- ConnectionHelper ---


def test_connection_helper_produces_correct_connection_string(monkeypatch):
    monkeypatch.setenv("ETL_DB_PASSWORD", "s3cret")
    config = AppConfig(
        database=DatabaseSettings(
            host="10.0.0.5",
            username="testuser",
            password="s3cret",
            database_name="testdb",
            timeout=10,
            command_timeout=120,
        )
    )
    connection_helper.initialize(config)
    cs = connection_helper.get_connection_string()
    assert "host=10.0.0.5" in cs
    assert "user=testuser" in cs
    assert "password=s3cret" in cs
    assert "dbname=testdb" in cs
    assert "connect_timeout=10" in cs


def test_connection_helper_default_config_uses_default_values(monkeypatch):
    monkeypatch.setenv("ETL_DB_PASSWORD", "required")
    config = AppConfig(database=DatabaseSettings(password="required"))
    connection_helper.initialize(config)
    cs = connection_helper.get_connection_string()
    assert "host=localhost" in cs
    assert "user=claude" in cs
    assert "password=required" in cs
    assert "dbname=atc" in cs
    assert "connect_timeout=15" in cs


def test_connection_helper_password_with_special_chars(monkeypatch):
    monkeypatch.setenv("ETL_DB_PASSWORD", "p@ss'w0rd!")
    config = AppConfig(database=DatabaseSettings(password="p@ss'w0rd!"))
    connection_helper.initialize(config)
    cs = connection_helper.get_connection_string()
    assert "p@ss'w0rd!" in cs
