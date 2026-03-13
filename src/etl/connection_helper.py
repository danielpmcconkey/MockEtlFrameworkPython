"""Builds a psycopg connection string from DatabaseSettings."""

from __future__ import annotations

from etl.app_config import AppConfig, DatabaseSettings

_settings: DatabaseSettings = DatabaseSettings()


def initialize(config: AppConfig) -> None:
    global _settings
    _settings = config.database


def get_connection_string() -> str:
    return (
        f"host={_settings.host} "
        f"user={_settings.username} "
        f"password={_settings.password} "
        f"dbname={_settings.database_name} "
        f"connect_timeout={_settings.timeout} "
        f"options='-c statement_timeout={_settings.command_timeout * 1000}'"
    )


def get_dsn_dict() -> dict:
    """Return connection params as a dict for psycopg.connect(**kwargs)."""
    return {
        "host": _settings.host,
        "user": _settings.username,
        "password": _settings.password,
        "dbname": _settings.database_name,
        "connect_timeout": _settings.timeout,
        "options": f"-c statement_timeout={_settings.command_timeout * 1000}",
    }
