"""Application-level configuration for the ETL framework.

Layered: compiled defaults -> appsettings.json -> environment variables.
All values immutable after construction. Env vars read once and cached.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class PathSettings:
    etl_root: str = ""
    etl_log_path: str = ""


@dataclass(frozen=True)
class DatabaseSettings:
    host: str = "localhost"
    username: str = "claude"
    password: str = ""
    database_name: str = "atc"
    timeout: int = 15
    command_timeout: int = 300


@dataclass(frozen=True)
class TaskQueueSettings:
    thread_count: int = 5
    poll_interval_ms: int = 5000
    idle_shutdown_seconds: int = 28_800


@dataclass(frozen=True)
class AppConfig:
    paths: PathSettings = field(default_factory=PathSettings)
    database: DatabaseSettings = field(default_factory=DatabaseSettings)
    task_queue: TaskQueueSettings = field(default_factory=TaskQueueSettings)


_current_config: AppConfig | None = None


def get_config() -> AppConfig | None:
    """Return the most recently loaded AppConfig, or None."""
    return _current_config


def load_config(appsettings_path: str | Path | None = None) -> AppConfig:
    """Build an AppConfig from defaults, appsettings.json, and env vars.

    Precedence (highest wins): env vars > appsettings.json > compiled defaults.
    Password is ONLY sourced from the ETL_DB_PASSWORD env var, never JSON.
    """
    db_overrides: dict = {}
    tq_overrides: dict = {}

    if appsettings_path is not None:
        p = Path(appsettings_path)
        if p.exists():
            with open(p) as f:
                raw = json.load(f)

            if "Database" in raw:
                db_json = raw["Database"]
                if "Host" in db_json:
                    db_overrides["host"] = db_json["Host"]
                if "Username" in db_json:
                    db_overrides["username"] = db_json["Username"]
                if "DatabaseName" in db_json:
                    db_overrides["database_name"] = db_json["DatabaseName"]
                if "Timeout" in db_json:
                    db_overrides["timeout"] = db_json["Timeout"]
                if "CommandTimeout" in db_json:
                    db_overrides["command_timeout"] = db_json["CommandTimeout"]

            if "TaskQueue" in raw:
                tq_json = raw["TaskQueue"]
                if "ThreadCount" in tq_json:
                    tq_overrides["thread_count"] = tq_json["ThreadCount"]
                if "PollIntervalMs" in tq_json:
                    tq_overrides["poll_interval_ms"] = tq_json["PollIntervalMs"]
                if "IdleShutdownSeconds" in tq_json:
                    tq_overrides["idle_shutdown_seconds"] = tq_json["IdleShutdownSeconds"]

    # Env vars override everything (except password which is env-only)
    paths = PathSettings(
        etl_root=os.environ.get("ETL_ROOT", ""),
        etl_log_path=os.environ.get("ETL_LOG_PATH", ""),
    )

    db_overrides["password"] = os.environ.get("ETL_DB_PASSWORD", "")
    database = DatabaseSettings(**db_overrides)

    task_queue = TaskQueueSettings(**tq_overrides)

    global _current_config
    _current_config = AppConfig(paths=paths, database=database, task_queue=task_queue)
    return _current_config
