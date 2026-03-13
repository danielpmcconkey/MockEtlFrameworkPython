# Configuration

All configuration is managed through `AppConfig` (`src/etl/app_config.py`). Four frozen dataclasses — `PathSettings`, `DatabaseSettings`, `TaskQueueSettings`, and the top-level `AppConfig` — are constructed once by `load_config()` and never mutated.

Precedence (highest wins): environment variables > `appsettings.json` > compiled defaults.

## AppConfig Sections

### PathSettings

| Property | Source | Default |
|---|---|---|
| `etl_root` | `ETL_ROOT` env var | `""` |
| `etl_log_path` | `ETL_LOG_PATH` env var | `""` |

Cannot be set via `appsettings.json`. Both values come exclusively from environment variables.

### DatabaseSettings

| Property | Source | Default |
|---|---|---|
| `host` | `appsettings.json` | `"localhost"` |
| `username` | `appsettings.json` | `"claude"` |
| `password` | `ETL_DB_PASSWORD` env var | `""` |
| `database_name` | `appsettings.json` | `"atc"` |
| `timeout` | `appsettings.json` | `15` |
| `command_timeout` | `appsettings.json` | `300` |

`password` **cannot** be set via `appsettings.json` — the loader ignores any `Password` key in the JSON file. The CLI fails fast if no password is available (except for `--show-config`).

### TaskQueueSettings

| Property | Source | Default |
|---|---|---|
| `thread_count` | `appsettings.json` | `5` |
| `poll_interval_ms` | `appsettings.json` | `5000` |
| `idle_shutdown_seconds` | `appsettings.json` | `28800` (8 hours) |

## Environment Variables

| Variable | Required | Purpose |
|---|---|---|
| `ETL_DB_PASSWORD` | Yes | Database password. CLI exits if missing. |
| `ETL_ROOT` | No | Project root path override. Used by `path_helper` for path resolution and `{ETL_ROOT}` token expansion. Falls back to `pyproject.toml` walk if unset. |
| `ETL_LOG_PATH` | No | Log file path. Stored in config but not consumed by `path_helper` tokens. |

All env vars are read once during `load_config()` and cached for the process lifetime via the frozen dataclass.

## appsettings.json

Located at the project root alongside `cli.py`. Only overridden values need to appear — defaults come from the dataclass definitions.

```json
{
  "Database": {
    "Host": "localhost"
  },
  "TaskQueue": {
    "ThreadCount": 5,
    "PollIntervalMs": 5000,
    "IdleShutdownSeconds": 28800
  }
}
```

## connection_helper

`src/etl/connection_helper.py`. Module-level helper that builds psycopg connection parameters from `DatabaseSettings`. Initialized at startup via `connection_helper.initialize(config)`.

- `get_connection_string()` — returns a libpq-style DSN string.
- `get_dsn_dict()` — returns connection params as a `dict` for use with `psycopg.connect(**kwargs)`.
- Sets `statement_timeout` (in milliseconds) via the PostgreSQL `options` parameter, derived from `command_timeout` (seconds) in config.

## path_helper

`src/etl/path_helper.py`. Module-level helper that resolves output paths against the project root. Initialized at startup via `path_helper.initialize(config)`.

- `resolve(path)` — expands `{TOKEN}` placeholders and resolves relative paths against the project root. Absolute paths are returned as-is.
- `get_project_root()` — returns the project root. First checks `ETL_ROOT` from config; if unset, walks up from `path_helper.py` looking for `pyproject.toml`.
- Known token (`ETL_ROOT`) is populated from `PathSettings` at initialization, not from a direct env var lookup.
- Token expansion is case-insensitive.

## date_partition_helper

`src/etl/date_partition_helper.py`. Shared utility for scanning date-partitioned output directories. Used by both `CsvFileWriter` and `ParquetFileWriter` for append-mode prior-partition lookups.

- `find_latest_partition(job_dir)` — returns the latest `YYYY-MM-DD`-named subdirectory within the given directory, or `None` if no valid partitions exist.
- Called on the table-level directory that contains date partitions.
