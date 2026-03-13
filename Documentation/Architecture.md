# MockEtlFrameworkPython -- Architecture

> **Note:** This monolithic doc is no longer the source of truth. It is kept for
> reference only. The authoritative documentation has been broken out into
> focused reference docs -- see [README.md](README.md) for the index.

## Purpose

This project is a Python port of the C# MockEtlFramework, which mirrors the behavior of a production ETL Framework built on PySpark/Python. The production framework is a core platform component of a large big data system. The goal is to replicate its execution model and module structure in Python, enabling reverse engineering and rewriting of production ETL jobs.

---

## Production ETL Framework -- Background

The production ETL Framework operates by reading **job configuration files** (JSON). Each job conf contains a serialized, ordered list of **ETL modules** to execute in series. Modules communicate with one another through a **shared state** -- a dictionary of named DataFrames that each module can read from and write to.

### Production Modules

| Module | Responsibility |
|---|---|
| **Data Sourcing** | Reads from the data lake. Users specify tables, columns, date ranges, and additional filters. Returns data as a PySpark DataFrame stored in shared state. |
| **Transformation** | Runs Spark SQL against DataFrames already in shared state, producing a new transformed DataFrame that is added back to shared state. |
| **DataFrame Writer** | Writes a named DataFrame from shared state out to a curation space for downstream consumers. |
| **External** | Executes a custom user-supplied Python class, allowing teams to perform any logic not covered by the standard modules. |

---

## This Project's Approach

The ETL Framework execution model is reproduced in Python using pandas DataFrames, SQLite for in-memory SQL, and psycopg for PostgreSQL access. The core concepts map as follows:

| Production Concept | Python Equivalent |
|---|---|
| PySpark DataFrame | `pandas.DataFrame` |
| Shared state | `dict[str, object]` passed through module chain |
| ETL module | Classes extending `etl.modules.base.Module` (ABC) |
| Job configuration | JSON file deserialized into `etl.job_conf.JobConf` |
| Spark SQL | In-memory SQLite connection (`sqlite3`) |
| Framework executor | `etl.job_runner.run()` loads the job conf and runs modules in sequence |

### Key Design Principles

- **Module chain execution:** Modules run in the order defined by the job conf. Each module receives the current shared state, performs its operation, and returns the updated state.
- **Shared state as the integration contract:** Modules are decoupled from one another. They communicate only through named entries in the shared state dictionary.
- **JSON-driven configuration:** Job behavior is defined externally in JSON, not in code. This keeps the framework generic and reusable across many different jobs.
- **pandas DataFrame as the data type:** No custom DataFrame wrapper. Standard pandas DataFrames are used throughout, providing familiar tabular operations (filter, merge, groupby, concat, etc.) that map directly to PySpark equivalents.
- **Full-load temporal data:** The data lake follows a snapshot (full-load) pattern. Each day's load is a complete picture of a table's state at that point in time, identified by an `ifw_effective_date` date column. `DataSourcing` returns a flat DataFrame that includes the `ifw_effective_date` column, allowing consumers to work across date ranges in a single DataFrame without requiring special date-loop logic in the framework itself.
- **Explicit effective date management:** The caller MUST supply an effective date for every invocation. There is no auto-advance or gap-fill logic -- the executor runs exactly one effective date per invocation. Effective dates are injected into shared state before the pipeline runs and picked up automatically by `DataSourcing`. For batch processing across date ranges, use the task queue (`control.task_queue`) with one row per (job, date) pair.
- **run_date vs. effective date:** `run_date` in `control.job_runs` is the calendar date the executor actually ran (always today). `min_effective_date` / `max_effective_date` record which data date that run processed. These are separate concerns.

---

## Package Structure

### `src/etl` (Framework Package)

The core framework library. All modules, configuration, and control logic live under this package.

#### `etl.modules`

| Module | Purpose |
|---|---|
| `Module` (ABC in `base.py`) | Core module interface: `execute(shared_state: dict[str, object]) -> dict[str, object]`. All modules extend this abstract base class. |
| `DataSourcing` | Queries a PostgreSQL data lake schema for a specified table, column list, and effective date range. Returns a single flat `pandas.DataFrame` with `ifw_effective_date` appended as a column (skipped if the caller already includes it). Supports an optional `additional_filter` clause. Date resolution supports five mutually exclusive modes (validated at construction): (1) static dates via `min_effective_date`/`max_effective_date` in config, (2) `lookback_days: N` -- pulls T-N through T-0 relative to `__etlEffectiveDate`, (3) `most_recent_prior: True` -- queries the datalake for the latest `ifw_effective_date` strictly before T-0 (handles weekends/gaps), (4) `most_recent: True` -- queries the datalake for the latest `ifw_effective_date` on or before T-0 (inclusive), (5) no date fields -- both min and max fall back to `__etlEffectiveDate`. For modes 3 and 4, if the datalake query finds no matching date, `execute` stores an empty DataFrame with the correct column schema (including `ifw_effective_date`) instead of raising. **Empty-result handling (all modes):** when a query executes successfully but returns zero rows, `_fetch_data` constructs the DataFrame using the known column names (including `ifw_effective_date`) instead of returning an empty result with no columns. This preserves column schema so downstream modules always receive a structurally valid DataFrame with zero rows but correct columns. |
| `Transformation` | Opens an in-memory SQLite connection, registers every `pandas.DataFrame` in the current shared state as a SQLite table (via `df.to_sql()`), executes user-supplied free-form SQL (via `pd.read_sql_query()`), and stores the result DataFrame back into shared state under a caller-specified result name. Empty DataFrames (those with columns but zero rows) are registered as schema-only SQLite tables. DataFrames with no columns at all are silently skipped. Date and datetime values in object columns are converted to ISO strings before registration for SQLite compatibility. **Empty-result handling:** when the transformation SQL returns zero rows, the result DataFrame retains its column schema via `pd.read_sql_query()`. |
| `DataFrameWriter` | Writes a named DataFrame from shared state to a PostgreSQL curation schema. Auto-creates the target table if it does not exist (type inference from sample values). Supports `Overwrite` mode (truncate then insert) and `Append` mode (insert only). All writes are transaction-wrapped. Values that lost type during SQLite round-tripping (date/datetime strings) are coerced back to native types before insertion. |
| `External` | Stub module (Phase 2). Accepts `assembly_path` and `type_name` parameters but raises `NotImplementedError` on execution. Placeholder for user-supplied Python logic injection. |
| `ParquetFileWriter` | Writes a named DataFrame from shared state to a directory of Parquet files (`part-00000.parquet`, `part-00001.parquet`, etc.) using PyArrow. Output path: `{outputDirectory}/{jobDirName}/{outputTableDirName}/{etl_effective_date}/{fileName}/part-N.parquet`. Supports configurable part count and `Overwrite`/`Append` write modes. Injects an `etl_effective_date` column. In append mode, reads prior partition data and concatenates before writing. |
| `CsvFileWriter` | Writes a named DataFrame from shared state to a date-partitioned CSV file. Output path: `{outputDirectory}/{jobDirName}/{outputTableDirName}/{etl_effective_date}/{fileName}`. Injects an `etl_effective_date` column (the current effective date string) into every row before writing. UTF-8 (no BOM), configurable line endings (LF or CRLF), RFC 4180 quoting. Supports optional trailer lines with token substitution (`{row_count}`, `{date}`, `{timestamp}`). In **Append** mode, reads the prior partition's CSV, strips the trailing record when `trailer_format` is set (reads file as raw lines, removes last line, parses via `pd.read_csv`), drops the prior `etl_effective_date` column, and concatenates with the current DataFrame before writing. |

#### `etl` (root)

| Module | Purpose |
|---|---|
| `app_config` | Configuration model using frozen dataclasses: `AppConfig`, `PathSettings`, `DatabaseSettings`, `TaskQueueSettings`. Populated via `load_config()` from defaults, `appsettings.json` overrides, and environment variable reads. All values are immutable after construction. `PathSettings` fields (`etl_root`, `etl_log_path`) are sourced exclusively from env vars. `DatabaseSettings.password` is sourced only from `ETL_DB_PASSWORD` -- never from JSON. A module-level `_current_config` cache allows retrieval via `get_config()`. |
| `connection_helper` | Module-level helper that builds a psycopg connection string from `DatabaseSettings`. Initialized at startup via `connection_helper.initialize(config)`. Provides both `get_connection_string()` (DSN string) and `get_dsn_dict()` (keyword dict for `psycopg.connect(**kwargs)`). |
| `date_partition_helper` | Shared utility for scanning date-partitioned output directories. `find_latest_partition(dir)` returns the latest `YYYY-MM-DD`-named subdirectory. Used by both `CsvFileWriter` and `ParquetFileWriter` for append-mode prior-partition lookups. |
| `path_helper` | Module-level helper that resolves output paths against the project root directory. Initialized at startup via `path_helper.initialize(config)`. Supports `{TOKEN}` expansion in paths (e.g., `{ETL_ROOT}`); known tokens are populated from `AppConfig.paths` at initialization. Project root resolution: first checks `ETL_ROOT` token, then walks up from the package directory to find `pyproject.toml`. |
| `module_factory` | Factory function `create(config: dict) -> Module`. Reads the `type` discriminator field from a config dict and instantiates the appropriate `Module` implementation using a `match/case` statement. Also defines the `WriteMode` enum (`OVERWRITE`, `APPEND`). Raises `ValueError` on unknown types or missing `type` field. |
| `job_conf` | JSON deserialization model (`JobConf` dataclass). Contains the job name, an optional `first_effective_date` (metadata -- not used by the executor), and an ordered `list[dict]` of module configurations. Constructed via `JobConf.from_file(path)` or `JobConf.from_dict(raw)`. |
| `job_runner` | Pipeline executor. `run(job_conf_path, initial_state)` loads a job conf via `JobConf.from_file()`, iterates the module list, creates each module via `module_factory.create()`, and threads shared state through the pipeline. Accepts an optional `initial_state` dictionary pre-populated by the executor (used to inject effective dates). Supports per-job file logging when `ETL_LOG_PATH` is configured. Returns the final shared state. |

#### `etl.control`

Orchestration layer that sits above `job_runner`. Reads job registrations and dependency graph from the PostgreSQL `control` schema, determines which jobs need to run, executes them in the correct order, and records the outcome of every run attempt.

| Module | Purpose |
|---|---|
| `control_db.JobRegistration` | Frozen dataclass for a `control.jobs` row -- job ID, name, description, conf path, and active flag. |
| `control_db.JobDependency` | Frozen dataclass for a `control.job_dependencies` row -- downstream job ID, upstream job ID, and dependency type (`SameDay` or `Latest`). |
| `control_db` | Data-access layer for the control schema. All methods open and close their own connections. Reads: `get_active_jobs`, `get_all_dependencies`, `get_succeeded_job_ids`, `get_ever_succeeded_job_ids`, `get_last_succeeded_max_effective_date`, `get_next_attempt_number` (keyed by effective date range). Writes: `insert_run` (records `run_date`, `min_effective_date`, `max_effective_date`), `mark_running`, `mark_succeeded`, `mark_failed`, `mark_skipped`. |
| `execution_plan` | `build()` function that applies Kahn's topological sort to produce an ordered run list. Only unsatisfied dependency edges are counted: `SameDay` edges are always treated as unsatisfied (checked at execution time); a `Latest` edge is satisfied if the upstream job has ever succeeded. Raises `ValueError` on cycle detection. |
| `job_executor_service` | Public orchestrator function `run(effective_date, specific_job_name)`. Requires an explicit effective date -- no auto-advance or gap-fill. Loads jobs and dependencies, builds the execution plan, injects the effective date into shared state, and runs each pipeline through `job_runner`. Records `Pending -> Running -> Succeeded / Failed` in `control.job_runs`. Failed jobs' `SameDay` dependents are recorded as `Skipped`. |
| `task_queue_service.TaskQueueService` | Long-running queue-based executor class. Polls `control.task_queue` for pending tasks and executes them across N threads (configurable via `TaskQueueSettings.thread_count`). Uses a claim-by-job model: each thread acquires a PostgreSQL advisory lock on a job name, claims all pending tasks for that job, and runs them sequentially in effective-date order. Different jobs run in parallel across threads. If a task fails, remaining tasks in the batch are marked Failed. A watchdog thread exits the service after `idle_shutdown_seconds` (default 8 hours) of inactivity. |
| `task_queue_service.TaskQueueItem` | Frozen dataclass for a claimed task from the queue -- task ID, job name, effective date. |

**Dependency types:**

| Type | Semantics |
|---|---|
| `SameDay` | The upstream job must have a `Succeeded` run with the same `run_date` (execution date) as the downstream job. Within a single executor invocation jobs run in topological order, so this is satisfied naturally when A precedes B in the plan. |
| `Latest` | The upstream job must have succeeded at least once for any `run_date`. Used for one-time setup jobs or slowly-changing reference data that only needs to be current, not date-aligned. |

---

### `cli.py` (CLI Entry Point)

Entry point for running jobs from the command line.

```
python cli.py --service                        # long-running queue executor (polls control.task_queue)
python cli.py --show-config                    # dump resolved config and exit
python cli.py <effective_date>                 # run all active jobs for that date
python cli.py <effective_date> <job_name>      # run one job for that date
```

An effective date argument (format: `YYYY-MM-DD`) is **required** for non-service, non-show-config invocations. **`--service` mode** delegates to `TaskQueueService`. All other modes delegate to `job_executor_service.run()`. `run_date` is always set to today internally and is never a CLI argument.

#### Configuration

At startup, the CLI calls `load_config()` with the path to `appsettings.json` (located next to `cli.py`). If the file is absent, defaults from `AppConfig` are used. All environment variable values (`ETL_DB_PASSWORD`, `ETL_ROOT`, `ETL_LOG_PATH`) are read once at `load_config()` time and cached in the frozen dataclasses for the process lifetime -- no repeated `os.environ` lookups. The database password (`DatabaseSettings.password`) **cannot** be set via `appsettings.json` (any `Password` key in the file is silently ignored). The CLI exits if no password is available. After config is loaded, `cli.py` calls both `connection_helper.initialize(config)` and `path_helper.initialize(config)` to wire up the module-level helpers.

**`appsettings.json`** (committed to the repo):
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

Only overridden values need to appear -- defaults come from the dataclass defaults.

#### Environment Variables

| Variable | Required | Purpose |
|---|---|---|
| `ETL_DB_PASSWORD` | Yes | Database password. CLI exits if missing. |
| `ETL_ROOT` | No | Project root path override. Used by `path_helper` for path resolution and `{ETL_ROOT}` token expansion. Falls back to `pyproject.toml` walk if unset. |
| `ETL_LOG_PATH` | No | Per-job log file output directory. If set, `job_runner` writes a log file per (job, date) invocation. |

#### Queue Executor (`--service`)

The queue executor is a long-running process that polls `control.task_queue` for pending tasks. It parallelizes work across configurable threads.

**Threading model (claim-by-job):**
- N worker threads, all identical (configurable via `TaskQueueSettings.thread_count`, default 5)
- Each thread claims ALL pending tasks for a single job using PostgreSQL advisory locks (`pg_try_advisory_xact_lock(hashtext(job_name))`)
- Tasks within a job are processed sequentially in effective-date order
- Different jobs run in parallel across threads (e.g., Oct 1 Job A and Oct 2 Job B run concurrently, but Oct 1 Job A and Oct 2 Job A never do)
- If a task fails, all remaining tasks in the batch are marked Failed (preserves append-mode / CDC ordering safety)
- Task claim uses `FOR UPDATE SKIP LOCKED` (Postgres row-level locking) within the advisory-locked transaction
- Poll interval and idle shutdown threshold are configurable via `appsettings.json`

**Lifecycle:** Start the executor, populate the queue via SQL, executor picks up work automatically. A watchdog thread exits the service after `idle_shutdown_seconds` of inactivity (default 8 hours).

**Queue population example:**
```sql
INSERT INTO control.task_queue (job_name, effective_date, execution_mode)
SELECT j.job_name, d.dt::date, 'parallel'
FROM control.jobs j
CROSS JOIN generate_series('2024-10-01'::date, '2024-12-31'::date, '1 day') d(dt)
WHERE j.is_active = true
ORDER BY d.dt, j.job_name;
```

**Monitoring:**
```sql
SELECT status, COUNT(*) FROM control.task_queue GROUP BY status;
```

**Sample job configuration** (registered in `control.jobs`):
```json
{
  "jobName": "CustomerAccountSummary",
  "firstEffectiveDate": "2024-10-01",
  "modules": [
    {
      "type": "DataSourcing",
      "resultName": "customers",
      "schema": "datalake",
      "table": "customers",
      "columns": ["id", "first_name", "last_name"]
    },
    {
      "type": "DataSourcing",
      "resultName": "accounts",
      "schema": "datalake",
      "table": "accounts",
      "columns": ["account_id", "customer_id", "account_type", "account_status", "current_balance"]
    },
    {
      "type": "Transformation",
      "resultName": "customer_account_summary",
      "sql": "SELECT c.id AS customer_id, c.first_name, c.last_name, COUNT(a.account_id) AS account_count, ROUND(SUM(CASE WHEN a.account_status = 'Active' THEN a.current_balance ELSE 0 END), 2) AS active_balance FROM customers c LEFT JOIN accounts a ON c.id = a.customer_id GROUP BY c.id, c.first_name, c.last_name ORDER BY c.id"
    },
    {
      "type": "DataFrameWriter",
      "source": "customer_account_summary",
      "targetTable": "customer_account_summary",
      "writeMode": "Overwrite"
    }
  ]
}
```

Note that no date fields appear in the `DataSourcing` modules above -- the executor injects `__etlEffectiveDate` at runtime via shared state, and both min and max default to that value. Other date modes are available:

```json
// Lookback: pull T-3 through T-0 (4 days inclusive)
{ "type": "DataSourcing", "lookbackDays": 3, ... }

// Most recent prior: query datalake for latest date strictly before T-0
{ "type": "DataSourcing", "mostRecentPrior": true, ... }

// Most recent: query datalake for latest date on or before T-0
{ "type": "DataSourcing", "mostRecent": true, ... }
```

Date modes are mutually exclusive -- mixing `lookbackDays`, `mostRecentPrior`, `mostRecent`, or static dates raises `ValueError` at construction time. For `mostRecentPrior` and `mostRecent`, if no matching date exists in the datalake, the module stores an empty DataFrame with the correct column schema rather than raising. More broadly, when any date mode's query returns zero rows, `DataSourcing` preserves column schema in the resulting DataFrame (see the module table above for details), so jobs that encounter no-data dates (weekends, holidays, filtered-to-nothing) produce header-only CSV output instead of crashing downstream.

---

### `tests/` (pytest Test Suite)

Unit test coverage for framework components. Tests do not require a live database -- all DataFrame and Transformation tests operate entirely in memory. File writer tests use temporary directories.

| Test Module | Coverage |
|---|---|
| `test_dataframe_ops` | pandas DataFrame operations -- len, columns, column selection, filter, withColumn (assign), drop, sort_values, head, concat, drop_duplicates, merge (inner + left), groupby/size, empty DataFrame with schema, CSV parsing |
| `test_transformation` | `Transformation` module -- basic SELECT, WHERE, column projection, JOIN across two DataFrames, GROUP BY aggregation, shared state preservation, non-DataFrame entries silently ignored, empty DataFrame schema registration, left join with empty DataFrame |
| `test_module_factory` | `module_factory.create()` -- all module types, optional fields (`additionalFilter`, `lookbackDays`, `mostRecentPrior`, `mostRecent`), both write modes, target schema override, mutually exclusive date mode validation, missing required fields, unknown type error, missing type field error |
| `test_data_sourcing` | `DataSourcing` date resolution -- lookback range calculation, zero-day lookback, static date passthrough, `__etlEffectiveDate` fallback, missing effective date errors, mutually exclusive mode validation (all conflict combinations), negative lookbackDays |
| `test_app_config` | `AppConfig` defaults, `DatabaseSettings` defaults, `TaskQueueSettings` defaults, `connection_helper` string building, env var sourcing for password, negative test proving `appsettings.json` cannot override the password env var |
| `test_csv_file_writer` | `CsvFileWriter` -- header/data row output, `etl_effective_date` injection, no-header mode, RFC 4180 quoting (commas, double-quotes), null rendering, trailer format tokens (`row_count`, `date`, `timestamp`), overwrite mode (date partitioning, idempotent reruns), append mode (union with prior partition, trailer stripping with/without trailer format), UTF-8 no BOM, LF/CRLF line endings, directory creation, missing DataFrame/effective date errors, shared state passthrough |
| `test_parquet_file_writer` | `ParquetFileWriter` -- single/multi-part file output, row count preservation across parts, overwrite mode (deletes existing), directory creation, missing DataFrame/effective date errors, shared state passthrough, null handling, `etl_effective_date` injection, schema validation, native date/datetime types, nullable date columns, append mode (first run, union with prior partition) |
| `test_v4_jobs` | V4 job SQL transformations -- PeakTransactionTimes (hourly aggregation, ordering, empty input, rounding), DailyBalanceMovement (debit/credit totals, net movement, unmatched accounts, non-standard txn types, empty input, output schema), CreditScoreDelta (change detection, no-prior handling, customer enrichment, ordering, customer scope), BranchVisitsByCustomer (customer join, ordering, column passthrough, missing customer, empty visits), DansTransactionSpecial (denormalization, ordering, address dedup, state/province aggregation, null address, output schema) |

---

## Technology Choices

| Concern | Choice | Rationale |
|---|---|---|
| Language / runtime | Python 3.11+ | Production framework is Python; direct port eliminates impedance mismatch |
| DataFrame library | pandas >= 2.1 | Standard tabular data library; no custom DataFrame class needed |
| PostgreSQL client | psycopg >= 3.1 | Modern Python Postgres driver |
| In-process SQL engine | sqlite3 (stdlib) | Enables free-form SQL in `Transformation` without a running server; part of the standard library |
| Test framework | pytest >= 7.4 | Industry standard for Python |
| Parquet file output | PyArrow >= 14.0 | Apache Arrow's Python bindings; native Parquet read/write |

---

## File Writer Modules

In addition to `DataFrameWriter` (which writes to PostgreSQL), the framework supports writing DataFrame output to files for file-to-file comparison workflows.

### Output Directory Convention

```
Output/
+-- poc4/                                    # Date-partitioned job outputs
    +-- {jobDirName}/
        +-- {outputTableDirName}/
            +-- {etl_effective_date}/
                +-- output.csv               # CSV output
                +-- output/                  # Parquet output dir
                    +-- part-00000.parquet
                    +-- part-00001.parquet
```

All output paths in job configs are resolved via `path_helper.resolve()` against the project root.

### ParquetFileWriter

Writes a DataFrame to one or more Parquet part files in a directory. Uses PyArrow (`pyarrow.parquet`).

| JSON Property | Required | Default | Description |
|---|---|---|---|
| `type` | Yes | -- | `"ParquetFileWriter"` |
| `source` | Yes | -- | Name of the DataFrame in shared state |
| `outputDirectory` | Yes | -- | Base directory path (resolved via `path_helper`) |
| `jobDirName` | Yes | -- | Subdirectory name for the job |
| `outputTableDirName` | Yes | -- | Subdirectory name for the output table (within the job directory) |
| `fileName` | Yes | -- | Name of the parquet output directory within the date partition |
| `numParts` | No | `1` | Number of part files to split output across |
| `writeMode` | Yes | -- | `"Overwrite"` or `"Append"` |

**Example:**
```json
{
  "type": "ParquetFileWriter",
  "source": "output",
  "outputDirectory": "Output/poc4",
  "jobDirName": "account_balance_snapshot",
  "outputTableDirName": "account_balance_snapshot",
  "fileName": "account_balance_snapshot",
  "numParts": 3,
  "writeMode": "Overwrite"
}
```

### CsvFileWriter

Writes a DataFrame to a date-partitioned CSV file. Output path: `{outputDirectory}/{jobDirName}/{outputTableDirName}/{etl_effective_date}/{fileName}`. Injects an `etl_effective_date` column into every row. UTF-8 encoding (no BOM), configurable line endings (LF or CRLF), RFC 4180 quoting rules.

| JSON Property | Required | Default | Description |
|---|---|---|---|
| `type` | Yes | -- | `"CsvFileWriter"` |
| `source` | Yes | -- | Name of the DataFrame in shared state |
| `outputDirectory` | Yes | -- | Base directory (resolved via `path_helper`) |
| `jobDirName` | Yes | -- | Subdirectory name under the base directory |
| `outputTableDirName` | Yes | -- | Subdirectory name for the output table (within the job directory) |
| `fileName` | Yes | -- | Name of the CSV file within the date partition |
| `includeHeader` | No | `true` | Whether to write a header row |
| `trailerFormat` | No | `null` | Trailer line format string (see below) |
| `writeMode` | Yes | -- | `"Overwrite"` or `"Append"` |
| `lineEnding` | No | `"LF"` | Line ending style: `"LF"` or `"CRLF"` |

**Trailer tokens:** `{row_count}` (data rows, excluding header/trailer), `{date}` (effective date from `__etlEffectiveDate` in shared state), `{timestamp}` (UTC now, ISO 8601).

**Append mode trailer stripping:** When `writeMode` is `Append` and `trailerFormat` is set, the writer reads the prior partition's CSV as raw lines, strips the last line (the prior trailer), and parses the remaining lines with `pd.read_csv`. This prevents the prior trailer from being carried forward as a data row. The prior partition's `etl_effective_date` column is dropped before concatenating with the current DataFrame (since the column is re-injected with the current date).

**Vanilla CSV example:**
```json
{
  "type": "CsvFileWriter",
  "source": "output",
  "outputDirectory": "Output/poc4",
  "jobDirName": "customer_contact_info",
  "outputTableDirName": "customer_contact_info",
  "fileName": "customer_contact_info.csv",
  "writeMode": "Overwrite"
}
```

**CSV with trailer example:**
```json
{
  "type": "CsvFileWriter",
  "source": "output",
  "outputDirectory": "Output/poc4",
  "jobDirName": "daily_txn_summary",
  "outputTableDirName": "daily_txn_summary",
  "fileName": "daily_txn_summary.csv",
  "trailerFormat": "TRAILER|{row_count}|{date}",
  "writeMode": "Overwrite"
}
```

---

*This document will be updated as architectural decisions are made.*
