# Overview

## Purpose

This project is a Python port of the C# MockEtlFramework, which itself mirrors the behavior of a production ETL Framework built on PySpark/Python. The production framework is a core platform component of a large big data system. The goal is to replicate its execution model and module structure, enabling reverse engineering and rewriting of production ETL jobs.

## Production ETL Framework

The production framework reads **job configuration files** (JSON). Each job conf contains a serialized, ordered list of **ETL modules** to execute in series. Modules communicate through a **shared state** -- a dictionary of named DataFrames that each module can read from and write to.

### Production Modules

| Module | Responsibility |
|---|---|
| **Data Sourcing** | Reads from the data lake. Users specify tables, columns, date ranges, and additional filters. Returns data as a PySpark DataFrame stored in shared state. |
| **Transformation** | Runs Spark SQL against DataFrames in shared state, producing a new transformed DataFrame. |
| **DataFrame Writer** | Writes a named DataFrame from shared state to a curation space. |
| **External** | Executes a custom user-supplied Python class for arbitrary logic. |

## Python Equivalents

| Production Concept | Python Equivalent |
|---|---|
| PySpark DataFrame | `pandas.DataFrame` |
| Shared state | `dict[str, object]` passed through module chain |
| ETL module | Classes extending `etl.modules.base.Module` (ABC) |
| Job configuration | JSON file deserialized into `etl.job_conf.JobConf` |
| Spark SQL | In-memory SQLite connection (`sqlite3`) |
| Framework executor | `etl.job_runner.run()` loads the job conf and runs modules in sequence |

## Key Design Principles

- **Module chain execution:** Modules run in the order defined by the job conf. Each module receives the current shared state, performs its operation, and returns the updated state.
- **Shared state as the integration contract:** Modules are decoupled from one another. They communicate only through named entries in the shared state dictionary.
- **JSON-driven configuration:** Job behavior is defined externally in JSON, not in code.
- **pandas DataFrame as the data type:** DataFrames are standard pandas DataFrames -- no custom wrapper. Transformation logic uses the same tabular operations familiar to PySpark users.
- **Full-load temporal data:** The data lake follows a snapshot (full-load) pattern. Each day's load is a complete picture identified by an `ifw_effective_date` column.
- **Explicit effective date management:** The caller MUST supply an effective date for every invocation. No auto-advance or gap-fill logic. For batch processing across date ranges, use the task queue.
- **run_date vs. effective date:** `run_date` in `control.job_runs` is the calendar date the executor actually ran (always today). `min_effective_date` / `max_effective_date` record which data date that run processed. Separate concerns.

## Technology Choices

| Concern | Choice | Rationale |
|---|---|---|
| Language / runtime | Python 3.11+ | Production framework is Python; direct port eliminates impedance mismatch |
| DataFrame library | pandas >= 2.1 | Standard tabular data library; no custom DataFrame needed |
| PostgreSQL client | psycopg >= 3.1 | Modern Python Postgres driver with native async support |
| In-process SQL engine | sqlite3 (stdlib) | Enables free-form SQL in Transformation without a running server |
| Test framework | pytest >= 7.4 | Industry standard for Python |
| Parquet file output | PyArrow >= 14.0 | Apache Arrow's Python bindings; native Parquet read/write |
| Configuration | dataclasses (frozen) | Immutable config objects; no third-party dependency needed |
| Packaging | setuptools / pyproject.toml | Standard Python packaging |

## Project Structure

```
MockEtlFrameworkPython/
+-- src/etl/                       # Framework package
|   +-- modules/                   # Module implementations
|   |   +-- base.py                #   Module ABC
|   |   +-- data_sourcing.py       #   DataSourcing
|   |   +-- transformation.py      #   Transformation
|   |   +-- dataframe_writer.py    #   DataFrameWriter
|   |   +-- csv_file_writer.py     #   CsvFileWriter
|   |   +-- parquet_file_writer.py #   ParquetFileWriter
|   |   +-- external.py            #   External (stub)
|   +-- control/                   # Orchestration layer
|   |   +-- control_db.py          #   ControlDb data access
|   |   +-- execution_plan.py      #   Topological sort (Kahn's)
|   |   +-- job_executor_service.py #  Single-date orchestrator
|   |   +-- task_queue_service.py  #   Long-running queue executor
|   +-- app_config.py              # Configuration model (frozen dataclasses)
|   +-- connection_helper.py       # DB connection string builder
|   +-- date_partition_helper.py   # Date-partitioned directory scanning
|   +-- module_factory.py          # Module instantiation from JSON config
|   +-- path_helper.py             # Path resolution and {TOKEN} expansion
|   +-- job_conf.py                # JSON config model
|   +-- job_runner.py              # Runs module chain
+-- cli.py                         # CLI entry point
+-- appsettings.json               # Runtime config overrides
+-- tests/                         # pytest tests
+-- Documentation/                 # Codebase docs
+-- pyproject.toml                 # Package definition and dependencies
```
