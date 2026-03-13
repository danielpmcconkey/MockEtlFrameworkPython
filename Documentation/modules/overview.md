# Modules

Located in `src/etl/modules/`.

## Module ABC

`src/etl/modules/base.py`

Core contract: `execute(shared_state: dict[str, object]) -> dict[str, object]`. All modules subclass the `Module` abstract base class. Each module receives the current shared state, performs its operation, and returns the updated state.

## module_factory

`src/etl/module_factory.py`

Factory function. Reads the `type` discriminator field from a module config dict and instantiates the appropriate `Module` subclass. Raises `ValueError` on unknown types or if the `type` field is absent.

Also defines the `WriteMode` enum (`Overwrite`, `Append`) used by writer modules.

Supported `type` values: `DataSourcing`, `Transformation`, `DataFrameWriter`, `CsvFileWriter`, `ParquetFileWriter`, `External`.

## Shared State

The shared state dictionary (`dict[str, object]`) is the integration contract between modules. Modules are decoupled from one another -- they communicate only through named entries. Typically these entries are pandas `DataFrame` instances, but the dictionary can hold any object.

The executor injects `__etlEffectiveDate` (a `datetime.date`) into shared state before the pipeline runs. Modules that need the effective date read it from there.

## Module Chain Execution

`job_runner` (`src/etl/job_runner.py`) deserializes a job conf from a JSON file via `JobConf.from_file()`, iterates the module list, creates each module via `module_factory.create()`, and threads shared state through the pipeline. Accepts an optional `initial_state` dictionary pre-populated by the executor. Logs progress via the `logging` module as each module executes. If `ETL_LOG_PATH` is configured, a per-job file handler is attached for the duration of the run.

### JobConf Model

`JobConf` (`src/etl/job_conf.py`) is a dataclass containing:
- `job_name` -- the job's name
- `first_effective_date` (optional) -- **metadata only**, not consumed by the executor. Informational field indicating when the job's data range begins.
- `modules` -- an ordered `list[dict]` of module configurations

### File Writer Rationale

The file writer modules (`CsvFileWriter`, `ParquetFileWriter`) exist to support **file-to-file comparison workflows** -- producing output files that can be compared against production ETL output for equivalence validation.

## Module Reference

| Module | Doc |
|---|---|
| DataSourcing | [data-sourcing.md](data-sourcing.md) |
| Transformation | [transformation.md](transformation.md) |
| DataFrameWriter | [data-frame-writer.md](data-frame-writer.md) |
| CsvFileWriter | [csv-file-writer.md](csv-file-writer.md) |
| ParquetFileWriter | [parquet-file-writer.md](parquet-file-writer.md) |
| External | [external.md](external.md) |

## Output Directory Convention

File writers share a common date-partitioned output structure:

```
Output/
└── poc6/
    └── {jobDirName}/
        └── {outputTableDirName}/
            └── {etl_effective_date}/
                ├── output.csv                  # CsvFileWriter
                └── output/                     # ParquetFileWriter
                    ├── part-00000.parquet
                    └── part-00001.parquet
```

The `Output/` directory is gitignored.

## Sample Job Configuration

A complete pipeline example:

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

Note: no date fields in the DataSourcing modules -- the executor injects `__etlEffectiveDate` at runtime, and both min and max default to that value.
