# MockEtlFrameworkPython Documentation

Quick reference for navigating the codebase. Each doc is self-contained.

| Looking for... | Go to |
|---|---|
| What this project is and why it exists | [overview.md](overview.md) |
| CLI usage (`cli.py` arguments) | [cli.md](cli.md) |
| Configuration (AppConfig, env vars, appsettings.json) | [configuration.md](configuration.md) |
| How modules work (Module ABC, shared state, module_factory) | [modules/overview.md](modules/overview.md) |
| DataSourcing (date modes, datalake queries) | [modules/data-sourcing.md](modules/data-sourcing.md) |
| Transformation (SQLite, free-form SQL) | [modules/transformation.md](modules/transformation.md) |
| DataFrameWriter (PostgreSQL output) | [modules/data-frame-writer.md](modules/data-frame-writer.md) |
| CsvFileWriter (CSV output, trailers, append mode) | [modules/csv-file-writer.md](modules/csv-file-writer.md) |
| ParquetFileWriter (Parquet output) | [modules/parquet-file-writer.md](modules/parquet-file-writer.md) |
| External module (custom Python modules) | [modules/external.md](modules/external.md) |
| Control layer (ControlDb, dependencies, execution plan) | [control/overview.md](control/overview.md) |
| JobExecutorService (single-date orchestration) | [control/job-executor-service.md](control/job-executor-service.md) |
| TaskQueueService (long-running queue executor) | [control/task-queue-service.md](control/task-queue-service.md) |
| Test coverage | [testing.md](testing.md) |
| Full monolithic architecture doc | [Architecture.md](Architecture.md) |
