# Tests

`tests/` -- pytest test suite. Tests do not require a live database. All DataFrame and Transformation tests operate entirely in memory. File writer tests use temporary directories.

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

## Running Tests

```bash
pytest
```

Or from the project root with verbose output:

```bash
pytest -v
```
