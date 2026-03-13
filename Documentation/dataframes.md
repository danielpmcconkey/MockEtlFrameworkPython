# DataFrames

The Python implementation uses **pandas DataFrames** (`pd.DataFrame`) as its in-memory tabular data structure. There is no custom `DataFrame` class — the C# project's bespoke DataFrame with a PySpark-inspired API has been replaced by pandas itself, which provides all the same operations natively.

DataFrames flow through the ETL pipeline via a `shared_state` dictionary (`dict[str, object]`). Modules read their inputs from and write their outputs to this dict by name.

## Construction (data ingress)

### DataSourcing (`src/etl/modules/data_sourcing.py`)

Reads from a PostgreSQL table via `pd.read_sql()`. Supports several date-resolution modes (mutually exclusive):

| Mode | Config Fields | Behaviour |
|---|---|---|
| Static range | `minEffectiveDate`, `maxEffectiveDate` | Query between fixed dates. Falls back to `__etlEffectiveDate` from shared state if either is omitted. |
| Lookback | `lookbackDays` | Query from `effective_date - N` to `effective_date`. |
| Most recent prior | `mostRecentPrior` | Single-date query for the latest partition **strictly before** the effective date. |
| Most recent | `mostRecent` | Single-date query for the latest partition **on or before** the effective date. |

Always appends `ifw_effective_date` to the result if not already in the column list.

### CSV / Parquet reading (append mode)

Both `CsvFileWriter` and `ParquetFileWriter` read prior partitions when `writeMode` is `Append`. Prior CSV data is read via `pd.read_csv()`; prior Parquet data is read via `pyarrow.parquet.read_table().to_pandas()`.

## Transformation (`src/etl/modules/transformation.py`)

Executes SQL against an in-memory SQLite database. Every `pd.DataFrame` in `shared_state` is registered as a SQLite table via `df.to_sql()`. The SQL query runs via `pd.read_sql_query()` and the result is stored back into `shared_state`.

Date and datetime values are converted to ISO strings before registration (SQLite has no native date type).

## Output (data egress)

### DataFrameWriter (`src/etl/modules/dataframe_writer.py`)

Writes a DataFrame to a PostgreSQL table. Supports `Overwrite` (truncate + insert) and `Append` (insert only) modes. Auto-creates the target table if it doesn't exist, inferring PostgreSQL column types from sample values:

| Python Type | PostgreSQL Type |
|---|---|
| `int` | `INTEGER` |
| `float` | `DOUBLE PRECISION` |
| `bool` | `BOOLEAN` |
| `date` | `DATE` |
| `datetime` | `TIMESTAMP` |
| everything else | `TEXT` |

### CsvFileWriter (`src/etl/modules/csv_file_writer.py`)

Writes date-partitioned CSV files. Output path: `{outputDirectory}/{jobDirName}/{outputTableDirName}/{YYYY-MM-DD}/{fileName}`. Injects an `etl_effective_date` column. Supports configurable line endings (`LF` or `CRLF`), optional headers, and trailer lines with `{row_count}`, `{date}`, and `{timestamp}` token expansion.

### ParquetFileWriter (`src/etl/modules/parquet_file_writer.py`)

Writes date-partitioned Parquet files via PyArrow. Output path: `{outputDirectory}/{jobDirName}/{outputTableDirName}/{YYYY-MM-DD}/{fileName}/part-NNNNN.parquet`. Supports multi-part output (`numParts`). Injects an `etl_effective_date` column.

## Operations Reference

Since this project uses pandas directly, the standard pandas API applies. The operations most commonly used in this codebase (via `Transformation` SQL or direct manipulation in modules):

| pandas | C# DataFrame equivalent | Notes |
|---|---|---|
| `df[columns]` | `Select(columns)` | Column projection |
| `df[df[col] == val]` | `Filter(predicate)` | Row filtering |
| `df[col] = expr` | `WithColumn(name, func)` | Add/replace column |
| `df.drop(columns=[col])` | `Drop(column)` | Remove column |
| `df.sort_values(col)` | `OrderBy(column)` | Sort rows |
| `df.head(n)` | `Limit(n)` | First N rows |
| `pd.concat([a, b])` | `Union(other)` | Combine DataFrames |
| `df.drop_duplicates()` | `Distinct()` | Remove duplicates |
| `df.merge(other, ...)` | `Join(other, ..., type)` | Inner/left join |
| `df.groupby(columns)` | `GroupBy(columns)` | Grouped aggregation |
| `len(df)` | `Count` | Row count |
| `df.columns` | `Columns` | Column names |

In practice, most row-level transformations happen inside `Transformation` SQL rather than via direct pandas calls.
