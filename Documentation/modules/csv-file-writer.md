# CsvFileWriter

`src/etl/modules/csv_file_writer.py`

Writes a named pandas `DataFrame` from shared state to a date-partitioned CSV file. Injects an `etl_effective_date` column into every row before writing.

## Output Path

```
{outputDirectory}/{jobDirName}/{outputTableDirName}/{etl_effective_date}/{fileName}
```

Paths are resolved via `path_helper.resolve()`.

## Encoding & Format

- UTF-8 (no BOM)
- Configurable line endings: `LF` (default) or `CRLF`
- RFC 4180 quoting rules (fields containing commas, double quotes, or newlines are quoted; embedded quotes are doubled)

## Trailer Support

Optional trailer line appended after data rows. Format string with token substitution:

| Token | Value |
|---|---|
| `{row_count}` | Data rows (excluding header and trailer) |
| `{date}` | Effective date from `__etlEffectiveDate` in shared state |
| `{timestamp}` | UTC now, ISO 8601 |

## Append Mode

When `writeMode` is `Append`:

1. Finds the prior partition's CSV via `date_partition_helper.find_latest_partition()`
2. If `trailerFormat` is set, strips the last line (the prior trailer) before parsing
3. Drops the prior `etl_effective_date` column (it gets re-injected with the current date)
4. Unions the prior data with the current DataFrame via `pd.concat()`
5. Writes the combined result

## Config Properties

| JSON Property | Required | Default | Description |
|---|---|---|---|
| `type` | Yes | -- | `"CsvFileWriter"` |
| `source` | Yes | -- | Name of the DataFrame in shared state |
| `outputDirectory` | Yes | -- | Base directory (resolved via `path_helper`) |
| `jobDirName` | Yes | -- | Subdirectory name under the base directory |
| `outputTableDirName` | Yes | -- | Subdirectory for the output table (within job dir) |
| `fileName` | Yes | -- | CSV file name within the date partition |
| `includeHeader` | No | `true` | Whether to write a header row |
| `trailerFormat` | No | `null` | Trailer line format string |
| `writeMode` | Yes | -- | `"Overwrite"` or `"Append"` |
| `lineEnding` | No | `"LF"` | `"LF"` or `"CRLF"` |

## Examples

```json
// Vanilla CSV
{
  "type": "CsvFileWriter",
  "source": "output",
  "outputDirectory": "Output/poc6",
  "jobDirName": "customer_contact_info",
  "outputTableDirName": "customer_contact_info",
  "fileName": "customer_contact_info.csv",
  "writeMode": "Overwrite"
}

// CSV with trailer
{
  "type": "CsvFileWriter",
  "source": "output",
  "outputDirectory": "Output/poc6",
  "jobDirName": "daily_txn_summary",
  "outputTableDirName": "daily_txn_summary",
  "fileName": "daily_txn_summary.csv",
  "trailerFormat": "TRAILER|{row_count}|{date}",
  "writeMode": "Overwrite"
}
```
