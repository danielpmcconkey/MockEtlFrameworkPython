# ParquetFileWriter

`src/etl/modules/parquet_file_writer.py`

Writes a named pandas `DataFrame` from shared state to a directory of Parquet files. Uses PyArrow (`pyarrow` / `pyarrow.parquet`) for Parquet serialisation.

## Output Path

```
{outputDirectory}/{jobDirName}/{outputTableDirName}/{etl_effective_date}/{fileName}/part-NNNNN.parquet
```

Paths are resolved via `path_helper.resolve()`.

## Write Modes

| Mode | Behavior |
|---|---|
| `Overwrite` | Deletes existing `.parquet` files in the partition directory, then writes |
| `Append` | Reads prior partition via `date_partition_helper`, unions with current data, writes |

## Part File Splitting

Output is split across `numParts` part files. Rows are distributed as evenly as possible -- when the row count is not evenly divisible, earlier parts receive one extra row each.

Empty DataFrames (zero rows or zero columns) short-circuit: the partition directory is created but no part files are written.

## Config Properties

| JSON Property | Required | Default | Description |
|---|---|---|---|
| `type` | Yes | -- | `"ParquetFileWriter"` |
| `source` | Yes | -- | Name of the DataFrame in shared state |
| `outputDirectory` | Yes | -- | Base directory (resolved via `path_helper`) |
| `jobDirName` | Yes | -- | Subdirectory name for the job |
| `outputTableDirName` | Yes | -- | Subdirectory name for the output table |
| `fileName` | Yes | -- | Name of the Parquet output directory within the date partition |
| `numParts` | No | `1` | Number of part files to split output across |
| `writeMode` | Yes | -- | `"Overwrite"` or `"Append"` |

## Example

```json
{
  "type": "ParquetFileWriter",
  "source": "output",
  "outputDirectory": "Output/poc6",
  "jobDirName": "account_balance_snapshot",
  "outputTableDirName": "account_balance_snapshot",
  "fileName": "account_balance_snapshot",
  "numParts": 3,
  "writeMode": "Overwrite"
}
```
