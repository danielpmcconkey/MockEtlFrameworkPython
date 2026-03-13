# DataFrameWriter

`src/etl/modules/dataframe_writer.py`

Writes a named pandas `DataFrame` from shared state to a PostgreSQL curation schema. Auto-creates the target table if it does not exist (type inference from sample values). All writes are transaction-wrapped via `psycopg`.

## Write Modes

| Mode | Behavior |
|---|---|
| `Overwrite` | Truncate then insert |
| `Append` | Insert only |

## Table Auto-Creation

If the target table does not exist, `_ensure_table_exists` builds a `CREATE TABLE IF NOT EXISTS` DDL by inspecting the first non-null value in each column to infer the PostgreSQL type:

| Python Type | PostgreSQL Type |
|---|---|
| `int` | `INTEGER` |
| `float` | `DOUBLE PRECISION` |
| `bool` | `BOOLEAN` |
| `datetime.date` | `DATE` |
| `datetime.datetime` | `TIMESTAMP` |
| Everything else | `TEXT` |

## Value Coercion

The `_coerce_value` helper handles values that lost their type during SQLite round-tripping in the Transformation module. String values are tested for date and datetime parsing (via `fromisoformat`) before being passed to PostgreSQL. `NaN` and `None` are normalised to `None`.

## Config Properties

| JSON Property | Required | Default | Description |
|---|---|---|---|
| `type` | Yes | -- | `"DataFrameWriter"` |
| `source` | Yes | -- | Name of the DataFrame in shared state |
| `targetTable` | Yes | -- | PostgreSQL table name |
| `targetSchema` | No | `"curated"` | PostgreSQL schema name |
| `writeMode` | Yes | -- | `"Overwrite"` or `"Append"` |

## Example

```json
{
  "type": "DataFrameWriter",
  "source": "customer_account_summary",
  "targetTable": "customer_account_summary",
  "writeMode": "Overwrite"
}
```
