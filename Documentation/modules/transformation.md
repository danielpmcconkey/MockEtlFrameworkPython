# Transformation

`src/etl/modules/transformation.py`

Opens an in-memory SQLite connection, registers every pandas `DataFrame` in the current shared state as a SQLite table, executes user-supplied free-form SQL, and stores the result `DataFrame` back into shared state under a caller-specified result name.

Uses `pandas.read_sql_query()` to execute the SQL and materialise results.

## SQLite Table Registration

All DataFrames in shared state are registered as SQLite tables at execution time via `DataFrame.to_sql()`. Non-DataFrame entries in shared state are silently ignored. This means any DataFrame produced by an earlier module in the chain is available as a SQL table name.

## Type Coercion

Date and datetime values in object-typed columns are converted to ISO 8601 strings before registration, since SQLite has no native date type. The helper `_to_sqlite_value` handles this:
- `datetime.date` becomes `YYYY-MM-DD`
- `datetime.datetime` becomes `YYYY-MM-DDTHH:MM:SS`
- `None` passes through as `None`

## Empty DataFrame Handling

Empty DataFrames (those with columns but zero rows) are registered as schema-only SQLite tables -- the `to_sql()` call creates the table structure but inserts no rows. DataFrames with no columns at all are skipped entirely. Downstream SQL can still reference empty tables in joins/subqueries without error.

## Config Properties

| JSON Property | Required | Description |
|---|---|---|
| `type` | Yes | `"Transformation"` |
| `resultName` | Yes | Name to store the result DataFrame under in shared state |
| `sql` | Yes | Free-form SQL to execute against registered DataFrames |

## Example

```json
{
  "type": "Transformation",
  "resultName": "customer_account_summary",
  "sql": "SELECT c.id AS customer_id, c.first_name, c.last_name, COUNT(a.account_id) AS account_count FROM customers c LEFT JOIN accounts a ON c.id = a.customer_id GROUP BY c.id, c.first_name, c.last_name ORDER BY c.id"
}
```
